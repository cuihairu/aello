#include "reliable_pipe.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>

#include "duct/protocol.h"
#include "duct/wire.h"

namespace duct {
namespace {

// The cumulative ACK payload is {session_id u64 BE, cumulative_ack u64 BE},
// exactly 16 bytes.
constexpr std::size_t kAckPayloadLen = 16;
// Holdback: how many out-of-order frames the receiver buffers before dropping
// the newcomer and letting the sender's replay win (decision point D3). On
// tcp/uds the stream is in-order so this stays empty; it guards a future
// datagram transport and a misbehaving peer.
constexpr std::size_t kMaxHoldback = 64;
// Replay and ACK sends are bounded so a slow peer cannot pin the reconnect
// worker or the recv path; a dropped ACK is safe because cumulative ACKs are
// idempotent.
constexpr auto kReplayTimeout = std::chrono::milliseconds(1000);
constexpr auto kAckTimeout = std::chrono::milliseconds(100);

std::uint64_t load_be64(const std::uint8_t* p) {
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<std::uint64_t>(p[i]);
  return v;
}

void store_be64(std::uint8_t* p, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) p[i] = static_cast<std::uint8_t>(v >> (56 - 8 * i));
}

}  // namespace

ReliabilitySessionTable::ReliabilitySessionTable(std::size_t max_sessions)
    : max_sessions_(max_sessions) {}

Result<std::shared_ptr<ReliabilitySessionState>> ReliabilitySessionTable::resume_or_start(
    std::uint64_t session_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    return it->second;
  }
  if (max_sessions_ != 0 && sessions_.size() >= max_sessions_) {
    return Status::queue_full("reliability session table full");
  }
  auto state = std::make_shared<ReliabilitySessionState>();
  sessions_.emplace(session_id, state);
  return state;
}

void ReliabilityReconnect::on_state(ConnectionState st) {
  std::function<void()> hook;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (st == ConnectionState::kDisconnected || st == ConnectionState::kReconnecting) {
      was_disconnected_ = true;
    } else if (st == ConnectionState::kConnected && was_disconnected_) {
      was_disconnected_ = false;
      hook = on_reconnect;
    }
  }
  // Fire outside the lock: the hook sends on the (now connected) inner pipe,
  // which may block on the socket.
  if (hook) hook();
}

namespace {

class ReliablePipe final : public Pipe {
 public:
  ReliablePipe(std::unique_ptr<Pipe> inner, const ReliablePipeConfig& cfg)
      : inner_(std::move(inner)),
        cfg_(cfg),
        session_id_(cfg.session_id),
        state_(cfg.state) {
    reliable_.store((inner_->negotiated_capabilities() & kCapabilityReliable) != 0);
    if (cfg.reconnect) {
      cfg.reconnect->on_reconnect = [this]() { on_reconnect(); };
    }
    // Listener-side resume: the redial landed, so push the window this session
    // still had unacknowledged before the connection died. The client dedups
    // anything it already delivered.
    if (cfg.replay_pending_window_on_bind) {
      std::lock_guard<std::mutex> lk(io_mu_);
      replay_window();
    }
  }

  ~ReliablePipe() override { close(); }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    if (!reliable_.load(std::memory_order_acquire)) {
      return inner_->send(msg, opt);  // v1 peer: byte-for-byte passthrough
    }

    std::uint64_t seq;
    {
      std::unique_lock<std::mutex> lk(state_->mu);
      // A message larger than the window always fits: the budget is a soft
      // in-flight watermark, not a per-message cap.
      const auto fits = [&] {
        return state_->unacked_bytes + msg.size() <= cfg_.window_bytes ||
               msg.size() > cfg_.window_bytes;
      };
      if ((cfg_.backpressure == BackpressurePolicy::kFailFast ||
           cfg_.backpressure == BackpressurePolicy::kDropNew) &&
          !fits()) {
        // kDropNew drops this message rather than blocking: report it the same
        // way the QoS queue reports a drop, so the caller can retry.
        return Status::queue_full("reliable window full");
      }
      if (cfg_.backpressure == BackpressurePolicy::kDropOld) {
        // Explicit policy: evict the oldest unacked messages until there is
        // room. Those messages degrade to at-most-once (never replayed).
        while (!state_->window.empty() && !fits()) {
          state_->unacked_bytes -= state_->window.front().second.size();
          state_->window.pop_front();
        }
      }
      if (!fits()) {
        // kBlock: wait for the window to clear. Only the recv path (processing
        // cumulative ACKs) opens it, so a send-only caller blocks — that is the
        // documented backpressure contract, matching TCP.
        if (opt.timeout.count() > 0) {
          const auto deadline = std::chrono::steady_clock::now() + opt.timeout;
          if (!state_->cv.wait_until(lk, deadline, fits)) {
            return Status::timeout("reliable window did not clear");
          }
        } else {
          state_->cv.wait(lk, fits);
        }
      }
      seq = state_->next_seq_out++;
      state_->window.emplace_back(seq, msg);
      state_->unacked_bytes += msg.size();
    }

    SendOptions sopt = opt;
    sopt.session_id = session_id_;
    sopt.seq = seq;
    sopt.wire_flags = 0;
    // Serialize every write (user send, ACK, replay): a concurrent send could
    // otherwise interleave mid-frame on the wire.
    std::lock_guard<std::mutex> lk(io_mu_);
    return inner_->send(msg, sopt);
  }

  Result<wire::FrameMessage> recv_frame(const RecvOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    if (!reliable_.load(std::memory_order_acquire)) {
      return inner_->recv_frame(opt);  // v1 peer: passthrough
    }
    const bool has_deadline = opt.timeout.count() > 0;
    const auto deadline = std::chrono::steady_clock::now() + opt.timeout;
    for (;;) {
      // Serve an in-order frame whose predecessors all arrived (a gap closed)
      // before blocking on the socket again.
      if (auto r = try_deliver_next()) return std::move(*r);

      RecvOptions frame_opt = opt;
      if (has_deadline) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) return Status::timeout("recv timed out");
        frame_opt.timeout =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        // Sub-millisecond truncation would clear the socket timeout and turn the
        // read into an unbounded block; clamp to 1ms so the deadline still holds.
        if (frame_opt.timeout <= std::chrono::milliseconds::zero()) {
          frame_opt.timeout = std::chrono::milliseconds(1);
        }
      }

      wire::FrameMessage fm;
      {
        // recv reads do not touch io_mu_ (TCP is full-duplex); only writes
        // serialize, so a blocked recv never stalls a send or an ACK.
        auto r = inner_->recv_frame(frame_opt);
        if (!r.ok()) return r.status();
        fm = std::move(r.value());
      }

      if ((fm.flags & to_u32(FrameFlags::kAck)) != 0) {
        on_ack(fm);
        continue;  // control frame: consume, keep reading within the deadline
      }
      if (fm.session_id == 0) {
        // A v1 frame on a reliable connection: not a sequenced data frame.
        // Pass it through (defensive; the reliability layer never emits one).
        return fm;
      }
      if (fm.session_id != session_id_) {
        continue;  // foreign session: cannot happen on this connection
      }

      std::unique_lock<std::mutex> lk(state_->mu);
      if (fm.seq == state_->next_expected_in) {
        ++state_->next_expected_in;
        const std::uint64_t ack = state_->next_expected_in;
        lk.unlock();
        emit_ack(ack);
        return fm;
      }
      if (fm.seq < state_->next_expected_in) {
        // Duplicate (a replay of something already delivered): re-ACK and drop.
        const std::uint64_t ack = state_->next_expected_in;
        lk.unlock();
        emit_ack(ack);
        continue;
      }
      // Gap: buffer out of order; overflow drops the newcomer and the sender's
      // replay (or a later in-order retransmit) wins.
      if (holdback_.size() < kMaxHoldback) {
        holdback_[fm.seq] = std::move(fm);
      }
      // fall through: keep reading within the deadline
    }
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    const bool has_deadline = opt.timeout.count() > 0;
    const auto deadline = std::chrono::steady_clock::now() + opt.timeout;
    for (;;) {
      {
        std::lock_guard<std::mutex> lk(ch_mu_);
        auto it = pending_.find(opt.channel_id);
        if (it != pending_.end() && !it->second.empty()) {
          auto msg = std::move(it->second.front().message);
          it->second.erase(it->second.begin());
          if (it->second.empty()) pending_.erase(it);
          pending_bytes_ -= msg.size();
          return msg;
        }
      }
      RecvOptions frame_opt = opt;
      if (has_deadline) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) return Status::timeout("recv timed out");
        frame_opt.timeout =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        if (frame_opt.timeout <= std::chrono::milliseconds::zero()) {
          frame_opt.timeout = std::chrono::milliseconds(1);
        }
      }
      auto r = recv_frame(frame_opt);
      if (!r.ok()) {
        // A frame for this channel may have been buffered while we were
        // blocked; serve it before surfacing the timeout.
        std::lock_guard<std::mutex> lk(ch_mu_);
        auto it = pending_.find(opt.channel_id);
        if (it != pending_.end() && !it->second.empty()) {
          auto msg = std::move(it->second.front().message);
          it->second.erase(it->second.begin());
          if (it->second.empty()) pending_.erase(it);
          pending_bytes_ -= msg.size();
          return msg;
        }
        return r.status();
      }
      if (opt.channel_id == 0 ||
          flags_to_channel_id(r.value().flags) == opt.channel_id) {
        return std::move(r.value().message);
      }
      {
        std::lock_guard<std::mutex> lk(ch_mu_);
        if (pending_bytes_ + r.value().message.size() > kMaxPendingBytes) {
          return Status::queue_full("channel recv buffer full");
        }
        pending_[flags_to_channel_id(r.value().flags)].push_back(std::move(r.value()));
        pending_bytes_ += r.value().message.size();
      }
    }
  }

  std::size_t max_message_bytes() const override { return inner_->max_message_bytes(); }

  std::uint32_t negotiated_capabilities() const override {
    return inner_->negotiated_capabilities();
  }

  void close() override {
    if (closed_.exchange(true)) return;
    // Clear the replay hook before closing inner_: ReconnectPipe::close() joins
    // its worker, so no hook can fire after this returns (and the object is
    // alive throughout close()).
    if (cfg_.reconnect) cfg_.reconnect->on_reconnect = nullptr;
    inner_->close();
  }

 private:
  // Called on the reconnect worker for every kConnected-after-disconnect.
  void on_reconnect() {
    reliable_.store((inner_->negotiated_capabilities() & kCapabilityReliable) != 0,
                    std::memory_order_release);
    if (!reliable_.load(std::memory_order_acquire)) {
      // The peer stopped negotiating reliability: no dedup on its side, so a
      // replay could duplicate. Drop the window and pass through from now on.
      std::lock_guard<std::mutex> lk(state_->mu);
      state_->window.clear();
      state_->unacked_bytes = 0;
      return;
    }
    std::lock_guard<std::mutex> lk(io_mu_);
    replay_window();
  }

  // Replays the unacked window in seq order. io_mu_ must be held.
  void replay_window() {
    std::vector<std::pair<std::uint64_t, Message>> snapshot;
    {
      std::lock_guard<std::mutex> lk(state_->mu);
      snapshot.assign(state_->window.begin(), state_->window.end());
    }
    for (const auto& entry : snapshot) {
      duct::SendOptions sopt;
      sopt.session_id = session_id_;
      sopt.seq = entry.first;
      sopt.timeout = kReplayTimeout;
      auto st = inner_->send(entry.second, sopt);
      if (!st.ok()) {
        // Stop: the next reconnect (or ACK) retries; the receiver dedups the
        // part already delivered.
        break;
      }
    }
  }

  // Emits a cumulative ACK for the data stream. Best-effort and bounded: a
  // dropped ACK is safe because cumulative ACKs are idempotent.
  void emit_ack(std::uint64_t cumulative) {
    std::uint8_t buf[kAckPayloadLen];
    store_be64(buf, session_id_);
    store_be64(buf + 8, cumulative);
    duct::SendOptions aopt;
    aopt.wire_flags = to_u32(FrameFlags::kAck);
    aopt.timeout = kAckTimeout;
    std::lock_guard<std::mutex> lk(io_mu_);
    (void)inner_->send(Message::from_bytes(buf, sizeof(buf)), aopt);
  }

  void on_ack(const wire::FrameMessage& fm) {
    if (fm.message.size() != kAckPayloadLen) return;
    const std::uint8_t* d = fm.message.data();
    const std::uint64_t ack_session = load_be64(d);
    const std::uint64_t cumulative = load_be64(d + 8);
    if (ack_session != session_id_) return;
    std::lock_guard<std::mutex> lk(state_->mu);
    while (!state_->window.empty() && state_->window.front().first < cumulative) {
      state_->unacked_bytes -= state_->window.front().second.size();
      state_->window.pop_front();
    }
    state_->cv.notify_all();
  }

  // Delivers the next in-order frame from the holdback once its predecessors
  // arrived; nullopt when the holdback does not contain the expected seq.
  std::optional<wire::FrameMessage> try_deliver_next() {
    std::unique_lock<std::mutex> lk(state_->mu);
    auto it = holdback_.find(state_->next_expected_in);
    if (it == holdback_.end()) return std::nullopt;
    wire::FrameMessage out = std::move(it->second);
    holdback_.erase(it);
    ++state_->next_expected_in;
    const std::uint64_t ack = state_->next_expected_in;
    lk.unlock();
    emit_ack(ack);
    return out;
  }

  std::unique_ptr<Pipe> inner_;
  ReliablePipeConfig cfg_;
  std::uint64_t session_id_ = 0;
  std::shared_ptr<ReliabilitySessionState> state_;
  std::atomic<bool> reliable_{false};
  std::atomic<bool> closed_{false};
  // Serializes every write on inner_ (data send, ACK, replay); a blocked read
  // on inner_ never holds it, so a send cannot deadlock on a recv.
  std::mutex io_mu_;
  std::map<std::uint64_t, wire::FrameMessage> holdback_;
  // Channel demux (mirrors the transports): frames read for other channels are
  // stashed here for their matching recv().
  std::mutex ch_mu_;
  std::map<std::uint32_t, std::vector<wire::FrameMessage>> pending_;
  std::size_t pending_bytes_ = 0;
  static constexpr std::size_t kMaxPendingBytes = 64 * 1024 * 1024;
};

}  // namespace

std::unique_ptr<Pipe> make_reliable_pipe(std::unique_ptr<Pipe> inner,
                                         const ReliablePipeConfig& cfg) {
  return std::make_unique<ReliablePipe>(std::move(inner), cfg);
}

ConnectionCallback chain_reliability_callback(std::shared_ptr<ReliabilityReconnect> rc,
                                              ConnectionCallback user_cb) {
  return [rc, user_cb = std::move(user_cb)](ConnectionState st, const std::string& reason) {
    if (rc) rc->on_state(st);
    if (user_cb) user_cb(st, reason);
  };
}

}  // namespace duct

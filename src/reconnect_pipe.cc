#include "reconnect_pipe.h"

#include "duct/wire.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <utility>

namespace duct {
namespace {

// Entry-point validation: a misconfigured policy would otherwise produce
// undefined backoff arithmetic (NaN/Inf multiplier, negative delays spin with
// zero waits). dial() reports kInvalidArgument; make_reconnect_pipe (which has
// no error channel) sanitizes instead.
Result<void> validate_reconnect_policy(const ReconnectPolicy& policy) {
  if (policy.initial_delay.count() < 0 || policy.max_delay.count() < 0) {
    return Status::invalid_argument("reconnect delays must be non-negative");
  }
  // >= 1.0 comparison also rejects NaN.
  if (!(policy.backoff_multiplier >= 1.0)) {
    return Status::invalid_argument("reconnect backoff multiplier must be >= 1.0");
  }
  if (policy.max_attempts < 0) {
    return Status::invalid_argument("reconnect max_attempts must be >= 0");
  }
  return {};
}

void sanitize_reconnect_policy(ReconnectPolicy& policy) {
  if (policy.initial_delay.count() < 0) policy.initial_delay = std::chrono::milliseconds(0);
  if (policy.max_delay.count() < 0) policy.max_delay = std::chrono::milliseconds(0);
  if (!(policy.backoff_multiplier >= 1.0)) policy.backoff_multiplier = 1.0;
  if (policy.max_attempts < 0) policy.max_attempts = 0;
}


class ReconnectPipe final : public Pipe {
 public:
  ReconnectPipe(DialOnceFn dial_once, ReconnectPolicy policy, ConnectionCallback on_state_change)
      : dial_once_(std::move(dial_once)),
        policy_(policy),
        on_state_change_(std::move(on_state_change)) {
    set_state(ConnectionState::kConnecting, "initial connect");
    worker_ = std::thread([this]() { worker_loop(); });
  }

  ~ReconnectPipe() override { close(); }

  // Public so the dial combinator can require the first connect to complete
  // before dial() returns (see make_connected_reconnect_pipe).
  Result<void> wait_connected(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(mu_);
    auto pred = [&] { return closed_ || inner_ != nullptr || permanently_failed_; };
    if (timeout.count() > 0) {
      if (!cv_.wait_for(lk, timeout, pred)) {
        return Status::timeout("connect timed out");
      }
    } else {
      cv_.wait(lk, pred);
    }
    if (closed_) return Status::closed("pipe closed");
    if (permanently_failed_) return Status::io_error("reconnect attempts exhausted: " + last_error_);
    return {};
  }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    for (;;) {
      auto st = wait_connected(opt.timeout);
      if (!st.ok()) return st;

      std::shared_ptr<Pipe> inner_snapshot = snapshot_inner();
      if (!inner_snapshot) continue;

      st = inner_snapshot->send(msg, opt);
      if (st.ok()) return st;
      if (st.status().code() == StatusCode::kTimeout) return st;
      if (st.status().code() == StatusCode::kClosed) {
        // kClosed means the transport wrote none of this frame (wire reports
        // it only when the peer closed before any byte went out), so the
        // message is safe to resend on the next connection.
        mark_disconnected(inner_snapshot, "send: " + st.status().message());
        continue;
      }
      if (st.status().code() == StatusCode::kIoError) {
        // kIoError includes mid-frame timeouts: the frame may have been
        // partially or fully written and delivered, so resending could
        // duplicate it. Tear the connection down (the worker redials) but
        // surface the error to the caller, who decides whether to retry.
        mark_disconnected(inner_snapshot, "send: " + st.status().message());
        return st;
      }
      return st;
    }
  }

  Result<Message> recv(const RecvOptions& opt) override {
    for (;;) {
      auto st = wait_connected(opt.timeout);
      if (!st.ok()) return st.status();

      std::shared_ptr<Pipe> inner_snapshot = snapshot_inner();
      if (!inner_snapshot) continue;

      auto r = inner_snapshot->recv(opt);
      if (r.ok()) return r;
      if (r.status().code() == StatusCode::kTimeout) return r;
      if (is_disconnect_error(r.status())) {
        mark_disconnected(inner_snapshot, "recv: " + r.status().message());
        continue;
      }
      return r;
    }
  }

  // Forward frame reads to the inner transport so channel_id multiplexing
  // (tcp/uds wire flags) survives the reconnect wrapper. Without this, the base
  // Pipe::recv_frame default delegates to recv() and returns flags==0, so a
  // QosPipe-over-ReconnectPipe-over-tcp recv_worker always sees channel 0.
  Result<wire::FrameMessage> recv_frame(const RecvOptions& opt) override {
    for (;;) {
      auto st = wait_connected(opt.timeout);
      if (!st.ok()) return st.status();

      std::shared_ptr<Pipe> inner_snapshot = snapshot_inner();
      if (!inner_snapshot) continue;

      auto r = inner_snapshot->recv_frame(opt);
      if (r.ok()) return r;
      if (r.status().code() == StatusCode::kTimeout) return r;
      if (is_disconnect_error(r.status())) {
        mark_disconnected(inner_snapshot, "recv_frame: " + r.status().message());
        continue;
      }
      return r;
    }
  }

  // Forward the transport's non-fragmenting send bound so a QosPipe wrapping a
  // reconnect pipe still rejects oversize messages synchronously (otherwise the
  // default unlimited cap lets the drain worker enqueue a message the inner
  // transport will permanently reject, silently dropping it).
  //
  // The bound is memoized: during a disconnect window inner_ is empty and
  // callers sizing messages must not see the unbounded base default again, or
  // the next transport would reject what the drain worker already queued.
  std::size_t max_message_bytes() const override {
    std::shared_ptr<Pipe> inner_snapshot = snapshot_inner();
    if (inner_snapshot) {
      last_bound_.store(inner_snapshot->max_message_bytes(),
                        std::memory_order_release);
    }
    return last_bound_.load(std::memory_order_acquire);
  }

  void close() override {
    std::shared_ptr<Pipe> inner_to_close;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (closed_) return;
      closed_ = true;
      inner_to_close = std::move(inner_);
      cv_.notify_all();
    }

    set_state(ConnectionState::kClosed, "closed");
    if (inner_to_close) inner_to_close->close();
    if (worker_.joinable()) worker_.join();
  }

 private:
  bool is_disconnect_error(const Status& st) const {
    return st.code() == StatusCode::kClosed || st.code() == StatusCode::kIoError;
  }

  void set_state(ConnectionState next, const std::string& reason) {
    ConnectionCallback cb;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (state_ == next) return;
      state_ = next;
      cb = on_state_change_;
    }
    if (cb) cb(next, reason);
  }

  std::shared_ptr<Pipe> snapshot_inner() const {
    std::lock_guard<std::mutex> lk(mu_);
    return inner_;
  }

  void mark_disconnected(const std::shared_ptr<Pipe>& which, const std::string& reason) {
    std::shared_ptr<Pipe> inner_to_close;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (closed_) return;
      if (!inner_) return;
      if (inner_ != which) return;  // Stale error from a prior connection.
      inner_to_close = std::move(inner_);
      last_error_ = reason;
      cv_.notify_all();
    }
    set_state(ConnectionState::kDisconnected, reason);
    if (inner_to_close) inner_to_close->close();
  }

  void worker_loop() {
    std::minstd_rand rng{std::random_device{}()};
    for (;;) {
      if (is_closed()) return;

      // If already connected, wait until disconnected or closed.
      {
        std::unique_lock<std::mutex> lk(mu_);
        if (inner_) {
          cv_.wait(lk, [&] { return closed_ || !inner_; });
          continue;
        }
        if (permanently_failed_) return;
      }

      set_state(ever_connected() ? ConnectionState::kReconnecting : ConnectionState::kConnecting,
                last_error_.empty() ? "connecting" : last_error_);

      int attempts = 0;
      auto delay = policy_.initial_delay;
      for (;;) {
        if (is_closed()) return;
        if (policy_.max_attempts != 0 && attempts >= policy_.max_attempts) {
          {
            std::lock_guard<std::mutex> lk(mu_);
            permanently_failed_ = true;
            cv_.notify_all();
          }
          set_state(ConnectionState::kDisconnected, last_error_.empty() ? "reconnect attempts exhausted" : last_error_);
          return;
        }

        auto r = dial_once_();
        if (r.ok()) {
          {
            std::lock_guard<std::mutex> lk(mu_);
            inner_ = std::shared_ptr<Pipe>(std::move(r.value()));
            ever_connected_ = true;
            last_error_.clear();
            cv_.notify_all();
          }
          // Publish kConnected only after inner_ is visible. A callback that
          // sends/recvs inside on_state_change sees a connected pipe instead of
          // deadlocking on wait_connected's inner_ != nullptr predicate.
          set_state(ConnectionState::kConnected, "connected");
          if (is_closed()) return;
          break;
        }

        ++attempts;
        {
          std::lock_guard<std::mutex> lk(mu_);
          last_error_ = r.status().message();
        }

        // Backoff with jitter in [0, delay/2].
        auto jitter = std::chrono::milliseconds(0);
        if (delay.count() > 0) {
          std::uniform_int_distribution<long long> dist(0, std::max<long long>(0, delay.count() / 2));
          jitter = std::chrono::milliseconds(dist(rng));
        }
        auto sleep_for = delay + jitter;

        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait_for(lk, sleep_for, [&] { return closed_ || inner_ != nullptr; });

        auto next_ms = static_cast<long long>(static_cast<double>(delay.count()) * policy_.backoff_multiplier);
        delay = std::chrono::milliseconds(std::min<long long>(policy_.max_delay.count(), std::max<long long>(0, next_ms)));
      }
    }
  }

  bool is_closed() const {
    std::lock_guard<std::mutex> lk(mu_);
    return closed_;
  }

  bool ever_connected() const {
    std::lock_guard<std::mutex> lk(mu_);
    return ever_connected_;
  }

  DialOnceFn dial_once_;
  ReconnectPolicy policy_;
  ConnectionCallback on_state_change_;

  mutable std::mutex mu_;
  std::condition_variable cv_;

  bool closed_ = false;
  bool permanently_failed_ = false;
  ConnectionState state_ = ConnectionState::kDisconnected;
  bool ever_connected_ = false;
  std::string last_error_;
  std::shared_ptr<Pipe> inner_;
  std::thread worker_;

  // Last inner bound seen by max_message_bytes(); unbounded until the first
  // successful dial lands an inner pipe.
  mutable std::atomic<std::size_t> last_bound_{std::numeric_limits<std::size_t>::max()};
};

}  // namespace

std::unique_ptr<Pipe> make_reconnect_pipe(DialOnceFn dial_once,
                                         ReconnectPolicy policy,
                                         ConnectionCallback on_state_change) {
  sanitize_reconnect_policy(policy);
  return std::make_unique<ReconnectPipe>(std::move(dial_once), policy, std::move(on_state_change));
}

Result<std::unique_ptr<Pipe>> make_connected_reconnect_pipe(
    DialOnceFn dial_once, const ReconnectPolicy& policy,
    ConnectionCallback on_state_change,
    std::chrono::milliseconds first_connect_timeout) {
  auto pv = validate_reconnect_policy(policy);
  if (!pv.ok()) return pv.status();
  auto pipe = std::make_unique<ReconnectPipe>(std::move(dial_once), policy,
                                              std::move(on_state_change));
  auto st = pipe->wait_connected(first_connect_timeout);
  if (!st.ok()) {
    pipe->close();
    return st.status();
  }
  return std::unique_ptr<Pipe>(std::move(pipe));
}

}  // namespace duct

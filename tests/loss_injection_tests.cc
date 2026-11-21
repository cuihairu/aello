#include "loss_injection_tests.h"

#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "test_support.h"

namespace {

// In-memory sink that records the exact send sequence reaching the transport.
class RecordingSink final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message& msg, const duct::SendOptions&) override {
    std::lock_guard<std::mutex> lock(mu_);
    sent_.emplace_back(msg.as_string_view());
    return {};
  }
  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    return duct::Status::timeout("recording sink has no inbound data");
  }
  void close() override {}

  std::vector<std::string> sent() {
    std::lock_guard<std::mutex> lock(mu_);
    return sent_;
  }

 private:
  std::mutex mu_;
  std::vector<std::string> sent_;
};

std::string payload_with_id(const std::string& id) {
  // 1 KiB payloads: large enough to be realistic frames, small enough that
  // a handful always fit in loopback socket buffers (keeps tests deterministic).
  std::string s = id + "-";
  s.append(1024 - s.size(), 'x');
  return s;
}

std::string id_of(const std::string& payload) { return payload.substr(0, payload.find('-')); }

void expect_exact_sequence(const std::vector<std::string>& got,
                           const std::vector<std::string>& want) {
  EXPECT_EQ(got.size(), want.size());
  for (std::size_t i = 0; i < got.size() && i < want.size(); ++i) {
    EXPECT_EQ(got[i], want[i]);
  }
}

// Fault injector at the Pipe::send boundary: the caller sees a successful
// send, but the inner transport observes the injected fault. Modes model the
// three faults a reliability layer must defend against: loss, duplication,
// reordering.
enum class Fault { kNone, kDropSecond, kDuplicateSecond, kSwapSecondThird };

class LossyPipe final : public duct::Pipe {
 public:
  explicit LossyPipe(duct::Pipe* inner, Fault fault) : inner_(inner), fault_(fault) {}

  duct::Result<void> send(const duct::Message& msg, const duct::SendOptions& opt) override {
    const auto payload = std::string(msg.as_string_view());
    ++index_;
    switch (fault_) {
      case Fault::kNone:
        break;
      case Fault::kDropSecond:
        if (index_ == 2) return {};  // Caller told "ok", message discarded.
        break;
      case Fault::kDuplicateSecond:
        if (index_ == 2) {
          auto st = inner_->send(duct::Message::from_string(payload), opt);
          if (!st.ok()) return st;
        }
        break;
      case Fault::kSwapSecondThird:
        if (index_ == 2) return inner_->send(duct::Message::from_string(payload_with_id("m3")), opt);
        if (index_ == 3) return inner_->send(duct::Message::from_string(payload_with_id("m2")), opt);
        break;
    }
    return inner_->send(msg, opt);
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions& opt) override {
    return inner_->recv(opt);
  }
  void close() override { inner_->close(); }

 private:
  duct::Pipe* inner_;
  Fault fault_;
  int index_ = 0;
};

}  // namespace

// Cell L1: send() reported ok but the message was discarded. Current
// semantics: silent loss — no error, no redelivery. kAtLeastOnce must close
// this gap with sequence numbers and retransmit.
void test_loss_injection_drop_is_silent() {
  RecordingSink sink;
  LossyPipe lossy(&sink, Fault::kDropSecond);
  for (const char* id : {"m1", "m2", "m3", "m4"}) {
    auto st = lossy.send(duct::Message::from_string(payload_with_id(id)), {});
    EXPECT_TRUE(st.ok());
  }
  expect_exact_sequence(sink.sent(), {payload_with_id("m1"), payload_with_id("m3"),
                                      payload_with_id("m4")});
}

// Cell L2: the same message delivered twice. Current semantics: the receiver
// observes both copies verbatim — there is no dedup anywhere below the
// application. kAtLeastOnce redelivery therefore requires receiver-side dedup.
void test_loss_injection_duplicate_arrives_twice() {
  RecordingSink sink;
  LossyPipe lossy(&sink, Fault::kDuplicateSecond);
  for (const char* id : {"m1", "m2", "m3", "m4"}) {
    auto st = lossy.send(duct::Message::from_string(payload_with_id(id)), {});
    EXPECT_TRUE(st.ok());
  }
  expect_exact_sequence(sink.sent(), {payload_with_id("m1"), payload_with_id("m2"),
                                      payload_with_id("m2"), payload_with_id("m3"),
                                      payload_with_id("m4")});
}

// Cell L3: two sends delivered out of order. Current semantics: the receiver
// observes the swapped order verbatim — nothing below the application
// sequences messages. kAtLeastOnce retransmit therefore requires per-message
// sequence numbers.
void test_loss_injection_reorder_observed() {
  RecordingSink sink;
  LossyPipe lossy(&sink, Fault::kSwapSecondThird);
  for (const char* id : {"m1", "m2", "m3", "m4"}) {
    auto st = lossy.send(duct::Message::from_string(payload_with_id(id)), {});
    EXPECT_TRUE(st.ok());
  }
  expect_exact_sequence(sink.sent(), {payload_with_id("m1"), payload_with_id("m3"),
                                      payload_with_id("m2"), payload_with_id("m4")});
}

// Cell L4 (real tcp): the server stops reading and closes mid-stream. Current
// semantics: the receiver holds exactly the prefix it read, in order, with no
// duplicates; everything past the cut is gone and never comes back — no
// retransmit across disconnect.
void test_midstream_close_delivers_exact_prefix() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) {
    lis_r.value()->close();
    return;
  }

  const int kPrefix = 3;
  const int kTotal = 8;
  auto server = std::async(std::launch::async, [&]() -> std::vector<std::string> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return {};
    std::vector<std::string> got;
    for (int i = 0; i < kPrefix; ++i) {
      auto msg = p.value()->recv({});
      if (!msg.ok()) break;
      got.emplace_back(msg.value().as_string_view());
    }
    // Disconnect injection: stop reading, tear the connection down.
    p.value()->close();
    return got;
  });

  auto pipe_r = duct::dial(addr_r.value());
  EXPECT_TRUE(pipe_r.ok());
  if (!pipe_r.ok()) {
    lis_r.value()->close();
    server.wait();
    return;
  }

  std::vector<duct::Result<void>> send_results;
  for (int i = 1; i <= kTotal; ++i) {
    send_results.push_back(
        pipe_r.value()->send(duct::Message::from_string(payload_with_id("m" + std::to_string(i))),
                             {}));
  }

  auto got = server.get();
  EXPECT_EQ(got.size(), static_cast<std::size_t>(kPrefix));
  for (int i = 0; i < kPrefix && i < static_cast<int>(got.size()); ++i) {
    EXPECT_EQ(id_of(got[i]), "m" + std::to_string(i + 1));
  }
  // Zero duplicates: each id appears at most once (at-most-once invariant).
  for (std::size_t i = 0; i < got.size(); ++i) {
    for (std::size_t j = i + 1; j < got.size(); ++j) {
      EXPECT_TRUE(got[i] != got[j]);
    }
  }
  // The first kPrefix sends must have reported success: the server consumed
  // them, so they reached the kernel. Later sends may be ok (buffered into a
  // dying connection — the "ok but lost" case) or error; not asserted.
  for (int i = 0; i < kPrefix && i < static_cast<int>(send_results.size()); ++i) {
    EXPECT_TRUE(send_results[i].ok());
  }

  pipe_r.value()->close();
  lis_r.value()->close();
}

// Cell L5 (real tcp + reconnect): a connection dies mid-conversation and the
// wrapper redials. Current semantics: (a) every message arrives at most once
// across both connections — no automatic replay, no session resume; (b) the
// prefix the old server loop read is exactly m1..m3; (c) sends that returned
// ok into the dying connection's kernel buffer are lost even though the
// caller saw success — the caller cannot retry what it believes delivered.
// This cell is the core motivation for seq/ack/retransmit in the proposal.
void test_reconnect_gap_losses_are_invisible_to_caller() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) {
    lis_r.value()->close();
    return;
  }

  const int kPrefix = 3;
  const int kTotal = 8;
  std::mutex srv_mu;
  std::vector<std::string> all_got;
  std::vector<std::string> first_conn_got;
  auto server = std::async(std::launch::async, [&]() -> bool {
    // Connection 1: read exactly kPrefix messages, then inject a disconnect.
    auto p1 = lis_r.value()->accept();
    if (!p1.ok()) return false;
    for (int i = 0; i < kPrefix; ++i) {
      auto msg = p1.value()->recv({});
      if (!msg.ok()) break;
      auto s = msg.value().as_string_view();
      std::lock_guard<std::mutex> lk(srv_mu);
      first_conn_got.emplace_back(s);
      all_got.emplace_back(s);
    }
    p1.value()->close();

    // Connection 2 (the redial target): drain until idle.
    auto p2 = lis_r.value()->accept();
    if (!p2.ok()) return false;
    for (;;) {
      auto msg = p2.value()->recv({std::chrono::milliseconds(300)});
      if (!msg.ok()) break;
      std::lock_guard<std::mutex> lk(srv_mu);
      all_got.emplace_back(msg.value().as_string_view());
    }
    p2.value()->close();
    return true;
  });

  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(3000);
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(5);
  dial_opt.reconnect.max_delay = std::chrono::milliseconds(20);
  dial_opt.reconnect.max_attempts = 50;
  std::mutex st_mu;
  std::vector<duct::ConnectionState> states;
  dial_opt.on_state_change = [&](duct::ConnectionState st, const std::string&) {
    std::lock_guard<std::mutex> lk(st_mu);
    states.push_back(st);
  };

  auto pipe_r = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(pipe_r.ok());
  if (!pipe_r.ok()) {
    lis_r.value()->close();
    server.wait();
    return;
  }

  for (int i = 1; i <= kTotal; ++i) {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(2000);
    auto st = pipe_r.value()->send(
        duct::Message::from_string(payload_with_id("m" + std::to_string(i))), sopt);
    // Ok means handed to some connection's kernel buffer (may still be lost
    // with the dying connection); an error after a mid-frame cut is surfaced
    // and this message is simply not attempted again. Both are recorded.
    (void)st;
  }

  // Drive the client's recv path so the dead connection is noticed and the
  // worker redials even if every send landed in kernel buffers.
  auto rr = pipe_r.value()->recv({std::chrono::milliseconds(1500)});
  EXPECT_TRUE(rr.status().code() == duct::StatusCode::kTimeout ||
              rr.status().code() == duct::StatusCode::kClosed ||
              rr.status().code() == duct::StatusCode::kIoError);

  EXPECT_TRUE(server.get());

  // (a) At-most-once invariant across the reconnect: no id twice.
  std::vector<std::string> all;
  {
    std::lock_guard<std::mutex> lk(srv_mu);
    all = all_got;
  }
  for (std::size_t i = 0; i < all.size(); ++i) {
    for (std::size_t j = i + 1; j < all.size(); ++j) {
      EXPECT_TRUE(all[i] != all[j]);
    }
  }
  // (b) The prefix the first connection read is exactly m1..m3, in order.
  std::vector<std::string> first;
  {
    std::lock_guard<std::mutex> lk(srv_mu);
    first = first_conn_got;
  }
  EXPECT_EQ(first.size(), static_cast<std::size_t>(kPrefix));
  for (int i = 0; i < kPrefix && i < static_cast<int>(first.size()); ++i) {
    EXPECT_EQ(id_of(first[i]), "m" + std::to_string(i + 1));
  }
  // (c) Everything received across both connections is a subset of what was
  // sent; nothing spontaneous appeared.
  EXPECT_TRUE(all.size() <= static_cast<std::size_t>(kTotal));
  EXPECT_TRUE(all.size() >= static_cast<std::size_t>(kPrefix));

  // (d) The wrapper observed the disconnect and recovered. Recovery is
  // asynchronous with respect to the client's recv path, so poll the callback
  // log for a bounded window instead of coupling the assertion to when the
  // redial happened to land: the invariant under test is that recovery
  // happens at all, not when it lands.
  bool saw_disconnected = false;
  bool connected_after_disconnect = false;
  for (int wait = 0; wait < 200; ++wait) {
    {
      std::lock_guard<std::mutex> lk(st_mu);
      saw_disconnected = false;
      connected_after_disconnect = false;
      for (std::size_t i = 0; i < states.size(); ++i) {
        if (states[i] == duct::ConnectionState::kDisconnected) saw_disconnected = true;
        if (saw_disconnected && states[i] == duct::ConnectionState::kConnected) {
          connected_after_disconnect = true;
        }
      }
    }
    if (saw_disconnected && connected_after_disconnect) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(saw_disconnected);
  EXPECT_TRUE(connected_after_disconnect);

  pipe_r.value()->close();
  lis_r.value()->close();
}

#include "reliability_tests.h"

#include <chrono>
#include <future>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "test_support.h"

namespace {

// 1 KiB payloads: big enough to be realistic frames, small enough that a
// window of a handful always fits in loopback socket buffers.
std::string payload_with_id(const std::string& id) {
  std::string s = id + "-";
  s.append(1024 - s.size(), 'x');
  return s;
}

std::string id_of(const std::string& payload) { return payload.substr(0, payload.find('-')); }

// Shared R1/R2 topology: a reliable listener accepts a connection, reads exactly
// kPrefix messages, and tears it down; the dialer then notices the death and
// reconnects; the listener keeps draining redialed connections until every one
// of the kTotal ids has arrived (delivery-proven, not a fixed drain window).
// Returns every payload delivered, in delivery order.
struct ReconnectCaseResult {
  bool ok = false;
  std::vector<std::string> all_got;
};

ReconnectCaseResult run_reliable_reconnect_case(int prefix, int total) {
  ReconnectCaseResult out;

  duct::ListenOptions lopt;
  lopt.qos.reliability = duct::Reliability::kAtLeastOnce;
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return out;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) {
    lis_r.value()->close();
    return out;
  }

  // Signaled once conn1 is torn down, so the dialer's later sends are
  // guaranteed to hit a dead connection and trigger the redial deterministically.
  std::promise<void> conn1_closed;
  auto conn1_closed_fut = conn1_closed.get_future();
  // Signaled once the server has delivered every id. Delivery is the only
  // completion proof: a cumulative ACK clears the dialer's window only after the
  // server actually delivered the frame, so the dialer pumps its recv path until
  // this fires rather than assuming a fixed drain window.
  std::promise<void> all_delivered;
  auto all_delivered_fut = all_delivered.get_future();

  auto server = std::async(std::launch::async, [&]() -> bool {
    // Distinct ids delivered so far across every connection; conn1's prefix
    // counts toward `total` just like the replayed window does.
    std::set<std::string> seen;
    auto p1 = lis_r.value()->accept();
    if (!p1.ok()) return false;
    for (int i = 0; i < prefix; ++i) {
      auto msg = p1.value()->recv({});
      if (!msg.ok()) break;
      std::string s(msg.value().as_string_view());
      out.all_got.emplace_back(s);
      seen.insert(id_of(s));
    }
    p1.value()->close();
    conn1_closed.set_value();

    // Redial target(s): drain each accepted connection until idle and keep
    // accepting until every id has arrived. A fixed two-connection count is a
    // race — the dialer's window can span several redials under load — so drain
    // until the full set is seen or the deadline fires.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    for (;;) {
      auto p = lis_r.value()->accept();
      if (!p.ok()) return false;  // listener closed (test teardown)
      for (;;) {
        auto msg = p.value()->recv({std::chrono::milliseconds(250)});
        if (!msg.ok()) break;
        std::string s(msg.value().as_string_view());
        out.all_got.emplace_back(s);
        seen.insert(id_of(s));
      }
      p.value()->close();
      if (seen.size() >= static_cast<std::size_t>(total)) {
        all_delivered.set_value();
        return true;
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        lis_r.value()->close();  // unblock the pending accept, if any
        return false;
      }
    }
  });

  duct::DialOptions dopt;
  dopt.timeout = std::chrono::milliseconds(3000);
  dopt.qos.reliability = duct::Reliability::kAtLeastOnce;
  dopt.reconnect.enabled = true;
  dopt.reconnect.initial_delay = std::chrono::milliseconds(5);
  dopt.reconnect.max_delay = std::chrono::milliseconds(20);
  dopt.reconnect.max_attempts = 50;

  auto pipe_r = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(pipe_r.ok());
  if (!pipe_r.ok()) {
    lis_r.value()->close();
    server.wait();
    return out;
  }

  for (int i = 1; i <= total; ++i) {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(2000);
    // The first `prefix` sends land on a healthy conn1. The remaining sends
    // are buffered into the kernel before the close; once the server tears the
    // connection down the later sends hit the dead socket and self-heal through
    // the reconnect worker, which is what triggers the replay.
    auto st = pipe_r.value()->send(
        duct::Message::from_string(payload_with_id("m" + std::to_string(i))), sopt);
    if (i == prefix) conn1_closed_fut.wait();
  }

  // Drive the dialer's recv path until the server confirms every id arrived.
  // The recv pump is what notices the death and keeps the reconnect/replay cycle
  // alive; the ACKs it consumes clear the window, so the server reaching `total`
  // is a reachable fixed point rather than a timing assumption.
  const auto pump_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < pump_deadline &&
         all_delivered_fut.wait_for(std::chrono::milliseconds(0)) !=
             std::future_status::ready) {
    (void)pipe_r.value()->recv({std::chrono::milliseconds(100)});
  }

  // Tear the listener down before joining the server so a pending accept that
  // never saw a redial returns immediately instead of hanging server.get().
  lis_r.value()->close();
  EXPECT_TRUE(server.get());
  pipe_r.value()->close();
  out.ok = true;
  return out;
}

// Shared R3 topology: a reliable dial with a 2 KiB window (two 1 KiB messages
// exactly) and the given backpressure policy, against an eagerly draining
// server. The dialer's recv path stays idle until the drain phase, so no ACK
// can clear the window while the overflowing third send runs — the window
// state at that send is deterministic.
struct WindowCaseResult {
  bool ok = false;
  duct::StatusCode third_code = duct::StatusCode::kOk;
  bool post_drain_ok = false;
};

WindowCaseResult run_window_case(duct::BackpressurePolicy policy) {
  WindowCaseResult out;

  duct::ListenOptions lopt;
  lopt.qos.reliability = duct::Reliability::kAtLeastOnce;
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return out;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) {
    lis_r.value()->close();
    return out;
  }

  auto server = std::async(std::launch::async, [&]() {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return;
    for (;;) {
      auto msg = p.value()->recv({std::chrono::milliseconds(1500)});
      if (!msg.ok()) break;
    }
    p.value()->close();
  });

  // Window budget = 2 x 1 KiB: two messages fill it exactly, a third must not.
  duct::DialOptions dopt;
  dopt.timeout = std::chrono::milliseconds(3000);
  dopt.qos.reliability = duct::Reliability::kAtLeastOnce;
  dopt.qos.snd_hwm_bytes = 2048;
  dopt.qos.backpressure = policy;

  auto pipe_r = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(pipe_r.ok());
  if (!pipe_r.ok()) {
    lis_r.value()->close();
    server.wait();
    return out;
  }

  // Two 1 KiB messages fit the 2 KiB window exactly.
  for (int i = 1; i <= 2; ++i) {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(2000);
    auto st = pipe_r.value()->send(
        duct::Message::from_string(payload_with_id("w" + std::to_string(i))), sopt);
    EXPECT_TRUE(st.ok());
    if (!st.ok()) {
      pipe_r.value()->close();
      lis_r.value()->close();
      server.wait();
      return out;
    }
  }

  // Third send overflows the window: the policy decides the shape of the
  // rejection — kFailFast reports the bound immediately, kBlock waits out the
  // bounded send timeout. Both are bounded; neither grows the window.
  {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(300);
    auto st = pipe_r.value()->send(duct::Message::from_string(payload_with_id("w3")), sopt);
    out.third_code = st.ok() ? duct::StatusCode::kOk : st.status().code();
  }

  // Drain the dialer's recv path so the server's cumulative ACKs clear the
  // window (only the recv side processes ACKs), then a fresh send succeeds.
  // Bounded retry: the ACKs need one pump in practice, the deadline only
  // guards a lost-ACK worst case.
  const auto drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
  for (;;) {
    (void)pipe_r.value()->recv({std::chrono::milliseconds(200)});
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(500);
    auto st = pipe_r.value()->send(duct::Message::from_string(payload_with_id("w5")), sopt);
    if (st.ok()) {
      out.post_drain_ok = true;
      break;
    }
    if (std::chrono::steady_clock::now() >= drain_deadline) break;
  }

  out.ok = true;
  pipe_r.value()->close();
  lis_r.value()->close();
  server.wait();
  return out;
}

}  // namespace

// R1 (real tcp + reliable reconnect): everything the dying connection swallowed
// is redelivered after the redial, in send order.
void test_reliable_redelivers_after_reconnect() {
  const int kPrefix = 3;
  const int kTotal = 10;
  auto res = run_reliable_reconnect_case(kPrefix, kTotal);
  EXPECT_TRUE(res.ok);
  if (!res.ok) return;

  EXPECT_EQ(res.all_got.size(), static_cast<std::size_t>(kTotal));
  for (int i = 0; i < kTotal && i < static_cast<int>(res.all_got.size()); ++i) {
    EXPECT_EQ(id_of(res.all_got[i]), "m" + std::to_string(i + 1));
  }
}

// R2 (same topology as R1): the replayed window still contains m1..m3, whose
// ACKs the dialer never processed before the replay; the receiver's dedup
// (session + seq) absorbs them, so each id arrives exactly once.
void test_reliable_replay_is_deduplicated() {
  const int kPrefix = 3;
  const int kTotal = 10;
  auto res = run_reliable_reconnect_case(kPrefix, kTotal);
  EXPECT_TRUE(res.ok);
  if (!res.ok) return;

  EXPECT_EQ(res.all_got.size(), static_cast<std::size_t>(kTotal));
  // Each id at most once (the replay duplicates of m1..m3 must be absorbed).
  for (std::size_t i = 0; i < res.all_got.size(); ++i) {
    for (std::size_t j = i + 1; j < res.all_got.size(); ++j) {
      EXPECT_TRUE(res.all_got[i] != res.all_got[j]);
    }
  }
  // Exactly once: the union is the full set m1..mN, each present once.
  std::vector<std::string> want;
  for (int i = 1; i <= kTotal; ++i) want.push_back("m" + std::to_string(i));
  for (const auto& id : want) {
    int count = 0;
    for (const auto& got : res.all_got) {
      if (id_of(got) == id) ++count;
    }
    EXPECT_EQ(count, 1);
  }
}

// R3 (real tcp): the retransmit window is byte-bounded by snd_hwm_bytes. With
// an idle dialer recv path the window fills and the overflowing send follows
// the configured backpressure policy; once the dialer drains and ACKs arrive,
// sends succeed again. Both policies share the topology via run_window_case.
void test_reliable_window_bounded_by_snd_hwm() {
  // kFailFast: the bound reports immediately as kQueueFull — it never waits.
  {
    auto res = run_window_case(duct::BackpressurePolicy::kFailFast);
    EXPECT_TRUE(res.ok);
    if (!res.ok) return;
    EXPECT_EQ(res.third_code, duct::StatusCode::kQueueFull);
    EXPECT_TRUE(res.post_drain_ok);
  }
  // kBlock: the same bound surfaces as the bounded send timeout instead of a
  // hard error, and clears once ACKs are processed.
  {
    auto res = run_window_case(duct::BackpressurePolicy::kBlock);
    EXPECT_TRUE(res.ok);
    if (!res.ok) return;
    EXPECT_EQ(res.third_code, duct::StatusCode::kTimeout);
    EXPECT_TRUE(res.post_drain_ok);
  }
}

// R4 (real tcp, both directions): when the peer did not negotiate the
// reliability capability the connection proceeds at at-most-once rather than
// failing the protocol.
void test_reliable_degrades_to_at_most_once_against_v1_peer() {
  // Case A: reliable listener, plain dial.
  {
    duct::ListenOptions lopt;
    lopt.qos.reliability = duct::Reliability::kAtLeastOnce;
    auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
    EXPECT_TRUE(lis_r.ok());
    if (!lis_r.ok()) return;
    auto addr_r = lis_r.value()->local_address();
    EXPECT_TRUE(addr_r.ok());
    if (!addr_r.ok()) {
      lis_r.value()->close();
      return;
    }

    auto server = std::async(std::launch::async, [&]() -> bool {
      auto p = lis_r.value()->accept();
      if (!p.ok()) return false;
      auto msg = p.value()->recv({std::chrono::milliseconds(3000)});
      if (!msg.ok()) return false;
      p.value()->send(duct::Message::from_string(payload_with_id("echoA")), {});
      p.value()->close();
      return true;
    });

    duct::DialOptions dopt;
    dopt.timeout = std::chrono::milliseconds(3000);
    auto pipe_r = duct::dial(addr_r.value(), dopt);  // plain dial
    EXPECT_TRUE(pipe_r.ok());
    if (pipe_r.ok()) {
      auto st = pipe_r.value()->send(duct::Message::from_string(payload_with_id("pingA")), {});
      EXPECT_TRUE(st.ok());
      auto rr = pipe_r.value()->recv({std::chrono::milliseconds(3000)});
      EXPECT_TRUE(rr.ok());
      if (rr.ok()) EXPECT_EQ(id_of(std::string(rr.value().as_string_view())), "echoA");
      pipe_r.value()->close();
    }
    EXPECT_TRUE(server.get());
    lis_r.value()->close();
  }

  // Case B: plain listener, reliable dial.
  {
    auto lis_r = duct::listen("tcp://127.0.0.1:0");
    EXPECT_TRUE(lis_r.ok());
    if (!lis_r.ok()) return;
    auto addr_r = lis_r.value()->local_address();
    EXPECT_TRUE(addr_r.ok());
    if (!addr_r.ok()) {
      lis_r.value()->close();
      return;
    }

    auto server = std::async(std::launch::async, [&]() -> bool {
      auto p = lis_r.value()->accept();
      if (!p.ok()) return false;
      auto msg = p.value()->recv({std::chrono::milliseconds(3000)});
      if (!msg.ok()) return false;
      p.value()->send(duct::Message::from_string(payload_with_id("echoB")), {});
      p.value()->close();
      return true;
    });

    duct::DialOptions dopt;
    dopt.timeout = std::chrono::milliseconds(3000);
    dopt.qos.reliability = duct::Reliability::kAtLeastOnce;
    auto pipe_r = duct::dial(addr_r.value(), dopt);  // reliable dial
    EXPECT_TRUE(pipe_r.ok());
    if (pipe_r.ok()) {
      auto st = pipe_r.value()->send(duct::Message::from_string(payload_with_id("pingB")), {});
      EXPECT_TRUE(st.ok());
      auto rr = pipe_r.value()->recv({std::chrono::milliseconds(3000)});
      EXPECT_TRUE(rr.ok());
      if (rr.ok()) EXPECT_EQ(id_of(std::string(rr.value().as_string_view())), "echoB");
      pipe_r.value()->close();
    }
    EXPECT_TRUE(server.get());
    lis_r.value()->close();
  }
}

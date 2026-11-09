#include "test_support.h"

#include "adapter_tests.h"

#include "duct/convenience.h"
#include "duct/raii.h"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

std::string text(const duct::Message& m) {
  return std::string(reinterpret_cast<const char*>(m.data()), m.size());
}

}  // namespace

// The dial builder must produce a pipe whose configured options actually take
// effect: a send watermark requires the QoS wrapper (otherwise the setting is
// silently dropped), and auto_reconnect must reach the state callback.
void test_adapter_dial_builder_propagates_options() {
  auto lis_r = duct::convenience::listen_shm("duct_adapter_bld").backlog(4).bind();
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({.timeout = std::chrono::seconds(2)});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Plain QoS dial: the watermark must be visible on the returned pipe.
  auto c = duct::convenience::shm("duct_adapter_bld")
               .timeout(std::chrono::milliseconds(2000))
               .send_hwm(1024)
               .recv_hwm(0)
               .backpressure(duct::BackpressurePolicy::kBlock)
               .connect();
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  // send_hwm was honored: the advertised send bound is the watermark, which
  // only happens when the dial actually wrapped the transport in QoS.
  EXPECT_EQ(c.value()->max_message_bytes(), static_cast<std::size_t>(1024));

  auto st = c.value()->send(duct::Message::from_string("bld"), {});
  EXPECT_TRUE(st.ok());
  auto echoed = c.value()->recv({.timeout = std::chrono::seconds(2)});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(text(echoed.value()), "bld");

  c.value()->close();

  // Reconnect-enabled dial: the initial connection must reach the callback
  // (the reconnect worker connects in the background).
  std::atomic<int> connected_states{0};
  auto r = duct::convenience::shm("duct_adapter_bld")
               .timeout(std::chrono::milliseconds(2000))
               .auto_reconnect()
               .on_state_change([&](duct::ConnectionState s, const std::string&) {
                 if (s == duct::ConnectionState::kConnected) ++connected_states;
               })
               .connect();
  EXPECT_TRUE(r.ok());
  if (r.ok()) {
    for (int i = 0; i < 40 && connected_states.load() == 0; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(connected_states.load() >= 1);
    r.value()->close();
  }

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) EXPECT_TRUE(server.get().ok());

  lis_r.value()->close();

  // operator() dials like connect(): a peer that never appears fails.
  auto absent = duct::convenience::shm("duct_adapter_bld_absent")
                    .timeout(std::chrono::milliseconds(300))();
  EXPECT_TRUE(!absent.ok());
}

// The listen builder binds through the adapter surface and accepts a dialer
// configured through the dial builder with defaults.
void test_adapter_listen_builder_binds_and_accepts() {
  auto lis_r = duct::convenience::listen_shm("duct_adapter_lis").bind();
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({.timeout = std::chrono::seconds(2)});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto c = duct::convenience::shm("duct_adapter_lis")
               .timeout(std::chrono::milliseconds(2000))
               .connect();
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto st = c.value()->send(duct::Message::from_string("lis"), {});
  EXPECT_TRUE(st.ok());
  auto echoed = c.value()->recv({.timeout = std::chrono::seconds(2)});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(text(echoed.value()), "lis");

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) EXPECT_TRUE(server.get().ok());

  c.value()->close();
  lis_r.value()->close();
}

// request()/reply() implement one request-response exchange over a real
// transport.
void test_adapter_request_reply_roundtrip() {
  auto lis_r = duct::convenience::bind_raw("shm://duct_adapter_req");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({.timeout = std::chrono::seconds(2)});
    if (!msg.ok()) return msg.status();
    return duct::convenience::reply(*p.value(), msg.value());
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto c = duct::convenience::connect_raw("shm://duct_adapter_req");
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto resp = duct::convenience::request(*c.value(),
                                         duct::Message::from_string("ping"),
                                         std::chrono::seconds(2));
  EXPECT_TRUE(resp.ok());
  if (resp.ok()) EXPECT_EQ(text(resp.value()), "ping");

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) EXPECT_TRUE(server.get().ok());

  c.value()->close();
  lis_r.value()->close();
}

// send_batch()/recv_batch() move an ordered group of messages end to end.
void test_adapter_batch_roundtrip() {
  auto lis_r = duct::convenience::bind_raw("shm://duct_adapter_batch");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto got = duct::convenience::recv_batch(*p.value(), 3,
                                             std::chrono::seconds(2));
    if (!got.ok()) return got.status();
    auto st = duct::convenience::send_batch(*p.value(), got.value());
    if (!st.ok()) return st.status();
    return {};
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto c = duct::convenience::connect_raw("shm://duct_adapter_batch");
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  std::vector<duct::Message> out;
  out.push_back(duct::Message::from_string("b0"));
  out.push_back(duct::Message::from_string("b1"));
  out.push_back(duct::Message::from_string("b2"));
  auto sent = duct::convenience::send_batch(*c.value(), out);
  EXPECT_TRUE(sent.ok());
  if (sent.ok()) EXPECT_EQ(sent.value(), static_cast<std::size_t>(3));

  auto in = duct::convenience::recv_batch(*c.value(), 3,
                                          std::chrono::seconds(2));
  EXPECT_TRUE(in.ok());
  if (in.ok()) {
    EXPECT_TRUE(in.value().size() == 3);
    if (in.value().size() == 3) {
      EXPECT_EQ(text(in.value()[0]), "b0");
      EXPECT_EQ(text(in.value()[1]), "b1");
      EXPECT_EQ(text(in.value()[2]), "b2");
    }
  }

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) EXPECT_TRUE(server.get().ok());

  c.value()->close();
  lis_r.value()->close();
}

// Scoped wrappers close on destruction, support moves, and round-trip data
// through the raii::connect/bind helpers.
void test_adapter_raii_scoped_roundtrip() {
  auto lis_r = duct::raii::bind("shm://duct_adapter_raii");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({.timeout = std::chrono::seconds(2)});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto c = duct::raii::connect("shm://duct_adapter_raii",
                               duct::DialOptions{});
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  // Moving transfers ownership; the source releases its pipe.
  duct::raii::ScopedPipe moved(std::move(c.value()));
  EXPECT_TRUE(c.value().get() == nullptr);
  EXPECT_TRUE(static_cast<bool>(moved));

  auto st = moved.send(duct::Message::from_string("raii"), {});
  EXPECT_TRUE(st.ok());
  auto echoed = moved.recv({.timeout = std::chrono::seconds(2)});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(text(echoed.value()), "raii");

  // Explicit close is idempotent and empties the wrapper.
  moved.close();
  moved.close();
  EXPECT_TRUE(!moved);

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) EXPECT_TRUE(server.get().ok());

  lis_r.value()->close();
}

// echo_server() loops until the peer closes, echoing every message; the loop
// ends with Ok on kClosed.
void test_adapter_echo_server_loop() {
  auto lis_r = duct::convenience::bind_raw("shm://duct_adapter_echo");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    return duct::convenience::echo_server(std::move(p.value()));
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto c = duct::convenience::connect_raw("shm://duct_adapter_echo");
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  for (int i = 0; i < 2; ++i) {
    std::string payload = "e" + std::to_string(i);
    auto st = c.value()->send(duct::Message::from_string(payload), {});
    EXPECT_TRUE(st.ok());
    auto echoed = c.value()->recv({.timeout = std::chrono::seconds(2)});
    EXPECT_TRUE(echoed.ok());
    if (echoed.ok()) EXPECT_EQ(text(echoed.value()), payload);
  }

  // Closing the client ends the server loop with Ok (kClosed is normal).
  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) EXPECT_TRUE(server.get().ok());

  lis_r.value()->close();
}

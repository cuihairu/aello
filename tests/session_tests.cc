#include "test_support.h"
#include "session_tests.h"


void test_rpc_request_correlation() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  // Echo server: returns the framed request verbatim, preserving the request id.
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    for (int i = 0; i < 3; ++i) {
      auto msg = p.value()->recv({});
      if (!msg.ok()) return msg.status();
      auto st = p.value()->send(msg.value(), {});
      if (!st.ok()) return st;
    }
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto pipe = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(pipe.ok());
  if (!pipe.ok()) {
    lis_r.value()->close();
    return;
  }

  duct::rpc::Client client(std::move(pipe.value()));

  // Three concurrent correlated calls; each must get its own response.
  std::vector<std::future<duct::Result<duct::Message>>> calls;
  for (int i = 0; i < 3; ++i) {
    calls.emplace_back(std::async(std::launch::async, [&, i] {
      return client.call(duct::Message::from_string("req-" + std::to_string(i)),
                         std::chrono::milliseconds(2000));
    }));
  }

  for (int i = 0; i < 3; ++i) {
    auto r = calls[i].get();
    EXPECT_TRUE(r.ok());
    if (r.ok()) {
      EXPECT_EQ(r.value().as_string_view(), "req-" + std::to_string(i));
    }
  }

  client.close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


void test_rpc_request_timeout() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  // Server accepts but never responds, so the call times out.
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    duct::RecvOptions ropt;
    ropt.timeout = std::chrono::milliseconds(200);
    auto msg = p.value()->recv(ropt);
    (void)msg;
    // Hang without replying so the client's call times out.
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto pipe = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(pipe.ok());
  if (!pipe.ok()) {
    lis_r.value()->close();
    return;
  }

  duct::rpc::Client client(std::move(pipe.value()));
  auto r = client.call(duct::Message::from_string("req-timeout"),
                       std::chrono::milliseconds(150));
  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kTimeout);
  }

  client.close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


// close() must not hang when the transport does not wake a blocked read; the
// RPC dispatch loop polls with a bounded timeout and joins promptly.
void test_rpc_close_joins_without_transport_wakeup() {
  auto pipe = std::make_unique<SleepyRecvPipe>();
  duct::rpc::Client client(std::move(pipe));
  auto done = std::async(std::launch::async, [&] { client.close(); });
  EXPECT_TRUE(done.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
  if (done.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
    done.get();
  }
}


void test_pubsub_multiple_subscribers() {
  duct::pubsub::Broker broker;

  std::mutex mu;
  std::map<std::string, std::vector<std::string>> received;
  auto make_sub = [&](const std::string& filter) {
    return broker.subscribe(filter, [&, filter](const std::string& topic, const duct::Message& msg) {
      std::lock_guard<std::mutex> lock(mu);
      received[filter].push_back(topic + ":" + std::string(msg.as_string_view()));
    });
  };

  auto s1 = make_sub("a");       // matches a, a/x, but not b
  auto s2 = make_sub("a/x");     // narrower
  auto s3 = make_sub("");        // everything

  broker.publish("a", duct::Message::from_string("hello"));
  broker.publish("a/x", duct::Message::from_string("world"));
  broker.publish("b", duct::Message::from_string("other"));

  {
    std::lock_guard<std::mutex> lock(mu);
    EXPECT_EQ(received["a"].size(), 2u);       // a + a/x
    EXPECT_EQ(received["a/x"].size(), 1u);     // a/x only
    EXPECT_EQ(received[""].size(), 3u);        // everything
  }

  // Unsubscribe: s1 is destroyed, so it stops receiving.
  s1.reset();
  broker.publish("a", duct::Message::from_string("again"));
  {
    std::lock_guard<std::mutex> lock(mu);
    EXPECT_EQ(received["a"].size(), 2u);       // no new "a" after unsubscribe
    EXPECT_EQ(received[""].size(), 4u);        // s3 still gets it
  }

  (void)s2;
  (void)s3;
}


// A Subscription may outlive its Broker (e.g. a scoped broker used to wire a
// one-shot dispatch). Destroying the broker first must not leave the later
// Subscription destructor calling into freed memory.
void test_pubsub_subscription_outlives_broker() {
  std::unique_ptr<duct::pubsub::Broker::Subscription> sub;
  {
    duct::pubsub::Broker broker;
    sub = broker.subscribe("x", [](const std::string&, const duct::Message&) {});
  }
  sub.reset();
}


// Broker and Subscription destruction may race on different threads. The
// shared atomic alive flag and state lifetime must prevent both the data race
// on the old plain bool and any use-after-free when Subscription loses the
// check-then-act race but outlives the Broker's members.
void test_pubsub_concurrent_broker_subscription_destruction() {
  for (int attempt = 0; attempt < 200; ++attempt) {
    auto broker = std::make_unique<duct::pubsub::Broker>();
    auto sub = broker->subscribe("x", [](const std::string&, const duct::Message&) {});
    auto broker_thread = std::async(std::launch::async, [&broker] { broker.reset(); });
    auto subscription_thread = std::async(std::launch::async, [&sub] { sub.reset(); });
    broker_thread.wait();
    subscription_thread.wait();
  }
}

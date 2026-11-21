#include "test_support.h"
#include "core_tests.h"

void test_address_parse() {
  {
    auto a = duct::Address::parse("127.0.0.1:1234");
    EXPECT_TRUE(a.ok());
    EXPECT_EQ(a.value().scheme, duct::Scheme::kTcp);
    EXPECT_EQ(a.value().tcp.host, "127.0.0.1");
    EXPECT_EQ(a.value().tcp.port, 1234);
  }
  {
    auto a = duct::Address::parse("tcp://:9");
    EXPECT_TRUE(a.ok());
    EXPECT_EQ(a.value().tcp.host, "127.0.0.1");
    EXPECT_EQ(a.value().tcp.port, 9);
  }
  {
    auto a = duct::Address::parse("shm://gamebus");
    EXPECT_TRUE(a.ok());
    EXPECT_EQ(a.value().scheme, duct::Scheme::kShm);
    EXPECT_EQ(a.value().name, "gamebus");
  }
  {
    auto a = duct::Address::parse("uds:///tmp/duct.sock");
    EXPECT_TRUE(a.ok());
    EXPECT_EQ(a.value().scheme, duct::Scheme::kUds);
    EXPECT_EQ(a.value().name, "/tmp/duct.sock");
  }
  {
    auto a = duct::Address::parse("pipe://mypipe");
    EXPECT_TRUE(a.ok());
    EXPECT_EQ(a.value().scheme, duct::Scheme::kPipe);
    EXPECT_EQ(a.value().name, "mypipe");
  }
}


void test_dial_rejects_negative_timeout() {
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(-1);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("tcp://127.0.0.1:1", dial_opt);
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kInvalidArgument);
  }
  auto s = duct::dial("shm://duct_negative_timeout", dial_opt);
  EXPECT_TRUE(!s.ok());
  if (!s.ok()) {
    EXPECT_EQ(s.status().code(), duct::StatusCode::kInvalidArgument);
  }
}


// Pinned current behavior: no TLS layer exists (see the proposal in
// docs/design/tls-mtls.md). tls:// is unknown at the address layer, so
// dial()/listen() reject it during Address::parse — before any scheme
// dispatch — with kInvalidArgument. When TLS lands this test is replaced by
// a real handshake test.
void test_tls_scheme_rejected_as_unsupported() {
  {
    auto r = duct::dial("tls://127.0.0.1:1");
    EXPECT_TRUE(!r.ok());
    EXPECT_EQ(r.status().code(), duct::StatusCode::kInvalidArgument);
    EXPECT_TRUE(r.status().message().find("unknown scheme") != std::string::npos);
  }
  {
    auto r = duct::listen("tls://127.0.0.1:0");
    EXPECT_TRUE(!r.ok());
    EXPECT_EQ(r.status().code(), duct::StatusCode::kInvalidArgument);
  }
}

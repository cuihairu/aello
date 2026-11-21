#include "test_support.h"
#include "tls_tests.h"

#include "duct/config.h"

#if DUCT_HAVE_TLS

#include <string>

namespace {

#if defined(DUCT_TLS_FIXTURE_DIR)
const std::string kFixtures = DUCT_TLS_FIXTURE_DIR;
std::string fixture(const std::string& name) { return kFixtures + "/" + name; }

// Server options for the TLS-positive cases: the CA-signed localhost leaf.
duct::ListenOptions server_tls_opts(bool mtls = false) {
  duct::ListenOptions opt;
  opt.tls.enabled = true;
  opt.tls.ca_path = fixture("ca.crt");
  opt.tls.cert_path = fixture("server.crt");
  opt.tls.key_path = fixture("server.key");
  opt.tls.require_client_cert = mtls;
  return opt;
}

// Drive accept() on another thread. The TLS handshake only runs there, so a
// negative test must accept concurrently: without it the client's handshake
// just times out instead of surfacing the verify failure.
std::future<duct::Result<void>> accept_async(duct::Listener& listener) {
  return std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = listener.accept();
    if (!p.ok()) return p.status();
    p.value()->close();
    return {};
  });
}

// One echo round-trip through a TLS listener: accept, mirror the first message
// back, and return the accepted pipe's status.
duct::Result<void> run_echo_roundtrip(duct::Listener& listener) {
  auto p = listener.accept();
  if (!p.ok()) return p.status();
  auto msg = p.value()->recv({});
  if (!msg.ok()) return msg.status();
  return p.value()->send(msg.value(), {});
}
#endif  // DUCT_TLS_FIXTURE_DIR

}  // namespace

// TlsOptions.enabled is an opt-in on tcp:// only; other schemes must reject it
// loudly at the entry point, not silently ignore the encryption request.
void test_tls_only_on_tcp() {
  {
    duct::DialOptions opt;
    opt.tls.enabled = true;
    auto r = duct::dial("uds:///tmp/duct_tls_wrong_scheme.sock", opt);
    EXPECT_TRUE(!r.ok());
    if (!r.ok()) EXPECT_EQ(r.status().code(), duct::StatusCode::kNotSupported);
  }
  {
    duct::ListenOptions opt;
    opt.tls.enabled = true;
    auto r = duct::listen("uds:///tmp/duct_tls_wrong_scheme.sock", opt);
    EXPECT_TRUE(!r.ok());
    if (!r.ok()) EXPECT_EQ(r.status().code(), duct::StatusCode::kNotSupported);
  }
}

#if defined(DUCT_TLS_FIXTURE_DIR)

// Positive: a TLS client verifies the CA-signed localhost server and the echo
// round-trips. The dial uses the hostname "localhost" so hostname verification
// is exercised against the SAN, not skipped by an IP literal.
void test_tls_echo_roundtrip() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0", server_tls_opts());
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    return run_echo_roundtrip(*lis_r.value());
  });

  // Dial "localhost" (not the IP) so X509 name verification matches the SAN.
  const std::string addr = "tcp://localhost:" +
                           addr_r.value().substr(addr_r.value().rfind(':') + 1);
  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  auto c = duct::dial(addr, dopt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  duct::SendOptions so;
  so.timeout = std::chrono::milliseconds(1000);
  auto st = c.value()->send(duct::Message::from_string("tls-echo"), so);
  EXPECT_TRUE(st.ok());
  duct::RecvOptions ro;
  ro.timeout = std::chrono::milliseconds(1000);
  auto echoed = c.value()->recv(ro);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "tls-echo");

  c.value()->close();
  auto fut = server.wait_for(std::chrono::seconds(2));
  if (fut == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}

// Positive: TLS + at-least-once reliability. The negotiated capability rides
// the TLS stream (handshake_client_sync / handshake_server_stoppable) and the
// reliability frames (v2 + ACKs) flow over the same encrypted channel.
void test_tls_reliable_echo_roundtrip() {
  duct::ListenOptions lopt = server_tls_opts();
  lopt.qos.reliability = duct::Reliability::kAtLeastOnce;
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    return run_echo_roundtrip(*lis_r.value());
  });

  const std::string addr = "tcp://localhost:" +
                           addr_r.value().substr(addr_r.value().rfind(':') + 1);
  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.qos.reliability = duct::Reliability::kAtLeastOnce;
  dopt.timeout = std::chrono::milliseconds(3000);
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  auto c = duct::dial(addr, dopt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  duct::SendOptions so;
  so.timeout = std::chrono::milliseconds(1000);
  auto st = c.value()->send(duct::Message::from_string("tls-reliable"), so);
  EXPECT_TRUE(st.ok());
  duct::RecvOptions ro;
  ro.timeout = std::chrono::milliseconds(1000);
  auto echoed = c.value()->recv(ro);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "tls-reliable");

  c.value()->close();
  auto fut = server.wait_for(std::chrono::seconds(2));
  if (fut == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}

// Negative: the server presents a CA-signed cert whose notAfter is already in
// the past; the client's chain verification must reject it (kInvalidArgument,
// not a generic io error).
void test_tls_expired_cert_rejected() {
  duct::ListenOptions lopt = server_tls_opts();
  lopt.tls.cert_path = fixture("expired.crt");
  lopt.tls.key_path = fixture("expired.key");
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto acc = accept_async(*lis_r.value());
  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.timeout = std::chrono::milliseconds(2000);
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  auto c = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kInvalidArgument);
  }
  if (acc.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
    (void)acc.get();  // the server side fails too; the client code is the pin
  }
  lis_r.value()->close();
}

// Negative: the chain is valid but the cert's SAN names evil.example.com; a
// dial to localhost with name verification on must be rejected.
void test_tls_wrong_host_rejected() {
  duct::ListenOptions lopt = server_tls_opts();
  lopt.tls.cert_path = fixture("wronghost.crt");
  lopt.tls.key_path = fixture("wronghost.key");
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto acc = accept_async(*lis_r.value());
  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.timeout = std::chrono::milliseconds(2000);
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  // verify_peer_name is true by default: 127.0.0.1 must match an iPAddress
  // SAN, which this leaf does not carry.
  auto c = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kInvalidArgument);
  }
  if (acc.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
    (void)acc.get();
  }
  lis_r.value()->close();
}

// Control: the same wrong-host cert is accepted when only the chain is checked
// (verify_peer_name=false skips the name check, not the CA check).
void test_tls_wrong_host_verify_name_skipped() {
  duct::ListenOptions lopt = server_tls_opts();
  lopt.tls.cert_path = fixture("wronghost.crt");
  lopt.tls.key_path = fixture("wronghost.key");
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    return run_echo_roundtrip(*lis_r.value());
  });

  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.timeout = std::chrono::milliseconds(2000);
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  dopt.tls.verify_peer_name = false;
  auto c = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  duct::SendOptions so;
  so.timeout = std::chrono::milliseconds(1000);
  auto st = c.value()->send(duct::Message::from_string("no-name-check"), so);
  EXPECT_TRUE(st.ok());
  duct::RecvOptions ro;
  ro.timeout = std::chrono::milliseconds(1000);
  auto echoed = c.value()->recv(ro);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "no-name-check");

  c.value()->close();
  auto fut = server.wait_for(std::chrono::seconds(2));
  if (fut == std::future_status::ready) (void)server.get();
  lis_r.value()->close();
}

// Negative: the server presents a self-signed leaf that is not in the client's
// trust store; the chain verification must reject it.
void test_tls_untrusted_ca_rejected() {
  duct::ListenOptions lopt = server_tls_opts();
  lopt.tls.cert_path = fixture("untrusted.crt");
  lopt.tls.key_path = fixture("untrusted.key");
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto acc = accept_async(*lis_r.value());
  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.timeout = std::chrono::milliseconds(2000);
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  auto c = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kInvalidArgument);
  }
  if (acc.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
    (void)acc.get();
  }
  lis_r.value()->close();
}

// Negative: the server's cert and key do not match; the listener must reject
// the misconfiguration at listen() time, before any connection is accepted.
void test_tls_listener_rejects_wrong_key() {
  duct::ListenOptions lopt = server_tls_opts();
  lopt.tls.key_path = fixture("wrongkey.key");  // unrelated to server.crt
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kInvalidArgument);
  }
}

// Negative: a TLS listener requires a cert and key; omitting them is a
// configuration error surfaced at listen().
void test_tls_listener_requires_cert_and_key() {
  duct::ListenOptions lopt;
  lopt.tls.enabled = true;
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kInvalidArgument);
  }
}

// Positive: mTLS — the server requires a client cert and the client presents
// the CA-signed leaf, so the echo round-trips.
void test_tls_mtls_requires_client_cert() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0", server_tls_opts(/*mtls=*/true));
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    return run_echo_roundtrip(*lis_r.value());
  });

  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.timeout = std::chrono::milliseconds(3000);
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  dopt.tls.cert_path = fixture("client.crt");
  dopt.tls.key_path = fixture("client.key");
  auto c = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  duct::SendOptions so;
  so.timeout = std::chrono::milliseconds(1000);
  auto st = c.value()->send(duct::Message::from_string("mtls-echo"), so);
  EXPECT_TRUE(st.ok());
  duct::RecvOptions ro;
  ro.timeout = std::chrono::milliseconds(1000);
  auto echoed = c.value()->recv(ro);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "mtls-echo");

  c.value()->close();
  auto fut = server.wait_for(std::chrono::seconds(2));
  if (fut == std::future_status::ready) (void)server.get();
  lis_r.value()->close();
}

// Negative: the server requires a client cert but the client presents none;
// the handshake must fail. Uses a reliable dial so the failure surfaces as a
// dial error (the sync capability handshake reads the server's rejection)
// rather than a pipe that dials "fine" and dies on first use.
void test_tls_mtls_client_without_cert_rejected() {
  duct::ListenOptions lopt = server_tls_opts(/*mtls=*/true);
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lopt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto acc = accept_async(*lis_r.value());
  duct::DialOptions dopt;
  dopt.qos.snd_hwm_bytes = 0;
  dopt.qos.rcv_hwm_bytes = 0;
  dopt.qos.reliability = duct::Reliability::kAtLeastOnce;
  dopt.timeout = std::chrono::milliseconds(2000);
  dopt.tls.enabled = true;
  dopt.tls.ca_path = fixture("ca.crt");
  // No cert_path/key_path: the client sends no certificate.
  auto c = duct::dial(addr_r.value(), dopt);
  EXPECT_TRUE(!c.ok());
  if (acc.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
    (void)acc.get();
  }
  lis_r.value()->close();
}

#endif  // DUCT_TLS_FIXTURE_DIR
#endif  // DUCT_HAVE_TLS

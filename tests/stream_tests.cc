#include "test_support.h"
#include "stream_tests.h"


void test_uds_echo_one() {
#if !defined(_WIN32)
  // Unique socket path so parallel/rerun invocations do not collide.
  std::string path = "/tmp/duct_uds_" + std::to_string(::getpid()) + ".sock";
  ::unlink(path.c_str());  // clear any stale socket from a crashed run

  auto lis_r = duct::listen("uds://" + path);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  // Disable QoS wrapper to exercise the raw uds transport path.
  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("uds://" + path, dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto st = c.value()->send(duct::Message::from_string("hello-uds"), {});
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    lis_r.value()->close();
    return;
  }

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    std::string s(reinterpret_cast<const char*>(echoed.value().data()), echoed.value().size());
    EXPECT_EQ(s, "hello-uds");
  }

  auto sst = server.wait_for(std::chrono::seconds(1));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  }

  lis_r.value()->close();
#endif
}


void test_uds_multi_message_echo() {
#if !defined(_WIN32)
  std::string path = "/tmp/duct_uds_multi_" + std::to_string(::getpid()) + ".sock";
  ::unlink(path.c_str());

  auto lis_r = duct::listen("uds://" + path);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

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
  auto c = duct::dial("uds://" + path, dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  for (int i = 0; i < 3; ++i) {
    const std::string payload = "uds-msg-" + std::to_string(i);
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(500);
    auto st = c.value()->send(duct::Message::from_string(payload), sopt);
    EXPECT_TRUE(st.ok());
    if (!st.ok()) break;
    auto echoed = c.value()->recv({});
    EXPECT_TRUE(echoed.ok());
    if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), payload);
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(1));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  }
  lis_r.value()->close();
#endif
}


// Same sub-millisecond deadline guarantee for the UDS transport, which shares
// the channel-filtered recv loop and the same duration_cast truncation bug.
void test_uds_channel_recv_subms_deadline_bounded() {
#if !defined(_WIN32)
  std::string path = "/tmp/duct_uds_subms_" + std::to_string(::getpid()) + ".sock";
  ::unlink(path.c_str());

  auto lis_r = duct::listen("uds://" + path);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("uds://" + path, dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  duct::RecvOptions r7;
  r7.timeout = std::chrono::milliseconds(1);
  const auto begin = std::chrono::steady_clock::now();
  auto recv_fut = std::async(std::launch::async, [&]() { return c.value()->recv(r7); });
  auto ready = recv_fut.wait_for(std::chrono::seconds(5));
  EXPECT_TRUE(ready == std::future_status::ready);
  const auto elapsed = std::chrono::steady_clock::now() - begin;
  if (ready == std::future_status::ready) {
    auto m7 = recv_fut.get();
    EXPECT_TRUE(!m7.ok());
    EXPECT_EQ(m7.status().code(), duct::StatusCode::kTimeout);
    EXPECT_TRUE(elapsed < std::chrono::seconds(5));
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(3));
  if (sst == std::future_status::ready) (void)server.get();
#endif
}


// Concurrent recv() calls on different UDS channels must not interleave raw
// frame reads on the same fd (the tcp equivalent is test_tcp_concurrent_channel_recv).
void test_uds_concurrent_channel_recv() {
#if defined(_WIN32)
  return;
#else
  std::string path = "/tmp/duct_uds_conc_" + std::to_string(::getpid()) + ".sock";
  ::unlink(path.c_str());
  auto lis_r = duct::listen("uds://" + path);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  std::atomic<bool> clients_done{false};
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    for (int i = 0; i < 60; ++i) {
      duct::SendOptions s7, s8;
      s7.channel_id = 7;
      s8.channel_id = 8;
      auto a = p.value()->send(duct::Message::from_string("a" + std::to_string(i)), s7);
      if (!a.ok()) return a.status();
      auto b = p.value()->send(duct::Message::from_string("b" + std::to_string(i)), s8);
      if (!b.ok()) return b.status();
    }
    while (!clients_done.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("uds://" + path, dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  std::atomic<int> ok7{0}, ok8{0};
  auto r7 = std::async(std::launch::async, [&]() {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(10000);
    opt.channel_id = 7;
    for (int i = 0; i < 60; ++i) {
      auto m = c.value()->recv(opt);
      if (!m.ok()) {
        std::cerr << "uds r7 failed at " << ok7.load() << "\n";
        return;
      }
      if (m.value().as_string_view().rfind("a", 0) != 0) {
        std::cerr << "uds r7 wrong payload\n";
        return;
      }
      ++ok7;
    }
  });
  auto r8 = std::async(std::launch::async, [&]() {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(10000);
    opt.channel_id = 8;
    for (int i = 0; i < 60; ++i) {
      auto m = c.value()->recv(opt);
      if (!m.ok()) {
        std::cerr << "uds r8 failed at " << ok8.load() << "\n";
        return;
      }
      if (m.value().as_string_view().rfind("b", 0) != 0) {
        std::cerr << "uds r8 wrong payload\n";
        return;
      }
      ++ok8;
    }
  });
  r7.wait();
  r8.wait();
  clients_done.store(true);
  EXPECT_EQ(ok7.load(), 60);
  EXPECT_EQ(ok8.load(), 60);

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) (void)server.get();
  ::unlink(path.c_str());
#endif
}


void test_uds_peer_credential() {
#if !defined(_WIN32) && defined(SO_PEERCRED)
  std::string path = "/tmp/duct_uds_cred_" + std::to_string(::getpid()) + ".sock";
  ::unlink(path.c_str());

  auto lis_r = duct::listen("uds://" + path);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto cred = p.value()->peer_credential();
    if (!cred.ok()) return cred.status();
    // The client is our own process; uid/gid/pid must match getuid/getgid/getpid.
    if (cred.value().uid != ::getuid() || cred.value().gid != ::getgid() ||
        cred.value().pid != ::getpid()) {
      return duct::Status::protocol_error("unexpected peer credential");
    }
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("uds://" + path, dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }
  // Client side also exposes the server's credential (same process here).
  auto cred = c.value()->peer_credential();
  EXPECT_TRUE(cred.ok());
  if (cred.ok()) {
    EXPECT_EQ(cred.value().uid, ::getuid());
    EXPECT_EQ(cred.value().gid, ::getgid());
  }

  auto st = c.value()->send(duct::Message::from_string("cred-ping"), {});
  EXPECT_TRUE(st.ok());
  if (st.ok()) {
    auto echoed = c.value()->recv({});
    EXPECT_TRUE(echoed.ok());
    if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "cred-ping");
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
#endif
}


void test_tcp_recv_timeout_honored() {
#if defined(_WIN32)
  return;
#else
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto fut = accepted.get_future();
  auto* listener = lis_r.value().get();
  std::thread t([&] { accepted.set_value(listener->accept()); });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto client = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(client.ok());
  if (!client.ok()) {
    t.join();
    return;
  }

  auto server = fut.get();
  EXPECT_TRUE(server.ok());
  if (!server.ok()) {
    t.join();
    return;
  }

  duct::RecvOptions opt;
  opt.timeout = std::chrono::milliseconds(50);
  const auto start = std::chrono::steady_clock::now();
  auto r = server.value()->recv(opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kTimeout);
  }
  EXPECT_TRUE(elapsed < std::chrono::seconds(2));

  t.join();
#endif
}


// A successful non-blocking TCP connect must restore blocking mode before
// send/recv; otherwise the first socket call would treat EAGAIN as I/O error.
void test_tcp_dial_timeout_happy_path() {
  auto listener = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(listener.ok());
  if (!listener.ok()) return;
  const auto local = listener.value()->local_address();
  EXPECT_TRUE(local.ok());
  if (!local.ok()) return;

  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(500);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto client = duct::dial(local.value(), dial_opt);
  EXPECT_TRUE(client.ok());
  if (!client.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(200);
  auto sent = client.value()->send(duct::Message::from_string("ping"), send_opt);
  EXPECT_TRUE(sent.ok());
  if (!sent.ok()) return;

  auto server = listener.value()->accept();
  EXPECT_TRUE(server.ok());
  if (!server.ok()) return;
  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(200);
  auto received = server.value()->recv(recv_opt);
  EXPECT_TRUE(received.ok());
  if (received.ok()) {
    EXPECT_TRUE(received.value().as_string_view() == "ping");
  }
}


// DialTimeout must bound the connect attempt for tcp. We dial a port that is
// unlikely to be listening (but reachable, not firewalled) and require a kTimeout
// rather than an indefinite kernel connect timeout.
void test_tcp_dial_timeout_honored() {
#if defined(_WIN32)
  return;
#else
  // RFC 6598 shared address space (100.64.0.0/10) is not globally routable;
  // most environments drop it silently, so a blocking connect() would stall
  // until the kernel timeout. The bounded dial must return promptly with a
  // failure rather than hang.
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;

  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("tcp://100.64.0.1:65000", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(!c.ok());
  // Allow generous slack for scheduler jitter but far below the default 60-130s.
  EXPECT_TRUE(elapsed < std::chrono::seconds(3));
#endif
}


void test_tcp_connect_refused_is_io_error() {
#if defined(_WIN32)
  return;
#else
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(500);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("tcp://127.0.0.1:1", dial_opt);
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kIoError);
  }
#endif
}


void test_uds_dial_rejects_negative_timeout() {
#if defined(_WIN32)
  return;
#else
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(-1);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("uds:///tmp/duct_negative_timeout.sock", dial_opt);
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kInvalidArgument);
  }
#endif
}


void test_uds_listen_rejects_non_socket_path() {
#if defined(_WIN32)
  return;
#else
  std::string path = "/tmp/duct_regular_file_" + std::to_string(::getpid()) + ".sock";
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  EXPECT_TRUE(fd >= 0);
  if (fd >= 0) ::close(fd);

  auto lis_r = duct::listen("uds://" + path);
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kInvalidArgument);
  }

  struct stat st {};
  EXPECT_EQ(::lstat(path.c_str(), &st), 0);
  EXPECT_TRUE(S_ISREG(st.st_mode));
  ::unlink(path.c_str());
#endif
}


void test_uds_positive_connect_timeout_is_preserved() {
#if defined(_WIN32)
  return;
#else
  std::string path = "/tmp/duct_uds_timeout_" + std::to_string(::getpid()) + ".sock";
  ::unlink(path.c_str());

  duct::ListenOptions listen_opt;
  listen_opt.backlog = 1;
  auto lis_r = duct::listen("uds://" + path, listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  // Occupy the listener's accept queue. A fresh connection is accepted into the
  // completed queue quickly, so dial a few times to guarantee the queue is full.
  duct::DialOptions occupy_opt;
  occupy_opt.timeout = std::chrono::milliseconds(200);
  occupy_opt.qos.snd_hwm_bytes = 0;
  occupy_opt.qos.rcv_hwm_bytes = 0;
  std::vector<std::shared_ptr<duct::Pipe>> occupied;
  for (int i = 0; i < 8; ++i) {
    auto occ = duct::dial("uds://" + path, occupy_opt);
    if (occ.ok()) {
      occupied.emplace_back(occ.value().release());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_TRUE(!occupied.empty());

  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("uds://" + path, dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(!c.ok());
  // A full-backlog connect surfaces an immediate EAGAIN/backlog-full error (or
  // a bounded timeout) on most platforms; the key guarantee is that the dial
  // returns promptly instead of hanging.
  EXPECT_TRUE(elapsed < std::chrono::seconds(2));

  for (auto& p : occupied) p->close();
  lis_r.value()->close();
#endif
}


void test_tcp_channel_multiplexing() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    opt.channel_id = 7;
    auto msg = p.value()->recv(opt);
    if (!msg.ok()) return msg.status();
    duct::SendOptions send_opt;
    send_opt.channel_id = 7;
    return p.value()->send(msg.value(), send_opt);
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  send_opt.channel_id = 7;
  auto st = c.value()->send(duct::Message::from_string("channel-7"), send_opt);
  EXPECT_TRUE(st.ok());

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  recv_opt.channel_id = 7;
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().as_string_view(), "channel-7");
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


void test_tcp_channel_recv_does_not_drop_other_channel() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    duct::SendOptions s7;
    s7.channel_id = 7;
    auto a = p.value()->send(duct::Message::from_string("ch-7"), s7);
    if (!a.ok()) return a.status();
    duct::SendOptions s8;
    s8.channel_id = 8;
    return p.value()->send(duct::Message::from_string("ch-8"), s8);
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::RecvOptions r7;
  r7.timeout = std::chrono::milliseconds(500);
  r7.channel_id = 7;
  auto m7 = c.value()->recv(r7);
  EXPECT_TRUE(m7.ok());
  if (m7.ok()) EXPECT_EQ(m7.value().as_string_view(), "ch-7");

  // The ch-8 frame must still be readable by its own consumer.
  duct::RecvOptions r8;
  r8.timeout = std::chrono::milliseconds(500);
  r8.channel_id = 8;
  auto m8 = c.value()->recv(r8);
  EXPECT_TRUE(m8.ok());
  if (m8.ok()) EXPECT_EQ(m8.value().as_string_view(), "ch-8");

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


// A channel-filtered recv must reject a stream of mismatched frames once the
// per-channel pending buffer exceeds its cap, instead of buffering without
// bound while the matching channel is never consumed.
void test_tcp_channel_pending_capped() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  // Each frame may carry up to the wire payload limit, so >64 MiB of ch-8
  // traffic fills the pending buffer even though ch-7 is never consumed.
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    duct::SendOptions s8;
    s8.channel_id = 8;
    s8.timeout = std::chrono::milliseconds(3000);
    const std::string payload(64 * 1024 - 16, 'p');
    for (int i = 0; i < 1100; ++i) {
      auto st = p.value()->send(duct::Message::from_string(payload), s8);
      if (!st.ok()) return st.status();
    }
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::RecvOptions r7;
  r7.timeout = std::chrono::milliseconds(15000);
  r7.channel_id = 7;
  auto m7 = c.value()->recv(r7);
  EXPECT_TRUE(!m7.ok());
  if (!m7.ok()) {
    EXPECT_EQ(m7.status().code(), duct::StatusCode::kQueueFull);
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(5));
  if (sst == std::future_status::ready) (void)server.get();
}


// A channel-filtered recv must honor the caller's timeout as a wall-clock
// deadline, even when the peer keeps sending frames for other channels.
void test_tcp_channel_recv_deadline_bounded() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    duct::SendOptions s8;
    s8.channel_id = 8;
    const std::string other(1024, 'x');
    const auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::milliseconds(1500)) {
      auto st = p.value()->send(duct::Message::from_string(other), s8);
      if (!st.ok()) return st.status();
    }
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::RecvOptions r7;
  r7.timeout = std::chrono::milliseconds(300);
  r7.channel_id = 7;
  const auto begin = std::chrono::steady_clock::now();
  auto m7 = c.value()->recv(r7);
  const auto elapsed = std::chrono::steady_clock::now() - begin;
  EXPECT_TRUE(!m7.ok());
  EXPECT_EQ(m7.status().code(), duct::StatusCode::kTimeout);
  // Must be bounded by roughly the requested timeout, not the peer's stream.
  EXPECT_TRUE(elapsed < std::chrono::milliseconds(900));

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) (void)server.get();
}


// A sub-millisecond remaining deadline must not be truncated to a zero socket
// timeout, which would turn recv() into an unbounded blocking read and hang far
// past the caller's wall-clock deadline. This exercises the <1ms boundary the
// 300ms-latency test above cannot reach.
void test_tcp_channel_recv_subms_deadline_bounded() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    // Do not send anything: the client's recv must time out on an empty socket.
    // A sub-millisecond remaining deadline truncated to a zero socket timeout
    // would block forever waiting for data that never arrives.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::RecvOptions r7;
  r7.timeout = std::chrono::milliseconds(1);
  const auto begin = std::chrono::steady_clock::now();
  // Watchdog: a zero socket timeout turns the read into an unbounded blocking
  // recv(), so without the clamp this recv() never returns.
  auto recv_fut = std::async(std::launch::async, [&]() { return c.value()->recv(r7); });
  auto ready = recv_fut.wait_for(std::chrono::seconds(5));
  EXPECT_TRUE(ready == std::future_status::ready);
  const auto elapsed = std::chrono::steady_clock::now() - begin;
  if (ready == std::future_status::ready) {
    auto m7 = recv_fut.get();
    EXPECT_TRUE(!m7.ok());
    EXPECT_EQ(m7.status().code(), duct::StatusCode::kTimeout);
    EXPECT_TRUE(elapsed < std::chrono::seconds(5));
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(3));
  if (sst == std::future_status::ready) (void)server.get();
}


// Concurrent recv() calls on different channels must not interleave raw frame
// reads on the same socket.
void test_tcp_concurrent_channel_recv() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  std::atomic<bool> clients_done{false};
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    for (int i = 0; i < 200; ++i) {
      duct::SendOptions s7, s8;
      s7.channel_id = 7;
      s8.channel_id = 8;
      auto a = p.value()->send(duct::Message::from_string("a" + std::to_string(i)), s7);
      if (!a.ok()) return a.status();
      auto b = p.value()->send(duct::Message::from_string("b" + std::to_string(i)), s8);
      if (!b.ok()) return b.status();
    }
    // Keep the connection open until both consumers have drained all frames so
    // the recv side never sees a premature kClosed.
    while (!clients_done.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  std::atomic<int> ok7{0}, ok8{0};
  auto r7 = std::async(std::launch::async, [&]() {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(10000);
    opt.channel_id = 7;
    for (int i = 0; i < 200; ++i) {
      auto m = c.value()->recv(opt);
      if (!m.ok()) {
        std::cerr << "r7 recv failed at " << ok7.load() << ": " << to_string(m.status().code()) << "\n";
        return;
      }
      if (m.value().as_string_view().rfind("a", 0) != 0) {
        std::cerr << "r7 got wrong payload: " << m.value().as_string_view().substr(0, 10) << "\n";
        return;
      }
      ++ok7;
    }
  });
  auto r8 = std::async(std::launch::async, [&]() {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(10000);
    opt.channel_id = 8;
    for (int i = 0; i < 200; ++i) {
      auto m = c.value()->recv(opt);
      if (!m.ok()) {
        std::cerr << "r8 recv failed at " << ok8.load() << ": " << to_string(m.status().code()) << "\n";
        return;
      }
      if (m.value().as_string_view().rfind("b", 0) != 0) {
        std::cerr << "r8 got wrong payload: " << m.value().as_string_view().substr(0, 10) << "\n";
        return;
      }
      ++ok8;
    }
  });
  r7.wait();
  r8.wait();
  clients_done.store(true);
  std::cerr << "concurrent done ok7=" << ok7.load() << " ok8=" << ok8.load() << "\n";
  EXPECT_EQ(ok7.load(), 200);
  EXPECT_EQ(ok8.load(), 200);

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) (void)server.get();
}


void test_tcp_handshake_negotiates_version() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  auto st = c.value()->send(duct::Message::from_string("after-handshake"), send_opt);
  EXPECT_TRUE(st.ok());

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().as_string_view(), "after-handshake");
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


void test_tcp_accept_rejects_silent_peer() {
  // A client that connects but never sends the handshake must not hang accept()
  // forever: the bounded handshake timeout rejects it and accept() returns.
  duct::ListenOptions lis_opt;
  lis_opt.handshake_timeout = std::chrono::milliseconds(200);
  auto lis_r = duct::listen("tcp://127.0.0.1:0", lis_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto accept_fut = std::async(std::launch::async, [&]() -> duct::Result<std::unique_ptr<duct::Pipe>> {
    return lis_r.value()->accept();
  });

  // Open a raw socket to the listener and send nothing.
  auto net = duct::socket_utils::ensure_networking();
  EXPECT_TRUE(net.ok());
  if (!net.ok()) return;
  duct::wire::SocketHandle fd =
      static_cast<duct::wire::SocketHandle>(::socket(AF_INET, SOCK_STREAM, 0));
  EXPECT_TRUE(fd != duct::wire::kInvalidSocket);
  if (fd == duct::wire::kInvalidSocket) {
    lis_r.value()->close();
    return;
  }
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  sin.sin_port = htons(static_cast<std::uint16_t>(std::stoi(addr_r.value().substr(addr_r.value().rfind(':') + 1))));
  sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
#if defined(_WIN32)
  EXPECT_TRUE(::connect(static_cast<SOCKET>(fd),
                        reinterpret_cast<sockaddr*>(&sin), sizeof(sin)) == 0);
#else
  EXPECT_TRUE(::connect(fd, reinterpret_cast<sockaddr*>(&sin), sizeof(sin)) == 0);
#endif

  auto st = accept_fut.wait_for(std::chrono::seconds(1));
  EXPECT_TRUE(st == std::future_status::ready);
  if (st == std::future_status::ready) {
    auto r = accept_fut.get();
    EXPECT_TRUE(!r.ok());
    if (!r.ok()) {
      EXPECT_EQ(r.status().code(), duct::StatusCode::kTimeout);
    }
  } else {
    EXPECT_TRUE(false);
  }
  duct::socket_utils::close_socket(fd);
  lis_r.value()->close();
}


void test_concurrent_send_recv_channels() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  // Echo server: concurrent client sends are serialized by TCP, but the server
  // loop exercises interleaved recv/send traffic.
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    for (int i = 0; i < 4; ++i) {
      duct::RecvOptions ropt;
      ropt.timeout = std::chrono::milliseconds(1000);
      auto msg = p.value()->recv(ropt);
      if (!msg.ok()) return msg.status();
      duct::SendOptions sopt;
      sopt.timeout = std::chrono::milliseconds(1000);
      auto st = p.value()->send(msg.value(), sopt);
      if (!st.ok()) return st;
    }
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = true;
  dial_opt.qos.snd_hwm_bytes = 64 * 1024;
  dial_opt.qos.rcv_hwm_bytes = 64 * 1024;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  // Four threads send and receive concurrently. The QoS wrapper serializes
  // sends and recv, but the calls themselves overlap.
  std::vector<std::thread> threads;
  std::vector<std::atomic<bool>> ok(4);
  std::mutex expected_mu;
  std::set<std::string> expected = {"payload-0", "payload-1", "payload-2", "payload-3"};
  for (int i = 0; i < 4; ++i) {
    ok[i] = true;
    threads.emplace_back([&, i] {
      duct::SendOptions sopt;
      sopt.timeout = std::chrono::milliseconds(1000);
      const std::string payload = "payload-" + std::to_string(i);
      if (!c.value()->send(duct::Message::from_string(payload), sopt).ok()) {
        ok[i] = false;
        return;
      }
      duct::RecvOptions ropt;
      ropt.timeout = std::chrono::milliseconds(1000);
      auto echoed = c.value()->recv(ropt);
      // Because recv pops any queued echo, validate that each popped payload
      // is one of the expected responses exactly once.
      std::string got = echoed.ok() ? std::string(echoed.value().as_string_view()) : std::string();
      std::lock_guard<std::mutex> lock(expected_mu);
      if (got.empty() || expected.erase(got) != 1) ok[i] = false;
    });
  }
  for (auto& t : threads) t.join();
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(ok[i].load());
  {
    std::lock_guard<std::mutex> lock(expected_mu);
    EXPECT_TRUE(expected.empty());
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}

// When poll flags an fd but SO_ERROR itself fails, wait_writable must report
// the getsockopt errno instead of a misleading "peer closed" or a generic
// poll error. A broken pipe's write end flags POLLERR, and getsockopt on a
// non-socket fd fails with ENOTSOCK — deterministic on Linux.
void test_wait_writable_getsockopt_failure_reports_errno() {
#if !defined(_WIN32)
  int fds[2];
  EXPECT_EQ(::pipe(fds), 0);
  if (fds[0] < 0) return;
  ::close(fds[0]);  // read end gone: the write end now polls POLLERR

  auto st = duct::socket_utils::wait_writable(
      duct::socket_utils::socket_t(fds[1]), std::chrono::milliseconds(100));
  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_EQ(st.status().code(), duct::StatusCode::kIoError);
    // The message must carry the real cause (ENOTSOCK), not "peer closed"
    // or a generic poll error.
    EXPECT_TRUE(st.status().message().find("getsockopt") != std::string::npos);
  }
  ::close(fds[1]);
#endif
}

// ensure_stale_uds_removed must never unlink an arbitrary file or steal a live
// socket (the shm bootstrap listener relies on this); it must remove only a
// stale socket owned by the current user, and accept an absent path.
void test_ensure_stale_uds_removed_policy() {
#if !defined(_WIN32)
  const std::string base = "/tmp/duct_uds_policy_" + std::to_string(::getpid());

  // Case 1: a regular file is not a usable socket and must be left untouched.
  {
    const std::string path = base + ".file";
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY, 0600);
    EXPECT_TRUE(fd >= 0);
    ::close(fd);
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    auto st = duct::socket_utils::ensure_stale_uds_removed(addr);
    EXPECT_TRUE(!st.ok());
    EXPECT_EQ(st.status().code(), duct::StatusCode::kInvalidArgument);
    struct stat s {};
    EXPECT_TRUE(::lstat(path.c_str(), &s) == 0);  // still exists
    ::unlink(path.c_str());
  }

  // Case 2: a live listener's socket must not be stolen.
  {
    const std::string path = base + ".live";
    int lfd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    EXPECT_TRUE(lfd >= 0);
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    EXPECT_TRUE(::bind(lfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    EXPECT_TRUE(::listen(lfd, 1) == 0);
    auto st = duct::socket_utils::ensure_stale_uds_removed(addr);
    EXPECT_TRUE(!st.ok());
    EXPECT_EQ(st.status().code(), duct::StatusCode::kInvalidArgument);
    struct stat s {};
    EXPECT_TRUE(::lstat(path.c_str(), &s) == 0);  // still bound
    ::close(lfd);
    ::unlink(path.c_str());
  }

  // Case 3: a stale socket (no live endpoint) is removed so bind can take over.
  {
    const std::string path = base + ".stale";
    int lfd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    EXPECT_TRUE(lfd >= 0);
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    EXPECT_TRUE(::bind(lfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    EXPECT_TRUE(::listen(lfd, 1) == 0);
    ::close(lfd);  // path remains, but no endpoint owns it
    auto st = duct::socket_utils::ensure_stale_uds_removed(addr);
    EXPECT_TRUE(st.ok());
    struct stat s {};
    EXPECT_TRUE(::lstat(path.c_str(), &s) != 0);  // gone
  }

  // Case 4: an absent path is fine (fresh listen).
  {
    const std::string path = base + ".absent";
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    auto st = duct::socket_utils::ensure_stale_uds_removed(addr);
    EXPECT_TRUE(st.ok());
  }
#endif
}

namespace {

// The pending-frame cap must be shared across channels: a peer can pick any
// number of channel_ids, so a per-channel ceiling multiplies by however many
// channels it sprays. 1024 full-size frames stay just under the old per-channel
// 64 MiB cap (67,064,320 <= 67,108,864), so two sprayed channels reject only
// under the shared accounting.
void run_channel_shared_cap_case(const char* url) {
  auto lis_r = duct::listen(url);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    const std::string payload(64 * 1024 - 16, 'p');
    duct::SendOptions s;
    s.timeout = std::chrono::milliseconds(30000);
    for (std::uint32_t ch : {8u, 9u}) {
      s.channel_id = ch;
      for (int i = 0; i < 1024; ++i) {
        auto st = p.value()->send(duct::Message::from_string(payload), s);
        if (!st.ok()) return st.status();
      }
    }
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    server.wait_for(std::chrono::seconds(5));
    return;
  }

  duct::RecvOptions r7;
  r7.timeout = std::chrono::milliseconds(20000);
  r7.channel_id = 7;
  auto m7 = c.value()->recv(r7);
  EXPECT_TRUE(!m7.ok());
  if (!m7.ok()) {
    EXPECT_EQ(m7.status().code(), duct::StatusCode::kQueueFull);
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(10));
  if (sst == std::future_status::ready) (void)server.get();
}

}  // namespace

void test_channel_pending_cap_is_shared_across_channels() {
  run_channel_shared_cap_case("tcp://127.0.0.1:0");
#if !defined(_WIN32)
  run_channel_shared_cap_case("uds://duct_test_shared_cap");
#endif
}

#include "test_support.h"
#include "wire_tests.h"


void test_wire_decode_rejects_bad_magic() {
  std::uint8_t hdr[duct::wire::kHeaderLen]{};
  auto decoded = duct::wire::decode_header(hdr);
  EXPECT_TRUE(!decoded.ok());
  if (!decoded.ok()) {
    EXPECT_EQ(decoded.status().code(), duct::StatusCode::kProtocolError);
  }
}


void test_wire_socketpair_frames() {
#if defined(_WIN32)
  WSADATA wsaData{};
  int wsa_rc = WSAStartup(MAKEWORD(2, 2), &wsaData);
  EXPECT_TRUE(wsa_rc == 0);
  if (wsa_rc != 0) return;

  SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  EXPECT_TRUE(listener != INVALID_SOCKET);
  if (listener == INVALID_SOCKET) {
    WSACleanup();
    return;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;

  int bind_rc = ::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  EXPECT_TRUE(bind_rc == 0);
  int listen_rc = ::listen(listener, 1);
  EXPECT_TRUE(listen_rc == 0);
  if (bind_rc != 0 || listen_rc != 0) {
    closesocket(listener);
    WSACleanup();
    return;
  }

  sockaddr_in bound{};
  int bound_len = sizeof(bound);
  int getsock_rc = ::getsockname(listener, reinterpret_cast<sockaddr*>(&bound), &bound_len);
  EXPECT_TRUE(getsock_rc == 0);
  if (getsock_rc != 0) {
    closesocket(listener);
    WSACleanup();
    return;
  }

  SOCKET client = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  EXPECT_TRUE(client != INVALID_SOCKET);
  if (client == INVALID_SOCKET) {
    closesocket(listener);
    WSACleanup();
    return;
  }

  sockaddr_in connect_addr{};
  connect_addr.sin_family = AF_INET;
  connect_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  connect_addr.sin_port = bound.sin_port;

  int conn_rc = ::connect(client, reinterpret_cast<sockaddr*>(&connect_addr), sizeof(connect_addr));
  EXPECT_TRUE(conn_rc == 0);
  if (conn_rc != 0) {
    closesocket(client);
    closesocket(listener);
    WSACleanup();
    return;
  }

  SOCKET server = ::accept(listener, nullptr, nullptr);
  EXPECT_TRUE(server != INVALID_SOCKET);
  closesocket(listener);
  if (server == INVALID_SOCKET) {
    closesocket(client);
    WSACleanup();
    return;
  }

  auto writer = std::thread([&] {
    (void)duct::wire::write_frame(static_cast<duct::wire::SocketHandle>(client),
                                 duct::Message::from_string("one"));
    (void)duct::wire::write_frame(static_cast<duct::wire::SocketHandle>(client),
                                 duct::Message::from_string("two"));

    std::string big(duct::wire::kMaxFramePayload, 'x');
    (void)duct::wire::write_frame(static_cast<duct::wire::SocketHandle>(client),
                                 duct::Message::from_string(big));
    closesocket(client);
  });

  auto r1 = duct::wire::read_frame(static_cast<duct::wire::SocketHandle>(server));
  EXPECT_TRUE(r1.ok());
  if (r1.ok()) {
    std::string s(reinterpret_cast<const char*>(r1.value().data()), r1.value().size());
    EXPECT_EQ(s, "one");
  }

  auto r2 = duct::wire::read_frame(static_cast<duct::wire::SocketHandle>(server));
  EXPECT_TRUE(r2.ok());
  if (r2.ok()) {
    std::string s(reinterpret_cast<const char*>(r2.value().data()), r2.value().size());
    EXPECT_EQ(s, "two");
  }

  auto r3 = duct::wire::read_frame(static_cast<duct::wire::SocketHandle>(server));
  EXPECT_TRUE(r3.ok());
  if (r3.ok()) {
    EXPECT_EQ(r3.value().size(), duct::wire::kMaxFramePayload);
  }

  closesocket(server);
  writer.join();
  WSACleanup();
#else
  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  auto writer = std::thread([&] {
    (void)duct::wire::write_frame(fds[0], duct::Message::from_string("one"));
    (void)duct::wire::write_frame(fds[0], duct::Message::from_string("two"));

    // Max payload frame.
    std::string big(duct::wire::kMaxFramePayload, 'x');
    (void)duct::wire::write_frame(fds[0], duct::Message::from_string(big));
    ::close(fds[0]);
  });

  auto r1 = duct::wire::read_frame(fds[1]);
  EXPECT_TRUE(r1.ok());
  if (r1.ok()) {
    std::string s(reinterpret_cast<const char*>(r1.value().data()), r1.value().size());
    EXPECT_EQ(s, "one");
  }

  auto r2 = duct::wire::read_frame(fds[1]);
  EXPECT_TRUE(r2.ok());
  if (r2.ok()) {
    std::string s(reinterpret_cast<const char*>(r2.value().data()), r2.value().size());
    EXPECT_EQ(s, "two");
  }

  auto r3 = duct::wire::read_frame(fds[1]);
  EXPECT_TRUE(r3.ok());
  if (r3.ok()) {
    EXPECT_EQ(r3.value().size(), duct::wire::kMaxFramePayload);
  }

  ::close(fds[1]);
  writer.join();
#endif
}


void test_wire_fragment_stream_size_capped() {
#if !defined(_WIN32)
  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  // A malformed peer declares an oversized first fragment; the reader must
  // reject it instead of growing the reassembly buffer without bound.
  duct::wire::FrameHeader h;
  h.magic = duct::kProtocolMagic;
  h.version = duct::kProtocolVersion;
  h.header_len = duct::wire::kHeaderLen;
  h.payload_len = 64 * 1024 * 1024 + 1;
  h.flags = static_cast<std::uint32_t>(duct::FrameFlags::kFrag);
  std::uint8_t hdr[duct::wire::kHeaderLen]{};
  duct::wire::encode_header(h, hdr);
  EXPECT_TRUE(::write(fds[0], hdr, sizeof(hdr)) == static_cast<ssize_t>(sizeof(hdr)));

  auto r = duct::wire::read_frame_with_flags(fds[1], std::chrono::milliseconds(500));
  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kProtocolError);
  }
  ::close(fds[0]);
  ::close(fds[1]);
#endif
}


void test_wire_read_timeout() {
#if defined(_WIN32)
  return;
#else
  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  const auto start = std::chrono::steady_clock::now();
  auto r = duct::wire::read_frame(fds[0], std::chrono::milliseconds(50));
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kTimeout);
  }
  EXPECT_TRUE(elapsed < std::chrono::seconds(2));

  ::close(fds[0]);
  ::close(fds[1]);
#endif
}


void test_wire_write_timeout() {
#if defined(_WIN32)
  return;
#else
  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  // Fill the socket send buffer first so the next frame cannot even write its
  // header. That keeps the timeout boundary at frame start, which must be
  // reported as kTimeout (mid-frame timeouts are kIoError, see below).
  char filler[4096] = {};
  for (;;) {
    ssize_t w = ::send(fds[0], filler, sizeof(filler), MSG_DONTWAIT);
    if (w < 0) {
      EXPECT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
      break;
    }
    EXPECT_TRUE(w > 0);
    if (w <= 0) break;
  }

  const std::string big(duct::wire::kMaxFramePayload, 'x');
  const auto start = std::chrono::steady_clock::now();
  auto st = duct::wire::write_frame(fds[0], duct::Message::from_string(big), /*flags=*/0,
                                    std::chrono::milliseconds(50));
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_EQ(st.status().code(), duct::StatusCode::kTimeout);
  }
  EXPECT_TRUE(elapsed < std::chrono::seconds(2));

  ::close(fds[0]);
  ::close(fds[1]);
#endif
}


void test_wire_partial_frame_timeout() {
#if defined(_WIN32)
  return;
#else
  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  // Send half a header, then stall: the stream is mid-frame, so the timeout must
  // surface as kIoError rather than kTimeout (which callers may retry).
  duct::wire::FrameHeader h;
  h.magic = duct::kProtocolMagic;
  h.version = duct::kProtocolVersion;
  h.header_len = static_cast<std::uint16_t>(duct::wire::kHeaderLen);
  h.payload_len = 4;
  std::uint8_t hdr[duct::wire::kHeaderLen];
  duct::wire::encode_header(h, hdr);
  EXPECT_EQ(::send(fds[0], hdr, sizeof(hdr) / 2, 0),
            static_cast<ssize_t>(sizeof(hdr) / 2));

  auto r = duct::wire::read_frame(fds[1], std::chrono::milliseconds(50));
  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kIoError);
  }

  ::close(fds[0]);
  ::close(fds[1]);
#endif
}


void test_wire_clears_send_timeout_for_blocking_write() {
#if defined(_WIN32)
  return;
#else
  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  // Leave a timeout behind as a prior bounded send would. The next unbounded
  // send must restore blocking behavior; we verify this directly on the fd.
  struct timeval timeout {};
  timeout.tv_sec = 0;
  timeout.tv_usec = 50 * 1000;
  EXPECT_EQ(::setsockopt(fds[0], SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)), 0);

  auto st = duct::wire::write_frame(
      fds[0], duct::Message::from_string("x"), /*flags=*/0,
      std::chrono::milliseconds(0));
  EXPECT_TRUE(st.ok());

  struct timeval actual {};
  socklen_t length = sizeof(actual);
  EXPECT_EQ(::getsockopt(fds[0], SOL_SOCKET, SO_SNDTIMEO, &actual, &length), 0);
  EXPECT_EQ(actual.tv_sec, 0);
  EXPECT_EQ(actual.tv_usec, 0);

  ::close(fds[0]);
  ::close(fds[1]);
#endif
}


void test_wire_write_no_sigpipe_on_macos() {
#if !defined(__APPLE__)
  return;
#elif defined(_WIN32)
  return;
#else
  static std::atomic<int> sigpipe_count{0};
  auto handler = +[](int) { sigpipe_count.fetch_add(1, std::memory_order_relaxed); };

  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  ::close(fds[1]);

  // Use sigaction for reliable SIGPIPE capture on macOS; std::signal may not
  // install the handler deterministically on arm64 runners.
  struct sigaction sa {};
  sa.sa_handler = handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  struct sigaction prev {};
  int sigrc = ::sigaction(SIGPIPE, &sa, &prev);
  EXPECT_TRUE(sigrc == 0);
  sigpipe_count.store(0, std::memory_order_relaxed);

  auto st = duct::wire::write_frame(fds[0], duct::Message::from_string("x"));
  EXPECT_TRUE(!st.ok());

  const int seen = sigpipe_count.load(std::memory_order_relaxed);
  if (seen != 0) {
    std::cerr << "[sigpipe] handler fired " << seen
              << " time(s): SO_NOSIGPIPE did not suppress SIGPIPE on this platform\n";
  }
  EXPECT_EQ(seen, 0);

  if (sigrc == 0) {
    (void)::sigaction(SIGPIPE, &prev, nullptr);
  } else {
    (void)std::signal(SIGPIPE, SIG_DFL);
  }
  ::close(fds[0]);
#endif
}


void test_wire_fragmentation_and_reassembly() {
#if defined(_WIN32)
  return;
#else
  int fds[2]{-1, -1};
  int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  EXPECT_TRUE(rc == 0);
  if (rc != 0) return;

  const std::string payload(duct::wire::kMaxFramePayload * 2 + 137, 'z');
  auto st = duct::wire::write_frame(fds[0], duct::Message::from_string(payload));
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    ::close(fds[0]);
    ::close(fds[1]);
    return;
  }

  auto r = duct::wire::read_frame(fds[1]);
  EXPECT_TRUE(r.ok());
  if (r.ok()) {
    EXPECT_EQ(r.value().size(), payload.size());
    EXPECT_EQ(r.value().as_string_view(), payload);
  }

  ::close(fds[0]);
  ::close(fds[1]);
#endif
}

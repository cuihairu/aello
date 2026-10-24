#include "duct/duct.h"
#include "duct/wire.h"

#include <atomic>
#include <csignal>
#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <thread>

#if !defined(_WIN32)
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace {

int g_failures = 0;

void fail(const char* file, int line, const std::string& msg) {
  std::cerr << file << ":" << line << ": " << msg << "\n";
  ++g_failures;
}

#define EXPECT_TRUE(x)                            \
  do {                                            \
    if (!(x)) fail(__FILE__, __LINE__, #x);       \
  } while (0)

#define EXPECT_EQ(a, b)                                                   \
  do {                                                                    \
    auto _a = (a);                                                        \
    auto _b = (b);                                                        \
    if (!((_a) == (_b))) {                                                \
      fail(__FILE__, __LINE__, std::string(#a) + " != " + std::string(#b)); \
    }                                                                     \
  } while (0)

static void test_address_parse() {
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

static void test_shm_echo_one() {
  auto lis_r = duct::listen("shm://duct_testbus");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  // Give the listener a moment to bind the bootstrap socket path.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Disable QoS wrapper for transport-level tests.
  // 关闭 QoS 包装层，专注验证底层 shm 传输语义。
  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testbus", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  auto st = c.value()->send(duct::Message::from_string("hello"), {});
  EXPECT_TRUE(st.ok());
  if (!st.ok()) return;

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (!echoed.ok()) return;

  std::string s(reinterpret_cast<const char*>(echoed.value().data()), echoed.value().size());
  EXPECT_EQ(s, "hello");

  auto sst = server.wait_for(std::chrono::seconds(1));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  }

  lis_r.value()->close();
}

static void test_pipe_echo_one() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_echo");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_echo");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("pipe://duct_testpipe_echo", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  auto st = c.value()->send(duct::Message::from_string("hello"), {});
  EXPECT_TRUE(st.ok());
  if (!st.ok()) return;

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (!echoed.ok()) return;

  std::string s(reinterpret_cast<const char*>(echoed.value().data()), echoed.value().size());
  EXPECT_EQ(s, "hello");

  auto sst = server.wait_for(std::chrono::seconds(1));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  }

  lis_r.value()->close();
#endif
}

static void test_shm_backpressure_timeout() {
  auto lis_r = duct::listen("shm://duct_testbp");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto fut = accepted.get_future();
  std::thread t([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testbp", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    t.join();
    return;
  }

  auto sr = fut.get();
  EXPECT_TRUE(sr.ok());
  if (!sr.ok()) {
    t.join();
    return;
  }

  // Do not recv on server side: fill client's TX ring until it blocks.
  (void)sr.value();

  duct::SendOptions opt;
  opt.timeout = std::chrono::milliseconds(50);

  bool saw_timeout = false;
  for (int i = 0; i < 256; ++i) {
    auto st = c.value()->send(duct::Message::from_string("x"), opt);
    if (!st.ok()) {
      EXPECT_EQ(st.status().code(), duct::StatusCode::kTimeout);
      saw_timeout = true;
      break;
    }
  }
  EXPECT_TRUE(saw_timeout);

  lis_r.value()->close();
  t.join();
}

static void test_wire_write_no_sigpipe_on_macos() {
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

static void test_shm_burst_without_receiver() {
  auto lis_r = duct::listen("shm://duct_testburst");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto fut = accepted.get_future();
  std::thread t([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testburst", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    t.join();
    return;
  }

  auto sr = fut.get();
  EXPECT_TRUE(sr.ok());
  if (!sr.ok()) {
    t.join();
    return;
  }

  // Do not recv on server side: client should be able to burst-send several messages
  // without blocking on a fresh ring.
  (void)sr.value();

  duct::SendOptions opt;
  opt.timeout = std::chrono::milliseconds(100);

  for (int i = 0; i < 8; ++i) {
    auto st = c.value()->send(duct::Message::from_string("x"), opt);
    EXPECT_TRUE(st.ok());
    if (!st.ok()) break;
  }

  lis_r.value()->close();
  t.join();
}

static void test_wire_decode_rejects_bad_magic() {
  std::uint8_t hdr[duct::wire::kHeaderLen]{};
  auto decoded = duct::wire::decode_header(hdr);
  EXPECT_TRUE(!decoded.ok());
  if (!decoded.ok()) {
    EXPECT_EQ(decoded.status().code(), duct::StatusCode::kProtocolError);
  }
}

static void test_wire_socketpair_frames() {
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

}  // namespace

int main() {
  test_address_parse();
  test_shm_echo_one();
  test_pipe_echo_one();
  test_shm_backpressure_timeout();
  test_shm_burst_without_receiver();
  test_wire_decode_rejects_bad_magic();
  test_wire_socketpair_frames();
  test_wire_write_no_sigpipe_on_macos();

  if (g_failures != 0) {
    std::cerr << "FAIL (" << g_failures << ")\n";
    return 1;
  }
  std::cerr << "OK\n";
  return 0;
}

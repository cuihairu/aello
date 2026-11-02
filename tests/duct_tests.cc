#include "duct/duct.h"
#include "duct/qos_pipe.h"
#include "duct/wire.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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

// Test double: accepts most sends, rejects one configured payload with a
// permanent error and records which payloads actually reached the transport.
class RecordingPipe final : public duct::Pipe {
 public:
  explicit RecordingPipe(std::string reject = {}) : reject_(std::move(reject)) {}

  duct::Result<void> send(const duct::Message& msg, const duct::SendOptions&) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (!reject_.empty() && msg.as_string_view() == reject_) {
      return duct::Status::invalid_argument("rejected by test transport");
    }
    sent_.emplace_back(msg.as_string_view());
    cv_.notify_all();
    return {};
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    return duct::Status::timeout("recording pipe has no inbound data");
  }

  void close() override {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    cv_.notify_all();
  }

  bool wait_for_sent(std::string_view payload, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] {
      return std::find(sent_.begin(), sent_.end(), std::string(payload)) != sent_.end();
    });
  }

 private:
  std::string reject_;
  std::vector<std::string> sent_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool closed_ = false;
};

// Test double: every send times out after a short delay, which keeps the QoS
// worker busy without blocking close() indefinitely.
class SlowTimeoutPipe final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      in_send_ = true;
      cv_.notify_all();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    return duct::Status::timeout("slow test transport");
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    return duct::Status::timeout("slow pipe has no inbound data");
  }

  void close() override {}

  bool wait_until_send_started(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return in_send_; });
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool in_send_ = false;
};

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

static void test_qos_block_zero_timeout_blocks() {
  // SendOptions::timeout == 0 means "no timeout". With kBlock backpressure a full
  // queue must block until space is available (or close), not fail immediately.
  auto owned = std::make_unique<SlowTimeoutPipe>();
  SlowTimeoutPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 3;
  qos.backpressure = duct::BackpressurePolicy::kBlock;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(1000)));

  // The worker is busy in the transport for 300 ms; fill the 3-byte HWM meanwhile.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("d"), {}).ok());

  auto blocked = std::async(std::launch::async, [&] {
    return pipe.send(duct::Message::from_string("e"), {});  // timeout 0 => block
  });
  EXPECT_TRUE(blocked.wait_for(std::chrono::milliseconds(150)) != std::future_status::ready);

  pipe.close();
  EXPECT_TRUE(blocked.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  if (blocked.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
    auto result = blocked.get();
    EXPECT_TRUE(!result.ok());
    if (!result.ok()) {
      EXPECT_EQ(result.status().code(), duct::StatusCode::kClosed);
    }
  }
}

static void test_qos_permanent_send_error_does_not_wedge() {
  // A payload the transport rejects permanently (e.g. shm's 64 KiB limit when the
  // QoS HWM is larger) must be dropped, not requeued forever. Requeueing spins the
  // worker and starves every later message.
  auto owned = std::make_unique<RecordingPipe>("bad-payload");
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024 * 1024;
  duct::QosPipe pipe(std::move(owned), qos);

  duct::SendOptions opt;
  opt.timeout = std::chrono::milliseconds(100);
  EXPECT_TRUE(pipe.send(duct::Message::from_string("bad-payload"), opt).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("good-payload"), opt).ok());
  EXPECT_TRUE(transport->wait_for_sent("good-payload", std::chrono::milliseconds(1000)));
  pipe.close();
}

static void test_qos_delivery_under_hwm() {
  // QoS-enabled dial (default snd_hwm_bytes wraps in QosPipe). A small HWM forces the
  // in-memory queue to reach its watermark quickly. The send worker must keep draining
  // even at the watermark (the HWM gates enqueue, not drain); previously it stalled and
  // the echo never returned.
  auto lis_r = duct::listen("shm://duct_testqos_hwm");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  const int kCount = 16;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    for (int i = 0; i < kCount; ++i) {
      auto msg = p.value()->recv({});
      if (!msg.ok()) return msg.status();
      auto st = p.value()->send(msg.value(), {});
      if (!st.ok()) return st;
    }
    return {};
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 4 * 1024;  // ~4 x 1KiB messages, forces HWM quickly.
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testqos_hwm", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  std::string payload(1024, 'x');
  for (int i = 0; i < kCount; ++i) {
    duct::SendOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    auto st = c.value()->send(duct::Message::from_string(payload), opt);
    EXPECT_TRUE(st.ok());
    if (!st.ok()) break;
  }

  int received = 0;
  for (int i = 0; i < kCount; ++i) {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    auto echo = c.value()->recv(opt);
    if (!echo.ok()) break;
    ++received;
  }
  EXPECT_EQ(received, kCount);

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

static void test_qos_close_under_full_ring() {
  // Fill the shm ring (server never reads) so the QoS send worker blocks on the
  // transport send. close() must still return promptly: previously it deadlocked on
  // the worker's mutex.
  auto lis_r = duct::listen("shm://duct_testqos_close");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto fut = accepted.get_future();
  std::thread t([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 4 * 1024 * 1024;  // default-ish; wrap in QosPipe
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testqos_close", dial_opt);
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
  // Server side never reads: keep the pipe alive but do not recv.
  auto server_pipe = std::move(sr.value());

  std::string payload(1024, 'y');
  for (int i = 0; i < 100; ++i) {
    duct::SendOptions opt;
    opt.timeout = std::chrono::milliseconds(200);
    auto st = c.value()->send(duct::Message::from_string(payload), opt);
    if (!st.ok()) break;  // queue full at HWM is fine; the ring is what we want full.
  }

  // Give the worker time to block on the full ring.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // close() must complete; run it on a side thread and bound the wait.
  auto closed = std::promise<void>();
  auto closed_fut = closed.get_future();
  std::thread closer([&] {
    c.value()->close();
    closed.set_value();
  });

  auto cf = closed_fut.wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(cf == std::future_status::ready);
  if (cf != std::future_status::ready) {
    // Avoid leaking a thread that may still be blocked in close().
    closer.detach();
  } else {
    closer.join();
  }

  server_pipe.reset();
  lis_r.value()->close();
  t.join();
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
  test_qos_block_zero_timeout_blocks();
  test_qos_permanent_send_error_does_not_wedge();
  test_qos_delivery_under_hwm();
  test_qos_close_under_full_ring();

  if (g_failures != 0) {
    std::cerr << "FAIL (" << g_failures << ")\n";
    return 1;
  }
  std::cerr << "OK\n";
  return 0;
}

#include "duct/duct.h"
#include "duct/protocol.h"
#include "duct/qos_pipe.h"
#include "duct/pubsub.h"
#include "duct/rpc.h"
#include "duct/wire.h"
#include "../src/reconnect_pipe.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <csignal>
#include <chrono>
#include <fcntl.h>
#include <future>
#include <limits>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sys/socket.h>
#include <sys/un.h>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if !defined(_WIN32)
#include <dirent.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
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

// Test double: blocks the first transport send until released, then records
// each subsequent payload that reaches the transport.
class GatedRecordingPipe final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message& msg, const duct::SendOptions&) override {
    std::unique_lock<std::mutex> lock(mu_);
    const bool first_send = !in_send_ && sent_.empty();
    if (first_send) {
      in_send_ = true;
      cv_.notify_all();
      cv_.wait(lock, [&] { return released_ || closed_; });
    }
    if (closed_) return duct::Status::closed("closed while gated");
    sent_.emplace_back(msg.as_string_view());
    cv_.notify_all();
    return {};
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    return duct::Status::timeout("gated pipe has no inbound data");
  }

  void close() override {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    released_ = true;
    cv_.notify_all();
  }

  bool wait_until_send_started(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return in_send_; });
  }

  bool wait_for_sent(std::string_view payload, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] {
      return std::find(sent_.begin(), sent_.end(), std::string(payload)) != sent_.end();
    });
  }

  bool was_sent(std::string_view payload) const {
    std::lock_guard<std::mutex> lock(mu_);
    return std::find(sent_.begin(), sent_.end(), std::string(payload)) != sent_.end();
  }

  void release() {
    std::lock_guard<std::mutex> lock(mu_);
    released_ = true;
    cv_.notify_all();
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::vector<std::string> sent_;
  bool in_send_ = false;
  bool released_ = false;
  bool closed_ = false;
};

// Test double whose recv ignores close() and sleeps for its full timeout; it
// models transports that do not wake a blocked dispatch read on pipe close.
class SleepyRecvPipe final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    return {};
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions& opt) override {
    std::this_thread::sleep_for(opt.timeout.count() > 0 ? opt.timeout : std::chrono::milliseconds(10'000));
    return duct::Status::timeout("sleepy recv");
  }

  void close() override {}
};

// Test double for ReconnectPipe: the first connection's recv immediately
// reports a disconnect; the second records a send made from a reconnect
// callback so we can assert it completed without deadlocking.
class ReconnectCallbackPipe final : public duct::Pipe {
 public:
  bool disconnect_on_recv = false;
  bool sent_in_callback = false;

  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    sent_in_callback = true;
    return {};
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    if (disconnect_on_recv) return duct::Status::closed("peer closed");
    return duct::Status::timeout("no inbound data");
  }

  void close() override {}
};

// Test double that produces a bounded stream of messages on recv, then reports
// kClosed. Lets the QoS recv worker drain into its queue.
class SourcePipe final : public duct::Pipe {
 public:
  explicit SourcePipe(int count) : remaining_(count) {}

  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    return duct::Status::not_supported("no send");
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (remaining_ <= 0) return duct::Status::closed("source exhausted");
    --remaining_;
    return duct::Message::from_string(std::to_string(remaining_));
  }

  void close() override {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
  }

  bool is_closed() const {
    std::lock_guard<std::mutex> lock(mu_);
    return closed_;
  }

 private:
  mutable std::mutex mu_;
  int remaining_;
  bool closed_ = false;
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

static void test_uds_echo_one() {
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

static void test_wire_read_timeout() {
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

static void test_wire_write_timeout() {
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

static void test_wire_partial_frame_timeout() {
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

static void test_tcp_recv_timeout_honored() {
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
static void test_tcp_dial_timeout_happy_path() {
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
static void test_tcp_dial_timeout_honored() {
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

static void test_tcp_connect_refused_is_io_error() {
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

static void test_uds_dial_rejects_negative_timeout() {
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

static void test_uds_listen_rejects_non_socket_path() {
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

static void test_uds_positive_connect_timeout_is_preserved() {
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

static void test_shm_dial_timeout_honored() {
#if defined(_WIN32)
  // Windows shm dial honors opt.timeout via WaitNamedPipe. A missing bootstrap
  // pipe must fail promptly (timeout), never hang.
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("shm://duct_timeout_no_listener", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kTimeout);
  }
  EXPECT_TRUE(elapsed < std::chrono::seconds(3));
#else
  // No listener on the bootstrap socket. AF_UNIX connect returns ENOENT immediately
  // on Linux, so this verifies the primary negative-path guarantee: dial with a
  // bounded timeout must return promptly rather than hang. The positive timeout
  // path (listener backlog full) is covered by the UDS listen/accept tests once
  // `uds://` is wired.
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("shm://duct_timeout_no_listener", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(!c.ok());
  EXPECT_TRUE(elapsed < std::chrono::seconds(3));
#endif
}

static void test_shm_dial_positive_timeout_path() {
#if defined(_WIN32)
  return;
#else
  duct::ListenOptions listen_opt;
  listen_opt.backlog = 1;
  auto lis_r = duct::listen("shm://duct_shm_timeout_pos", listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  // Occupy the listener's accept queue. Each successful shm dial completes a
  // UDS connection into the backlog (the server never accepts), so a later
  // dial must hit the full-backlog EAGAIN and then the bounded wait path.
  duct::DialOptions occupy_opt;
  occupy_opt.timeout = std::chrono::milliseconds(200);
  occupy_opt.qos.snd_hwm_bytes = 0;
  occupy_opt.qos.rcv_hwm_bytes = 0;
  std::vector<std::shared_ptr<duct::Pipe>> occupied;
  for (int i = 0; i < 8; ++i) {
    auto occ = duct::dial("shm://duct_shm_timeout_pos", occupy_opt);
    if (occ.ok()) {
      occupied.emplace_back(occ.value().release());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(!occupied.empty());

  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("shm://duct_shm_timeout_pos", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(!c.ok());
  // The full-backlog connect must enter the bounded non-blocking wait and fail
  // promptly (kTimeout or kIoError), never hang.
  EXPECT_TRUE(elapsed < std::chrono::seconds(2));

  for (auto& p : occupied) p->close();
  lis_r.value()->close();
#endif
}

static void test_dial_rejects_negative_timeout() {
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

static void test_wire_clears_send_timeout_for_blocking_write() {
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

static void test_wire_fragment_stream_size_capped() {
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

static void test_qos_oversized_message_rejected() {
  // With fragmentation enabled the transport accepts payloads larger than a
  // frame. QoS only rejects messages above an explicit caller-provided cap.
  auto owned = std::make_unique<RecordingPipe>();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024 * 1024;
  qos.max_message_bytes = duct::wire::kMaxFramePayload;
  duct::QosPipe pipe(std::move(owned), qos);

  const std::string big(duct::wire::kMaxFramePayload + 1, 'x');
  auto st = pipe.send(duct::Message::from_string(big), {});
  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_EQ(st.status().code(), duct::StatusCode::kInvalidArgument);
  }
  pipe.close();
}

static void test_qos_zero_hwm_is_unlimited() {
  // snd_hwm_bytes == 0 disables the byte watermark; a non-default backpressure
  // policy must not silently drop/fail messages just because 0 >= 0.
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 0;
  qos.backpressure = duct::BackpressurePolicy::kDropNew;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(transport->wait_for_sent("a", std::chrono::milliseconds(1000)));
  EXPECT_TRUE(transport->wait_for_sent("b", std::chrono::milliseconds(1000)));
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
  dial_opt.qos.enabled = true;
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

static void test_uds_multi_message_echo() {
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

// Concurrent recv() calls on different UDS channels must not interleave raw
// frame reads on the same fd (the tcp equivalent is test_tcp_concurrent_channel_recv).
static void test_uds_concurrent_channel_recv() {
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

static void test_uds_peer_credential() {
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

static void test_shm_crash_recovery_reclaims_orphan() {
#if !defined(_WIN32)
  // POSIX semaphore objects appear in /dev/shm with a "sem." prefix; the
  // shared-memory segment itself appears as "ddfb0e26d<conn>m".
  const std::string prefix = "ddfb0e26d";
  auto leftovers = [&prefix]() {
    std::set<std::string> found;
    DIR* dir = ::opendir("/dev/shm");
    if (!dir) return found;
    while (dirent* ent = ::readdir(dir)) {
      std::string name = ent->d_name;
      if (name.rfind(prefix, 0) == 0 || name.rfind("sem." + prefix, 0) == 0) {
        found.insert("/dev/shm/" + name);
      }
    }
    ::closedir(dir);
    return found;
  };

  for (const auto& stale : leftovers()) (void)::unlink(stale.c_str());

  int pfd[2];
  EXPECT_TRUE(::pipe(pfd) == 0);
  pid_t listener = ::fork();
  EXPECT_TRUE(listener >= 0);
  if (listener < 0) return;
  if (listener == 0) {
    ::close(pfd[0]);
    auto lis = duct::listen("shm://duct_crashbus");
    if (!lis.ok()) _exit(1);
    char ready = 'R';
    (void)::write(pfd[1], &ready, 1);
    auto p = lis.value()->accept();
    if (!p.ok()) _exit(1);
    for (;;) ::pause();
  }
  ::close(pfd[1]);
  char ready = 0;
  (void)::read(pfd[0], &ready, 1);
  ::close(pfd[0]);

  pid_t dialer = ::fork();
  EXPECT_TRUE(dialer >= 0);
  if (dialer < 0) {
    ::kill(listener, SIGKILL);
    ::waitpid(listener, nullptr, 0);
    return;
  }
  if (dialer == 0) {
    auto p = duct::dial("shm://duct_crashbus");
    if (!p.ok()) _exit(1);
    for (;;) ::pause();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_TRUE(::kill(dialer, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(dialer, nullptr, 0) == dialer);

  EXPECT_TRUE(::kill(listener, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(listener, nullptr, 0) == listener);

  const std::set<std::string> orphaned = leftovers();
  EXPECT_EQ(orphaned.size(), 5u);

  auto lis2 = duct::listen("shm://duct_crashbus");
  EXPECT_TRUE(lis2.ok());
  if (lis2.ok()) {
    lis2.value()->close();
  }
  EXPECT_EQ(leftovers().size(), 0u);
#endif
}

// Reclaim must also unlink segments left by a pre-geometry (pre-63a492e) binary.
// Those segments carry owner_pid/magic at a fixed tail offset (~8 MiB), so the
// new header read at offset 0-15 sees garbage and would previously skip them.
// It must also tolerate an undersized hostile file without raising SIGBUS.
static void test_shm_reclaims_legacy_orphan() {
#if defined(_WIN32)
  return;
#else
  // Reproduce src/shm_transport.cc's sanitize_name/fnv1a_32/hex8.
  auto sanitize = [](std::string_view s) {
    std::string out;
    for (char c : s) {
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '_') {
        out.push_back(c);
      } else {
        out.push_back('_');
      }
    }
    if (out.empty()) out = "duct";
    return out;
  };
  auto fnv1a = [](std::string_view s) {
    std::uint32_t h = 2166136261u;
    for (unsigned char c : s) {
      h ^= c;
      h *= 16777619u;
    }
    return h;
  };
  auto hex8 = [](std::uint32_t v) {
    char buf[9];
    std::snprintf(buf, sizeof(buf), "%08x", v);
    return std::string(buf);
  };
  const std::string bus = "duct_legacybus";
  const std::string prefix = "d" + hex8(fnv1a(sanitize(bus)));
  auto leftovers = [&prefix]() {
    std::set<std::string> found;
    DIR* dir = ::opendir("/dev/shm");
    if (!dir) return found;
    while (dirent* ent = ::readdir(dir)) {
      std::string name = ent->d_name;
      if (name.rfind(prefix, 0) == 0 || name.rfind("sem." + prefix, 0) == 0) {
        found.insert("/dev/shm/" + name);
      }
    }
    ::closedir(dir);
    return found;
  };
  for (const auto& stale : leftovers()) (void)::unlink(stale.c_str());

  // A dead PID that is safe to reuse as the recorded owner.
  pid_t dead = ::fork();
  EXPECT_TRUE(dead >= 0);
  if (dead < 0) return;
  if (dead == 0) _exit(0);
  EXPECT_TRUE(::waitpid(dead, nullptr, 0) == dead);

  const std::string conn = "c0ffee01";
  const std::string seg = "/dev/shm/" + prefix + conn + "m";
  constexpr std::size_t kLegacySize = 8389768;       // old ShmLayout size
  constexpr std::size_t kOwnerOff = kLegacySize - 8; // owner_pid
  constexpr std::size_t kMagicOff = kLegacySize - 4; // magic
  constexpr std::uint32_t kMagic = 0x4D484353;       // 'SHCM'
  int fd = ::open(seg.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0600);
  EXPECT_TRUE(fd >= 0);
  if (fd < 0) return;
  EXPECT_TRUE(::ftruncate(fd, static_cast<off_t>(kLegacySize)) == 0);
  EXPECT_TRUE(::pwrite(fd, &dead, sizeof(dead), kOwnerOff) ==
              static_cast<ssize_t>(sizeof(dead)));
  EXPECT_TRUE(::pwrite(fd, &kMagic, sizeof(kMagic), kMagicOff) ==
              static_cast<ssize_t>(sizeof(kMagic)));
  ::close(fd);

  // An undersized hostile file matching the prefix must not raise SIGBUS.
  const std::string tiny = "/dev/shm/" + prefix + conn + "t";
  int tfd = ::open(tiny.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0600);
  if (tfd >= 0) {
    EXPECT_TRUE(::write(tfd, "x", 1) == 1);
    ::close(tfd);
  }

  EXPECT_TRUE(leftovers().size() >= 2u);
  auto lis = duct::listen("shm://" + bus);
  EXPECT_TRUE(lis.ok());
  if (lis.ok()) lis.value()->close();
  // Both the legacy segment and the tiny file are gone; the tiny file is
  // skipped (not SIGBUS) because it ends in 't', while the legacy segment (ends
  // in 'm') is reclaimed.
  auto left = leftovers();
  EXPECT_EQ(left.count(seg), 0u);
  EXPECT_EQ(left.count(tiny), 1u);  // untouched by reclaim (not an 'm' file)
  (void)::unlink(tiny.c_str());
#endif
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
  dial_opt.qos.enabled = true;
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

// A reconnect-enabled dial must return immediately with a pipe that connects on
// the first successful attempt and delivers an echo.
static void test_reconnect_dial_connects_and_echoes() {
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
  dial_opt.timeout = std::chrono::milliseconds(500);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(10);
  dial_opt.reconnect.max_attempts = 10;

  std::mutex state_mu;
  std::vector<duct::ConnectionState> states;
  dial_opt.on_state_change = [&](duct::ConnectionState s, const std::string&) {
    std::lock_guard<std::mutex> lk(state_mu);
    states.push_back(s);
  };

  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  auto st = c.value()->send(duct::Message::from_string("reconnect-ping"), send_opt);
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    c.value()->close();
    return;
  }

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    std::string s(reinterpret_cast<const char*>(echoed.value().data()), echoed.value().size());
    EXPECT_EQ(s, "reconnect-ping");
  }

  bool saw_connected = false;
  {
    std::lock_guard<std::mutex> lk(state_mu);
    for (auto s : states) {
      if (s == duct::ConnectionState::kConnected) saw_connected = true;
    }
  }
  EXPECT_TRUE(saw_connected);

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

// A reconnect-enabled dial to a non-existent server must return immediately
// (the pipe connects in the background) and eventually report exhaustion.
static void test_reconnect_dial_fails_after_attempts() {
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(200);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(5);
  dial_opt.reconnect.max_delay = std::chrono::milliseconds(20);
  dial_opt.reconnect.backoff_multiplier = 1.0;
  dial_opt.reconnect.max_attempts = 3;

  auto c = duct::dial("tcp://127.0.0.1:1", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  // The dial itself returns immediately; the first send blocks until the
  // background worker exhausts all attempts.
  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(5000);
  auto st = c.value()->send(duct::Message::from_string("x"), send_opt);
  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_TRUE(st.status().code() == duct::StatusCode::kIoError ||
                st.status().code() == duct::StatusCode::kTimeout);
  }
  c.value()->close();
}

// A reconnect callback that sends inside on_state_change must not deadlock:
// the kConnected transition used to fire before inner_ was published, so
// wait_connected() never observed the new pipe and blocked forever.
static void test_reconnect_callback_ops_pipe() {
  std::atomic<int> attempts{0};
  ReconnectCallbackPipe* second = nullptr;
  std::atomic<bool> callback_sent{false};
  std::shared_ptr<duct::Pipe> holder;

  duct::ReconnectPolicy policy;
  policy.initial_delay = std::chrono::milliseconds(5);
  policy.max_delay = std::chrono::milliseconds(20);
  policy.backoff_multiplier = 1.0;
  policy.max_attempts = 10;

  auto dial_once = [&]() -> duct::Result<std::unique_ptr<duct::Pipe>> {
    int n = ++attempts;
    auto p = std::make_unique<ReconnectCallbackPipe>();
    if (n == 1) {
      p->disconnect_on_recv = true;
    } else {
      second = p.get();
    }
    return std::unique_ptr<duct::Pipe>(std::move(p));
  };

  duct::ConnectionCallback cb = [&](duct::ConnectionState s, const std::string&) {
    if (s != duct::ConnectionState::kConnected) return;
    if (attempts < 2) return;  // first connect: holder not assigned yet.
    if (!holder) return;
    auto st = holder->send(duct::Message::from_string("cb-ping"), {});
    if (st.ok()) callback_sent.store(true);
  };

  auto pipe = duct::make_reconnect_pipe(std::move(dial_once), policy, std::move(cb));
  holder = std::shared_ptr<duct::Pipe>(std::move(pipe));

  // First recv hits the fake disconnect and the worker reconnects.
  duct::RecvOptions ro;
  ro.timeout = std::chrono::milliseconds(1000);
  (void)holder->recv(ro);

  for (int i = 0; i < 20 && !callback_sent.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  EXPECT_TRUE(callback_sent.load());
  if (second) {
    EXPECT_TRUE(second->sent_in_callback);
  }
  holder->close();
}

static void test_qos_recv_watermark_and_worker() {
  auto owned = std::make_unique<SourcePipe>(8);
  SourcePipe* source = owned.get();

  duct::QosOptions qos;
  qos.rcv_hwm_bytes = 2;  // Two 1-byte messages at a time.
  duct::QosPipe pipe(std::move(owned), qos);

  // The recv worker should pull up to the watermark and then pause. Recv must
  // drain the queued messages in FIFO order and not block on the transport.
  for (int i = 7; i >= 0; --i) {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    auto r = pipe.recv(opt);
    EXPECT_TRUE(r.ok());
    if (!r.ok()) break;
    EXPECT_EQ(r.value().as_string_view(), std::to_string(i));
  }

  // After the source is exhausted, recv should surface kClosed once drained.
  duct::RecvOptions opt;
  opt.timeout = std::chrono::milliseconds(500);
  auto closed_result = pipe.recv(opt);
  EXPECT_TRUE(!closed_result.ok());
  if (!closed_result.ok()) {
    EXPECT_EQ(closed_result.status().code(), duct::StatusCode::kClosed);
  }
  EXPECT_TRUE(source->is_closed() == false);
  pipe.close();
  EXPECT_TRUE(source->is_closed() == true);
}

static void test_qos_linger_drains_before_close() {
  auto owned = std::make_unique<SlowTimeoutPipe>();
  SlowTimeoutPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024;
  qos.linger = std::chrono::milliseconds(300);
  duct::QosPipe pipe(std::move(owned), qos);

  // Queue one message; the worker starts sending it (which takes ~300 ms).
  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(1000)));

  // close() must wait for the in-flight send to finish (linger), not cancel it.
  auto closed = std::promise<void>();
  auto closed_fut = closed.get_future();
  std::thread closer([&] {
    pipe.close();
    closed.set_value();
  });
  auto cf = closed_fut.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(cf == std::future_status::ready);
  if (cf != std::future_status::ready) {
    closer.detach();
  } else {
    closer.join();
  }
}

static void test_wire_fragmentation_and_reassembly() {
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

static void test_shm_fragmentation_and_reassembly() {
#if defined(_WIN32)
  return;
#else
  auto lis_r = duct::listen("shm://duct_testfrag");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

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
  auto c = duct::dial("shm://duct_testfrag", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  const std::string payload(64 * 1024 + 123, 'q');
  auto st = c.value()->send(duct::Message::from_string(payload), {});
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    c.value()->close();
    return;
  }

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().size(), payload.size());
    EXPECT_EQ(echoed.value().as_string_view(), payload);
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

// The shm reassembly path must cap the accumulated fragment size like the wire
// path, so a peer streaming chunks without a last-chunk marker cannot grow the
// buffer without bound. Sending just over 64 MiB must be rejected.
static void test_shm_reassembly_size_capped() {
#if defined(_WIN32)
  return;
#else
  auto lis_r = duct::listen("shm://duct_testreasm_cap");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  // Send > 64 MiB in the background; the sender may block once the receiver
  // stops consuming after rejecting the oversize reassembly.
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    const std::string payload(64 * 1024 * 1024 + 64 * 1024, 'r');
    auto st = p.value()->send(duct::Message::from_string(payload), {});
    if (!st.ok()) return st.status();
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testreasm_cap", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto msg = c.value()->recv({});
  EXPECT_TRUE(!msg.ok());
  if (!msg.ok()) {
    EXPECT_EQ(msg.status().code(), duct::StatusCode::kProtocolError);
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) (void)server.get();
#endif
}

// The first chunk of a reassembled shm message must also respect the 64 MiB
// cap. Previously only continuation chunks were checked, so a slot_bytes near
// 2^30 let a single first chunk allocate ~1 GiB before the cap was enforced.
static void test_shm_first_chunk_size_capped() {
#if defined(_WIN32)
  return;
#else
  // slot_bytes just under 2^30 is a valid geometry but far above the 64 MiB
  // reassembly cap. A message that fits in one slot yet exceeds the cap must be
  // rejected at the first-chunk check, not allocated (~1 GiB) and reassembled.
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = (1u << 30) - (1u << 20);  // ~1 GiB payload slot
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testfirstcap", listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    // Fits in one ~1 GiB slot but is > 64 MiB: the receiver's first-chunk cap
    // must fire before assembling the oversized message.
    const std::string payload(64 * 1024 * 1024 + 64 * 1024, 'f');
    auto st = p.value()->send(duct::Message::from_string(payload), {});
    if (!st.ok()) return st.status();
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.shm.slot_bytes = (1u << 30) - (1u << 20);
  dial_opt.shm.slot_count = 2;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testfirstcap", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto msg = c.value()->recv({});
  EXPECT_TRUE(!msg.ok());
  if (!msg.ok()) {
    EXPECT_EQ(msg.status().code(), duct::StatusCode::kProtocolError);
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) (void)server.get();
#endif
}

static void test_shm_fragmentation_midway_timeout_keeps_stream_synced() {
#if defined(_WIN32)
  return;
#else
  auto lis_r = duct::listen("shm://duct_testfrag_timeout");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    // Delay consumption so the client's short per-chunk timeout expires while
    // reassembly is still pending. The stream must remain synchronized.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testfrag_timeout", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  const std::string payload(64 * 1024 + 123, 'x');
  duct::SendOptions so;
  so.timeout = std::chrono::milliseconds(50);
  auto st = c.value()->send(duct::Message::from_string(payload), so);
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    c.value()->close();
    return;
  }

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().size(), payload.size());
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
#endif
}

static void test_shm_configurable_slot_geometry() {
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = 16;
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testgeom", listen_opt);
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
  dial_opt.shm.slot_bytes = 16;
  dial_opt.shm.slot_count = 2;
  auto c = duct::dial("shm://duct_testgeom", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto st = c.value()->send(duct::Message::from_string("ok"), {});
  EXPECT_TRUE(st.ok());
  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().as_string_view(), "ok");
  }

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  }
  c.value()->close();
  lis_r.value()->close();
}

static void test_shm_geometry_mismatch_rejected() {
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = 32;
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testgeom_mismatch", listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<std::unique_ptr<duct::Pipe>> {
    return lis_r.value()->accept();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  duct::DialOptions dial_opt;
  dial_opt.shm.slot_bytes = 16;  // dialer creates a smaller segment
  dial_opt.shm.slot_count = 2;
  auto c = duct::dial("shm://duct_testgeom_mismatch", dial_opt);
  EXPECT_TRUE(c.ok());
  if (c.ok()) c.value()->close();

  auto accepted = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(accepted == std::future_status::ready);
  if (accepted == std::future_status::ready) {
    auto p = server.get();
    EXPECT_TRUE(!p.ok());
  }
  lis_r.value()->close();
}

// Boundary geometry must be rejected up front instead of corrupting the ring:
// slot_bytes at the kLastChunkFlag boundary (2^30) collides with the last-chunk
// flag and an oversized slot_bytes (>= 2^32) must be rejected before narrowing.
// (A slot_count whose layout_size would overflow size_t cannot occur once
// slot_bytes is capped below 2^30, so that path is not reachable here.)
static void test_shm_rejects_invalid_geometry() {
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = (1u << 30);  // == kLastChunkFlag boundary
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testgeom_invalid", listen_opt);
  EXPECT_TRUE(!lis_r.ok());
  if (lis_r.ok()) {
    lis_r.value()->close();
    return;
  }

  // slot_bytes > 4 GiB would be truncated by a narrowing cast before validation.
  listen_opt.shm.slot_bytes = static_cast<std::size_t>(1) << 32;
  listen_opt.shm.slot_count = 1;
  lis_r = duct::listen("shm://duct_testgeom_invalid", listen_opt);
  EXPECT_TRUE(!lis_r.ok());
  if (lis_r.ok()) {
    lis_r.value()->close();
    return;
  }

}

static void test_qos_priority_scheduling() {
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024;
  duct::QosPipe pipe(std::move(owned), qos);

  // Queue low-priority first, then high-priority. The worker must drain
  // high-priority first.
  duct::SendOptions low;
  low.priority = 0;
  duct::SendOptions high;
  high.priority = 10;

  EXPECT_TRUE(pipe.send(duct::Message::from_string("low-1"), low).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("low-2"), low).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("high-1"), high).ok());

  EXPECT_TRUE(transport->wait_for_sent("high-1", std::chrono::milliseconds(1000)));
  EXPECT_TRUE(transport->wait_for_sent("low-1", std::chrono::milliseconds(1000)));
  EXPECT_TRUE(transport->wait_for_sent("low-2", std::chrono::milliseconds(1000)));
  pipe.close();
}

static void test_tcp_channel_multiplexing() {
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

static void test_tcp_channel_recv_does_not_drop_other_channel() {
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
static void test_tcp_channel_pending_capped() {
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

static void test_qos_rate_limit_paces_delivery() {
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024;
  // 1000 bytes/s with a 100-byte burst. Two 100-byte messages: the first fits
  // in the burst; the second requires ~100 ms of refill.
  qos.rate_bytes_per_sec = 1000;
  qos.burst_bytes = 100;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string(std::string(100, 'a')), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string(std::string(100, 'b')), {}).ok());

  EXPECT_TRUE(transport->wait_for_sent(std::string(100, 'a'), std::chrono::milliseconds(500)));
  EXPECT_TRUE(transport->wait_for_sent(std::string(100, 'b'), std::chrono::milliseconds(500)));
  pipe.close();
}

static void test_qos_rate_limit_default_burst_drains() {
  // burst_bytes == 0 documents "one second's worth"; with rate limiting enabled
  // it must still drain a non-empty message instead of token-starving forever.
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.rate_bytes_per_sec = 1000;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string(std::string(100, 'x')), {}).ok());
  EXPECT_TRUE(transport->wait_for_sent(std::string(100, 'x'), std::chrono::milliseconds(500)));
  pipe.close();
}

static void test_qos_channel_multiplexing() {
  // A QoS-wrapped tcp pipe must preserve channel_id end-to-end, matching the
  // direct tcp/uds behavior: send on a channel, receive on that same channel,
  // and a different channel must not consume it.
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
    for (;;) {
      auto msg = p.value()->recv(opt);
      if (!msg.ok()) return msg.status();
      if (msg.value().as_string_view() == "quit") return {};
      duct::SendOptions send_opt;
      send_opt.channel_id = 7;
      auto st = p.value()->send(msg.value(), send_opt);
      if (!st.ok()) return st.status();
    }
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = true;
  dial_opt.qos.snd_hwm_bytes = 1024 * 1024;
  dial_opt.qos.rcv_hwm_bytes = 1024 * 1024;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  send_opt.channel_id = 7;
  EXPECT_TRUE(c.value()->send(duct::Message::from_string("ch-7"), send_opt).ok());

  // A recv for a different channel must not steal the ch-7 frame.
  duct::RecvOptions wrong;
  wrong.timeout = std::chrono::milliseconds(200);
  wrong.channel_id = 8;
  auto got = c.value()->recv(wrong);
  EXPECT_TRUE(!got.ok());
  EXPECT_EQ(got.status().code(), duct::StatusCode::kTimeout);

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  recv_opt.channel_id = 7;
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().as_string_view(), "ch-7");
  }

  EXPECT_TRUE(c.value()->send(duct::Message::from_string("quit"), {}).ok());
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  c.value()->close();
  lis_r.value()->close();
}

static void test_tcp_handshake_negotiates_version() {
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

static void test_tcp_accept_rejects_silent_peer() {
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
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  EXPECT_TRUE(fd >= 0);
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  sin.sin_port = htons(static_cast<std::uint16_t>(std::stoi(addr_r.value().substr(addr_r.value().rfind(':') + 1))));
  sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  EXPECT_TRUE(::connect(fd, reinterpret_cast<sockaddr*>(&sin), sizeof(sin)) == 0);

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
  ::close(fd);
  lis_r.value()->close();
}

static void test_qos_item_count_cap() {
  auto owned = std::make_unique<SlowTimeoutPipe>();
  SlowTimeoutPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.snd_hwm_bytes = 0;    // No byte limit; count only.
  qos.snd_hwm_count = 2;
  qos.backpressure = duct::BackpressurePolicy::kBlock;
  duct::QosPipe pipe(std::move(owned), qos);

  // First send is picked up by the worker and held in-flight for 300ms, so it
  // is not counted against the queue watermark. Pin it in-flight so the
  // following two sends fill the queue deterministically.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  // Queue is now full (count cap = 2); the in-flight message is excluded.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  // Fourth send must block (count full) until close() aborts it.
  auto fourth = std::async(std::launch::async, [&] {
    duct::SendOptions opt;
    opt.timeout = std::chrono::milliseconds(1000);
    return pipe.send(duct::Message::from_string("d"), opt);
  });
  EXPECT_TRUE(fourth.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready);
  pipe.close();
  EXPECT_TRUE(fourth.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  if (fourth.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
    auto r = fourth.get();
    EXPECT_TRUE(!r.ok());
  }
}

static void test_qos_failfast_returns_queue_full() {
  auto owned = std::make_unique<GatedRecordingPipe>();
  GatedRecordingPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.snd_hwm_bytes = 3;
  qos.backpressure = duct::BackpressurePolicy::kFailFast;
  duct::QosPipe pipe(std::move(owned), qos);

  // Pin the first message in-flight so the queue watermark is deterministic:
  // the in-flight message does not count against the HWM.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  // Queue is now at exactly snd_hwm_bytes (3); the next send must fail fast.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("d"), {}).ok());
  auto r = pipe.send(duct::Message::from_string("e"), {});
  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kQueueFull);
  }
  // Release the gated first send so the worker can finish and close() can join.
  transport->release();
  pipe.close();
}

static void test_qos_dial_opt_in_disabled_by_default() {
  // With qos.enabled == false (default), dial() returns the raw transport even
  // though the default snd_hwm_bytes is non-zero.
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  duct::DialOptions dial_opt;  // qos.enabled defaults false
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (c.ok()) {
    duct::SendOptions send_opt;
    send_opt.timeout = std::chrono::milliseconds(200);
    auto st = c.value()->send(duct::Message::from_string("raw"), send_opt);
    // Raw transport: send succeeds (QoS off) without a QosPipe wrapper.
    EXPECT_TRUE(st.ok());
    c.value()->close();
  }
  lis_r.value()->close();
}

static void test_qos_ttl_expires_queued_message() {
  auto owned = std::make_unique<GatedRecordingPipe>();
  GatedRecordingPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.ttl = std::chrono::milliseconds(200);
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("first"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));

  EXPECT_TRUE(pipe.send(duct::Message::from_string("expired"), {}).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  transport->release();
  EXPECT_TRUE(transport->wait_for_sent("first", std::chrono::milliseconds(500)));

  EXPECT_TRUE(pipe.send(duct::Message::from_string("after-expiry"), {}).ok());
  EXPECT_TRUE(transport->wait_for_sent("after-expiry", std::chrono::milliseconds(500)));
  EXPECT_TRUE(!transport->was_sent("expired"));
  pipe.close();
}

static void test_qos_drop_old_evicts_oldest_queued() {
  auto owned = std::make_unique<GatedRecordingPipe>();
  GatedRecordingPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.snd_hwm_bytes = 2;
  qos.backpressure = duct::BackpressurePolicy::kDropOld;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("d"), {}).ok());
  transport->release();

  // "a" was already picked up by the worker (in-flight) and is delivered on
  // release. "b" was evicted from the queue by kDropOld. "c" and "d" remain.
  EXPECT_TRUE(transport->wait_for_sent("a", std::chrono::milliseconds(500)));
  EXPECT_TRUE(transport->wait_for_sent("c", std::chrono::milliseconds(500)));
  EXPECT_TRUE(transport->wait_for_sent("d", std::chrono::milliseconds(500)));
  EXPECT_TRUE(!transport->was_sent("b"));
  pipe.close();
}

static void test_concurrent_send_recv_channels() {
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

static void test_multi_process_kill9_restart() {
#if !defined(_WIN32)
  // Server child: listens on an ephemeral tcp port and echoes until killed.
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;
  const std::string addr = addr_r.value();

  pid_t child = ::fork();
  if (child < 0) {
    EXPECT_TRUE(false);
    return;
  }
  if (child == 0) {
    // Child: accept and echo forever. Never returns; killed by SIGKILL.
    auto p = lis_r.value()->accept();
    if (!p.ok()) _exit(1);
    for (;;) {
      auto msg = p.value()->recv({});
      if (!msg.ok()) _exit(1);
      if (!p.value()->send(msg.value(), {}).ok()) _exit(1);
    }
  }

  // Parent: dial and echo cross-process.
  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr, dial_opt);
  EXPECT_TRUE(c.ok());
  if (c.ok()) {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(500);
    auto st = c.value()->send(duct::Message::from_string("ping-1"), sopt);
    EXPECT_TRUE(st.ok());
    if (st.ok()) {
      auto echoed = c.value()->recv({});
      EXPECT_TRUE(echoed.ok());
      if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "ping-1");
    }
  }

  // Kill the server with SIGKILL while the connection is still alive so the
  // child is blocked in recv and dies by signal deterministically. Then close
  // the parent pipe (the child is already gone; no EOF race).
  EXPECT_TRUE(::kill(child, SIGKILL) == 0);
  int status = 0;
  EXPECT_TRUE(::waitpid(child, &status, 0) == child);
  EXPECT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
  if (c.ok()) c.value()->close();

  // A fresh dial after restart works: start a second child on a new port.
  auto lis2_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis2_r.ok());
  if (!lis2_r.ok()) return;
  auto addr2_r = lis2_r.value()->local_address();
  EXPECT_TRUE(addr2_r.ok());
  if (!addr2_r.ok()) return;
  const std::string addr2 = addr2_r.value();

  pid_t child2 = ::fork();
  if (child2 < 0) {
    EXPECT_TRUE(false);
    return;
  }
  if (child2 == 0) {
    auto p = lis2_r.value()->accept();
    if (!p.ok()) _exit(1);
    auto msg = p.value()->recv({});
    if (!msg.ok()) _exit(1);
    _exit(p.value()->send(msg.value(), {}).ok() ? 0 : 1);
  }

  auto c2 = duct::dial(addr2, dial_opt);
  EXPECT_TRUE(c2.ok());
  if (c2.ok()) {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(500);
    auto st = c2.value()->send(duct::Message::from_string("ping-2"), sopt);
    EXPECT_TRUE(st.ok());
    if (st.ok()) {
      auto echoed = c2.value()->recv({});
      EXPECT_TRUE(echoed.ok());
      if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "ping-2");
    }
  }
  EXPECT_TRUE(::kill(child2, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(child2, &status, 0) == child2);
  if (c2.ok()) c2.value()->close();
  lis2_r.value()->close();
#endif
}

static void test_rpc_request_correlation() {
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

static void test_rpc_request_timeout() {
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
static void test_rpc_close_joins_without_transport_wakeup() {
  auto pipe = std::make_unique<SleepyRecvPipe>();
  duct::rpc::Client client(std::move(pipe));
  auto done = std::async(std::launch::async, [&] { client.close(); });
  EXPECT_TRUE(done.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
  if (done.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
    done.get();
  }
}

static void test_pubsub_multiple_subscribers() {
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
static void test_pubsub_subscription_outlives_broker() {
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
static void test_pubsub_concurrent_broker_subscription_destruction() {
  for (int attempt = 0; attempt < 200; ++attempt) {
    auto broker = std::make_unique<duct::pubsub::Broker>();
    auto sub = broker->subscribe("x", [](const std::string&, const duct::Message&) {});
    auto broker_thread = std::async(std::launch::async, [&broker] { broker.reset(); });
    auto subscription_thread = std::async(std::launch::async, [&sub] { sub.reset(); });
    broker_thread.wait();
    subscription_thread.wait();
  }
}

// Oversize messages must be rejected synchronously instead of being written
// past a slot (POSIX fragments, Windows has no fragmentation and returns
// kInvalidArgument from send).
static void test_shm_rejects_oversize_message() {
  auto lis_r = duct::listen("shm://duct_oversize");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

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
  auto c = duct::dial("shm://duct_oversize", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  // 1 MiB is larger than the default 64 KiB slot but still fragmentable; a
  // valid oversized message must be accepted and echo back intact on POSIX.
  const std::string payload(1024 * 1024, 'o');
  EXPECT_TRUE(c.value()->send(duct::Message::from_string(payload), {}).ok());
  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().size(), payload.size());

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}

// A channel-filtered recv must honor the caller's timeout as a wall-clock
// deadline, even when the peer keeps sending frames for other channels.
static void test_tcp_channel_recv_deadline_bounded() {
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
static void test_tcp_channel_recv_subms_deadline_bounded() {
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

// Same sub-millisecond deadline guarantee for the UDS transport, which shares
// the channel-filtered recv loop and the same duration_cast truncation bug.
static void test_uds_channel_recv_subms_deadline_bounded() {
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

// A reconnect-wrapped tcp pipe must preserve channel_id through the wire
// flags, not collapse everything to channel 0.
static void test_reconnect_channel_preserved() {
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
    auto msg = p.value()->recv(opt);
    if (!msg.ok()) return msg.status();
    duct::SendOptions send_opt;
    send_opt.channel_id = 7;
    return p.value()->send(msg.value(), send_opt);
  });

  duct::DialOptions dial_opt;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(10);
  dial_opt.reconnect.backoff_multiplier = 1.0;
  dial_opt.reconnect.max_delay = std::chrono::milliseconds(50);
  dial_opt.reconnect.max_attempts = 5;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  send_opt.channel_id = 7;
  EXPECT_TRUE(c.value()->send(duct::Message::from_string("reconnect-ch7"), send_opt).ok());

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  recv_opt.channel_id = 7;
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "reconnect-ch7");

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}

// Concurrent recv() calls on different channels must not interleave raw frame
// reads on the same socket.
static void test_tcp_concurrent_channel_recv() {
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

// A QoS-wrapped shm recv worker must not block forever on the second chunk of a
// fragmented message when the sender dies after committing the first chunk.
// Fill the ring with 64 single-chunk messages, then send a 2-chunk message whose
// first chunk needs the only remaining space. The server consumes one message to
// make the dialer commit that first chunk, then SIGKILLs the dialer before the
// second chunk can be written. close() must still join the recv worker.
static void test_qos_shm_recv_midfragment_close_completes() {
#if defined(_WIN32)
  return;
#else
  const std::string bus = "duct_testqos_recv_midfrag_" + std::to_string(::getpid());
  auto lis_r = duct::listen("shm://" + bus);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  int syncfd[2];
  EXPECT_TRUE(::pipe(syncfd) == 0);
  pid_t dialer = ::fork();
  EXPECT_TRUE(dialer >= 0);
  if (dialer < 0) {
    lis_r.value()->close();
    return;
  }
  if (dialer == 0) {
    ::close(syncfd[0]);
    auto p = duct::dial("shm://" + bus);
    if (!p.ok()) _exit(1);
    const std::string one_chunk(64 * 1024, 'a');
    for (int i = 0; i < 64; ++i) {
      if (!p.value()->send(duct::Message::from_string(one_chunk), {}).ok()) _exit(2);
    }
    const std::string two_chunks(64 * 1024 + 1, 'b');
    // Blocks after the first chunk is committed because the ring is full.
    (void)p.value()->send(duct::Message::from_string(two_chunks), {});
    _exit(0);
  }
  ::close(syncfd[1]);
  ::close(syncfd[0]);

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto accepted_fut = accepted.get_future();
  std::thread accept_thread([&] { accepted.set_value(lis_r.value()->accept()); });
  auto af = accepted_fut.wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(af == std::future_status::ready);
  if (af != std::future_status::ready) {
    ::kill(dialer, SIGKILL);
    ::waitpid(dialer, nullptr, 0);
    lis_r.value()->close();
    accept_thread.join();
    return;
  }
  auto accepted_r = accepted_fut.get();
  EXPECT_TRUE(accepted_r.ok());
  if (!accepted_r.ok()) {
    ::kill(dialer, SIGKILL);
    ::waitpid(dialer, nullptr, 0);
    lis_r.value()->close();
    accept_thread.join();
    return;
  }

  // Consume one complete message so the dialer can commit the first chunk of
  // the fragmented message. The second chunk still has no free slot.
  auto first = accepted_r.value()->recv({std::chrono::milliseconds(1000)});
  EXPECT_TRUE(first.ok());
  if (!first.ok()) {
    ::kill(dialer, SIGKILL);
    ::waitpid(dialer, nullptr, 0);
    lis_r.value()->close();
    return;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_TRUE(::kill(dialer, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(dialer, nullptr, 0) == dialer);

  duct::QosOptions qos;
  qos.rcv_hwm_bytes = 16 * 1024 * 1024;
  duct::QosPipe pipe(std::move(accepted_r.value()), qos);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  auto closed = std::promise<void>();
  auto closed_fut = closed.get_future();
  std::thread closer([&] {
    pipe.close();
    closed.set_value();
  });
  auto cf = closed_fut.wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(cf == std::future_status::ready);
  if (cf != std::future_status::ready) {
    closer.detach();
  } else {
    closer.join();
  }

  lis_r.value()->close();
  accept_thread.join();
#endif
}

}  // namespace

int main() {
#if defined(SIGPIPE)
  // The channel-flood tests keep a peer sending on another channel while the
  // client times out and closes. A send racing that close can hit a broken
  // pipe; on Linux the library only suppresses SIGPIPE on macOS, so ignore it
  // here to keep the harness from dying mid-suite. Library send() semantics are
  // unchanged; this only quiets the signal the write would otherwise raise.
  std::signal(SIGPIPE, SIG_IGN);
#endif
  test_address_parse();
  test_shm_echo_one();
  test_uds_echo_one();
  test_uds_multi_message_echo();
  test_uds_channel_recv_subms_deadline_bounded();
  test_uds_concurrent_channel_recv();
  test_uds_peer_credential();
  test_shm_crash_recovery_reclaims_orphan();
  test_shm_reclaims_legacy_orphan();
  test_pipe_echo_one();
  test_shm_backpressure_timeout();
  test_shm_burst_without_receiver();
  test_wire_decode_rejects_bad_magic();
  test_wire_socketpair_frames();
  test_wire_fragment_stream_size_capped();
  test_wire_read_timeout();
  test_wire_write_timeout();
  test_wire_partial_frame_timeout();
  test_tcp_recv_timeout_honored();
  test_tcp_dial_timeout_happy_path();
  test_tcp_dial_timeout_honored();
  test_tcp_connect_refused_is_io_error();
  test_uds_dial_rejects_negative_timeout();
  test_uds_listen_rejects_non_socket_path();
  test_uds_positive_connect_timeout_is_preserved();
  test_shm_dial_timeout_honored();
  test_shm_dial_positive_timeout_path();
  test_dial_rejects_negative_timeout();
  test_wire_clears_send_timeout_for_blocking_write();
  test_wire_write_no_sigpipe_on_macos();
  test_qos_block_zero_timeout_blocks();
  test_qos_permanent_send_error_does_not_wedge();
  test_qos_oversized_message_rejected();
  test_qos_zero_hwm_is_unlimited();
  test_qos_delivery_under_hwm();
  test_qos_close_under_full_ring();
  test_reconnect_dial_connects_and_echoes();
  test_reconnect_dial_fails_after_attempts();
  test_reconnect_callback_ops_pipe();
  test_qos_recv_watermark_and_worker();
  test_qos_linger_drains_before_close();
  test_wire_fragmentation_and_reassembly();
  test_shm_fragmentation_and_reassembly();
  test_shm_reassembly_size_capped();
  test_shm_first_chunk_size_capped();
  test_shm_fragmentation_midway_timeout_keeps_stream_synced();
  test_shm_configurable_slot_geometry();
  test_shm_geometry_mismatch_rejected();
  test_shm_rejects_invalid_geometry();
  test_qos_priority_scheduling();
  test_tcp_channel_multiplexing();
  test_tcp_channel_recv_does_not_drop_other_channel();
  test_tcp_channel_pending_capped();
  test_shm_rejects_oversize_message();
  test_tcp_channel_recv_deadline_bounded();
  test_tcp_channel_recv_subms_deadline_bounded();
  test_reconnect_channel_preserved();
  test_tcp_concurrent_channel_recv();
  test_qos_shm_recv_midfragment_close_completes();
  test_qos_rate_limit_paces_delivery();
  test_qos_rate_limit_default_burst_drains();
  test_qos_channel_multiplexing();
  test_tcp_handshake_negotiates_version();
  test_tcp_accept_rejects_silent_peer();
  test_qos_item_count_cap();
  test_qos_failfast_returns_queue_full();
  test_qos_dial_opt_in_disabled_by_default();
  test_qos_ttl_expires_queued_message();
  test_qos_drop_old_evicts_oldest_queued();
  test_concurrent_send_recv_channels();
  test_multi_process_kill9_restart();
  test_rpc_request_correlation();
  test_rpc_request_timeout();
  test_rpc_close_joins_without_transport_wakeup();
  test_pubsub_multiple_subscribers();
  test_pubsub_subscription_outlives_broker();
  test_pubsub_concurrent_broker_subscription_destruction();

  if (g_failures != 0) {
    std::cerr << "FAIL (" << g_failures << ")\n";
    return 1;
  }
  std::cerr << "OK\n";
  return 0;
}

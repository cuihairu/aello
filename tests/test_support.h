#pragma once

// Shared test harness and transport test doubles for the duct test suite.
// Each tests/<layer>_tests.cc includes this header; the [RUN]/[DONE]
// markers stay on stderr so a hang is visible as the last [RUN] line in
// CI logs. g_failures/fail are inline so every translation unit shares
// one failure counter.

#include "duct/duct.h"
#include "duct/protocol.h"
#include "duct/qos_pipe.h"
#include "duct/pubsub.h"
#include "duct/rpc.h"
#include "duct/socket_utils.h"
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
#include <functional>
#include <limits>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if !defined(_WIN32)
#include <dirent.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif


inline int g_failures = 0;

inline void fail(const char* file, int line, const std::string& msg) {
  std::cerr << file << ":" << line << ": " << msg << "\n";
  ++g_failures;
}

// Run one test with a progress marker on stderr so a hang (or slow test) is
// visible in CI logs as the last "[RUN] ..." line.
#define RUN_TEST(fn)                 \
  do {                               \
    std::cerr << "[RUN] " #fn "\n";  \
    std::cerr.flush();               \
    fn();                            \
    std::cerr << "[DONE] " #fn "\n"; \
    std::cerr.flush();               \
  } while (0)

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

// Test double with a fixed send-side bound (like pipe:// or Windows shm://).
class BoundedPipe final : public duct::Pipe {
 public:
  explicit BoundedPipe(std::size_t cap) : cap_(cap) {}

  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    return {};
  }
  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    return duct::Status::timeout("no inbound data");
  }
  std::size_t max_message_bytes() const override { return cap_; }
  void close() override {}

 private:
  std::size_t cap_;
};

// Test double: send() fails with kIoError as if the frame timed out mid-write
// (some of the frame's bytes may already have reached the peer).
class MidFrameFailPipe final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    return duct::Status::io_error("send() timed out mid-frame");
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    return duct::Status::timeout("no inbound data");
  }

  void close() override {}
};

// Test double: send() always reports a terminal transport failure (kClosed),
// while recv() holds inbound data back until released, then delivers exactly
// one message and keeps timing out afterwards. Models a connection whose send
// side died while the recv side still has data to deliver.
class SendFailsGatedRecvPipe final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    return duct::Status::closed("transport closed under test");
  }

  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    std::unique_lock<std::mutex> lock(mu_);
    if (!released_) {
      // Bounded park; the QoS recv worker treats it as a transient timeout.
      cv_.wait_for(lock, std::chrono::milliseconds(50));
      return duct::Status::timeout("gated");
    }
    if (delivered_) return duct::Status::timeout("no more inbound data");
    delivered_ = true;
    return duct::Message::from_string("inbound");
  }

  void release() {
    std::lock_guard<std::mutex> lock(mu_);
    released_ = true;
    cv_.notify_all();
  }

  void close() override {}

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool released_ = false;
  bool delivered_ = false;
};

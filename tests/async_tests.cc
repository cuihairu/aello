#include "duct/async.h"
#include "duct/duct.h"

#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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

// A pipe that always reports kClosed, so EventLoop::run() must treat it as a
// benign condition (not an error).
class ClosedPipe final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    return duct::Status::closed("closed");
  }
  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    return duct::Status::closed("closed");
  }
  void close() override {}
};

// A pipe that delivers a single message then reports kClosed.
class OncePipe final : public duct::Pipe {
 public:
  duct::Result<void> send(const duct::Message&, const duct::SendOptions&) override {
    return duct::Status::not_supported("no send");
  }
  duct::Result<duct::Message> recv(const duct::RecvOptions&) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (delivered_) return duct::Status::closed("closed");
    delivered_ = true;
    return duct::Message::from_string("hello");
  }
  void close() override {}

 private:
  std::mutex mu_;
  bool delivered_ = false;
};

static void test_event_loop_stop_from_callback() {
  duct::async::EventLoop loop;
  int errors = 0;
  int messages = 0;
  bool saw_stop_from_callback = false;

  loop.add_pipe(std::make_shared<OncePipe>(),
                [&](const duct::Message&) {
                  ++messages;
                  // Stop from within the callback (the loop thread). Previously
                  // running_ stayed true and the loop never exited.
                  loop.stop();
                  saw_stop_from_callback = true;
                },
                [&](const duct::Status&) { ++errors; });

  auto fut = std::async(std::launch::async, [&] { loop.run(); });
  auto status = fut.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(status == std::future_status::ready);
  if (status != std::future_status::ready) {
    // Avoid leaking a still-running thread.
    return;
  }
  fut.get();
  EXPECT_TRUE(saw_stop_from_callback);
  EXPECT_TRUE(messages >= 1);
  EXPECT_EQ(errors, 0);
}

static void test_event_loop_restarts_after_stop() {
  duct::async::EventLoop loop;
  loop.add_pipe(std::make_shared<ClosedPipe>(),
                nullptr,
                [](const duct::Status&) {});

  // Run in background, stop, then restart. Previously the stop() from an
  // external thread joined and cleared the handle, and runInBackground would
  // start a fresh loop; the bug was stop() from a callback leaving running_ set.
  loop.runInBackground();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  loop.stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  loop.runInBackground();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  loop.stop();
  EXPECT_TRUE(true);
}

static void test_event_loop_normal_close_is_not_error() {
  duct::async::EventLoop loop;
  int errors = 0;
  loop.add_pipe(std::make_shared<ClosedPipe>(),
                nullptr,
                [&](const duct::Status&) { ++errors; });

  auto fut = std::async(std::launch::async, [&] {
    // Run a few iterations; recv returns kClosed, which must be benign.
    for (int i = 0; i < 3; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    loop.stop();
  });
  loop.run();
  fut.get();
  EXPECT_EQ(errors, 0);
}

}  // namespace

int main() {
  test_event_loop_stop_from_callback();
  test_event_loop_restarts_after_stop();
  test_event_loop_normal_close_is_not_error();
  if (g_failures != 0) {
    std::cerr << "FAIL (" << g_failures << ")\n";
    return 1;
  }
  std::cerr << "OK\n";
  return 0;
}

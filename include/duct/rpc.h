#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "duct/duct.h"
#include "duct/message.h"
#include "duct/status.h"

namespace duct::rpc {

// Wire framing for correlated request/response. Layout:
//   [0..4)  request id (u32 LE)
//   [4..N)  payload
//
// The same frame is used for requests and responses: a response carries the
// request id it answers, so a client can match it back to a pending call.
inline std::uint32_t read_request_id(const Message& m) {
  if (m.size() < 4) return 0;
  const std::uint8_t* p = m.data();
  return static_cast<std::uint32_t>(p[0]) |
         (static_cast<std::uint32_t>(p[1]) << 8) |
         (static_cast<std::uint32_t>(p[2]) << 16) |
         (static_cast<std::uint32_t>(p[3]) << 24);
}

inline Message frame_with_id(std::uint32_t id, const Message& payload) {
  std::vector<std::uint8_t> buf(4 + payload.size());
  buf[0] = static_cast<std::uint8_t>(id & 0xFF);
  buf[1] = static_cast<std::uint8_t>((id >> 8) & 0xFF);
  buf[2] = static_cast<std::uint8_t>((id >> 16) & 0xFF);
  buf[3] = static_cast<std::uint8_t>((id >> 24) & 0xFF);
  if (payload.size() != 0) {
    std::memcpy(buf.data() + 4, payload.data(), payload.size());
  }
  return Message::from_bytes(buf.data(), buf.size());
}

inline Message payload_of(const Message& m) {
  if (m.size() <= 4) return Message::from_string({});
  return Message::from_bytes(m.data() + 4, m.size() - 4);
}

// A correlated RPC client that multiplexes concurrent requests over a single
// Pipe. Each call carries a unique request id; a background thread dispatches
// responses back to the matching caller. Calls support a per-call timeout and
// cooperative cancellation.
class Client {
 public:
  explicit Client(std::unique_ptr<Pipe> pipe) : pipe_(std::move(pipe)), running_(true) {
    worker_ = std::thread([this] { dispatch_loop(); });
  }

  ~Client() { close(); }

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  // Perform a correlated request/response round trip. Returns the response
  // payload (without the id frame) or an error (kTimeout, kCancelled, kClosed).
  Result<Message> call(const Message& request, std::chrono::milliseconds timeout) {
    const std::uint32_t id = next_id_.fetch_add(1, std::memory_order_relaxed) + 1;
    auto pending = std::make_shared<Pending>();
    {
      std::lock_guard<std::mutex> lock(pending_mu_);
      if (!running_) return Status::closed("rpc client closed");
      pending_.emplace(id, pending);
    }

    auto framed = frame_with_id(id, request);
    std::lock_guard<std::mutex> send_lock(send_mu_);
    auto st = pipe_->send(framed, {.timeout = timeout});
    if (!st.ok()) {
      erase_pending(id);
      return st.status();
    }

    // Wait on the pending entry we inserted. The dispatch thread may deliver a
    // fast response and erase the map entry before we reach this point; holding
    // the shared_ptr keeps the entry alive so we can still observe `done`.
    std::unique_lock<std::mutex> lock(pending->mu);
    const bool ready = pending->cv.wait_for(lock, timeout, [&] {
      return pending->done || !running_;
    });
    if (!ready) {
      // Timeout: remove the pending entry so a late response is ignored.
      erase_pending(id);
      if (!running_) return Status::closed("rpc client closed");
      return Status::timeout("rpc call timed out");
    }
    erase_pending(id);
    if (!pending->status.ok()) return pending->status;
    return std::move(pending->response);
  }

  // Cancel an in-flight call by id (best-effort; the caller is released with
  // kCancelled). The request may still be delivered; a late response is dropped.
  void cancel(std::uint32_t id) {
    std::shared_ptr<Pending> p;
    {
      std::lock_guard<std::mutex> lock(pending_mu_);
      auto it = pending_.find(id);
      if (it == pending_.end()) return;
      p = it->second;
      pending_.erase(it);
    }
    std::lock_guard<std::mutex> lock(p->mu);
    p->done = true;
    p->status = Status::cancelled("rpc call cancelled");
    p->cv.notify_all();
  }

  void close() {
    {
      std::lock_guard<std::mutex> lock(pending_mu_);
      if (closed_) return;
      closed_ = true;
      running_ = false;
    }
    pipe_->close();
    if (worker_.joinable()) worker_.join();
    // Release any waiters with kClosed.
    std::lock_guard<std::mutex> lock(pending_mu_);
    for (auto& [id, p] : pending_) {
      std::lock_guard<std::mutex> plock(p->mu);
      p->done = true;
      p->status = Status::closed("rpc client closed");
      p->cv.notify_all();
    }
    pending_.clear();
  }

 private:
  struct Pending {
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    Status status;
    Message response;
  };

  void erase_pending(std::uint32_t id) {
    std::lock_guard<std::mutex> lock(pending_mu_);
    pending_.erase(id);
  }

  void dispatch_loop() {
    for (;;) {
      // Bound the transport recv so close() (which sets running_ false and
      // closes the pipe) can always join the worker promptly, even if the
      // underlying recv does not wake on close (e.g. a blocking tcp read).
      auto msg = pipe_->recv({.timeout = std::chrono::milliseconds(100)});
      if (!msg.ok()) {
        if (msg.status().code() == StatusCode::kTimeout) {
          if (!running_) break;
          continue;
        }
        // Transport closed or errored: fail all waiters.
        std::lock_guard<std::mutex> lock(pending_mu_);
        running_ = false;
        for (auto& [id, p] : pending_) {
          std::lock_guard<std::mutex> plock(p->mu);
          p->done = true;
          p->status = msg.status();
          p->cv.notify_all();
        }
        return;
      }
      const std::uint32_t id = read_request_id(msg.value());
      std::shared_ptr<Pending> p;
      {
        std::lock_guard<std::mutex> lock(pending_mu_);
        auto it = pending_.find(id);
        if (it == pending_.end()) continue;  // late/cancelled response
        p = it->second;
        pending_.erase(it);
      }
      std::lock_guard<std::mutex> lock(p->mu);
      p->done = true;
      p->response = payload_of(msg.value());
      p->cv.notify_all();
    }
  }

  std::unique_ptr<Pipe> pipe_;
  std::thread worker_;
  std::atomic<bool> running_{true};
  std::atomic<std::uint32_t> next_id_{0};
  std::mutex pending_mu_;
  std::map<std::uint32_t, std::shared_ptr<Pending>> pending_;
  std::mutex send_mu_;
  bool closed_ = false;
};

}  // namespace duct::rpc

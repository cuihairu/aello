#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include "duct/duct.h"
#include "duct/message.h"
#include "duct/status.h"

namespace duct {

// Internal queue entry with timestamp for TTL
struct QueuedMessage {
  Message message;
  std::chrono::steady_clock::time_point timestamp;
  int priority = 0;
  std::uint32_t channel_id = 0;

  QueuedMessage(const Message& msg, int prio = 0, std::uint32_t ch = 0)
    : message(msg), timestamp(std::chrono::steady_clock::now()),
      priority(prio), channel_id(ch) {}
};

class QosPipe : public Pipe {
 public:
  QosPipe(std::unique_ptr<Pipe> underlying, const QosOptions& qos);
  ~QosPipe() override;

  Result<void> send(const Message& msg, const SendOptions& opt) override;
  Result<Message> recv(const RecvOptions& opt) override;
  std::size_t max_message_bytes() const override;
  void close() override;

 private:
  // Background thread for processing send queue
  void send_worker();
  // Background thread for draining the underlying transport into the recv queue.
  void recv_worker();

  std::unique_ptr<Pipe> underlying_;
  QosOptions qos_;

  // Send queue management
  std::deque<QueuedMessage> send_queue_;
  std::mutex send_mutex_;
  std::condition_variable send_cv_;
  std::thread send_thread_;
  std::atomic<bool> running_{false};

  // Current queue sizes
  std::atomic<size_t> send_bytes_{0};
  std::atomic<size_t> send_count_{0};

  // Recv queue management. recv_worker() drains underlying_->recv() into this
  // bounded queue so that recv() is a fast, non-blocking (subject to rcv_hwm)
  // dequeue that the caller controls.
  std::deque<QueuedMessage> recv_queue_;
  std::mutex recv_mutex_;
  std::condition_variable recv_cv_;
  std::thread recv_thread_;
  std::atomic<size_t> recv_bytes_{0};
  std::atomic<size_t> recv_count_{0};
  // Set by recv_worker when the underlying transport reports a terminal error
  // (kClosed/kIoError). recv() surfaces it after draining queued messages.
  std::atomic<bool> recv_errored_{false};
  Status recv_error_;
};

}  // namespace duct

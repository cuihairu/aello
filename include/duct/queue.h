#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>

#include "duct/duct.h"
#include "duct/message.h"
#include "duct/status.h"

namespace duct {

// Internal message wrapper with metadata for TTL and priority.
struct QueueEntry {
  Message msg;
  std::chrono::steady_clock::time_point enqueue_time;
  std::chrono::steady_clock::time_point deadline;  // zero if no TTL
  // Reserved for future: priority, channel_id.
};

// Thread-safe message queue with backpressure support.
// Used for both send and receive directions.
class MessageQueue {
 public:
  explicit MessageQueue(std::size_t hwm_bytes, BackpressurePolicy policy, std::chrono::milliseconds ttl);

  // Enqueue a message. Behavior depends on backpressure policy when HWM is reached:
  // - kBlock: blocks until space is available or timeout
  // - kDropNew: drops the new message immediately
  // - kDropOld: drops oldest message(s) to make room
  // - kFailFast: returns EAGAIN-style error immediately
  Result<void> push(const Message& msg, std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

  // Dequeue a message. Blocks until available or timeout.
  Result<Message> pop(std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

  // Try to pop without blocking. Returns nullopt if empty.
  std::optional<Message> try_pop();

  // Current queue size in bytes.
  std::size_t size_bytes() const;

  // Current queue size in message count.
  std::size_t size_msgs() const;

  // Check if queue is at or above high water mark.
  bool at_hwm() const;

  // Close the queue. All waiting operations will return kClosed.
  void close();

  // Check if closed.
  bool is_closed() const;

  // Purge expired messages (TTL). Returns number of purged messages.
  std::size_t purge_expired();

 private:
  void drop_oldest_unlocked();
  bool has_space_unlocked() const;
  bool has_space_for_unlocked(std::size_t msg_size) const;

  mutable std::mutex mu_;
  std::condition_variable not_full_cv_;
  std::condition_variable not_empty_cv_;

  std::deque<QueueEntry> queue_;
  std::size_t total_bytes_ = 0;
  bool closed_ = false;

  // Configuration.
  std::size_t hwm_bytes_;
  BackpressurePolicy policy_;
  std::chrono::milliseconds ttl_;
};

}  // namespace duct

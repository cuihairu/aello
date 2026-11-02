#include "duct/queue.h"

#include <algorithm>

namespace duct {

MessageQueue::MessageQueue(std::size_t hwm_bytes, BackpressurePolicy policy, std::chrono::milliseconds ttl)
    : hwm_bytes_(hwm_bytes),
      policy_(policy),
      ttl_(ttl) {}

Result<void> MessageQueue::push(const Message& msg, std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mu_);

  if (closed_) {
    return Status::closed("queue closed");
  }

  const std::size_t msg_size = msg.size();

  // Check HWM and apply backpressure policy.
  if (!has_space_unlocked() || (total_bytes_ + msg_size > hwm_bytes_ && hwm_bytes_ > 0)) {
    switch (policy_) {
      case BackpressurePolicy::kBlock: {
        // Wait for space.
        auto pred = [this, msg_size]() {
          return closed_ || has_space_unlocked() ||
                 (hwm_bytes_ == 0) ||
                 (total_bytes_ + msg_size <= hwm_bytes_);
        };

        if (timeout.count() > 0) {
          if (!not_full_cv_.wait_for(lock, timeout, pred)) {
            return Status::timeout("push timed out waiting for queue space");
          }
        } else {
          not_full_cv_.wait(lock, pred);
        }

        if (closed_) {
          return Status::closed("queue closed");
        }
        break;
      }

      case BackpressurePolicy::kDropNew:
        // Drop this message silently.
        return Result<void>();

      case BackpressurePolicy::kDropOld:
        // Drop oldest messages until we have space.
        while (!has_space_unlocked() && !queue_.empty()) {
          drop_oldest_unlocked();
        }
        break;

      case BackpressurePolicy::kFailFast:
        return Status::io_error("queue at high water mark (EAGAIN)");
    }
  }

  // Enqueue the message.
  QueueEntry qm;
  qm.msg = msg;
  qm.enqueue_time = std::chrono::steady_clock::now();
  if (ttl_.count() > 0) {
    qm.deadline = qm.enqueue_time + ttl_;
  } else {
    qm.deadline = std::chrono::steady_clock::time_point{};
  }

  queue_.push_back(std::move(qm));
  total_bytes_ += msg_size;

  not_empty_cv_.notify_one();
  return Result<void>();
}

Result<Message> MessageQueue::pop(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mu_);

  auto pred = [this]() { return closed_ || !queue_.empty(); };

  if (timeout.count() > 0) {
    if (!not_empty_cv_.wait_for(lock, timeout, pred)) {
      return Status::timeout("pop timed out waiting for message");
    }
  } else {
    not_empty_cv_.wait(lock, pred);
  }

  if (queue_.empty()) {
    if (closed_) {
      return Status::closed("queue closed");
    }
    return Status::timeout("queue empty");
  }

  // Skip expired messages.
  auto now = std::chrono::steady_clock::now();
  while (!queue_.empty()) {
    auto& front = queue_.front();
    // Check TTL: deadline of zero means no expiration.
    if (front.deadline != std::chrono::steady_clock::time_point{} && now > front.deadline) {
      total_bytes_ -= front.msg.size();
      queue_.pop_front();
      not_full_cv_.notify_one();
      continue;
    }
    break;
  }

  if (queue_.empty()) {
    if (closed_) {
      return Status::closed("queue closed");
    }
    return Status::timeout("all messages expired");
  }

  Message result = std::move(queue_.front().msg);
  total_bytes_ -= result.size();
  queue_.pop_front();

  not_full_cv_.notify_one();
  return result;
}

std::optional<Message> MessageQueue::try_pop() {
  std::lock_guard<std::mutex> lock(mu_);

  if (queue_.empty()) {
    return std::nullopt;
  }

  // Skip expired messages.
  auto now = std::chrono::steady_clock::now();
  while (!queue_.empty()) {
    auto& front = queue_.front();
    if (front.deadline != std::chrono::steady_clock::time_point{} && now > front.deadline) {
      total_bytes_ -= front.msg.size();
      queue_.pop_front();
      continue;
    }
    break;
  }

  if (queue_.empty()) {
    return std::nullopt;
  }

  Message result = std::move(queue_.front().msg);
  total_bytes_ -= result.size();
  queue_.pop_front();

  not_full_cv_.notify_one();
  return result;
}

std::size_t MessageQueue::size_bytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return total_bytes_;
}

std::size_t MessageQueue::size_msgs() const {
  std::lock_guard<std::mutex> lock(mu_);
  return queue_.size();
}

bool MessageQueue::at_hwm() const {
  std::lock_guard<std::mutex> lock(mu_);
  return hwm_bytes_ > 0 && total_bytes_ >= hwm_bytes_;
}

void MessageQueue::close() {
  std::lock_guard<std::mutex> lock(mu_);
  closed_ = true;
  not_full_cv_.notify_all();
  not_empty_cv_.notify_all();
}

bool MessageQueue::is_closed() const {
  std::lock_guard<std::mutex> lock(mu_);
  return closed_;
}

std::size_t MessageQueue::purge_expired() {
  std::lock_guard<std::mutex> lock(mu_);

  if (ttl_.count() == 0) {
    return 0;
  }

  auto now = std::chrono::steady_clock::now();
  std::size_t purged = 0;

  auto it = queue_.begin();
  while (it != queue_.end()) {
    if (it->deadline != std::chrono::steady_clock::time_point{} && now > it->deadline) {
      total_bytes_ -= it->msg.size();
      it = queue_.erase(it);
      ++purged;
    } else {
      ++it;
    }
  }

  if (purged > 0) {
    not_full_cv_.notify_all();
  }

  return purged;
}

void MessageQueue::drop_oldest_unlocked() {
  if (queue_.empty()) return;
  total_bytes_ -= queue_.front().msg.size();
  queue_.pop_front();
}

bool MessageQueue::has_space_unlocked() const {
  if (hwm_bytes_ == 0) return true;  // No limit.
  return total_bytes_ < hwm_bytes_;
}

}  // namespace duct

#include "duct/qos_pipe.h"

#include <algorithm>

namespace duct {

QosPipe::QosPipe(std::unique_ptr<Pipe> underlying, const QosOptions& qos)
    : underlying_(std::move(underlying)), qos_(qos) {
  running_ = true;
  send_thread_ = std::thread(&QosPipe::send_worker, this);
}

QosPipe::~QosPipe() {
  close();
  if (send_thread_.joinable()) {
    send_thread_.join();
  }
}

void QosPipe::send_worker() {
  while (running_) {
    QueuedMessage msg_to_send{Message{}};
    {
      std::unique_lock<std::mutex> lock(send_mutex_);

      // Wait for messages or shutdown
      send_cv_.wait(lock, [this] { return !send_queue_.empty() || !running_; });

      if (!running_) break;

      // Process expired messages (TTL)
      if (qos_.ttl.count() > 0) {
        auto now = std::chrono::steady_clock::now();

        send_queue_.erase(
          std::remove_if(send_queue_.begin(), send_queue_.end(),
            [&](const QueuedMessage& queued_msg) {
              auto age = now - queued_msg.timestamp;
              if (age > qos_.ttl) {
                send_bytes_ -= queued_msg.message.size();
                send_count_ -= 1;
                return true;  // Remove expired message
              }
              return false;
            }),
          send_queue_.end());
      }

      if (send_queue_.empty()) continue;

      // Drain unconditionally. The HWM water mark gates the *enqueue* side (send()),
      // not the drain side: gating the worker here would stall the queue permanently
      // once it reaches snd_hwm_bytes (the worker is the only path that frees bytes).
      // Pop the head before sending so a concurrent send() (kDropOld) or close()
      // cannot race with an in-flight transport send.
      msg_to_send = std::move(send_queue_.front());
      send_queue_.pop_front();
      send_bytes_ -= msg_to_send.message.size();
      send_count_ -= 1;
      // Wake a blocked kBlock sender: queue space was just freed.
      send_cv_.notify_all();
    }

    // Send outside the lock, with a bounded timeout: a blocked transport send
    // (e.g. a full shm ring) must not wedge close(), which joins this worker, and
    // must not block concurrent send() calls behind it.
    SendOptions opt;
    opt.timeout = std::chrono::milliseconds(100);
    auto result = underlying_->send(msg_to_send.message, opt);

    if (!result.ok()) {
      const StatusCode code = result.status().code();
      std::lock_guard<std::mutex> lock(send_mutex_);
      if (code == StatusCode::kClosed || code == StatusCode::kIoError) {
        running_ = false;
        send_cv_.notify_all();
        break;
      }
      // A timeout is transient: keep the message and retry it at the head so
      // ordering is preserved (e.g. a full ring or a slow peer).
      if (code == StatusCode::kTimeout) {
        const std::size_t requeue_size = msg_to_send.message.size();
        if (running_) {
          send_queue_.push_front(std::move(msg_to_send));
          send_bytes_ += requeue_size;
          send_count_ += 1;
        }
        continue;
      }
      // Any other status (kInvalidArgument, kProtocolError, kNotSupported) is
      // permanent for this message. Requeueing it would spin forever and block
      // every message behind it, so drop it and keep draining.
    }
  }
}

Result<void> QosPipe::send(const Message& msg, const SendOptions& opt) {
  if (!running_) {
    return Status::closed("pipe closed");
  }

  // Check message size against limits
  if (msg.size() > qos_.snd_hwm_bytes) {
    return Status::invalid_argument("message too large for queue limits");
  }

  std::unique_lock<std::mutex> lock(send_mutex_);

  // Handle backpressure policies
  if (send_bytes_.load() >= qos_.snd_hwm_bytes) {
    switch (qos_.backpressure) {
      case BackpressurePolicy::kFailFast:
        return Status::io_error("send queue at high water mark (fail fast)");

      case BackpressurePolicy::kDropNew:
        // Drop the new message
        return Status::Ok();

      case BackpressurePolicy::kDropOld:
        // Drop the oldest message if queue is not empty
        if (!send_queue_.empty()) {
          send_bytes_ -= send_queue_.front().message.size();
          send_count_ -= 1;
          send_queue_.pop_front();
        }
        break;

      case BackpressurePolicy::kBlock:
        // SendOptions::timeout == 0 means "no timeout" (see duct.h). Only use a
        // timed wait when the caller asked for one; wait_for(0ms) would return
        // immediately and report a spurious timeout under backpressure.
        if (opt.timeout.count() > 0) {
          send_cv_.wait_for(lock, opt.timeout, [this] {
            return send_bytes_.load() < qos_.snd_hwm_bytes || !running_;
          });
        } else {
          send_cv_.wait(lock, [this] {
            return send_bytes_.load() < qos_.snd_hwm_bytes || !running_;
          });
        }
        if (!running_) {
          return Status::closed("pipe closed");
        }
        if (send_bytes_.load() >= qos_.snd_hwm_bytes) {
          return Status::timeout("send queue full (timeout)");
        }
        break;
    }
  }

  // Add message to send queue
  send_queue_.emplace_back(msg);
  send_bytes_ += msg.size();
  send_count_ += 1;

  lock.unlock();
  send_cv_.notify_one();

  return Status::Ok();
}

Result<Message> QosPipe::recv(const RecvOptions& opt) {
  return underlying_->recv(opt);
}

void QosPipe::close() {
  {
    std::lock_guard<std::mutex> lock(send_mutex_);
    running_ = false;
  }
  send_cv_.notify_all();
  // Join the worker before closing the underlying transport. The worker's send is
  // bounded (see send_worker), so this cannot hang; joining first avoids closing the
  // transport while a send is still in flight.
  if (send_thread_.joinable()) {
    send_thread_.join();
  }
  underlying_->close();
}

}  // namespace duct
#include "duct/qos_pipe.h"

#include "duct/wire.h"

#include <algorithm>
#include <chrono>
#include <iterator>

namespace duct {

QosPipe::QosPipe(std::unique_ptr<Pipe> underlying, const QosOptions& qos)
    : underlying_(std::move(underlying)), qos_(qos) {
  running_ = true;
  send_thread_ = std::thread(&QosPipe::send_worker, this);
  if (qos_.rcv_hwm_bytes != 0) {
    recv_thread_ = std::thread(&QosPipe::recv_worker, this);
  }
}

QosPipe::~QosPipe() {
  close();
  if (send_thread_.joinable()) {
    send_thread_.join();
  }
  if (recv_thread_.joinable()) {
    recv_thread_.join();
  }
  // No recv worker: recv() passes through to the underlying transport.
  return;
}

void QosPipe::send_worker() {
  // Token-bucket state for rate limiting. Tokens are refilled continuously up
  // to the effective burst; each drained message consumes msg.size() tokens.
  // A zero burst_bytes means "one second's worth" of rate (the documented
  // default), so the limiter never deadlocks on a non-empty message.
  const std::size_t burst = qos_.burst_bytes != 0 ? qos_.burst_bytes : qos_.rate_bytes_per_sec;
  std::size_t tokens = burst;
  auto last_refill = std::chrono::steady_clock::now();

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
        bool expired_messages_removed = false;

        send_queue_.erase(
          std::remove_if(send_queue_.begin(), send_queue_.end(),
            [&](const QueuedMessage& queued_msg) {
              auto age = now - queued_msg.timestamp;
              if (age > qos_.ttl) {
                send_bytes_ -= queued_msg.message.size();
                send_count_ -= 1;
                expired_messages_removed = true;
                return true;  // Remove expired message
              }
              return false;
            }),
          send_queue_.end());
        // A blocked kBlock sender only relies on queue space released by TTL;
        // wake it as soon as that space becomes available.
        if (expired_messages_removed) {
          send_cv_.notify_all();
        }
      }

      if (send_queue_.empty()) continue;

      // Enforce the token bucket before draining the next message. Block until
      // enough tokens are available (or the worker is stopped).
      if (qos_.rate_bytes_per_sec != 0) {
        auto best_cost = [this]() -> std::size_t {
          auto best = send_queue_.begin();
          for (auto it = std::next(send_queue_.begin()); it != send_queue_.end(); ++it) {
            if (it->priority > best->priority) best = it;
          }
          return best->message.size();
        };
        for (;;) {
          const std::size_t next_cost = best_cost();
          auto now = std::chrono::steady_clock::now();
          const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
              now - last_refill).count();
          if (elapsed_ms > 0) {
            const std::size_t added = static_cast<std::size_t>(
                (static_cast<double>(qos_.rate_bytes_per_sec) * elapsed_ms) / 1000.0);
            tokens = std::min<std::size_t>(burst, tokens + added);
            last_refill = now;
          }
          if (tokens >= next_cost) break;
          // Wait for enough refill time.
          const auto needed_ms = static_cast<long long>(
              (static_cast<double>(next_cost - tokens) * 1000.0) /
              static_cast<double>(qos_.rate_bytes_per_sec));
          send_cv_.wait_for(lock, std::chrono::milliseconds(std::max<long long>(1, needed_ms)),
                            [this] { return !running_ || send_failed_.load(); });
          if (!running_ || send_failed_.load()) break;
        }
        if (!running_ || send_failed_.load()) break;
      }

      // Drain unconditionally. The HWM water mark gates the *enqueue* side (send()),
      // not the drain side: gating the worker here would stall the queue permanently
      // once it reaches snd_hwm_bytes (the worker is the only path that frees bytes).
      // Pop the head before sending so a concurrent send() (kDropOld) or close()
      // cannot race with an in-flight transport send.
      // Drain by priority: pick the highest-priority message (stable FIFO for
      // equal priorities). Linear scan keeps the queue small and deterministic.
      auto best = send_queue_.begin();
      for (auto it = std::next(send_queue_.begin()); it != send_queue_.end(); ++it) {
        if (it->priority > best->priority) best = it;
      }
      msg_to_send = std::move(*best);
      if (qos_.rate_bytes_per_sec != 0) {
        const std::size_t cost = msg_to_send.message.size();
        tokens = tokens >= cost ? tokens - cost : 0;
      }
      send_queue_.erase(best);
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
    opt.channel_id = msg_to_send.channel_id;
    auto result = underlying_->send(msg_to_send.message, opt);

    if (!result.ok()) {
      const StatusCode code = result.status().code();
      std::lock_guard<std::mutex> lock(send_mutex_);
      if (code == StatusCode::kClosed || code == StatusCode::kIoError) {
        // The transport is terminally broken: record the failure so send()
        // stops accepting messages that can never be delivered, blocked
        // senders wake with the error, and close()'s linger does not wait on
        // a dead queue. running_ stays true — it is the close() lifecycle
        // flag, and the recv side must keep draining independently.
        send_error_ = result.status();
        send_failed_ = true;
        send_cv_.notify_all();
        return;
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
  // send_error_ is published before the flag store, so observing the flag
  // also makes the message visible (release/acquire via the atomic).
  if (send_failed_.load()) {
    return Status::closed("send worker failed: " + send_error_.message());
  }

  // Check message size against an explicit caller-provided cap (defaults to
  // unlimited; the transport fragments large payloads) and the underlying
  // transport's non-fragmenting bound. snd_hwm_bytes is a queue *watermark*
  // (backpressure), not a per-message cap: a message larger than the watermark
  // may occupy an empty queue and drain normally, so it is not rejected here.
  const std::size_t max_message_bytes = qos_.max_message_bytes;
  const std::size_t transport_max = underlying_->max_message_bytes();
  if (msg.size() > max_message_bytes || msg.size() > transport_max) {
    return Status::invalid_argument("message too large for the transport");
  }

  std::unique_lock<std::mutex> lock(send_mutex_);

  // Re-check under the lock: close() sets running_ = false and joins the worker.
  // Without this check a send() that passed the fast-path check above could
  // enqueue after the worker exited, returning Ok for a message that will never
  // be sent.
  if (!running_) {
    return Status::closed("pipe closed");
  }
  if (send_failed_.load()) {
    return Status::closed("send worker failed: " + send_error_.message());
  }

  // Handle backpressure policies. A zero watermark means unlimited. Both the
  // byte watermark and the item-count cap gate enqueue.
  const bool byte_full = qos_.snd_hwm_bytes != 0 &&
                         send_bytes_.load() >= qos_.snd_hwm_bytes;
  const bool count_full = qos_.snd_hwm_count != 0 &&
                          send_count_.load() >= qos_.snd_hwm_count;
  if (byte_full || count_full) {
    switch (qos_.backpressure) {
      case BackpressurePolicy::kFailFast:
        return Status::queue_full("send queue at high water mark (fail fast)");

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
            return !running_ || send_failed_.load() ||
                   ((qos_.snd_hwm_bytes == 0 ||
                     send_bytes_.load() < qos_.snd_hwm_bytes) &&
                    (qos_.snd_hwm_count == 0 ||
                     send_count_.load() < qos_.snd_hwm_count));
          });
        } else {
          send_cv_.wait(lock, [this] {
            return !running_ || send_failed_.load() ||
                   ((qos_.snd_hwm_bytes == 0 ||
                     send_bytes_.load() < qos_.snd_hwm_bytes) &&
                    (qos_.snd_hwm_count == 0 ||
                     send_count_.load() < qos_.snd_hwm_count));
          });
        }
        if (!running_) {
          return Status::closed("pipe closed");
        }
        if (send_failed_.load()) {
          return Status::closed("send worker failed: " + send_error_.message());
        }
        if ((qos_.snd_hwm_bytes != 0 && send_bytes_.load() >= qos_.snd_hwm_bytes) ||
            (qos_.snd_hwm_count != 0 && send_count_.load() >= qos_.snd_hwm_count)) {
          return Status::queue_full("send queue full");
        }
        break;
    }
  }

  // Add message to send queue
  send_queue_.emplace_back(msg, opt.priority, opt.channel_id);
  send_bytes_ += msg.size();
  send_count_ += 1;

  lock.unlock();
  send_cv_.notify_one();

  return Status::Ok();
}

Result<Message> QosPipe::recv(const RecvOptions& opt) {
  // Without a recv worker (rcv_hwm_bytes == 0) recv passes through to the
  // underlying transport, preserving the pre-QoS-queue behavior.
  if (qos_.rcv_hwm_bytes == 0) {
    return underlying_->recv(opt);
  }
  std::unique_lock<std::mutex> lock(recv_mutex_);
  auto pred = [this, &opt] {
    if (!running_ || recv_errored_.load()) return true;
    if (opt.channel_id == 0) return !recv_queue_.empty();
    return std::any_of(recv_queue_.begin(), recv_queue_.end(),
                       [&](const QueuedMessage& queued) {
                         return queued.channel_id == opt.channel_id;
                       });
  };
  if (opt.timeout.count() > 0) {
    const auto deadline = std::chrono::steady_clock::now() + opt.timeout;
    if (!recv_cv_.wait_until(lock, deadline, pred)) {
      return Status::timeout("recv timed out");
    }
  } else {
    recv_cv_.wait(lock, pred);
  }

  auto match = recv_queue_.begin();
  if (opt.channel_id != 0) {
    match = std::find_if(recv_queue_.begin(), recv_queue_.end(),
                         [&](const QueuedMessage& queued) {
                           return queued.channel_id == opt.channel_id;
                         });
  }
  if (match != recv_queue_.end()) {
    Message msg = std::move(match->message);
    recv_bytes_ -= msg.size();
    recv_count_ -= 1;
    recv_queue_.erase(match);
    // The recv watermark gates the *enqueue* side (recv_worker); draining a
    // message frees space and lets the worker resume pulling from the transport.
    recv_cv_.notify_all();
    return msg;
  }

  if (recv_errored_.load()) {
    return recv_error_;
  }
  if (!running_) {
    return Status::closed("pipe closed");
  }
  return Status::timeout("recv timed out");
}

void QosPipe::recv_worker() {
  while (running_) {
    // Wait for space below the recv watermark. A zero watermark means
    // unlimited. Never hold the lock while blocking on the transport recv.
    {
      std::unique_lock<std::mutex> lock(recv_mutex_);
      recv_cv_.wait(lock, [this] {
        return !running_ || qos_.rcv_hwm_bytes == 0 ||
               recv_bytes_.load() < qos_.rcv_hwm_bytes;
      });
      if (!running_) return;
    }

    RecvOptions opt;
    // Bound the transport recv so close() can interrupt this worker promptly.
    opt.timeout = std::chrono::milliseconds(100);
    auto result = underlying_->recv_frame(opt);

    if (!result.ok()) {
      const StatusCode code = result.status().code();
      if (code == StatusCode::kTimeout) continue;
      if (code == StatusCode::kClosed || code == StatusCode::kIoError ||
          code == StatusCode::kProtocolError) {
        {
          std::lock_guard<std::mutex> lock(recv_mutex_);
          recv_errored_ = true;
          recv_error_ = result.status();
          recv_cv_.notify_all();
        }
        return;
      }
      {
        std::lock_guard<std::mutex> lock(recv_mutex_);
        recv_errored_ = true;
        recv_error_ = result.status();
        recv_cv_.notify_all();
      }
      return;
    }

    {
      std::lock_guard<std::mutex> lock(recv_mutex_);
      if (!running_) return;
      const auto channel_id = flags_to_channel_id(result.value().flags);
      recv_queue_.emplace_back(result.value().message, 0, channel_id);
      recv_bytes_ += result.value().message.size();
      recv_count_ += 1;
      recv_cv_.notify_all();
    }
  }
}

std::size_t QosPipe::max_message_bytes() const {
  // Mirror every per-message bound send() enforces synchronously (qos cap and
  // transport cap) so a caller sizing messages against this value is never
  // rejected by a limit the accessor failed to report. snd_hwm_bytes is a
  // queue watermark, not a message bound, so it does not participate here.
  return std::min(qos_.max_message_bytes, underlying_->max_message_bytes());
}

void QosPipe::close() {
  // Linger: allow queued outbound messages to drain for up to qos_.linger
  // before force-closing. Zero means best-effort immediate close. A failed
  // send worker cannot drain anything, so the wait ends immediately.
  if (qos_.linger.count() > 0) {
    std::unique_lock<std::mutex> lock(send_mutex_);
    send_cv_.wait_for(lock, qos_.linger, [this] {
      return !running_ || send_failed_.load() || send_queue_.empty();
    });
  }

  {
    std::lock_guard<std::mutex> lock(send_mutex_);
    running_ = false;
  }
  send_cv_.notify_all();
  recv_cv_.notify_all();
  // Join the worker before closing the underlying transport. The worker's send is
  // bounded (see send_worker), so this cannot hang; joining first avoids closing the
  // transport while a send is still in flight.
  if (send_thread_.joinable()) {
    send_thread_.join();
  }
  if (recv_thread_.joinable()) {
    recv_thread_.join();
  }
  underlying_->close();
}

}  // namespace duct

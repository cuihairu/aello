#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "duct/duct.h"

namespace duct::async {

// ==============================================================================
// 异步操作封装
// ==============================================================================

/**
 * @brief 异步操作结果
 */
template <class T>
class AsyncResult {
 public:
  AsyncResult() = default;

  // 从 std::future 构建
  explicit AsyncResult(std::future<T>&& future) : future_(std::move(future)) {}

  // 等待结果
  T get() { return future_.get(); }

  // 等待指定时间
  template <class Rep, class Period>
  std::future_status wait_for(const std::chrono::duration<Rep, Period>& timeout) {
    return future_.wait_for(timeout);
  }

  // 检查是否就绪
  bool is_ready() const {
    return future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
  }

 private:
  std::future<T> future_;
};

/**
 * @brief 异步发送操作
 */
inline AsyncResult<void> async_send(std::shared_ptr<Pipe> pipe, const Message& msg) {
  return AsyncResult<void>(std::async(std::launch::async, [pipe, msg]() {
    return pipe->send(msg, {}).throw_if_error();
  }));
}

/**
 * @brief 异步接收操作
 */
inline AsyncResult<Message> async_recv(std::shared_ptr<Pipe> pipe) {
  return AsyncResult<Message>(std::async(std::launch::async, [pipe]() {
    return pipe->recv({}).value_or_throw();
  }));
}

/**
 * @brief 异步连接操作
 */
inline AsyncResult<std::shared_ptr<Pipe>> async_dial(const std::string& address, const DialOptions& opt = {}) {
  return AsyncResult<std::shared_ptr<Pipe>>(std::async(std::launch::async, [address, opt]() {
    auto result = dial(address, opt);
    return std::shared_ptr<Pipe>(result.value_or_throw().release());
  }));
}

/**
 * @brief 异步监听操作
 */
inline AsyncResult<std::shared_ptr<Listener>> async_listen(const std::string& address, const ListenOptions& opt = {}) {
  return AsyncResult<std::shared_ptr<Listener>>(std::async(std::launch::async, [address, opt]() {
    auto result = listen(address, opt);
    return std::shared_ptr<Listener>(result.value_or_throw().release());
  }));
}

// ==============================================================================
// 回调风格 API
// ==============================================================================

/**
 * @brief 回调类型定义
 */
template <class T>
using Callback = std::function<void(const Result<T>&)>;

using MessageCallback = std::function<void(const Message&)>;
using ErrorCallback = std::function<void(const Status&)>;

/**
 * @brief 带回调的异步发送
 */
inline void async_send(std::shared_ptr<Pipe> pipe, const Message& msg, Callback<void> callback) {
  std::thread([pipe, msg, callback = std::move(callback)]() mutable {
    auto result = pipe->send(msg, {});
    callback(result);
  }).detach();
}

/**
 * @brief 带回调的异步接收
 */
inline void async_recv(std::shared_ptr<Pipe> pipe, Callback<Message> callback) {
  std::thread([pipe, callback = std::move(callback)]() mutable {
    auto result = pipe->recv({});
    callback(result);
  }).detach();
}

// ==============================================================================
// 事件循环
// ==============================================================================

/**
 * @brief 简单的事件循环，用于管理多个 Pipe 的 I/O
 */
class EventLoop {
 public:
  EventLoop() = default;
  ~EventLoop() { stop(); }

  // 禁止拷贝
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  /**
   * @brief 添加一个管道到事件循环
   * @param pipe 要管理的管道
   * @param on_message 收到消息时的回调
   * @param on_error 发生错误时的回调（可选）
   */
  void add_pipe(std::shared_ptr<Pipe> pipe,
                MessageCallback on_message,
                ErrorCallback on_error = nullptr) {
    std::lock_guard<std::mutex> lock(mutex_);
    pipes_.push_back({std::move(pipe), std::move(on_message), std::move(on_error)});
    cv_.notify_one();
  }

  /**
   * @brief 启动事件循环（阻塞调用）
   */
  void run() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (running_) {
      // Never hold the mutex while blocking on I/O or invoking user callbacks:
      // stop() and add_pipe() must be able to proceed concurrently.
      auto pipes = pipes_;
      lock.unlock();

      bool has_activity = false;
      for (auto& entry : pipes) {
        // 尝试接收消息（非阻塞）
        // 注意：当前实现使用轮询，实际应用中应该使用更高效的机制
        // 如 epoll/kqueue/IOCP 等
        auto msg = entry.pipe->recv({.timeout = std::chrono::milliseconds(100)});
        if (msg.ok()) {
          has_activity = true;
          if (entry.on_message) {
            entry.on_message(msg.value());
          }
        } else if (msg.status().code() != StatusCode::kTimeout &&
                   msg.status().code() != StatusCode::kClosed) {
          // 发生错误
          if (entry.on_error) {
            entry.on_error(msg.status());
          }
        }
      }

      lock.lock();
      // 如果没有活动，等待新管道添加或停止信号
      if (running_ && !has_activity) {
        cv_.wait_for(lock, std::chrono::milliseconds(100));
      }
    }
  }

  /**
   * @brief 在后台线程运行事件循环
   */
  void runInBackground() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!thread_.joinable()) {
      running_ = true;
      thread_ = std::thread([this]() { this->run(); });
    }
  }

  /**
   * @brief 停止事件循环
   */
  void stop() {
    std::unique_lock<std::mutex> lock(mutex_);
    running_ = false;
    cv_.notify_all();
    if (!thread_.joinable() || thread_.get_id() == std::this_thread::get_id()) {
      // Called from a loop callback: the thread will observe running_ == false
      // and exit; joining ourselves here would deadlock.
      return;
    }
    // Join without the mutex so run() can reacquire it to observe running_.
    std::thread worker = std::move(thread_);
    lock.unlock();
    worker.join();
  }

 private:
  struct PipeEntry {
    std::shared_ptr<Pipe> pipe;
    MessageCallback on_message;
    ErrorCallback on_error;
  };

  std::vector<PipeEntry> pipes_;
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool running_ = true;
};

// ==============================================================================
// 线程安全的消息队列
// ==============================================================================

/**
 * @brief 线程安全的消息队列
 */
template <class T>
class Channel {
 public:
  Channel() = default;
  ~Channel() { close(); }

  // 禁止拷贝
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  /**
   * @brief 发送消息（阻塞）
   */
  bool send(T item) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!closed_) {
      queue_.push(std::move(item));
      cv_.notify_one();
      return true;
    }
    return false;
  }

  /**
   * @brief 接收消息（阻塞）
   */
  bool recv(T& item) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this]() { return !queue_.empty() || closed_; });

    if (queue_.empty()) {
      return false;  // 队列已关闭且为空
    }

    item = std::move(queue_.front());
    queue_.pop();
    return true;
  }

  /**
   * @brief 尝试接收消息（非阻塞）
   */
  bool try_recv(T& item) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty()) {
      return false;
    }
    item = std::move(queue_.front());
    queue_.pop();
    return true;
  }

  /**
   * @brief 关闭队列
   */
  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    cv_.notify_all();
  }

  /**
   * @brief 检查队列是否为空
   */
  bool empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
  }

 private:
  std::queue<T> queue_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool closed_ = false;
};

// ==============================================================================
// 便捷函数：在单独的线程中运行服务器
// ==============================================================================

/**
 * @brief 在后台线程运行 echo 服务器
 * @param address 监听地址
 * @return 可以用于停止服务器的函数
 */
inline std::function<void()> run_echo_serverInBackground(const std::string& address) {
  struct ServerState {
    std::thread thread;
    std::shared_ptr<Listener> listener;
    std::atomic<bool> running{true};
  };

  auto state = std::make_shared<ServerState>();

  state->listener = std::shared_ptr<Listener>(listen(address).value_or_throw().release());

  state->thread = std::thread([state]() {
    while (state->running) {
      auto pipe_result = state->listener->accept();
      if (!pipe_result.ok()) {
        break;
      }

      auto pipe = std::shared_ptr<Pipe>(pipe_result.value().release());

      // 为每个连接创建一个处理线程
      std::thread([pipe]() {
        while (true) {
          auto msg = pipe->recv({});
          if (!msg.ok()) {
            break;
          }
          pipe->send(msg.value(), {});
        }
      }).detach();
    }
  });

  return [state]() {
    state->running = false;
    state->listener->close();
    if (state->thread.joinable()) {
      state->thread.join();
    }
  };
}

}  // namespace duct::async

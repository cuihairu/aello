#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "duct/duct.h"

namespace duct::convenience {

// ==============================================================================
// 便捷构建器
// ==============================================================================

/**
 * @brief Dial 选项构建器，提供链式 API
 */
class DialBuilder {
 public:
  explicit DialBuilder(std::string address) : address_(std::move(address)) {}

  // 设置超时
  DialBuilder& timeout(std::chrono::milliseconds ms) {
    options_.timeout = ms;
    return *this;
  }

  // 设置发送高水位
  DialBuilder& send_hwm(std::size_t bytes) {
    options_.qos.snd_hwm_bytes = bytes;
    return *this;
  }

  // 设置接收高水位
  DialBuilder& recv_hwm(std::size_t bytes) {
    options_.qos.rcv_hwm_bytes = bytes;
    return *this;
  }

  // 设置背压策略
  DialBuilder& backpressure(BackpressurePolicy policy) {
    options_.qos.backpressure = policy;
    return *this;
  }

  // 启用自动重连
  DialBuilder& auto_reconnect(std::chrono::milliseconds initial_delay = std::chrono::milliseconds(100)) {
    options_.reconnect.enabled = true;
    options_.reconnect.initial_delay = initial_delay;
    return *this;
  }

  // 设置心跳间隔
  DialBuilder& heartbeat(std::chrono::milliseconds interval) {
    options_.reconnect.heartbeat_interval = interval;
    return *this;
  }

  // 设置状态回调
  DialBuilder& on_state_change(ConnectionCallback cb) {
    options_.on_state_change = std::move(cb);
    return *this;
  }

  // 执行连接
  Result<std::unique_ptr<Pipe>> connect() const {
    return dial(address_, options_);
  }

  // 隐式转换 operator()，提供更简洁的调用
  Result<std::unique_ptr<Pipe>> operator()() const {
    return connect();
  }

 private:
  std::string address_;
  DialOptions options_;
};

/**
 * @brief Listen 选项构建器，提供链式 API
 */
class ListenBuilder {
 public:
  explicit ListenBuilder(std::string address) : address_(std::move(address)) {}

  // 设置 backlog
  ListenBuilder& backlog(int value) {
    options_.backlog = value;
    return *this;
  }

  // 设置发送高水位
  ListenBuilder& send_hwm(std::size_t bytes) {
    options_.qos.snd_hwm_bytes = bytes;
    return *this;
  }

  // 设置接收高水位
  ListenBuilder& recv_hwm(std::size_t bytes) {
    options_.qos.rcv_hwm_bytes = bytes;
    return *this;
  }

  // 设置背压策略
  ListenBuilder& backpressure(BackpressurePolicy policy) {
    options_.qos.backpressure = policy;
    return *this;
  }

  // 执行监听
  Result<std::unique_ptr<Listener>> bind() const {
    return listen(address_, options_);
  }

  // 隐式转换 operator()，提供更简洁的调用
  Result<std::unique_ptr<Listener>> operator()() const {
    return bind();
  }

 private:
  std::string address_;
  ListenOptions options_;
};

// ==============================================================================
// 便捷工厂函数
// ==============================================================================

/**
 * @brief 创建一个用于 TCP 连接的构建器
 */
inline DialBuilder tcp(std::string_view host, std::uint16_t port) {
  return DialBuilder("tcp://" + std::string(host) + ":" + std::to_string(port));
}

/**
 * @brief 创建一个用于共享内存连接的构建器
 */
inline DialBuilder shm(std::string_view name) {
  return DialBuilder("shm://" + std::string(name));
}

/**
 * @brief 创建一个用于 Unix 域套接字连接的构建器
 */
inline DialBuilder uds(std::string_view path) {
  return DialBuilder("uds://" + std::string(path));
}

/**
 * @brief 创建一个用于 TCP 监听的构建器
 */
inline ListenBuilder listen_tcp(std::string_view host, std::uint16_t port) {
  return ListenBuilder("tcp://" + std::string(host) + ":" + std::to_string(port));
}

/**
 * @brief 创建一个用于共享内存监听的构建器
 */
inline ListenBuilder listen_shm(std::string_view name) {
  return ListenBuilder("shm://" + std::string(name));
}

/**
 * @brief 创建一个用于 Unix 域套接字监听的构建器
 */
inline ListenBuilder listen_uds(std::string_view path) {
  return ListenBuilder("uds://" + std::string(path));
}

/**
 * @brief 简单连接（使用默认选项）
 * @note convenience 命名空间中的函数返回裸指针，使用 raii 命名空间获取 RAII 包装版本
 */
inline Result<std::unique_ptr<Pipe>> connect_raw(std::string_view address) {
  return dial(std::string(address));
}

/**
 * @brief 简单监听（使用默认选项）
 * @note convenience 命名空间中的函数返回裸指针，使用 raii 命名空间获取 RAII 包装版本
 */
inline Result<std::unique_ptr<Listener>> bind_raw(std::string_view address) {
  return listen(std::string(address));
}

// ==============================================================================
// 便捷操作函数
// ==============================================================================

/**
 * @brief 发送并等待响应（请求-响应模式）
 * @param pipe 通信管道
 * @param request 请求数据
 * @param timeout 超时时间
 * @return 响应数据，失败时返回错误状态
 */
inline Result<Message> request(Pipe& pipe, const Message& request,
                               std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  // 发送请求
  auto st = pipe.send(request, {.timeout = timeout});
  if (!st.ok()) {
    return st.status();
  }

  // 接收响应
  return pipe.recv({.timeout = timeout});
}

/**
 * @brief 回复消息（发送接收到的消息的响应）
 * @param pipe 通信管道
 * @param response 响应数据
 */
inline Result<void> reply(Pipe& pipe, const Message& response) {
  return pipe.send(response, {});
}

/**
 * @brief 批量发送消息
 * @param pipe 通信管道
 * @param messages 消息范围
 * @return 成功发送的消息数量，失败时返回错误状态
 */
inline Result<std::size_t> send_batch(Pipe& pipe,
                                      const std::vector<Message>& messages) {
  std::size_t sent = 0;
  for (const auto& msg : messages) {
    auto st = pipe.send(msg, {});
    if (!st.ok()) {
      return st.status();
    }
    ++sent;
  }
  return sent;
}

/**
 * @brief 接收指定数量的消息
 * @param pipe 通信管道
 * @param count 要接收的消息数量
 * @param timeout 单条消息的超时时间
 * @return 接收到的消息向量，失败时返回错误状态
 */
inline Result<std::vector<Message>> recv_batch(Pipe& pipe, std::size_t count,
                                                std::chrono::milliseconds timeout = {}) {
  std::vector<Message> messages;
  messages.reserve(count);

  for (std::size_t i = 0; i < count; ++i) {
    auto msg = pipe.recv({.timeout = timeout});
    if (!msg.ok()) {
      return msg.status();
    }
    messages.push_back(std::move(msg).value());
  }

  return messages;
}

// ==============================================================================
// 便捷回调辅助
// ==============================================================================

/**
 * @brief 消息处理器类型
 */
using MessageHandler = std::function<Result<void>(Message&)>;

/**
 * @brief 运行 echo 服务器（回显接收到的所有消息）
 * @param pipe 通信管道
 * @return 错误状态，如果成功结束则返回 Ok
 */
inline Result<void> echo_server(std::unique_ptr<Pipe> pipe) {
  while (true) {
    auto msg = pipe->recv({});
    if (!msg.ok()) {
      if (msg.status().code() == StatusCode::kClosed) {
        return Status::Ok();  // 正常关闭
      }
      return msg.status();
    }

    auto st = pipe->send(msg.value(), {});
    if (!st.ok()) {
      return st;
    }
  }
}

/**
 * @brief 使用消息处理器运行服务器
 * @param pipe 通信管道
 * @param handler 消息处理函数
 * @return 错误状态，如果成功结束则返回 Ok
 */
inline Result<void> serve(std::unique_ptr<Pipe> pipe, MessageHandler handler) {
  while (true) {
    auto msg = pipe->recv({});
    if (!msg.ok()) {
      if (msg.status().code() == StatusCode::kClosed) {
        return Status::Ok();  // 正常关闭
      }
      return msg.status();
    }

    auto st = handler(msg.value());
    if (!st.ok()) {
      return st;
    }
  }
}

/**
 * @brief 简单的消息生产者-消费者循环
 * @param pipe 通信管道
 * @param produce 消息生产函数
 * @param timeout 发送超时
 * @return 错误状态
 */
inline Result<void> produce_loop(Pipe& pipe,
                                 std::function<std::optional<Message>()> produce,
                                 std::chrono::milliseconds timeout = {}) {
  while (auto msg = produce()) {
    auto st = pipe.send(*msg, {.timeout = timeout});
    if (!st.ok()) {
      return st;
    }
  }
  return Status::Ok();
}

/**
 * @brief 简单的消息消费者循环
 * @param pipe 通信管道
 * @param consume 消息消费函数
 * @param timeout 接收超时
 * @return 错误状态
 */
inline Result<void> consume_loop(Pipe& pipe,
                                 std::function<Result<void>(Message)> consume,
                                 std::chrono::milliseconds timeout = {}) {
  while (true) {
    auto msg = pipe.recv({.timeout = timeout});
    if (!msg.ok()) {
      if (msg.status().code() == StatusCode::kClosed) {
        return Status::Ok();  // 正常关闭
      }
      return msg.status();
    }

    auto st = consume(std::move(msg).value());
    if (!st.ok()) {
      return st;
    }
  }
}

}  // namespace duct::convenience

#pragma once

#include <exception>
#include <string>
#include <string_view>
#include <utility>

namespace duct {

enum class StatusCode {
  kOk = 0,
  kInvalidArgument,
  kNotSupported,
  kIoError,
  kTimeout,
  kClosed,
  kProtocolError,
  kQueueFull,
  kCancelled,
};

/**
 * @brief 将状态码转换为可读字符串
 */
constexpr std::string_view to_string(StatusCode code) {
  switch (code) {
    case StatusCode::kOk:             return "Ok";
    case StatusCode::kInvalidArgument: return "Invalid argument";
    case StatusCode::kNotSupported:   return "Not supported";
    case StatusCode::kIoError:        return "I/O error";
    case StatusCode::kTimeout:        return "Timeout";
    case StatusCode::kClosed:         return "Closed";
    case StatusCode::kProtocolError:  return "Protocol error";
    case StatusCode::kQueueFull:      return "Queue full";
    case StatusCode::kCancelled:      return "Cancelled";
    default:                          return "Unknown";
  }
}

/**
 * @brief 异常类，用于将 Status 转换为异常抛出
 */
class Exception : public std::exception {
 public:
  Exception(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {
    full_message_ = "[" + std::string(to_string(code_)) + "] " + message_;
  }

  explicit Exception(const class Status& status);

  const char* what() const noexcept override { return full_message_.c_str(); }

  StatusCode code() const noexcept { return code_; }
  const std::string& message() const noexcept { return message_; }

 private:
  StatusCode code_;
  std::string message_;
  std::string full_message_;
};

/**
 * @brief 状态类，用于表示操作结果（非异常方式）
 */
class Status {
 public:
  Status() : code_(StatusCode::kOk) {}
  Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }
  static Status invalid_argument(std::string m) { return Status(StatusCode::kInvalidArgument, std::move(m)); }
  static Status not_supported(std::string m) { return Status(StatusCode::kNotSupported, std::move(m)); }
  static Status io_error(std::string m) { return Status(StatusCode::kIoError, std::move(m)); }
  static Status timeout(std::string m) { return Status(StatusCode::kTimeout, std::move(m)); }
  static Status closed(std::string m) { return Status(StatusCode::kClosed, std::move(m)); }
  static Status protocol_error(std::string m) { return Status(StatusCode::kProtocolError, std::move(m)); }
  static Status queue_full(std::string m) { return Status(StatusCode::kQueueFull, std::move(m)); }
  static Status cancelled(std::string m) { return Status(StatusCode::kCancelled, std::move(m)); }

  bool ok() const { return code_ == StatusCode::kOk; }
  explicit operator bool() const { return ok(); }

  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }

  /**
   * @brief 获取完整的错误消息（包含状态码名称）
   */
  std::string to_string() const {
    if (ok()) return "Ok";
    return "[" + std::string(duct::to_string(code_)) + "] " + message_;
  }

  /**
   * @brief 如果状态不是 Ok，则抛出异常
   * @throws Exception 如果状态不是 Ok
   */
  void throw_if_error() const {
    if (!ok()) {
      throw Exception(*this);
    }
  }

 private:
  StatusCode code_;
  std::string message_;
};

inline Exception::Exception(const Status& status)
    : Exception(status.code(), status.message()) {}

template <class T>
class Result {
 public:
  Result(T v) : ok_(true), value_(std::move(v)), status_() {}
  Result(Status s) : ok_(false), value_(), status_(std::move(s)) {}

  bool ok() const { return ok_; }
  explicit operator bool() const { return ok_; }
  const Status& status() const { return status_; }

  T& value() & { return value_; }
  const T& value() const& { return value_; }
  T&& value() && { return std::move(value_); }

  /**
   * @brief 如果结果不是 Ok，则抛出异常
   * @throws Exception 如果结果不是 Ok
   */
  T& value_or_throw() & {
    status_.throw_if_error();
    return value_;
  }

  const T& value_or_throw() const& {
    status_.throw_if_error();
    return value_;
  }

  T&& value_or_throw() && {
    status_.throw_if_error();
    return std::move(value_);
  }

  /**
   * @brief 返回值，如果失败则返回默认值
   */
  template <class U>
  T value_or(U&& default_value) const& {
    if (ok_) {
      return value_;
    }
    return static_cast<T>(std::forward<U>(default_value));
  }

  template <class U>
  T value_or(U&& default_value) && {
    if (ok_) {
      return std::move(value_);
    }
    return static_cast<T>(std::forward<U>(default_value));
  }

 private:
  bool ok_ = false;
  T value_{};
  Status status_{StatusCode::kInvalidArgument, "uninitialized"};
};

template <>
class Result<void> {
 public:
  Result() : status_(Status::Ok()) {}
  Result(Status s) : status_(std::move(s)) {}

  bool ok() const { return status_.ok(); }
  explicit operator bool() const { return ok(); }
  const Status& status() const { return status_; }

  /**
   * @brief 如果结果不是 Ok，则抛出异常
   * @throws Exception 如果结果不是 Ok
   */
  void throw_if_error() const {
    status_.throw_if_error();
  }

 private:
  Status status_;
};

}  // namespace duct

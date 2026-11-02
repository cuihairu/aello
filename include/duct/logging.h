#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace duct {

// ==============================================================================
// 日志级别
// ==============================================================================

enum class LogLevel {
  kTrace = 0,
  kDebug,
  kInfo,
  kWarning,
  kError,
  kFatal,
};

constexpr std::string_view to_string(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:   return "TRACE";
    case LogLevel::kDebug:   return "DEBUG";
    case LogLevel::kInfo:    return "INFO";
    case LogLevel::kWarning: return "WARNING";
    case LogLevel::kError:   return "ERROR";
    case LogLevel::kFatal:   return "FATAL";
    default:                 return "UNKNOWN";
  }
}

// ==============================================================================
// 日志记录器接口
// ==============================================================================

/**
 * @brief 日志记录器接口
 */
class Logger {
 public:
  virtual ~Logger() = default;

  // 记录日志
  virtual void log(LogLevel level, std::string_view message) = 0;

  // 刷新日志
  virtual void flush() = 0;

  // 设置日志级别
  virtual void set_level(LogLevel level) { level_ = level; }
  virtual LogLevel level() const { return level_; }

  // 便捷方法
  void trace(std::string_view msg) { log(LogLevel::kTrace, msg); }
  void debug(std::string_view msg) { log(LogLevel::kDebug, msg); }
  void info(std::string_view msg) { log(LogLevel::kInfo, msg); }
  void warning(std::string_view msg) { log(LogLevel::kWarning, msg); }
  void error(std::string_view msg) { log(LogLevel::kError, msg); }
  void fatal(std::string_view msg) { log(LogLevel::kFatal, msg); }

 protected:
  LogLevel level_ = LogLevel::kInfo;
};

/**
 * @brief 默认的控制台日志记录器
 */
class ConsoleLogger : public Logger {
 public:
  ConsoleLogger() = default;
  explicit ConsoleLogger(LogLevel level) { level_ = level; }

  void log(LogLevel level, std::string_view message) override;
  void flush() override;

 private:
  std::mutex mutex_;
};

/**
 * @brief 空日志记录器（禁用日志）
 */
class NullLogger : public Logger {
 public:
  void log(LogLevel, std::string_view) override {}
  void flush() override {}
};

/**
 * @brief 自定义日志记录器（使用回调函数）
 */
class CallbackLogger : public Logger {
 public:
  using LogCallback = std::function<void(LogLevel, std::string_view)>;

  explicit CallbackLogger(LogCallback callback)
      : callback_(std::move(callback)) {}

  void log(LogLevel level, std::string_view message) override {
    if (callback_ && level >= level_) {
      callback_(level, message);
    }
  }

  void flush() override {
    // CallbackLogger 不需要刷新
  }

 private:
  LogCallback callback_;
};

/**
 * @brief 带前缀的日志记录器装饰器
 */
class PrefixLogger : public Logger {
 public:
  PrefixLogger(std::shared_ptr<Logger> base, std::string prefix)
      : base_(std::move(base)), prefix_(std::move(prefix)) {}

  void log(LogLevel level, std::string_view message) override {
    if (level >= level_) {
      base_->log(level, prefix_ + std::string(message));
    }
  }

  void flush() override { base_->flush(); }
  void set_level(LogLevel level) override {
    Logger::set_level(level);
    base_->set_level(level);
  }

 private:
  std::shared_ptr<Logger> base_;
  std::string prefix_;
};

// ==============================================================================
// 全局日志记录器
// ==============================================================================

namespace detail {
  Logger& get_default_logger();
  void set_default_logger(std::shared_ptr<Logger> logger);
}  // namespace detail

inline void set_logger(std::shared_ptr<Logger> logger) {
  detail::set_default_logger(std::move(logger));
}

inline Logger& get_logger() {
  return detail::get_default_logger();
}

inline void set_log_level(LogLevel level) {
  get_logger().set_level(level);
}

// 全局便捷函数
inline void log(LogLevel level, std::string_view msg) {
  get_logger().log(level, msg);
}

inline void trace(std::string_view msg) { get_logger().trace(msg); }
inline void debug(std::string_view msg) { get_logger().debug(msg); }
inline void info(std::string_view msg) { get_logger().info(msg); }
inline void warning(std::string_view msg) { get_logger().warning(msg); }
inline void error(std::string_view msg) { get_logger().error(msg); }
inline void fatal(std::string_view msg) { get_logger().fatal(msg); }

// ==============================================================================
// 日志宏
// ==============================================================================

// 日志流式构建器
class LogStream {
 public:
  LogStream(LogLevel level, Logger& logger)
      : level_(level), logger_(logger) {}

  ~LogStream() {
    if (!handled_) {
      logger_.log(level_, buffer_);
    }
  }

  // 禁止拷贝
  LogStream(const LogStream&) = delete;
  LogStream& operator=(const LogStream&) = delete;

  // 移动支持
  LogStream(LogStream&& other) noexcept
      : level_(other.level_), logger_(other.logger_),
        buffer_(std::move(other.buffer_)), handled_(other.handled_) {
    other.handled_ = true;
  }

  template <class T>
  LogStream& operator<<(const T& value) {
    std::ostringstream oss;
    oss << value;
    buffer_ += oss.str();
    return *this;
  }

 private:
  LogLevel level_;
  Logger& logger_;
  std::string buffer_;
  bool handled_ = false;
};

#define DUCT_LOG_STREAM(level) \
  ::duct::LogStream(::duct::LogLevel::level, ::duct::get_logger())

#define DUCT_LOG(level, msg) ::duct::log(::duct::LogLevel::level, msg)
#define DUCT_TRACE(msg) DUCT_LOG(kTrace, msg)
#define DUCT_DEBUG(msg) DUCT_LOG(kDebug, msg)
#define DUCT_INFO(msg) DUCT_LOG(kInfo, msg)
#define DUCT_WARNING(msg) DUCT_LOG(kWarning, msg)
#define DUCT_ERROR(msg) DUCT_LOG(kError, msg)
#define DUCT_FATAL(msg) DUCT_LOG(kFatal, msg)

// ==============================================================================
// 指标/可观察性
// ==============================================================================

/**
 * @brief 指标类型
 */
enum class MetricType {
  kCounter,   // 计数器（只增不减）
  kGauge,     // 仪表（可增可减）
  kHistogram, // 直方图（分布统计）
};

/**
 * @brief 指标接口
 */
class Metric {
 public:
  virtual ~Metric() = default;
  virtual std::string name() const = 0;
  virtual MetricType type() const = 0;
  virtual double value() const = 0;
};

/**
 * @brief 计数器
 */
class Counter : public Metric {
 public:
  explicit Counter(std::string name) : name_(std::move(name)) {}

  void increment(double delta = 1.0) {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ += delta;
  }

  void reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ = 0.0;
  }

  std::string name() const override { return name_; }
  MetricType type() const override { return MetricType::kCounter; }
  double value() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return value_;
  }

 private:
  std::string name_;
  double value_ = 0.0;
  mutable std::mutex mutex_;
};

/**
 * @brief 仪表
 */
class Gauge : public Metric {
 public:
  explicit Gauge(std::string name) : name_(std::move(name)) {}

  void set(double value) {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ = value;
  }

  void increment(double delta = 1.0) {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ += delta;
  }

  void decrement(double delta = 1.0) {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ -= delta;
  }

  std::string name() const override { return name_; }
  MetricType type() const override { return MetricType::kGauge; }
  double value() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return value_;
  }

 private:
  std::string name_;
  double value_ = 0.0;
  mutable std::mutex mutex_;
};

/**
 * @brief 直方图（简化版）
 */
class Histogram : public Metric {
 public:
  explicit Histogram(std::string name) : name_(std::move(name)) {}

  void observe(double value) {
    std::lock_guard<std::mutex> lock(mutex_);
    values_.push_back(value);
    count_++;
    sum_ += value;
  }

  std::string name() const override { return name_; }
  MetricType type() const override { return MetricType::kHistogram; }
  double value() const override { return count_; }

  size_t count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return count_;
  }

  double sum() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sum_;
  }

  double mean() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return count_ > 0 ? sum_ / count_ : 0.0;
  }

 private:
  std::string name_;
  std::vector<double> values_;
  size_t count_ = 0;
  double sum_ = 0.0;
  mutable std::mutex mutex_;
};

/**
 * @brief 指标注册表
 */
class MetricRegistry {
 public:
  static MetricRegistry& instance() {
    static MetricRegistry registry;
    return registry;
  }

  std::shared_ptr<Counter> get_counter(std::string_view name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = counters_.find(std::string(name));
    if (it != counters_.end()) {
      return it->second;
    }
    auto counter = std::make_shared<Counter>(std::string(name));
    counters_[std::string(name)] = counter;
    return counter;
  }

  std::shared_ptr<Gauge> get_gauge(std::string_view name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = gauges_.find(std::string(name));
    if (it != gauges_.end()) {
      return it->second;
    }
    auto gauge = std::make_shared<Gauge>(std::string(name));
    gauges_[std::string(name)] = gauge;
    return gauge;
  }

  std::shared_ptr<Histogram> get_histogram(std::string_view name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = histograms_.find(std::string(name));
    if (it != histograms_.end()) {
      return it->second;
    }
    auto histogram = std::make_shared<Histogram>(std::string(name));
    histograms_[std::string(name)] = histogram;
    return histogram;
  }

 private:
  MetricRegistry() = default;

  std::unordered_map<std::string, std::shared_ptr<Counter>> counters_;
  std::unordered_map<std::string, std::shared_ptr<Gauge>> gauges_;
  std::unordered_map<std::string, std::shared_ptr<Histogram>> histograms_;
  mutable std::mutex mutex_;
};

}  // namespace duct

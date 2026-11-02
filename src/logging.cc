#include "duct/logging.h"

#include <iostream>
#include <mutex>

namespace duct {

// ==============================================================================
// ConsoleLogger 实现
// ==============================================================================

void ConsoleLogger::log(LogLevel level, std::string_view message) {
  if (level < level_) return;

  std::lock_guard<std::mutex> lock(mutex_);

  std::ostream& out = (level >= LogLevel::kWarning) ? std::cerr : std::cout;
  out << '[' << to_string(level) << "] " << message << std::endl;
}

void ConsoleLogger::flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::cout.flush();
  std::cerr.flush();
}

// ==============================================================================
// 全局日志记录器实现
// ==============================================================================

namespace detail {

struct LoggerHolder {
  std::shared_ptr<Logger> logger = std::make_shared<ConsoleLogger>();
  // Keep replaced loggers alive: get() hands out references, so destroying the
  // previous instance while a caller still logs through it would be a UAF.
  std::vector<std::shared_ptr<Logger>> retired;
  std::mutex mutex;

  Logger& get() {
    std::lock_guard<std::mutex> lock(mutex);
    return *logger;
  }

  void set(std::shared_ptr<Logger> new_logger) {
    std::lock_guard<std::mutex> lock(mutex);
    retired.push_back(std::move(logger));
    logger = std::move(new_logger);
  }
};

LoggerHolder& get_logger_holder() {
  static LoggerHolder holder;
  return holder;
}

Logger& get_default_logger() {
  return get_logger_holder().get();
}

void set_default_logger(std::shared_ptr<Logger> logger) {
  get_logger_holder().set(std::move(logger));
}

}  // namespace detail

}  // namespace duct

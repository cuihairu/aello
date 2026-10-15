#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <span>

namespace duct {

class Message {
 public:
  Message() = default;

  // 从字节创建消息
  static Message from_bytes(const void* data, size_t size);
  static Message from_bytes(std::span<const std::uint8_t> bytes);

  // 从字符串创建消息
  static Message from_string(std::string_view s);

  // 创建空的预分配消息
  static Message with_capacity(size_t capacity);

  // 基本访问
  const std::uint8_t* data() const { return data_; }
  std::uint8_t* data() { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  // 获取所有权的共享存储
  const std::shared_ptr<std::vector<std::uint8_t>>& backing() const { return backing_; }

  // 视图访问
  std::span<const std::uint8_t> view() const { return std::span(data_, size_); }
  std::span<std::uint8_t> view() { return std::span(data_, size_); }

  // 转换为字符串视图
  std::string_view as_string_view() const {
    return std::string_view(reinterpret_cast<const char*>(data_), size_);
  }

  // 复制数据到目标缓冲区
  size_t copy_to(void* dest, size_t max_size) const {
    size_t copy_size = std::min(size_, max_size);
    if (copy_size > 0) {
      std::memcpy(dest, data_, copy_size);
    }
    return copy_size;
  }

  // 比较消息内容
  bool equals(const Message& other) const {
    return size_ == other.size_ &&
           (size_ == 0 || std::memcmp(data_, other.data_, size_) == 0);
  }

  // 运算符
  bool operator==(const Message& other) const { return equals(other); }
  bool operator!=(const Message& other) const { return !equals(other); }

 private:
  std::shared_ptr<std::vector<std::uint8_t>> backing_;
  std::uint8_t* data_ = nullptr;
  size_t size_ = 0;
};

}  // namespace duct


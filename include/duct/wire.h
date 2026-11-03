#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

#include "duct/message.h"
#include "duct/protocol.h"
#include "duct/status.h"

namespace duct::wire {

constexpr std::size_t kHeaderLen = 16;
constexpr std::size_t kMaxFramePayload = 64 * 1024;

#if defined(_WIN32)
using SocketHandle = std::uintptr_t;  // Compatible with WinSock's SOCKET (UINT_PTR); avoid 64-bit handle truncation. // 兼容 SOCKET，避免 64 位句柄截断。
#else
using SocketHandle = int;  // POSIX file descriptor. // POSIX 文件描述符。
#endif

// Use -1 as a sentinel across platforms.
// Windows: INVALID_SOCKET is ~(0); we only use this for our own closed-state tracking.
// 使用 -1 作为跨平台哨兵值；Windows 下 INVALID_SOCKET 是 ~(0)，这里仅用于内部关闭状态标记。
constexpr SocketHandle kInvalidSocket = static_cast<SocketHandle>(-1);

struct FrameHeader {
  std::uint32_t magic = 0;
  std::uint16_t version = 0;
  std::uint16_t header_len = 0;
  std::uint32_t payload_len = 0;
  std::uint32_t flags = 0;
};

// Encoding/decoding uses network byte order for integral fields.
void encode_header(const FrameHeader& h, std::uint8_t out[kHeaderLen]);
Result<FrameHeader> decode_header(const std::uint8_t in[kHeaderLen]);

// Socket I/O functions (cross-platform).
// timeout == 0 keeps the blocking behavior; a positive value bounds each
// underlying send()/recv() call and yields StatusCode::kTimeout on expiry.
// A timeout after a frame has partially transferred returns kIoError instead:
// the byte stream is out of sync, so callers must close the connection rather
// than resending the message on the same handle.
Result<void> write_frame(SocketHandle fd, const Message& msg, std::uint32_t flags = 0,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds{0});
Result<Message> read_frame(SocketHandle fd,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

// A frame together with its flags. Used by multiplexing/reliability layers to
// inspect channel id and control bits without losing them.
struct FrameMessage {
  Message message;
  std::uint32_t flags = 0;
};

Result<FrameMessage> read_frame_with_flags(
    SocketHandle fd,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

// Capability/version negotiation. The client sends its capabilities; the server
// validates the protocol version and replies with its own. Payload layout:
//   [0..1) version (u8)
//   [1..5) capability bitmask (u32 LE)
Result<void> handshake_client(SocketHandle fd, std::uint32_t capabilities,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds{0});
Result<std::uint32_t> handshake_server(
    SocketHandle fd, std::uint32_t server_capabilities,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

}  // namespace duct::wire


#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include "duct/message.h"
#include "duct/protocol.h"
#include "duct/status.h"

namespace duct::wire {

constexpr std::size_t kHeaderLen = 16;
// v2 header: base 16 bytes + session_id (u64) + seq (u64), both network order.
constexpr std::size_t kHeaderLenV2 = 32;
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
  // v2 reliability trailer; 0 on v1 frames (never encoded).
  std::uint64_t session_id = 0;
  std::uint64_t seq = 0;
};

// Reliability metadata a caller attaches to a frame. session_id == 0 selects the
// v1 frame encoding (no trailer); a nonzero session_id selects the v2 encoding
// with the trailer. Both are default 0, so existing call sites are untouched.
struct FrameMeta {
  std::uint64_t session_id = 0;
  std::uint64_t seq = 0;
};

// Encoding/decoding uses network byte order for integral fields.
// The v1 entry points read/write exactly kHeaderLen bytes. The v2 entry points
// read/write kHeaderLenV2 bytes and carry the reliability trailer.
void encode_header(const FrameHeader& h, std::uint8_t out[kHeaderLen]);
Result<FrameHeader> decode_header(const std::uint8_t in[kHeaderLen]);
void encode_header_v2(const FrameHeader& h, std::uint8_t out[kHeaderLenV2]);
Result<FrameHeader> decode_header_v2(const std::uint8_t in[kHeaderLenV2]);

// A byte stream the framing layer reads/writes through. Raw sockets and
// encrypted transports (TLS) implement this so the framing code is the same
// either way; the design (docs/design/tls-mtls.md) layers TLS *before*
// framing: connect → TLS handshake → capability handshake → frames.
class Stream {
 public:
  virtual ~Stream() = default;
  // Single timed write. Returns the number of bytes written (partial writes
  // are reported, never an error). kTimeout when nothing moved within
  // `timeout` (0 = block forever); kClosed when the peer closed; kIoError
  // otherwise. Mid-frame timeout classification is the caller's job
  // (write_frame), so a stream never reports a partial write as an error.
  virtual Result<std::size_t> write_some(const void* data, std::size_t len,
                                         std::chrono::milliseconds timeout) = 0;
  // Single timed read. Returns bytes read; 0 means a clean EOF. kTimeout when
  // nothing arrived within `timeout` (0 = block forever). A partial header is
  // the caller's responsibility to classify (read_frame_with_flags).
  virtual Result<std::size_t> read_some(void* buf, std::size_t cap,
                                        std::chrono::milliseconds timeout) = 0;
  // Wake any in-flight read/write so close() cannot hang on a blocked call.
  virtual void shutdown() = 0;
};

// Stream over a raw socket fd: applies SO_SNDTIMEO/SO_RCVTIMEO per call and
// performs one send()/recv(). This is the current framing behavior moved
// behind the Stream interface (no behavior change).
class SocketStream final : public Stream {
 public:
  explicit SocketStream(SocketHandle fd) : fd_(fd) {}
  Result<std::size_t> write_some(const void* data, std::size_t len,
                                 std::chrono::milliseconds timeout) override;
  Result<std::size_t> read_some(void* buf, std::size_t cap,
                                std::chrono::milliseconds timeout) override;
  void shutdown() override;

 private:
  SocketHandle fd_;
};

// Socket I/O functions (cross-platform).
// timeout == 0 keeps the blocking behavior; a positive value bounds each
// underlying send()/recv() call and yields StatusCode::kTimeout on expiry.
// A timeout after a frame has partially transferred returns kIoError instead:
// the byte stream is out of sync, so callers must close the connection rather
// than resending the message on the same handle.
//
// The framing primitives read/write through a Stream. The fd-based overloads
// wrap the fd in a SocketStream for callers without an explicit stream (the
// raw transports, tests); transports that add encryption (TLS) pass their own
// Stream instead.
Result<void> write_frame(Stream& stream, const Message& msg, std::uint32_t flags = 0,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
                         const FrameMeta& meta = FrameMeta{});
Result<Message> read_frame(Stream& stream,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds{0});
inline Result<void> write_frame(SocketHandle fd, const Message& msg, std::uint32_t flags = 0,
                                std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
                                const FrameMeta& meta = FrameMeta{}) {
  SocketStream s(fd);
  return write_frame(s, msg, flags, timeout, meta);
}
inline Result<Message> read_frame(SocketHandle fd,
                                  std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
  SocketStream s(fd);
  return read_frame(s, timeout);
}

// A frame together with its flags and reliability trailer. Used by
// multiplexing/reliability layers to inspect channel id, control bits, session
// identity, and sequence numbers without losing them.
struct FrameMessage {
  Message message;
  std::uint32_t flags = 0;
  std::uint64_t session_id = 0;
  std::uint64_t seq = 0;
};

Result<FrameMessage> read_frame_with_flags(
    Stream& stream,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0});
inline Result<FrameMessage> read_frame_with_flags(
    SocketHandle fd,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
  SocketStream s(fd);
  return read_frame_with_flags(s, timeout);
}

// Capability/version negotiation. The client sends its capabilities and, for a
// reliability dial, its session id; the server validates the protocol version
// and replies with its own capabilities. Payload layout (both directions):
//   [0..1) version (u8)
//   [1..5) capability bitmask (u32 LE)
//   [5..13) session id (u64 BE) — 0 unless a reliability dial announced one
// The session id rides the handshake (rather than being inferred from the
// first data frame) so the listener can resume a known session's counters
// before any frame flows — a server that pushes first needs the dialer's
// session id to keep its own send-side sequence across redials.
Result<void> handshake_client(Stream& stream, std::uint32_t capabilities,
                              std::uint64_t session_id = 0,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds{0});
// Request+reply variant: writes the request, then reads the server's kCtrl
// reply and returns its capability bitmask. Callers that need the negotiated
// capabilities before their first frame (reliability) use this; the
// fire-and-forget variant above is enough for everyone else.
Result<std::uint32_t> handshake_client_sync(
    Stream& stream, std::uint32_t capabilities,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
    std::uint64_t session_id = 0);

// What a server learned from the client's handshake.
struct HandshakeResult {
  std::uint32_t capabilities = 0;
  std::uint64_t session_id = 0;  // the client's reliability session, 0 if none
};

Result<HandshakeResult> handshake_server(
    Stream& stream, std::uint32_t server_capabilities,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

// Stop-aware variant of handshake_server(): the client handshake read is
// sliced in 100ms windows, so a silent client (connected but never writing the
// handshake) cannot pin accept() past a listener close — a blocked recv is not
// woken by close() on POSIX. Returns kClosed as soon as `stop` is observed
// set. The overall handshake deadline still bounds the wait.
Result<HandshakeResult> handshake_server_stoppable(
    Stream& stream, std::uint32_t server_capabilities,
    std::chrono::milliseconds timeout, const std::atomic<bool>& stop);

// fd convenience overloads (wrapping a SocketStream) for the handshake entry
// points; transports that layer encryption pass their own Stream instead.
inline Result<void> handshake_client(SocketHandle fd, std::uint32_t capabilities,
                                     std::uint64_t session_id = 0,
                                     std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
  SocketStream s(fd);
  return handshake_client(s, capabilities, session_id, timeout);
}
inline Result<std::uint32_t> handshake_client_sync(
    SocketHandle fd, std::uint32_t capabilities,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
    std::uint64_t session_id = 0) {
  SocketStream s(fd);
  return handshake_client_sync(s, capabilities, timeout, session_id);
}
inline Result<HandshakeResult> handshake_server(
    SocketHandle fd, std::uint32_t server_capabilities,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
  SocketStream s(fd);
  return handshake_server(s, server_capabilities, timeout);
}
inline Result<HandshakeResult> handshake_server_stoppable(
    SocketHandle fd, std::uint32_t server_capabilities,
    std::chrono::milliseconds timeout, const std::atomic<bool>& stop) {
  SocketStream s(fd);
  return handshake_server_stoppable(s, server_capabilities, timeout, stop);
}

}  // namespace duct::wire


#include "duct/wire.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#if defined(__APPLE__)
#include <mutex>
#endif

#include "duct/protocol.h"

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <csignal>
#include <pthread.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace duct::wire {

void encode_header(const FrameHeader& h, std::uint8_t out[kHeaderLen]) {
  std::uint32_t magic = htonl(h.magic);
  std::uint16_t version = htons(h.version);
  std::uint16_t header_len = htons(h.header_len);
  std::uint32_t payload_len = htonl(h.payload_len);
  std::uint32_t flags = htonl(h.flags);

  std::memcpy(out + 0, &magic, 4);
  std::memcpy(out + 4, &version, 2);
  std::memcpy(out + 6, &header_len, 2);
  std::memcpy(out + 8, &payload_len, 4);
  std::memcpy(out + 12, &flags, 4);
}

Result<FrameHeader> decode_header(const std::uint8_t in[kHeaderLen]) {
  FrameHeader h;
  std::uint32_t magic = 0;
  std::uint16_t version = 0;
  std::uint16_t header_len = 0;
  std::uint32_t payload_len = 0;
  std::uint32_t flags = 0;

  std::memcpy(&magic, in + 0, 4);
  std::memcpy(&version, in + 4, 2);
  std::memcpy(&header_len, in + 6, 2);
  std::memcpy(&payload_len, in + 8, 4);
  std::memcpy(&flags, in + 12, 4);

  h.magic = ntohl(magic);
  h.version = ntohs(version);
  h.header_len = ntohs(header_len);
  h.payload_len = ntohl(payload_len);
  h.flags = ntohl(flags);

  if (h.magic != kProtocolMagic) {
    return Status::protocol_error("bad magic");
  }
  if (h.version != kProtocolVersion) {
    return Status::protocol_error("unsupported version");
  }
  if (h.header_len != kHeaderLen) {
    return Status::protocol_error("bad header_len");
  }
  if (h.payload_len > kMaxFramePayload) {
    return Status::protocol_error("payload too large (frame)");
  }
  return h;
}

namespace {

#if defined(_WIN32)
// Windows socket error handling
inline Result<void> ensure_winsock() {
  // Lazy, thread-safe Winsock init. Avoid WSACleanup to prevent shutdown ordering issues.
  // 延迟、线程安全地初始化 Winsock；避免 WSACleanup 带来的退出阶段销毁顺序问题。
  static int wsa_rc = [] {
    WSADATA wsaData{};
    return WSAStartup(MAKEWORD(2, 2), &wsaData);
  }();
  if (wsa_rc != 0) {
    return Status::io_error("WSAStartup failed with error: " + std::to_string(wsa_rc));
  }
  return {};
}

inline int get_last_error() {
  return WSAGetLastError();
}
inline bool interrupted(int error) {
  return error == WSAEINTR;
}
#else
// POSIX error handling
inline int get_last_error() {
  return errno;
}
inline bool interrupted(int error) {
  return error == EINTR;
}
#endif

// True when a send()/recv() hit the socket timeout configured below.
inline bool timed_out(int error) {
#if defined(_WIN32)
  return error == WSAEWOULDBLOCK || error == WSAETIMEDOUT;
#else
  return error == EAGAIN || error == EWOULDBLOCK;
#endif
}

// Apply a per-call socket send/receive timeout. A zero duration restores the
// blocking behavior; this also clears a timeout left by a previous call.
Result<void> apply_socket_timeout(SocketHandle fd, bool writing,
                                  std::chrono::milliseconds timeout) {
#if defined(_WIN32)
  DWORD ms = timeout.count() > 0 ? static_cast<DWORD>(timeout.count()) : 0;
  int opt = writing ? SO_SNDTIMEO : SO_RCVTIMEO;
  if (::setsockopt(static_cast<SOCKET>(fd), SOL_SOCKET, opt,
                   reinterpret_cast<const char*>(&ms), static_cast<int>(sizeof(ms))) != 0) {
    return Status::io_error("setsockopt(timeout) failed");
  }
#else
  struct timeval tv{};
  if (timeout.count() > 0) {
    tv.tv_sec = static_cast<time_t>(timeout.count() / 1000);
    tv.tv_usec = static_cast<suseconds_t>((timeout.count() % 1000) * 1000);
  }
  int opt = writing ? SO_SNDTIMEO : SO_RCVTIMEO;
  if (::setsockopt(fd, SOL_SOCKET, opt, &tv, sizeof(tv)) != 0) {
    return Status::io_error("setsockopt(timeout) failed");
  }
#endif
  return {};
}

Result<void> write_all(SocketHandle fd, const std::uint8_t* p, std::size_t n,
                       bool* frame_started,
                       std::chrono::steady_clock::time_point deadline) {
#if defined(_WIN32)
  auto wsa = ensure_winsock();
  if (!wsa.ok()) return wsa;
#endif
#if defined(__APPLE__)
  // Avoid SIGPIPE when writing to a closed socket. macOS offers SO_NOSIGPIPE
  // per socket, but newer XNU (macos-latest runners) rejects it with EINVAL,
  // and merely blocking SIGPIPE does not help: XNU queues the blocked signal
  // and delivers it once the mask is restored (both confirmed by the
  // sigpipe-probe workflow). Where SO_NOSIGPIPE is unavailable, temporarily
  // set SIGPIPE to SIG_IGN for the duration of the send loop; a mutex
  // serializes concurrent writers so the previous handler is restored once.
  static std::mutex sigpipe_ignore_mu;
  int one = 1;
  const bool nosigpipe_ok =
      ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) == 0;
  std::unique_lock<std::mutex> ignore_lock;
  struct sigaction ign {};
  struct sigaction prev_handler {};
  if (!nosigpipe_ok) {
    ignore_lock = std::unique_lock<std::mutex>(sigpipe_ignore_mu);
    sigemptyset(&ign.sa_mask);
    ign.sa_handler = SIG_IGN;
    (void)::sigaction(SIGPIPE, &ign, &prev_handler);
  }
#endif
  while (n != 0) {
    if (deadline.time_since_epoch().count() != 0) {
      auto remaining = deadline - std::chrono::steady_clock::now();
      if (remaining <= std::chrono::milliseconds::zero()) {
        if (*frame_started) {
          return Status::io_error("send() timed out mid-frame");
        }
        return Status::timeout("send() timed out");
      }
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
      if (ms <= std::chrono::milliseconds::zero()) {
        ms = std::chrono::milliseconds(1);
      }
      auto timeout_st = apply_socket_timeout(fd, /*writing=*/true, ms);
      if (!timeout_st.ok()) return timeout_st;
    }
#if defined(_WIN32)
    SOCKET sock = static_cast<SOCKET>(fd);
    int w = ::send(sock, reinterpret_cast<const char*>(p), static_cast<int>(n), 0);
#else
    ssize_t w = ::send(fd, p, n, 0);
#endif
    if (w < 0) {
      int error = get_last_error();
      if (interrupted(error)) continue;
#if defined(__APPLE__)
      if (!nosigpipe_ok) {
        (void)::sigaction(SIGPIPE, &prev_handler, nullptr);
      }
#endif
      if (timed_out(error)) {
        // Only a timeout before the first byte of a frame is safe to retry.
        // Mid-frame the stream is already desynchronized; report a hard error
        // so callers (e.g. QosPipe) close instead of resending the message.
        if (*frame_started) {
          return Status::io_error("send() timed out mid-frame");
        }
        return Status::timeout("send() timed out");
      }
      return Status::io_error("send() failed");
    }
    if (w == 0) {
#if defined(__APPLE__)
      if (!nosigpipe_ok) {
        (void)::sigaction(SIGPIPE, &prev_handler, nullptr);
      }
#endif
      return Status::closed("peer closed");
    }
    *frame_started = true;
    p += static_cast<std::size_t>(w);
    n -= static_cast<std::size_t>(w);
  }
#if defined(__APPLE__)
  if (!nosigpipe_ok) {
    (void)::sigaction(SIGPIPE, &prev_handler, nullptr);
  }
#endif
  return {};
}

Result<void> read_exact(SocketHandle fd, std::uint8_t* p, std::size_t n,
                        bool* frame_started) {
#if defined(_WIN32)
  auto wsa = ensure_winsock();
  if (!wsa.ok()) return wsa;
#endif
  while (n != 0) {
#if defined(_WIN32)
    SOCKET sock = static_cast<SOCKET>(fd);
    int r = ::recv(sock, reinterpret_cast<char*>(p), static_cast<int>(n), 0);
#else
    ssize_t r = ::recv(fd, p, n, 0);
#endif
    if (r < 0) {
      int error = get_last_error();
      if (interrupted(error)) continue;
      if (timed_out(error)) {
        if (*frame_started) {
          return Status::io_error("recv() timed out mid-frame");
        }
        return Status::timeout("recv() timed out");
      }
      return Status::io_error("recv() failed");
    }
    if (r == 0) return Status::closed("peer closed");
    *frame_started = true;
    p += static_cast<std::size_t>(r);
    n -= static_cast<std::size_t>(r);
  }
  return {};
}

}  // namespace

Result<void> write_frame(SocketHandle fd, const Message& msg, std::uint32_t flags,
                         std::chrono::milliseconds timeout) {
  if (msg.size() > kMaxFramePayload) {
    return Status::invalid_argument("message too large; enable fragmentation (todo)");
  }

  auto deadline = timeout.count() > 0
      ? std::chrono::steady_clock::now() + timeout
      : std::chrono::steady_clock::time_point{};

  // Explicitly clear the socket timeout before a "no timeout" send. A prior
  // bounded write may have left SO_SNDTIMEO in place; leaving it set would
  // silently turn this blocking send into a timed-out send.
  if (timeout.count() == 0) {
    auto timeout_st = apply_socket_timeout(fd, /*writing=*/true,
                                           std::chrono::milliseconds(0));
    if (!timeout_st.ok()) return timeout_st;
  }

  FrameHeader h;
  h.magic = kProtocolMagic;
  h.version = kProtocolVersion;
  h.header_len = static_cast<std::uint16_t>(kHeaderLen);
  h.payload_len = static_cast<std::uint32_t>(msg.size());
  h.flags = flags;

  std::uint8_t hdr[kHeaderLen];
  encode_header(h, hdr);
  bool frame_started = false;
  auto st = write_all(fd, hdr, sizeof(hdr), &frame_started, deadline);
  if (!st.ok()) return st;
  if (msg.size() == 0) return {};
  return write_all(fd, msg.data(), msg.size(), &frame_started, deadline);
}

Result<Message> read_frame(SocketHandle fd, std::chrono::milliseconds timeout) {
  auto timeout_st = apply_socket_timeout(fd, /*writing=*/false, timeout);
  if (!timeout_st.ok()) return timeout_st.status();

  std::uint8_t hdr[kHeaderLen];
  bool frame_started = false;
  auto st = read_exact(fd, hdr, sizeof(hdr), &frame_started);
  if (!st.ok()) return st.status();
  auto decoded = decode_header(hdr);
  if (!decoded.ok()) return decoded.status();

  FrameHeader h = decoded.value();
  std::vector<std::uint8_t> buf(h.payload_len);
  if (h.payload_len != 0) {
    // The header was consumed, so a timeout from here on is mid-frame.
    bool payload_started = true;
    st = read_exact(fd, buf.data(), buf.size(), &payload_started);
    if (!st.ok()) return st.status();
  }
  return Message::from_bytes(buf.data(), buf.size());
}

}  // namespace duct::wire

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

  // Fragment payloads larger than one frame into kMaxFramePayload chunks. The
  // reader concatenates them until the kFragFinal chunk. Each chunk carries the
  // same flags so the receiver can identify the message family.
  const std::size_t total = msg.size();
  const std::uint8_t* data = msg.data();
  std::size_t offset = 0;
  const bool fragmented = total > kMaxFramePayload;

  do {
    const std::size_t chunk = std::min(kMaxFramePayload, total - offset);
    const bool last = (offset + chunk == total);

    FrameHeader h;
    h.magic = kProtocolMagic;
    h.version = kProtocolVersion;
    h.header_len = static_cast<std::uint16_t>(kHeaderLen);
    h.payload_len = static_cast<std::uint32_t>(chunk);
    h.flags = flags;
    if (fragmented) {
      h.flags |= to_u32(FrameFlags::kFrag);
      if (last) h.flags |= to_u32(FrameFlags::kFragFinal);
    }

    std::uint8_t hdr[kHeaderLen];
    encode_header(h, hdr);
    bool frame_started = false;
    auto st = write_all(fd, hdr, sizeof(hdr), &frame_started, deadline);
    if (!st.ok()) return st;
    if (chunk != 0) {
      st = write_all(fd, data + offset, chunk, &frame_started, deadline);
      if (!st.ok()) return st;
    }
    offset += chunk;
  } while (offset < total);

  return {};
}

Result<Message> read_frame(SocketHandle fd, std::chrono::milliseconds timeout) {
  auto r = read_frame_with_flags(fd, timeout);
  if (!r.ok()) return r.status();
  return std::move(r.value().message);
}

Result<FrameMessage> read_frame_with_flags(SocketHandle fd,
                                           std::chrono::milliseconds timeout) {
  constexpr std::size_t kMaxReassembledBytes = 64 * 1024 * 1024;  // 64 MiB
  // Zero-length kFrag continuations never grow the reassembly buffer, so the
  // byte cap alone cannot stop a peer from spinning the reader on 16-byte
  // headers forever. Bound the chunk count too: a legitimate sender fills
  // 64 KiB chunks (1024 chunks for a full 64 MiB message), so this only ever
  // rejects malformed streams.
  constexpr std::size_t kMaxFragChunks = 4096;
  auto timeout_st = apply_socket_timeout(fd, /*writing=*/false, timeout);
  if (!timeout_st.ok()) return timeout_st.status();

  std::vector<std::uint8_t> assembled;
  std::uint32_t frame_flags = 0;
  bool first = true;
  std::size_t chunks = 0;
  for (;;) {
    std::uint8_t hdr[kHeaderLen];
    bool frame_started = false;
    auto st = read_exact(fd, hdr, sizeof(hdr), &frame_started);
    if (!st.ok()) return st.status();
    auto decoded = decode_header(hdr);
    if (!decoded.ok()) return decoded.status();

    if (++chunks > kMaxFragChunks) {
      return Status::protocol_error("fragment stream has too many chunks");
    }

    const std::uint32_t payload_len = decoded.value().payload_len;
    const bool is_frag = (decoded.value().flags & to_u32(FrameFlags::kFrag)) != 0;
    const bool is_final = (decoded.value().flags & to_u32(FrameFlags::kFragFinal)) != 0;
    frame_flags = decoded.value().flags;

    if (first) {
      if (!is_frag) {
        if (payload_len > kMaxReassembledBytes) {
          return Status::protocol_error("frame payload too large");
        }
        // Single-frame message.
        std::vector<std::uint8_t> buf(payload_len);
        if (payload_len != 0) {
          bool payload_started = true;
          st = read_exact(fd, buf.data(), buf.size(), &payload_started);
          if (!st.ok()) return st.status();
        }
        return FrameMessage{Message::from_bytes(buf.data(), buf.size()), frame_flags};
      }
      if (payload_len > kMaxReassembledBytes) {
        return Status::protocol_error("fragment stream too large");
      }
      assembled.resize(payload_len);
      if (payload_len != 0) {
        bool payload_started = true;
        st = read_exact(fd, assembled.data(), payload_len, &payload_started);
        if (!st.ok()) return st.status();
      }
      first = false;
      if (is_final) return FrameMessage{Message::from_bytes(assembled.data(), assembled.size()), frame_flags};
      continue;
    }

    // Continuation chunk: grow and append.
    const std::size_t old_size = assembled.size();
    if (old_size + payload_len > kMaxReassembledBytes) {
      return Status::protocol_error("fragment stream too large");
    }
    assembled.resize(old_size + payload_len);
    if (payload_len != 0) {
      bool payload_started = true;
      st = read_exact(fd, assembled.data() + old_size, payload_len, &payload_started);
      if (!st.ok()) return st.status();
    }
    if (is_final) return FrameMessage{Message::from_bytes(assembled.data(), assembled.size()), frame_flags};
  }
}

namespace {

constexpr std::uint8_t kHandshakeKind = 0x01;
constexpr std::size_t kHandshakeLen = 5;

}  // namespace

Result<void> handshake_client(SocketHandle fd, std::uint32_t capabilities,
                              std::chrono::milliseconds timeout) {
  std::uint8_t payload[kHandshakeLen] = {};
  payload[0] = kProtocolVersion;
  std::memcpy(payload + 1, &capabilities, sizeof(capabilities));
  // Fire-and-forget: write the request so a dial() that returns before the peer
  // calls accept() cannot deadlock. The server validates the version and
  // closes the connection on mismatch, so no synchronous reply is needed.
  return write_frame(fd, Message::from_bytes(payload, sizeof(payload)),
                     to_u32(FrameFlags::kCtrl), timeout);
}

Result<std::uint32_t> handshake_server(SocketHandle fd, std::uint32_t server_capabilities,
                                       std::chrono::milliseconds timeout) {
  auto r = read_frame_with_flags(fd, timeout);
  if (!r.ok()) return r.status();
  const auto& sv = r.value().message.as_string_view();
  if (sv.size() != kHandshakeLen) {
    return Status::protocol_error("handshake length mismatch");
  }
  if (static_cast<std::uint8_t>(sv[0]) != kProtocolVersion) {
    return Status::protocol_error("unsupported protocol version");
  }
  std::uint32_t client_caps = 0;
  std::memcpy(&client_caps, sv.data() + 1, sizeof(client_caps));
  std::uint8_t reply[kHandshakeLen] = {};
  reply[0] = kProtocolVersion;
  std::memcpy(reply + 1, &server_capabilities, sizeof(server_capabilities));
  auto ws = write_frame(fd, Message::from_bytes(reply, sizeof(reply)),
                        to_u32(FrameFlags::kCtrl), timeout);
  if (!ws.ok()) return ws.status();
  return client_caps;
}

}  // namespace duct::wire

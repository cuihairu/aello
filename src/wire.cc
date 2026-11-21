#include "duct/wire.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
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

namespace {

// 64-bit network order helpers. ntohll/htonll are not standard (POSIX only
// defines the 32-bit pair; glibc/macOS/WinSock disagree on the 64-bit names),
// so the v2 trailer uses explicit byte shuffling like the 16-bit fields do.
inline std::uint64_t load_be64(const std::uint8_t* p) {
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<std::uint64_t>(p[i]);
  return v;
}

inline void store_be64(std::uint8_t* p, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    p[i] = static_cast<std::uint8_t>(v >> (56 - 8 * i));
  }
}

void encode_base_fields(const FrameHeader& h, std::uint8_t* out) {
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

// Decodes the 16-byte base. `ext` (optional) points at the 16-byte v2 trailer
// that follows the base when header_len == kHeaderLenV2.
Result<FrameHeader> decode_impl(const std::uint8_t* base, const std::uint8_t* ext) {
  FrameHeader h;
  std::uint32_t magic = 0;
  std::uint16_t version = 0;
  std::uint16_t header_len = 0;
  std::uint32_t payload_len = 0;
  std::uint32_t flags = 0;

  std::memcpy(&magic, base + 0, 4);
  std::memcpy(&version, base + 4, 2);
  std::memcpy(&header_len, base + 6, 2);
  std::memcpy(&payload_len, base + 8, 4);
  std::memcpy(&flags, base + 12, 4);

  h.magic = ntohl(magic);
  h.version = ntohs(version);
  h.header_len = ntohs(header_len);
  h.payload_len = ntohl(payload_len);
  h.flags = ntohl(flags);

  if (h.magic != kProtocolMagic) {
    return Status::protocol_error("bad magic");
  }
  if (h.version == kProtocolVersion && h.header_len == kHeaderLen) {
    // v1 frame: no trailer.
  } else if (h.version == kProtocolVersionV2 && h.header_len == kHeaderLenV2) {
    // v2 frame: the reliability trailer follows the base. Without the bytes
    // in hand the stream cannot be trusted, so report a protocol error.
    if (ext == nullptr) {
      return Status::protocol_error("truncated v2 header");
    }
    h.session_id = load_be64(ext + 0);
    h.seq = load_be64(ext + 8);
    if (h.session_id == 0) {
      return Status::protocol_error("v2 frame without session id");
    }
  } else {
    return Status::protocol_error("unsupported version");
  }
  if (h.payload_len > kMaxFramePayload) {
    return Status::protocol_error("payload too large (frame)");
  }
  return h;
}

}  // namespace

void encode_header(const FrameHeader& h, std::uint8_t out[kHeaderLen]) {
  encode_base_fields(h, out);
}

Result<FrameHeader> decode_header(const std::uint8_t in[kHeaderLen]) {
  return decode_impl(in, nullptr);
}

void encode_header_v2(const FrameHeader& h, std::uint8_t out[kHeaderLenV2]) {
  encode_base_fields(h, out);
  store_be64(out + kHeaderLen, h.session_id);
  store_be64(out + kHeaderLen + 8, h.seq);
}

Result<FrameHeader> decode_header_v2(const std::uint8_t in[kHeaderLenV2]) {
  return decode_impl(in, in + kHeaderLen);
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

#if defined(__APPLE__)
// Avoid SIGPIPE when writing to a closed socket. macOS offers SO_NOSIGPIPE per
// socket, but newer XNU (macos-latest runners) rejects it with EINVAL, and
// merely blocking SIGPIPE does not help: XNU queues the blocked signal and
// delivers it once the mask is restored (both confirmed by the sigpipe-probe
// workflow). Where SO_NOSIGPIPE is unavailable, temporarily set SIGPIPE to
// SIG_IGN for the duration of the send call; a mutex serializes concurrent
// writers so the previous handler is restored once.
class SigpipeGuard {
 public:
  explicit SigpipeGuard(SocketHandle fd) {
    int one = 1;
    ok_ = ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) == 0;
    if (!ok_) {
      lock_ = std::unique_lock<std::mutex>(mu());
      sigemptyset(&ign_.sa_mask);
      ign_.sa_handler = SIG_IGN;
      (void)::sigaction(SIGPIPE, &ign_, &prev_);
    }
  }
  ~SigpipeGuard() {
    if (!ok_) (void)::sigaction(SIGPIPE, &prev_, nullptr);
  }

 private:
  static std::mutex& mu() {
    static std::mutex m;
    return m;
  }
  bool ok_ = false;
  std::unique_lock<std::mutex> lock_;
  struct sigaction ign_ {};
  struct sigaction prev_ {};
};
#endif

}  // namespace

// SocketStream: the framing behavior moved behind the Stream interface.
Result<std::size_t> SocketStream::write_some(const void* data, std::size_t len,
                                             std::chrono::milliseconds timeout) {
#if defined(_WIN32)
  auto wsa = ensure_winsock();
  if (!wsa.ok()) return wsa.status();
#endif
  auto st = apply_socket_timeout(fd_, /*writing=*/true, timeout);
  if (!st.ok()) return st.status();
#if defined(__APPLE__)
  SigpipeGuard guard(fd_);
#endif
  for (;;) {
#if defined(_WIN32)
    int w = ::send(static_cast<SOCKET>(fd_), reinterpret_cast<const char*>(data),
                   static_cast<int>(len), 0);
#else
    ssize_t w = ::send(fd_, data, len, 0);
#endif
    if (w < 0) {
      int error = get_last_error();
      if (interrupted(error)) continue;
      if (timed_out(error)) return Status::timeout("send() timed out");
      return Status::io_error("send() failed");
    }
    if (w == 0) return Status::closed("peer closed");
    return static_cast<std::size_t>(w);
  }
}

Result<std::size_t> SocketStream::read_some(void* buf, std::size_t cap,
                                            std::chrono::milliseconds timeout) {
#if defined(_WIN32)
  auto wsa = ensure_winsock();
  if (!wsa.ok()) return wsa.status();
#endif
  auto st = apply_socket_timeout(fd_, /*writing=*/false, timeout);
  if (!st.ok()) return st.status();
  for (;;) {
#if defined(_WIN32)
    int r = ::recv(static_cast<SOCKET>(fd_), reinterpret_cast<char*>(buf),
                   static_cast<int>(cap), 0);
#else
    ssize_t r = ::recv(fd_, buf, cap, 0);
#endif
    if (r < 0) {
      int error = get_last_error();
      if (interrupted(error)) continue;
      if (timed_out(error)) return Status::timeout("recv() timed out");
      return Status::io_error("recv() failed");
    }
    // 0 is a clean EOF; the caller (read_exact) classifies it as kClosed.
    return static_cast<std::size_t>(r);
  }
}

void SocketStream::shutdown() {
#if defined(_WIN32)
  ::shutdown(static_cast<SOCKET>(fd_), SD_BOTH);
#else
  ::shutdown(fd_, SHUT_RDWR);
#endif
}

namespace {

// Framing over a Stream. `frame_started` tracks whether any byte of the current
// frame already moved: a timeout after that point desynchronizes the byte
// stream, so it is a hard error (the connection must be closed) rather than a
// retryable timeout.
Result<void> write_all(Stream& stream, const std::uint8_t* p, std::size_t n,
                       bool* frame_started,
                       std::chrono::steady_clock::time_point deadline) {
  while (n != 0) {
    std::chrono::milliseconds ms(0);
    if (deadline.time_since_epoch().count() != 0) {
      auto remaining = deadline - std::chrono::steady_clock::now();
      if (remaining <= std::chrono::milliseconds::zero()) {
        if (*frame_started) {
          return Status::io_error("send() timed out mid-frame");
        }
        return Status::timeout("send() timed out");
      }
      ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
      // duration_cast truncates a sub-millisecond remaining wait to 0, which
      // would clear the socket timeout and turn the send into an unbounded
      // blocking call. Clamp to 1ms so the deadline still bounds the write.
      if (ms <= std::chrono::milliseconds::zero()) {
        ms = std::chrono::milliseconds(1);
      }
    }
    auto r = stream.write_some(p, n, ms);
    if (!r.ok()) {
      if (r.status().code() == StatusCode::kTimeout) {
        // Only a timeout before the first byte of a frame is safe to retry.
        // Mid-frame the stream is already desynchronized; report a hard error
        // so callers (e.g. QosPipe) close instead of resending the message.
        if (*frame_started) {
          return Status::io_error("send() timed out mid-frame");
        }
        return Status::timeout("send() timed out");
      }
      return r.status();
    }
    if (r.value() == 0) return Status::closed("peer closed");
    *frame_started = true;
    p += r.value();
    n -= r.value();
  }
  return {};
}

Result<void> read_exact(Stream& stream, std::uint8_t* p, std::size_t n,
                        bool* frame_started, std::chrono::milliseconds timeout) {
  while (n != 0) {
    auto r = stream.read_some(p, n, timeout);
    if (!r.ok()) {
      if (r.status().code() == StatusCode::kTimeout) {
        if (*frame_started) {
          return Status::io_error("recv() timed out mid-frame");
        }
        return Status::timeout("recv() timed out");
      }
      return r.status();
    }
    if (r.value() == 0) return Status::closed("peer closed");
    *frame_started = true;
    p += r.value();
    n -= r.value();
  }
  return {};
}

}  // namespace

Result<void> write_frame(Stream& stream, const Message& msg, std::uint32_t flags,
                         std::chrono::milliseconds timeout, const FrameMeta& meta) {
  auto deadline = timeout.count() > 0
      ? std::chrono::steady_clock::now() + timeout
      : std::chrono::steady_clock::time_point{};

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
    h.payload_len = static_cast<std::uint32_t>(chunk);
    h.flags = flags;
    if (fragmented) {
      h.flags |= to_u32(FrameFlags::kFrag);
      if (last) h.flags |= to_u32(FrameFlags::kFragFinal);
    }
    // A nonzero session id selects the v2 encoding: the same session/seq repeats
    // on every fragment chunk so the reassembled FrameMessage keeps them.
    if (meta.session_id != 0) {
      h.version = kProtocolVersionV2;
      h.header_len = static_cast<std::uint16_t>(kHeaderLenV2);
      h.session_id = meta.session_id;
      h.seq = meta.seq;
    } else {
      h.version = kProtocolVersion;
      h.header_len = static_cast<std::uint16_t>(kHeaderLen);
    }

    std::uint8_t hdr[kHeaderLenV2];
    if (meta.session_id != 0) {
      encode_header_v2(h, hdr);
    } else {
      encode_header(h, hdr);
    }
    bool frame_started = false;
    auto st = write_all(stream, hdr, h.header_len, &frame_started, deadline);
    if (!st.ok()) return st;
    if (chunk != 0) {
      st = write_all(stream, data + offset, chunk, &frame_started, deadline);
      if (!st.ok()) return st;
    }
    offset += chunk;
  } while (offset < total);

  return {};
}

Result<Message> read_frame(Stream& stream, std::chrono::milliseconds timeout) {
  auto r = read_frame_with_flags(stream, timeout);
  if (!r.ok()) return r.status();
  return std::move(r.value().message);
}

Result<FrameMessage> read_frame_with_flags(Stream& stream,
                                           std::chrono::milliseconds timeout) {
  constexpr std::size_t kMaxReassembledBytes = 64 * 1024 * 1024;  // 64 MiB
  // Zero-length kFrag continuations never grow the reassembly buffer, so the
  // byte cap alone cannot stop a peer from spinning the reader on 16-byte
  // headers forever. Bound the chunk count too: a legitimate sender fills
  // 64 KiB chunks (1024 chunks for a full 64 MiB message), so this only ever
  // rejects malformed streams.
  constexpr std::size_t kMaxFragChunks = 4096;

  std::vector<std::uint8_t> assembled;
  std::uint32_t frame_flags = 0;
  std::uint64_t frame_session = 0;
  std::uint64_t frame_seq = 0;
  bool first = true;
  std::size_t chunks = 0;
  for (;;) {
    // Read the 16-byte base first, then the 16-byte v2 trailer when the base
    // header announces header_len == kHeaderLenV2. The base read is the single
    // place a partial header can strand the stream, so it alone owns the
    // mid-frame timeout classification.
    std::uint8_t hdr[kHeaderLenV2];
    bool frame_started = false;
    auto st = read_exact(stream, hdr, kHeaderLen, &frame_started, timeout);
    if (!st.ok()) return st.status();
    std::uint16_t header_len = 0;
    std::memcpy(&header_len, hdr + 6, 2);
    header_len = ntohs(header_len);
    if (header_len == kHeaderLenV2) {
      st = read_exact(stream, hdr + kHeaderLen, kHeaderLenV2 - kHeaderLen,
                      &frame_started, timeout);
      if (!st.ok()) return st.status();
    }
    auto decoded = header_len == kHeaderLenV2 ? decode_header_v2(hdr) : decode_header(hdr);
    if (!decoded.ok()) return decoded.status();
    const FrameHeader h = decoded.value();
    if (++chunks > kMaxFragChunks) {
      return Status::protocol_error("fragment stream has too many chunks");
    }

    const std::uint32_t payload_len = h.payload_len;
    const bool is_frag = (h.flags & to_u32(FrameFlags::kFrag)) != 0;
    const bool is_final = (h.flags & to_u32(FrameFlags::kFragFinal)) != 0;
    frame_flags = h.flags;
    frame_session = h.session_id;
    frame_seq = h.seq;

    if (first) {
      if (!is_frag) {
        if (payload_len > kMaxReassembledBytes) {
          return Status::protocol_error("frame payload too large");
        }
        // Single-frame message.
        std::vector<std::uint8_t> buf(payload_len);
        if (payload_len != 0) {
          bool payload_started = true;
          st = read_exact(stream, buf.data(), buf.size(), &payload_started, timeout);
          if (!st.ok()) return st.status();
        }
        return FrameMessage{Message::from_bytes(buf.data(), buf.size()), frame_flags,
                            frame_session, frame_seq};
      }
      if (payload_len > kMaxReassembledBytes) {
        return Status::protocol_error("fragment stream too large");
      }
      assembled.resize(payload_len);
      if (payload_len != 0) {
        bool payload_started = true;
        st = read_exact(stream, assembled.data(), payload_len, &payload_started, timeout);
        if (!st.ok()) return st.status();
      }
      first = false;
      if (is_final) {
        return FrameMessage{Message::from_bytes(assembled.data(), assembled.size()),
                            frame_flags, frame_session, frame_seq};
      }
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
      st = read_exact(stream, assembled.data() + old_size, payload_len, &payload_started, timeout);
      if (!st.ok()) return st.status();
    }
    if (is_final) {
      return FrameMessage{Message::from_bytes(assembled.data(), assembled.size()),
                          frame_flags, frame_session, frame_seq};
    }
  }
}

namespace {

constexpr std::uint8_t kHandshakeKind = 0x01;
constexpr std::size_t kHandshakeLen = 13;

// Parse the fixed handshake payload: [0]=version, [1..5)=caps (host order,
// memcpy — matches the historical layout), [5..13)=session id (BE).
Result<std::pair<std::uint32_t, std::uint64_t>> parse_handshake_payload(
    const std::string_view& sv) {
  if (sv.size() != kHandshakeLen) {
    return Status::protocol_error("handshake length mismatch");
  }
  if (static_cast<std::uint8_t>(sv[0]) != kProtocolVersion) {
    return Status::protocol_error("unsupported protocol version");
  }
  std::uint32_t caps = 0;
  std::memcpy(&caps, sv.data() + 1, sizeof(caps));
  return std::make_pair(caps, load_be64(reinterpret_cast<const std::uint8_t*>(sv.data()) + 5));
}

void build_handshake_payload(std::uint8_t payload[kHandshakeLen], std::uint32_t caps,
                             std::uint64_t session_id) {
  payload[0] = kProtocolVersion;
  std::memcpy(payload + 1, &caps, sizeof(caps));
  store_be64(payload + 5, session_id);
}

}  // namespace

Result<void> handshake_client(Stream& stream, std::uint32_t capabilities,
                              std::uint64_t session_id,
                              std::chrono::milliseconds timeout) {
  std::uint8_t payload[kHandshakeLen] = {};
  build_handshake_payload(payload, capabilities, session_id);
  // Fire-and-forget: write the request so a dial() that returns before the peer
  // calls accept() cannot deadlock. The server validates the version and
  // closes the connection on mismatch, so no synchronous reply is needed.
  return write_frame(stream, Message::from_bytes(payload, sizeof(payload)),
                     to_u32(FrameFlags::kCtrl), timeout);
}

Result<std::uint32_t> handshake_client_sync(Stream& stream, std::uint32_t capabilities,
                                            std::chrono::milliseconds timeout,
                                            std::uint64_t session_id) {
  auto ws = handshake_client(stream, capabilities, session_id, timeout);
  if (!ws.ok()) return ws.status();
  // Read the server's reply. The dial needs the negotiated capabilities before
  // its first frame (reliability chooses v2 vs v1), so this is a synchronous
  // round-trip bounded by `timeout`.
  auto r = read_frame_with_flags(stream, timeout);
  if (!r.ok()) return r.status();
  auto parsed = parse_handshake_payload(r.value().message.as_string_view());
  if (!parsed.ok()) return parsed.status();
  return parsed.value().first;
}

Result<HandshakeResult> handshake_server(Stream& stream, std::uint32_t server_capabilities,
                                         std::chrono::milliseconds timeout) {
  auto r = read_frame_with_flags(stream, timeout);
  if (!r.ok()) return r.status();
  auto parsed = parse_handshake_payload(r.value().message.as_string_view());
  if (!parsed.ok()) return parsed.status();
  std::uint8_t reply[kHandshakeLen] = {};
  build_handshake_payload(reply, server_capabilities, /*session_id=*/0);
  auto ws = write_frame(stream, Message::from_bytes(reply, sizeof(reply)),
                        to_u32(FrameFlags::kCtrl), timeout);
  if (!ws.ok()) return ws.status();
  return HandshakeResult{parsed.value().first, parsed.value().second};
}

Result<HandshakeResult> handshake_server_stoppable(
    Stream& stream, std::uint32_t server_capabilities,
    std::chrono::milliseconds timeout, const std::atomic<bool>& stop) {
  // Slice the handshake read into 100ms windows: the stop flag must be
  // observed even while no bytes arrive (a blocked recv is not woken by
  // close() on POSIX). The overall handshake deadline still bounds the wait.
  // Note: a timeout that fires on a partial frame poisons this connection —
  // the listener discards it, same as the unsliced variant's midframe timeout.
  const bool has_deadline = timeout.count() > 0;
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    if (stop.load(std::memory_order_acquire)) {
      return Status::closed("listener closed");
    }
    std::chrono::milliseconds slice(100);
    if (has_deadline) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) return Status::timeout("handshake timed out");
      slice = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
      if (slice <= std::chrono::milliseconds::zero()) {
        return Status::timeout("handshake timed out");
      }
      if (slice > std::chrono::milliseconds(100)) slice = std::chrono::milliseconds(100);
    }
    auto r = read_frame_with_flags(stream, slice);
    if (r.ok()) {
      auto parsed = parse_handshake_payload(r.value().message.as_string_view());
      if (!parsed.ok()) return parsed.status();
      if (stop.load(std::memory_order_acquire)) {
        return Status::closed("listener closed");
      }
      std::uint8_t reply[kHandshakeLen] = {};
      build_handshake_payload(reply, server_capabilities, /*session_id=*/0);
      auto ws = write_frame(stream, Message::from_bytes(reply, sizeof(reply)),
                            to_u32(FrameFlags::kCtrl), timeout);
      if (!ws.ok()) return ws.status();
      return HandshakeResult{parsed.value().first, parsed.value().second};
    }
    if (r.status().code() == StatusCode::kTimeout) continue;  // slice elapsed
    return r.status();
  }
}

}  // namespace duct::wire

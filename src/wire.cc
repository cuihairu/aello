#include "duct/wire.h"

#include <cerrno>
#include <cstdint>
#include <cstring>

#include "duct/protocol.h"

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <csignal>
#include <pthread.h>
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

Result<void> write_all(SocketHandle fd, const std::uint8_t* p, std::size_t n) {
#if defined(_WIN32)
  auto wsa = ensure_winsock();
  if (!wsa.ok()) return wsa;
#endif
#if defined(__APPLE__)
  // Avoid SIGPIPE on macOS when writing to a closed socket.
  int one = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
  while (n != 0) {
#if defined(__APPLE__)
    // Belt-and-suspenders: also mask SIGPIPE for the duration of the send so a
    // write to a closed peer cannot raise the signal even if SO_NOSIGPIPE is
    // unavailable or ineffective. The mask is thread-local, so other threads
    // are unaffected.
    sigset_t block, old_mask;
    sigemptyset(&block);
    sigaddset(&block, SIGPIPE);
    (void)::pthread_sigmask(SIG_BLOCK, &block, &old_mask);
#endif
#if defined(_WIN32)
    SOCKET sock = static_cast<SOCKET>(fd);
    int w = ::send(sock, reinterpret_cast<const char*>(p), static_cast<int>(n), 0);
#else
    ssize_t w = ::send(fd, p, n, 0);
#endif
#if defined(__APPLE__)
    (void)::pthread_sigmask(SIG_SETMASK, &old_mask, nullptr);
#endif
    if (w < 0) {
      int error = get_last_error();
      if (interrupted(error)) continue;
      return Status::io_error("send() failed");
    }
    if (w == 0) return Status::closed("peer closed");
    p += static_cast<std::size_t>(w);
    n -= static_cast<std::size_t>(w);
  }
  return {};
}

Result<void> read_exact(SocketHandle fd, std::uint8_t* p, std::size_t n) {
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
      return Status::io_error("recv() failed");
    }
    if (r == 0) return Status::closed("peer closed");
    p += static_cast<std::size_t>(r);
    n -= static_cast<std::size_t>(r);
  }
  return {};
}

}  // namespace

Result<void> write_frame(SocketHandle fd, const Message& msg, std::uint32_t flags) {
  if (msg.size() > kMaxFramePayload) {
    return Status::invalid_argument("message too large; enable fragmentation (todo)");
  }

  FrameHeader h;
  h.magic = kProtocolMagic;
  h.version = kProtocolVersion;
  h.header_len = static_cast<std::uint16_t>(kHeaderLen);
  h.payload_len = static_cast<std::uint32_t>(msg.size());
  h.flags = flags;

  std::uint8_t hdr[kHeaderLen];
  encode_header(h, hdr);
  auto st = write_all(fd, hdr, sizeof(hdr));
  if (!st.ok()) return st;
  return write_all(fd, msg.data(), msg.size());
}

Result<Message> read_frame(SocketHandle fd) {
  std::uint8_t hdr[kHeaderLen];
  auto st = read_exact(fd, hdr, sizeof(hdr));
  if (!st.ok()) return st.status();
  auto decoded = decode_header(hdr);
  if (!decoded.ok()) return decoded.status();

  FrameHeader h = decoded.value();
  std::vector<std::uint8_t> buf(h.payload_len);
  if (h.payload_len != 0) {
    st = read_exact(fd, buf.data(), buf.size());
    if (!st.ok()) return st.status();
  }
  return Message::from_bytes(buf.data(), buf.size());
}

}  // namespace duct::wire

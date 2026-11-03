#include "duct/duct.h"

#include "duct/protocol.h"
#include "duct/socket_utils.h"
#include "duct/wire.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace duct {
namespace {

#if defined(_WIN32)
static Result<void> ensure_winsock() {
  // Lazy, thread-safe Winsock init. We intentionally don't call WSACleanup here:
  // global teardown can race with other static destructors using sockets.
  // 延迟、线程安全地初始化 Winsock；这里刻意不调用 WSACleanup，避免进程退出阶段与其他静态析构竞态。
  static int wsa_rc = [] {
    WSADATA wsaData{};
    return WSAStartup(MAKEWORD(2, 2), &wsaData);
  }();
  if (wsa_rc != 0) {
    return Status::io_error("WSAStartup failed with error: " + std::to_string(wsa_rc));
  }
  return {};
}
#endif

// Cross-platform socket close
static void close_socket(wire::SocketHandle fd) {
#if defined(_WIN32)
  closesocket(static_cast<SOCKET>(fd));
#else
  ::close(fd);
#endif
}

class TcpPipe final : public Pipe {
 public:
  explicit TcpPipe(wire::SocketHandle fd) : fd_(fd) {}
  ~TcpPipe() override { close(); }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    if (fd_ == wire::kInvalidSocket) return Status::closed("pipe closed");
    return wire::write_frame(fd_, msg, channel_id_to_flags(opt.channel_id), opt.timeout);
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (fd_ == wire::kInvalidSocket) return Status::closed("pipe closed");
    auto r = wire::read_frame_with_flags(fd_, opt.timeout);
    if (!r.ok()) return r.status();
    if (opt.channel_id != 0 &&
        flags_to_channel_id(r.value().flags) != opt.channel_id) {
      // Not our channel. Re-read until a matching frame or timeout.
      return recv(opt);
    }
    return std::move(r.value().message);
  }

  void close() override {
    if (fd_ != wire::kInvalidSocket) {
      close_socket(fd_);
      fd_ = wire::kInvalidSocket;
    }
  }

 private:
  wire::SocketHandle fd_ = wire::kInvalidSocket;
};

class TcpListener final : public Listener {
 public:
  TcpListener(wire::SocketHandle fd, std::string host, std::uint16_t port)
      : fd_(fd), host_(std::move(host)), port_(port) {}
  ~TcpListener() override { close(); }

  Result<std::unique_ptr<Pipe>> accept() override {
    if (fd_ == wire::kInvalidSocket) return Status::closed("listener closed");
#if defined(_WIN32)
    auto cfd = static_cast<wire::SocketHandle>(::accept(static_cast<SOCKET>(fd_), nullptr, nullptr));
#else
    auto cfd = static_cast<wire::SocketHandle>(::accept(fd_, nullptr, nullptr));
#endif
    if (cfd == wire::kInvalidSocket) {
      return Status::io_error("accept() failed");
    }
    return std::unique_ptr<Pipe>(new TcpPipe(cfd));
  }

  Result<std::string> local_address() const override {
    if (fd_ == wire::kInvalidSocket) return Status::closed("listener closed");
    return std::string("tcp://") + (host_.empty() ? "127.0.0.1" : host_) + ":" + std::to_string(port_);
  }

  void close() override {
    if (fd_ != wire::kInvalidSocket) {
      close_socket(fd_);
      fd_ = wire::kInvalidSocket;
    }
  }

 private:
  wire::SocketHandle fd_ = wire::kInvalidSocket;
  std::string host_;
  std::uint16_t port_ = 0;
};

// Non-blocking connect bounded by `timeout`. Returns kTimeout on expiry.
// On failure the caller must still close `fd` (we only report, we do not own it).
static Result<void> connect_with_timeout(wire::SocketHandle fd, const sockaddr* addr, int addrlen,
                                         std::chrono::milliseconds timeout) {
  auto st = socket_utils::set_nonblocking(fd, true);
  if (!st.ok()) return st.status();

  int rc = 0;
#if defined(_WIN32)
  rc = ::connect(static_cast<SOCKET>(fd), addr, addrlen);
#else
  rc = ::connect(fd, addr, static_cast<socklen_t>(addrlen));
#endif
  if (rc == 0) {
    (void)socket_utils::set_nonblocking(fd, false);
    return {};
  }

#if defined(_WIN32)
  int err = ::WSAGetLastError();
  if (err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) {
    return Status::io_error("connect() failed");
  }
#else
  if (errno != EINPROGRESS && errno != EALREADY && errno != EINTR) {
    return Status::io_error("connect() failed");
  }
#endif

  st = socket_utils::wait_writable(fd, timeout);
  if (!st.ok()) {
    if (st.status().code() == StatusCode::kTimeout) {
      return Status::timeout("connect() timed out");
    }
    return st.status();
  }

  int so_error = 0;
  socklen_t errlen = sizeof(so_error);
#if defined(_WIN32)
  if (::getsockopt(static_cast<SOCKET>(fd), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&so_error), &errlen) < 0 ||
      so_error != 0) {
    return Status::io_error("connect() failed: SO_ERROR=" + std::to_string(so_error));
  }
#else
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &errlen) < 0 || so_error != 0) {
    return Status::io_error("connect() failed: SO_ERROR=" +
                            std::to_string(so_error) + " " + std::strerror(so_error));
  }
#endif

  (void)socket_utils::set_nonblocking(fd, false);
  return {};
}

static Result<wire::SocketHandle> connect_tcp(const std::string& host, std::uint16_t port,
                                              std::chrono::milliseconds timeout) {
#if defined(_WIN32)
  auto wsa = ensure_winsock();
  if (!wsa.ok()) return wsa.status();
#endif
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo* res = nullptr;
  std::string port_s = std::to_string(port);
  int rc = ::getaddrinfo(host.c_str(), port_s.c_str(), &hints, &res);
  if (rc != 0) {
    return Status::io_error("getaddrinfo() failed");
  }

  const auto deadline = timeout.count() > 0
      ? std::chrono::steady_clock::now() + timeout
      : std::chrono::steady_clock::time_point{};

  wire::SocketHandle fd = wire::kInvalidSocket;
  for (auto* p = res; p != nullptr; p = p->ai_next) {
    fd = static_cast<wire::SocketHandle>(::socket(p->ai_family, p->ai_socktype, p->ai_protocol));
    if (fd == wire::kInvalidSocket) continue;

    int one = 1;
#if defined(_WIN32)
    SOCKET sock = static_cast<SOCKET>(fd);
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
#else
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#endif

    if (timeout.count() > 0) {
      const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - std::chrono::steady_clock::now());
      if (remaining <= std::chrono::milliseconds::zero()) {
        close_socket(fd);
        ::freeaddrinfo(res);
        return Status::timeout("connect deadline expired");
      }
      auto connected = connect_with_timeout(fd, p->ai_addr, static_cast<int>(p->ai_addrlen), remaining);
      if (connected.ok()) {
        break;
      }
      close_socket(fd);
      // A bounded dial stops at the first address that times out. Other hard
      // errors (e.g. connection refused) fall through to the next address, as
      // the blocking path does.
      if (connected.status().code() == StatusCode::kTimeout) {
        ::freeaddrinfo(res);
        return connected.status();
      }
      fd = wire::kInvalidSocket;
      continue;
    }

#if defined(_WIN32)
    if (::connect(static_cast<SOCKET>(fd), p->ai_addr, static_cast<int>(p->ai_addrlen)) == 0) {
#else
    if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
#endif
      break;
    }
    close_socket(fd);
    fd = wire::kInvalidSocket;
  }

  ::freeaddrinfo(res);
  if (fd == wire::kInvalidSocket) {
    return Status::io_error("connect() failed");
  }
  return fd;
}

static Result<wire::SocketHandle> listen_tcp(const std::string& host, std::uint16_t port, int backlog) {
#if defined(_WIN32)
  auto wsa = ensure_winsock();
  if (!wsa.ok()) return wsa.status();
#endif
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  struct addrinfo* res = nullptr;
  std::string port_s = std::to_string(port);
  const char* node = host.empty() ? nullptr : host.c_str();
  int rc = ::getaddrinfo(node, port_s.c_str(), &hints, &res);
  if (rc != 0) {
    return Status::io_error("getaddrinfo() failed");
  }

  wire::SocketHandle fd = wire::kInvalidSocket;
  for (auto* p = res; p != nullptr; p = p->ai_next) {
    fd = static_cast<wire::SocketHandle>(::socket(p->ai_family, p->ai_socktype, p->ai_protocol));
    if (fd == wire::kInvalidSocket) continue;

    int one = 1;
#if defined(_WIN32)
    SOCKET sock = static_cast<SOCKET>(fd);
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
#else
    (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#endif

#if defined(_WIN32)
    if (::bind(static_cast<SOCKET>(fd), p->ai_addr, static_cast<int>(p->ai_addrlen)) == 0 &&
        ::listen(static_cast<SOCKET>(fd), backlog) == 0) {
#else
    if (::bind(fd, p->ai_addr, p->ai_addrlen) == 0 && ::listen(fd, backlog) == 0) {
#endif
      break;
    }
    close_socket(fd);
    fd = wire::kInvalidSocket;
  }

  ::freeaddrinfo(res);
  if (fd == wire::kInvalidSocket) {
    return Status::io_error("bind/listen failed");
  }
  return fd;
}

}  // namespace

Result<std::unique_ptr<Listener>> tcp_listen(const TcpAddress& addr, const ListenOptions& opt) {
  auto fd = listen_tcp(addr.host, addr.port, opt.backlog);
  if (!fd.ok()) return fd.status();

  // Determine effective port (supports binding to port 0).
  std::uint16_t effective_port = addr.port;
  if (effective_port == 0) {
    sockaddr_storage ss{};
    socklen_t slen = sizeof(ss);
#if defined(_WIN32)
    if (::getsockname(static_cast<SOCKET>(fd.value()), reinterpret_cast<sockaddr*>(&ss), &slen) == 0) {
#else
    if (::getsockname(fd.value(), reinterpret_cast<sockaddr*>(&ss), &slen) == 0) {
#endif
      if (ss.ss_family == AF_INET) {
        auto* sin = reinterpret_cast<sockaddr_in*>(&ss);
        effective_port = ntohs(sin->sin_port);
      } else if (ss.ss_family == AF_INET6) {
        auto* sin6 = reinterpret_cast<sockaddr_in6*>(&ss);
        effective_port = ntohs(sin6->sin6_port);
      }
    }
  }

  return std::unique_ptr<Listener>(new TcpListener(fd.value(), addr.host, effective_port));
}

Result<std::unique_ptr<Pipe>> tcp_dial(const TcpAddress& addr, const DialOptions& opt) {
  if (opt.timeout.count() < 0) {
    return Status::invalid_argument("dial timeout must not be negative");
  }
  auto fd = connect_tcp(addr.host, addr.port, opt.timeout);
  if (!fd.ok()) return fd.status();
  return std::unique_ptr<Pipe>(new TcpPipe(fd.value()));
}

}  // namespace duct

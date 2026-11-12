#include "duct/duct.h"

#include "duct/protocol.h"
#include "duct/socket_utils.h"
#include "duct/wire.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

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

// Wake any send/recv blocked on the socket without releasing the fd number:
// a blocked ::close(fd) does not interrupt the syscall (the kernel holds the
// file reference), and once the number is freed a new connection can reuse
// it mid-read. shutdown() delivers the wake-up while the fd stays owned.
static void shutdown_socket(wire::SocketHandle fd) {
#if defined(_WIN32)
  ::shutdown(static_cast<SOCKET>(fd), SD_BOTH);
#else
  ::shutdown(fd, SHUT_RDWR);
#endif
}

// Socket ownership shared between the pipe and any in-flight send/recv. The
// fd is only released when no io is registered against it, so close() can
// shutdown() a blocked syscall without racing fd reuse. The state is shared so
// an io on another thread can outlive the pipe object and close the socket
// when the last one unwinds.
struct PipeIoState {
  std::mutex mu;
  std::condition_variable cv;
  wire::SocketHandle fd = wire::kInvalidSocket;
  bool closing = false;
  int active = 0;
};

class TcpPipe final : public Pipe {
 public:
  // handshake_ready: true when the server side already completed negotiation in
  // accept(). The client side performs it lazily on the first send/recv so a
  // blocking dial() does not deadlock before the peer calls accept().
  TcpPipe(wire::SocketHandle fd, bool handshake_ready = true)
      : io_(std::make_shared<PipeIoState>()), handshake_ready_(handshake_ready) {
    io_->fd = fd;
    // Send the client's handshake eagerly (write-only, fire-and-forget). The
    // peer's accept() reads it whenever it runs, so this never deadlocks even
    // when accept() is called after dial() returns.
    if (!handshake_ready_) {
      auto hs = wire::handshake_client(io_->fd, /*capabilities=*/0);
      if (!hs.ok()) {
        // Same discipline as close(): shutdown first, and let the last
        // registered io (or close()) release the fd.
        std::lock_guard<std::mutex> lock(io_->mu);
        if (!io_->closing) {
          io_->closing = true;
          shutdown_socket(io_->fd);
        }
      } else {
        handshake_ready_ = true;
      }
    }
  }
  ~TcpPipe() override {
    close();
    // Wait for already-registered io to unwind before the shared state dies.
    std::unique_lock<std::mutex> lock(io_->mu);
    io_->cv.wait_for(lock, std::chrono::seconds(2), [&] { return io_->active == 0; });
  }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    wire::SocketHandle fd;
    if (!io_begin(&fd)) return Status::closed("pipe closed");
    IoGuard guard{this};
    auto hs = ensure_handshake(fd);
    if (!hs.ok()) return hs;
    return wire::write_frame(fd, msg, channel_id_to_flags(opt.channel_id), opt.timeout);
  }

  Result<wire::FrameMessage> recv_frame(const RecvOptions& opt) override {
    wire::SocketHandle fd;
    if (!io_begin(&fd)) return Status::closed("pipe closed");
    IoGuard guard{this};
    auto hs = ensure_handshake(fd);
    if (!hs.ok()) return hs.status();
    // Serialize socket reads: channel demux is designed for concurrent recv()
    // on different channels, and a frame read is not atomic on the wire.
    std::lock_guard<std::mutex> lock(read_mu_);
    // Transparently consume the server's kCtrl handshake reply; it is protocol
    // metadata, not an application frame.
    for (;;) {
      auto r = wire::read_frame_with_flags(fd, opt.timeout);
      if (!r.ok()) return r.status();
      if ((r.value().flags & to_u32(FrameFlags::kCtrl)) == 0) return r;
    }
  }

  Result<Message> recv(const RecvOptions& opt) override {
    // Track the deadline across the filtering loop so a peer that keeps sending
    // frames for other channels cannot extend the wait indefinitely.
    const bool has_deadline = opt.timeout.count() > 0;
    const auto deadline = std::chrono::steady_clock::now() + opt.timeout;
    for (;;) {
      // First serve any frame buffered for the requested channel so a message
      // read while waiting on another channel is not lost.
      {
        std::lock_guard<std::mutex> lock(ch_mu_);
        auto it = pending_.find(opt.channel_id);
        if (it != pending_.end() && !it->second.empty()) {
          auto msg = std::move(it->second.front().message);
          it->second.erase(it->second.begin());
          if (it->second.empty()) pending_.erase(it);
          pending_bytes_ -= msg.size();
          if (msg.size() == 0) --pending_zero_len_;
          return msg;
        }
      }
      // Bound the remaining wait to the original deadline. recv_frame applies
      // its own socket timeout; a zero remaining means we already exhausted it.
      RecvOptions frame_opt = opt;
      if (has_deadline) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) return Status::timeout("recv timed out");
        frame_opt.timeout = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        // duration_cast truncates a remaining sub-millisecond wait to 0, which
        // would clear SO_RCVTIMEO and turn the read into an unbounded blocking
        // recv(). Clamp to 1ms so the wall-clock deadline still bounds the wait.
        if (frame_opt.timeout <= std::chrono::milliseconds::zero()) {
          frame_opt.timeout = std::chrono::milliseconds(1);
        }
      }
      auto r = recv_frame(frame_opt);
      if (!r.ok()) {
        // Another thread may have buffered a matching frame for this channel
        // while we were blocked in recv_frame (e.g. the peer kept sending on
        // other channels). Serve it before surfacing the timeout/error.
        std::lock_guard<std::mutex> lock(ch_mu_);
        auto it = pending_.find(opt.channel_id);
        if (it != pending_.end() && !it->second.empty()) {
          auto msg = std::move(it->second.front().message);
          it->second.erase(it->second.begin());
          if (it->second.empty()) pending_.erase(it);
          pending_bytes_ -= msg.size();
          if (msg.size() == 0) --pending_zero_len_;
          return msg;
        }
        return r.status();
      }
      if (opt.channel_id == 0 ||
          flags_to_channel_id(r.value().flags) == opt.channel_id) {
        return std::move(r.value().message);
      }
      // Not our channel: stash it for the matching consumer and keep reading.
      {
        std::lock_guard<std::mutex> lock(ch_mu_);
        // Bound the buffered frames so unconsumed channels cannot grow memory
        // without limit. The cap is shared across channels: a per-channel
        // ceiling would let a peer multiply it by the number of channel_ids it
        // chooses to spray (channel ids come from the wire, not us). 64 MiB of
        // pending frames is a generous ceiling for the wire-level max
        // reassembled size. Zero-length frames charge no bytes and each map
        // node / vector slot costs real memory regardless of payload, so the
        // buffered-channel count and the zero-length frame count are capped as
        // well: empty frames sprayed on many channel ids would otherwise stash
        // unbounded entries while the byte counter stays at zero.
        const std::uint32_t stash_chan = flags_to_channel_id(r.value().flags);
        const bool empty_frame = r.value().message.size() == 0;
        if (empty_frame && pending_zero_len_ >= kMaxPendingZeroLenFrames) {
          return Status::queue_full("channel recv buffer full");
        }
        auto it = pending_.find(stash_chan);
        if (it == pending_.end()) {
          if (pending_.size() >= kMaxPendingChannels) {
            return Status::queue_full("too many pending channels");
          }
          it = pending_.emplace(stash_chan, std::vector<wire::FrameMessage>{}).first;
        }
        pending_bytes_ += r.value().message.size();
        if (pending_bytes_ > kMaxPendingBytes) {
          pending_bytes_ -= r.value().message.size();
          if (it->second.empty()) pending_.erase(it);
          return Status::queue_full("channel recv buffer full");
        }
        if (empty_frame) ++pending_zero_len_;
        it->second.push_back(std::move(r.value()));
      }
    }
  }

  void close() override {
#if defined(_WIN32)
    // On Windows, closesocket() wakes a blocked recv/send and the kernel pins
    // the socket object for the in-flight io, so closing immediately is safe
    // (there is no POSIX-style fd-number-reuse race for the pending operation).
    wire::SocketHandle fd = wire::kInvalidSocket;
    {
      std::lock_guard<std::mutex> lock(io_->mu);
      io_->closing = true;
      if (io_->fd != wire::kInvalidSocket) {
        fd = io_->fd;
        io_->fd = wire::kInvalidSocket;
      }
    }
    if (fd != wire::kInvalidSocket) close_socket(fd);
#else
    // On POSIX ::close() does not wake a blocked read and the freed fd number
    // can be reused by a new connection mid-read, so shutdown() delivers the
    // wake-up and the close is deferred to the last registered io.
    wire::SocketHandle fd_to_shutdown = wire::kInvalidSocket;
    wire::SocketHandle fd_to_close = wire::kInvalidSocket;
    {
      std::lock_guard<std::mutex> lock(io_->mu);
      io_->closing = true;
      if (io_->fd != wire::kInvalidSocket) {
        fd_to_shutdown = io_->fd;
        if (io_->active == 0) {
          fd_to_close = io_->fd;
          io_->fd = wire::kInvalidSocket;
        }
      }
    }
    if (fd_to_shutdown != wire::kInvalidSocket) shutdown_socket(fd_to_shutdown);
    if (fd_to_close != wire::kInvalidSocket) close_socket(fd_to_close);
#endif
  }

 private:
  // Register this io on the socket. Returns false once the pipe is closed; the
  // captured fd stays open until io_end (via IoGuard), so close() can
  // shutdown() to wake a blocked syscall without racing fd reuse.
  bool io_begin(wire::SocketHandle* fd_out) {
    std::lock_guard<std::mutex> lock(io_->mu);
    if (io_->closing || io_->fd == wire::kInvalidSocket) return false;
    ++io_->active;
    *fd_out = io_->fd;
    return true;
  }

  void io_end() {
    wire::SocketHandle fd_to_close = wire::kInvalidSocket;
    {
      std::lock_guard<std::mutex> lock(io_->mu);
      --io_->active;
      if (io_->closing && io_->active == 0 && io_->fd != wire::kInvalidSocket) {
        fd_to_close = io_->fd;
        io_->fd = wire::kInvalidSocket;
      }
    }
    io_->cv.notify_all();
    if (fd_to_close != wire::kInvalidSocket) close_socket(fd_to_close);
  }

  struct IoGuard {
    TcpPipe* p;
    ~IoGuard() { p->io_end(); }
  };

  Result<void> ensure_handshake(wire::SocketHandle fd) {
    if (!handshake_ready_) {
      auto hs = wire::handshake_client(fd, /*capabilities=*/0);
      if (!hs.ok()) {
        // Shutdown the socket so any io blocked elsewhere wakes; the fd is
        // released by the last registered io (or close()).
        std::lock_guard<std::mutex> lock(io_->mu);
        if (!io_->closing) {
          io_->closing = true;
          shutdown_socket(io_->fd);
        }
        return hs;
      }
      handshake_ready_ = true;
    }
    return {};
  }

  std::shared_ptr<PipeIoState> io_;
  bool handshake_ready_ = true;
  std::mutex ch_mu_;
  std::mutex read_mu_;
  std::map<std::uint32_t, std::vector<wire::FrameMessage>> pending_;
  // Total bytes stashed in pending_ across all channels, guarded by ch_mu_.
  static constexpr std::size_t kMaxPendingBytes = 64 * 1024 * 1024;
  // Zero-length frames charge no bytes, so their count and the number of
  // buffered channels are capped separately (both guarded by ch_mu_).
  static constexpr std::size_t kMaxPendingChannels = 1024;
  static constexpr std::size_t kMaxPendingZeroLenFrames = 4096;
  std::size_t pending_bytes_ = 0;
  std::size_t pending_zero_len_ = 0;
};

class TcpListener final : public Listener {
 public:
  TcpListener(wire::SocketHandle fd, std::string host, std::uint16_t port,
              std::chrono::milliseconds handshake_timeout)
      : fd_(fd), host_(std::move(host)), port_(port), handshake_timeout_(handshake_timeout) {}
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
    // Disable Nagle on the accepted socket so echo replies are not delayed by
    // up to 40ms (delayed ACK + Nagle) on every small round-trip. The dial side
    // already sets TCP_NODELAY; the accept side must match.
    int one = 1;
#if defined(_WIN32)
    SOCKET sock = static_cast<SOCKET>(cfd);
    (void)::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
                       reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
#else
    (void)::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#endif
    auto hs = wire::handshake_server(cfd, /*server_capabilities=*/0, handshake_timeout_);
    if (!hs.ok()) {
      close_socket(cfd);
      return hs.status();
    }
    return std::unique_ptr<Pipe>(new TcpPipe(cfd, /*handshake_ready=*/true));
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
  std::chrono::milliseconds handshake_timeout_{5000};
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

  return std::unique_ptr<Listener>(
      new TcpListener(fd.value(), addr.host, effective_port, opt.handshake_timeout));
}

Result<std::unique_ptr<Pipe>> tcp_dial(const TcpAddress& addr, const DialOptions& opt) {
  if (opt.timeout.count() < 0) {
    return Status::invalid_argument("dial timeout must not be negative");
  }
  auto fd = connect_tcp(addr.host, addr.port, opt.timeout);
  if (!fd.ok()) return fd.status();
  return std::unique_ptr<Pipe>(new TcpPipe(fd.value(), /*handshake_ready=*/false));
}

}  // namespace duct

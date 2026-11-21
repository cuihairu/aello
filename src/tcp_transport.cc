#include "duct/duct.h"

#include "duct/config.h"
#include "duct/protocol.h"
#include "duct/socket_utils.h"
#include "duct/wire.h"

#include "reliable_pipe.h"

#if DUCT_HAVE_TLS
#include "tls_stream.h"
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#endif

#include <atomic>
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
  // handshake_ready: true when negotiation already completed (server side in
  // accept(), reliable client side in tcp_dial()). The plain client side
  // performs it lazily on the first send/recv so a blocking dial() does not
  // deadlock before the peer calls accept().
  // negotiated_caps: the capability bitmask both peers advertised (the AND of
  // the two), known once the handshake completed. Plain clients never negotiate
  // (they advertise 0), so this stays 0 and the pipe emits v1 frames.
  TcpPipe(wire::SocketHandle fd, bool handshake_ready = true,
          std::uint32_t negotiated_caps = 0
#if DUCT_HAVE_TLS
          // `ssl` is an established SSL bound to `fd` (SSL_set_fd, so it does not
          // own the fd — the transport still releases it); empty for plain TCP.
          // `ssl_mu` serializes SSL ops across every TlsStream of this connection.
          , std::shared_ptr<SSL> ssl = nullptr, std::shared_ptr<std::mutex> ssl_mu = nullptr
#endif
      )
      : io_(std::make_shared<PipeIoState>()),
        handshake_ready_(handshake_ready),
        negotiated_caps_(negotiated_caps)
#if DUCT_HAVE_TLS
        , ssl_(std::move(ssl)), ssl_mu_(std::move(ssl_mu))
#endif
 {
    io_->fd = fd;
    // Send the client's handshake eagerly (write-only, fire-and-forget). The
    // peer's accept() reads it whenever it runs, so this never deadlocks even
    // when accept() is called after dial() returns.
    if (!handshake_ready_) {
      auto hs = write_handshake(io_->fd);
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
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    wire::SocketHandle fd;
    if (!io_begin(&fd)) return Status::closed("pipe closed");
    IoGuard guard{io_
#if DUCT_HAVE_TLS
                  , ssl_, ssl_mu_
#endif
    };
    auto hs = ensure_handshake(fd);
    if (!hs.ok()) return hs;
    // The reliability layer sets session_id only after the connection negotiated
    // kCapabilityReliable, so a v2 frame is never sent to a v1 peer (that is
    // what makes degradation clean instead of a protocol error). wire_flags
    // carries protocol control bits (e.g. kAck) the application never sets.
    std::uint32_t flags = channel_id_to_flags(opt.channel_id) | opt.wire_flags;
    wire::FrameMeta meta;
    if (opt.session_id != 0) {
      meta.session_id = opt.session_id;
      meta.seq = opt.seq;
    }
#if DUCT_HAVE_TLS
    if (ssl_) {
      StreamHolder holder{fd, ssl_, ssl_mu_};
      return wire::write_frame(holder.tls, msg, flags, opt.timeout, meta);
    }
#endif
    return wire::write_frame(fd, msg, flags, opt.timeout, meta);
  }

  Result<wire::FrameMessage> recv_frame(const RecvOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    wire::SocketHandle fd;
    if (!io_begin(&fd)) return Status::closed("pipe closed");
    IoGuard guard{io_
#if DUCT_HAVE_TLS
                  , ssl_, ssl_mu_
#endif
    };
    auto hs = ensure_handshake(fd);
    if (!hs.ok()) return hs.status();
    // Serialize socket reads: channel demux is designed for concurrent recv()
    // on different channels, and a frame read is not atomic on the wire.
    std::lock_guard<std::mutex> lock(read_mu_);
    // Transparently consume the server's kCtrl handshake reply; it is protocol
    // metadata, not an application frame.
    for (;;) {
      auto r = read_one_frame(fd, opt.timeout);
      if (!r.ok()) return r.status();
      if ((r.value().flags & to_u32(FrameFlags::kCtrl)) == 0) return r;
    }
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
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

  std::uint32_t negotiated_capabilities() const override { return negotiated_caps_; }

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

  // Static on the shared state (not the pipe): a guard whose io outlives a
  // timed-out destructor must never touch freed pipe memory.
  static void io_end(std::shared_ptr<PipeIoState> io) {
    wire::SocketHandle fd_to_close = wire::kInvalidSocket;
    {
      std::lock_guard<std::mutex> lock(io->mu);
      --io->active;
      if (io->closing && io->active == 0 && io->fd != wire::kInvalidSocket) {
        fd_to_close = io->fd;
        io->fd = wire::kInvalidSocket;
      }
    }
    io->cv.notify_all();
    if (fd_to_close != wire::kInvalidSocket) close_socket(fd_to_close);
  }

  // Holds the shared state, not the pipe: if the pipe is destroyed after its
  // destructor's bounded wait expires while this io is still in flight, the
  // state stays alive and io_end remains memory-safe. It also pins the SSL and
  // its op mutex, so a TLS op in flight survives the pipe being destroyed.
  struct IoGuard {
    std::shared_ptr<PipeIoState> io;
#if DUCT_HAVE_TLS
    std::shared_ptr<SSL> ssl;
    std::shared_ptr<std::mutex> ssl_mu;
#endif
    ~IoGuard() { io_end(io); }
  };

  Result<void> ensure_handshake(wire::SocketHandle fd) {
    if (!handshake_ready_) {
      auto hs = write_handshake(fd);
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

  // Send the client's capability handshake through the TLS stream when
  // encrypted, else straight over the socket. (Fire-and-forget; the peer's
  // accept() reads it whenever it runs.)
  Result<void> write_handshake(wire::SocketHandle fd) {
#if DUCT_HAVE_TLS
    if (ssl_) {
      tls::TlsStream ts(ssl_.get(), fd, ssl_mu_);
      return wire::handshake_client(ts, /*capabilities=*/0);
    }
#endif
    return wire::handshake_client(fd, /*capabilities=*/0);
  }

  // Read one frame through the TLS stream when encrypted, else the raw socket.
  Result<wire::FrameMessage> read_one_frame(wire::SocketHandle fd,
                                            std::chrono::milliseconds timeout) {
#if DUCT_HAVE_TLS
    if (ssl_) {
      StreamHolder holder{fd, ssl_, ssl_mu_};
      return wire::read_frame_with_flags(holder.tls, timeout);
    }
#endif
    return wire::read_frame_with_flags(fd, timeout);
  }

  // The TLS framing stream for one io. The holder pins the SSL and the op mutex
  // for the duration of the read/write so an in-flight op never sees freed
  // memory if the pipe is destroyed while it runs.
#if DUCT_HAVE_TLS
  struct StreamHolder {
    std::shared_ptr<SSL> ssl;
    std::shared_ptr<std::mutex> ssl_mu;
    tls::TlsStream tls;
    StreamHolder(wire::SocketHandle fd, std::shared_ptr<SSL> s, std::shared_ptr<std::mutex> m)
        : ssl(std::move(s)), ssl_mu(std::move(m)), tls(ssl.get(), fd, ssl_mu) {}
  };
#endif

  std::shared_ptr<PipeIoState> io_;
  bool handshake_ready_ = true;
  std::uint32_t negotiated_caps_ = 0;
  std::mutex ch_mu_;
  std::mutex read_mu_;
#if DUCT_HAVE_TLS
  std::shared_ptr<SSL> ssl_;
  std::shared_ptr<std::mutex> ssl_mu_;
#endif
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

// Listener socket ownership shared with in-flight accept() calls. close()
// cannot wake a blocked ::accept(2) on POSIX (the kernel holds the file
// reference; the fd number could even be reused by a new connection), so
// accept() slices its wait and re-checks `closing` every 100ms. The fd is
// only released when no accept is registered against it: close() defers the
// release to the last registered accept's finish_accept.
struct TcpListenerState {
  std::mutex mu;
  std::condition_variable cv;
  wire::SocketHandle fd = wire::kInvalidSocket;
  std::atomic<bool> closing{false};
  std::atomic<int> active_accepts{0};
};

class TcpListener final : public Listener {
 public:
  // server_caps: capabilities this listener advertises in the handshake reply
  // (kCapabilityReliable when the listener was created with reliability on).
  // The accepted pipe's negotiated caps are the AND of what both sides sent, so
  // a plain client silently degrades instead of erroring.
  TcpListener(wire::SocketHandle fd, std::string host, std::uint16_t port,
              std::chrono::milliseconds handshake_timeout,
              std::uint32_t server_caps = 0,
              ReliabilityListenerConfig reliability = {}
#if DUCT_HAVE_TLS
              , std::shared_ptr<tls::SslContext> tls_ctx = nullptr
#endif
      )
      : state_(std::make_shared<TcpListenerState>()),
        host_(std::move(host)),
        port_(port),
        handshake_timeout_(handshake_timeout),
        server_caps_(server_caps),
        reliability_(std::move(reliability))
#if DUCT_HAVE_TLS
        , tls_ctx_(std::move(tls_ctx))
#endif
 {
    state_->fd = fd;
  }
  ~TcpListener() override {
    close();
    // Wait for already-registered accepts to unwind before the shared state
    // (and possibly the fd) dies.
    std::unique_lock<std::mutex> lock(state_->mu);
    state_->cv.wait_for(lock, std::chrono::seconds(2),
                        [&] { return state_->active_accepts.load() == 0; });
  }

  // Decrement the in-flight accept count and wake the destructor's wait.
  // Every accept() return path calls this exactly once after registering.
  // When the last accept unwinds after close(), it owns the fd release.
  static void finish_accept(const std::shared_ptr<TcpListenerState>& s) {
    wire::SocketHandle fd_to_close = wire::kInvalidSocket;
    {
      std::lock_guard<std::mutex> lock(s->mu);
      int prev = s->active_accepts.fetch_sub(1, std::memory_order_acq_rel);
      if (prev == 1 && s->closing.load(std::memory_order_acquire) &&
          s->fd != wire::kInvalidSocket) {
        fd_to_close = s->fd;
        s->fd = wire::kInvalidSocket;
      }
    }
    if (fd_to_close != wire::kInvalidSocket) close_socket(fd_to_close);
    s->cv.notify_all();
  }

  Result<std::unique_ptr<Pipe>> accept() override {
    auto state = state_;
    {
      std::lock_guard<std::mutex> lock(state->mu);
      if (state->closing.load(std::memory_order_acquire)) {
        return Status::closed("listener closed");
      }
      state->active_accepts.fetch_add(1, std::memory_order_relaxed);
    }

    // Slice the listener wait: a blocked ::accept(2) cannot be woken by
    // close() on POSIX, so poll in 100ms slices and re-check `closing` (and
    // re-read the fd under the lock) at least once per slice.
    wire::SocketHandle cfd = wire::kInvalidSocket;
    bool stopped = false;
    for (;;) {
      wire::SocketHandle lfd;
      {
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->closing.load(std::memory_order_acquire)) {
          stopped = true;
          break;
        }
        lfd = state->fd;
      }
      if (lfd == wire::kInvalidSocket) {
        stopped = true;
        break;
      }
      auto wr = socket_utils::wait_readable(lfd, std::chrono::milliseconds(100));
      if (!wr.ok()) {
        if (wr.status().code() == StatusCode::kTimeout) continue;  // slice elapsed
        finish_accept(state);
        return wr.status();
      }
#if defined(_WIN32)
      cfd = static_cast<wire::SocketHandle>(
          ::accept(static_cast<SOCKET>(lfd), nullptr, nullptr));
#else
      cfd = static_cast<wire::SocketHandle>(::accept(lfd, nullptr, nullptr));
#endif
      if (cfd == wire::kInvalidSocket) {
#if defined(_WIN32)
        finish_accept(state);
        return Status::io_error("accept() failed");
#else
        const int e = errno;
        // A concurrent accept may have consumed the connection between our
        // poll wake and ::accept(2); retry in that case.
        if (e == EINTR || e == ECONNABORTED || e == EAGAIN || e == EWOULDBLOCK) {
          continue;
        }
        finish_accept(state);
        return Status::io_error("accept() failed");
#endif
      }
      break;
    }
    if (stopped) {
      finish_accept(state);
      return Status::closed("listener closed during accept");
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
    // TLS: establish the encrypted channel before the wire handshake, so the
    // capability handshake (and every frame after it) rides the TLS stream. The
    // cert/key were validated at listen() time; the SSL_CTX is shared across
    // accepted sockets, each gets its own SSL + op mutex.
#if DUCT_HAVE_TLS
    std::shared_ptr<SSL> ssl;
    std::shared_ptr<std::mutex> ssl_mu;
    if (tls_ctx_) {
      SSL* raw = SSL_new(tls_ctx_->get());
      if (raw == nullptr) {
        close_socket(cfd);
        finish_accept(state);
        return Status::io_error("SSL_new failed");
      }
      // SSL_set_fd takes an int; a Windows SOCKET (UINT_PTR) is passed through
      // the OpenSSL convention of storing the handle in BIO's num field.
      (void)SSL_set_fd(raw, static_cast<int>(cfd));
      auto tls_hs = tls::handshake(raw, cfd, /*server=*/true, handshake_timeout_);
      if (!tls_hs.ok()) {
        SSL_free(raw);
        close_socket(cfd);
        finish_accept(state);
        return tls_hs.status();
      }
      ssl.reset(raw, SSL_free);
      ssl_mu = std::make_shared<std::mutex>();
    }
#endif

    // Stop-aware handshake: a silent client (connected but never writing the
    // handshake) must not pin accept() past a listener close.
    wire::HandshakeResult hs;
#if DUCT_HAVE_TLS
    if (ssl) {
      tls::TlsStream ts(ssl.get(), cfd, ssl_mu);
      auto r = wire::handshake_server_stoppable(ts, server_caps_, handshake_timeout_,
                                                state->closing);
      if (!r.ok()) {
        close_socket(cfd);
        finish_accept(state);
        return r.status();
      }
      hs = r.value();
    } else
#endif
    {
      auto r = wire::handshake_server_stoppable(cfd, server_caps_, handshake_timeout_,
                                                state->closing);
      if (!r.ok()) {
        close_socket(cfd);
        finish_accept(state);
        return r.status();
      }
      hs = r.value();
    }
    // Negotiated caps are the AND of what both sides advertised: a reliable
    // listener facing a plain client degrades to v1 on this accepted pipe.
    const std::uint32_t negotiated = server_caps_ & hs.capabilities;
    std::unique_ptr<Pipe> pipe;
#if DUCT_HAVE_TLS
    if (ssl) {
      pipe.reset(new TcpPipe(cfd, /*handshake_ready=*/true, negotiated, ssl, ssl_mu));
    } else
#endif
    {
      pipe.reset(new TcpPipe(cfd, /*handshake_ready=*/true, negotiated));
    }
    if ((negotiated & kCapabilityReliable) != 0 && reliability_.table &&
        hs.session_id != 0) {
      // Resume (or start) this session's counters in the table shared by every
      // accepted connection of this listener: a redial continues the same
      // next_expected_in / next_seq_out instead of restarting them, which is
      // what lets a replay be recognized as a replay rather than delivered
      // twice. The session id rides the handshake so this happens before any
      // frame flows.
      auto st = reliability_.table->resume_or_start(hs.session_id);
      if (!st.ok()) {
        pipe->close();
        finish_accept(state);
        return st.status();
      }
      ReliablePipeConfig cfg;
      cfg.session_id = hs.session_id;
      cfg.state = st.value();
      cfg.window_bytes = reliability_.window_bytes;
      cfg.backpressure = reliability_.backpressure;
      // This session may already have unacked outbound messages from a
      // connection that died before their ACK came back. The fresh connection
      // is live now, so push them out; the first connection starts empty, so
      // this is a no-op there.
      cfg.replay_pending_window_on_bind = true;
      pipe = make_reliable_pipe(std::move(pipe), cfg);
    }
    finish_accept(state);
    return pipe;
  }

  Result<std::string> local_address() const override {
    if (state_->fd == wire::kInvalidSocket) return Status::closed("listener closed");
    return std::string("tcp://") + (host_.empty() ? "127.0.0.1" : host_) + ":" + std::to_string(port_);
  }

  void close() override {
    wire::SocketHandle fd_to_close = wire::kInvalidSocket;
    {
      std::lock_guard<std::mutex> lock(state_->mu);
      if (state_->closing.exchange(true, std::memory_order_acq_rel)) return;
      if (state_->fd != wire::kInvalidSocket &&
          state_->active_accepts.load(std::memory_order_acquire) == 0) {
        // No accept in flight: this thread owns the fd release. Otherwise the
        // last registered accept releases it in finish_accept.
        fd_to_close = state_->fd;
        state_->fd = wire::kInvalidSocket;
      }
    }
    if (fd_to_close != wire::kInvalidSocket) close_socket(fd_to_close);
    state_->cv.notify_all();
  }

 private:
  std::shared_ptr<TcpListenerState> state_;
  std::string host_;
  std::uint16_t port_ = 0;
  std::chrono::milliseconds handshake_timeout_{5000};
  std::uint32_t server_caps_ = 0;
  // Empty (no table) unless the listener was created with reliability on.
  ReliabilityListenerConfig reliability_;
#if DUCT_HAVE_TLS
  // Empty (no TLS) unless the listener was created with TlsOptions.enabled.
  std::shared_ptr<tls::SslContext> tls_ctx_;
#endif
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
  // Validate the server credentials before binding: a bad cert/key must fail
  // the listen() call, not each accept() later.
#if DUCT_HAVE_TLS
  std::shared_ptr<tls::SslContext> tls_ctx;
  if (opt.tls.enabled) {
    auto ctx = tls::SslContext::make_server(opt.tls);
    if (!ctx.ok()) return ctx.status();
    tls_ctx = ctx.value();
  }
#else
  if (opt.tls.enabled) {
    return Status::not_supported("TLS support not built into this library");
  }
#endif

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

  // The listener advertises the reliability capability in its handshake reply
  // when the caller opted in; each accepted pipe negotiates it down to what
  // both sides support.
  const std::uint32_t server_caps =
      opt.qos.reliability == Reliability::kAtLeastOnce ? kCapabilityReliable : 0;

  // One session table per listener: it is what lets an accepted connection
  // resume a session a redial continues (see ReliabilitySessionTable). Capped
  // so a peer spraying session ids cannot grow it without bound.
  ReliabilityListenerConfig reliability;
  if (opt.qos.reliability == Reliability::kAtLeastOnce) {
    constexpr std::size_t kMaxReliableSessions = 4096;
    reliability.table = std::make_shared<ReliabilitySessionTable>(kMaxReliableSessions);
    reliability.window_bytes = opt.qos.snd_hwm_bytes;
    reliability.backpressure = opt.qos.backpressure;
  }

  return std::unique_ptr<Listener>(
      new TcpListener(fd.value(), addr.host, effective_port, opt.handshake_timeout,
                      server_caps, std::move(reliability)
#if DUCT_HAVE_TLS
                      , std::move(tls_ctx)
#endif
                      ));
}

Result<std::unique_ptr<Pipe>> tcp_dial(const TcpAddress& addr, const DialOptions& opt,
                                       std::uint64_t reliability_session_id) {
  if (opt.timeout.count() < 0) {
    return Status::invalid_argument("dial timeout must not be negative");
  }

#if DUCT_HAVE_TLS
  std::shared_ptr<tls::SslContext> tls_ctx;
  if (opt.tls.enabled) {
    auto ctx = tls::SslContext::make_client(opt.tls);
    if (!ctx.ok()) return ctx.status();
    tls_ctx = ctx.value();
  }
#else
  if (opt.tls.enabled) {
    return Status::not_supported("TLS support not built into this library");
  }
#endif

  auto fd = connect_tcp(addr.host, addr.port, opt.timeout);
  if (!fd.ok()) return fd.status();

#if DUCT_HAVE_TLS
  std::shared_ptr<SSL> ssl;
  std::shared_ptr<std::mutex> ssl_mu;
  if (tls_ctx) {
    SSL* raw = SSL_new(tls_ctx->get());
    if (raw == nullptr) {
      close_socket(fd.value());
      return Status::io_error("SSL_new failed");
    }
    // Hostname/SAN verification. An IP literal is matched against the cert's
    // iPAddress SAN (not a dNSName); a hostname against its dNSName SAN.
    // verify_peer_name == false skips only this name check — the chain is
    // still verified against the CA bundle.
    if (opt.tls.verify_peer_name) {
      X509_VERIFY_PARAM* param = SSL_get0_param(raw);
      unsigned char v4[sizeof(in_addr)];
      unsigned char v6[sizeof(in6_addr)];
      if (inet_pton(AF_INET, addr.host.c_str(), v4) == 1 ||
          inet_pton(AF_INET6, addr.host.c_str(), v6) == 1) {
        if (X509_VERIFY_PARAM_set1_ip_asc(param, addr.host.c_str()) != 1) {
          SSL_free(raw);
          close_socket(fd.value());
          return Status::invalid_argument(
              "failed to set the IP literal for hostname verification");
        }
      } else if (X509_VERIFY_PARAM_set1_host(param, addr.host.c_str(), 0) != 1) {
        SSL_free(raw);
        close_socket(fd.value());
        return Status::invalid_argument("failed to set the expected hostname");
      }
    }
    // SSL_set_fd takes an int; a Windows SOCKET (UINT_PTR) is passed through
    // the OpenSSL convention of storing the handle in BIO's num field.
    (void)SSL_set_fd(raw, static_cast<int>(fd.value()));
    // The TLS handshake is bounded by the dial timeout (0 = design default 10s)
    // so a silent/mismatched peer cannot pin dial() forever.
    const auto tls_timeout = opt.timeout.count() > 0
        ? opt.timeout : std::chrono::milliseconds(10000);
    auto tls_hs = tls::handshake(raw, fd.value(), /*server=*/false, tls_timeout);
    if (!tls_hs.ok()) {
      SSL_free(raw);
      close_socket(fd.value());
      return tls_hs.status();
    }
    ssl.reset(raw, SSL_free);
    ssl_mu = std::make_shared<std::mutex>();
  }
#endif

  // Reliable dials advertise kCapabilityReliable and read the server's reply
  // synchronously so the negotiated caps (and with them v2 vs v1 framing) are
  // settled before dial() returns — the reconnect worker publishes kConnected
  // from this same call, and the reliability layer replays based on the caps it
  // sees then. Every in-tree listener replies, so the bounded wait cannot hang;
  // a truly silent peer surfaces as a handshake timeout dial error.
  if (opt.qos.reliability == Reliability::kAtLeastOnce) {
    // reliability_session_id is this logical dial's stable identity (0 only for
    // non-reliable dials, which never enter this branch): the listener uses it
    // to resume the session's counters on a redial instead of restarting them.
    std::uint32_t server_caps = 0;
#if DUCT_HAVE_TLS
    if (ssl) {
      tls::TlsStream ts(ssl.get(), fd.value(), ssl_mu);
      auto caps = wire::handshake_client_sync(ts, kCapabilityReliable, opt.timeout,
                                              reliability_session_id);
      if (!caps.ok()) {
        close_socket(fd.value());
        return caps.status();
      }
      server_caps = caps.value();
    } else
#endif
    {
      auto caps = wire::handshake_client_sync(fd.value(), kCapabilityReliable,
                                              opt.timeout, reliability_session_id);
      if (!caps.ok()) {
        close_socket(fd.value());
        return caps.status();
      }
      server_caps = caps.value();
    }
#if DUCT_HAVE_TLS
    if (ssl) {
      return std::unique_ptr<Pipe>(new TcpPipe(fd.value(), /*handshake_ready=*/true,
                                               kCapabilityReliable & server_caps,
                                               ssl, ssl_mu));
    }
#endif
    return std::unique_ptr<Pipe>(new TcpPipe(fd.value(), /*handshake_ready=*/true,
                                             kCapabilityReliable & server_caps));
  }
#if DUCT_HAVE_TLS
  if (ssl) {
    return std::unique_ptr<Pipe>(new TcpPipe(fd.value(), /*handshake_ready=*/false,
                                             /*negotiated_caps=*/0, ssl, ssl_mu));
  }
#endif
  return std::unique_ptr<Pipe>(new TcpPipe(fd.value(), /*handshake_ready=*/false));
}

}  // namespace duct

#include "duct/duct.h"

#include "duct/protocol.h"
#include "duct/socket_utils.h"
#include "duct/wire.h"

#include <cstring>
#include <cerrno>
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#if defined(_WIN32)
// UDS is not available on Windows; use named pipes instead.
#else
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#endif

namespace duct {
namespace {

#if !defined(_WIN32)

// Socket ownership shared between the pipe and any in-flight send/recv. The
// fd is only released when no io is registered against it, so close() can
// shutdown() a blocked syscall without racing fd reuse (a closed fd number
// can be handed to a new connection while a read still holds the old one).
struct PipeIoState {
  std::mutex mu;
  std::condition_variable cv;
  int fd = -1;
  bool closing = false;
  int active = 0;
};

class UdsPipe final : public Pipe {
 public:
  explicit UdsPipe(int fd) : io_(std::make_shared<PipeIoState>()) { io_->fd = fd; }
  ~UdsPipe() override {
    close();
    // Wait for already-registered io to unwind before the shared state dies.
    std::unique_lock<std::mutex> lock(io_->mu);
    io_->cv.wait_for(lock, std::chrono::seconds(2), [&] { return io_->active == 0; });
  }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    int fd;
    if (!io_begin(&fd)) return Status::closed("pipe closed");
    IoGuard guard{this};

    // Wait for writable if timeout is specified.
    if (opt.timeout.count() > 0) {
      auto st = socket_utils::wait_writable(fd, opt.timeout);
      if (!st.ok()) return st;
    }

    return wire::write_frame(fd, msg, channel_id_to_flags(opt.channel_id), opt.timeout);
  }

  Result<wire::FrameMessage> recv_frame(const RecvOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    int fd;
    if (!io_begin(&fd)) return Status::closed("pipe closed");
    IoGuard guard{this};
    // Serialize socket reads: concurrent recv() calls for different channels
    // must not interleave their raw frame reads on the same fd.
    std::lock_guard<std::mutex> lock(read_mu_);
    if (opt.timeout.count() > 0) {
      auto st = socket_utils::wait_readable(fd, opt.timeout);
      if (!st.ok()) return st.status();
    }
    return wire::read_frame_with_flags(fd, opt.timeout);
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (opt.timeout.count() < 0) {
      return Status::invalid_argument("negative timeout");
    }
    const bool has_deadline = opt.timeout.count() > 0;
    const auto deadline = std::chrono::steady_clock::now() + opt.timeout;
    for (;;) {
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
      {
        std::lock_guard<std::mutex> lock(ch_mu_);
        // The cap is shared across channels: a per-channel ceiling would let a
        // peer multiply it by the number of channel_ids it sprays (channel ids
        // come from the wire, not us). Zero-length frames charge no bytes and
        // each map node / vector slot costs real memory regardless of payload,
        // so the buffered-channel count and the zero-length frame count are
        // capped as well: empty frames sprayed on many channel ids would
        // otherwise stash unbounded entries while the byte counter stays at
        // zero.
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
    int fd_to_shutdown = -1;
    int fd_to_close = -1;
    {
      std::lock_guard<std::mutex> lock(io_->mu);
      io_->closing = true;
      if (io_->fd >= 0) {
        fd_to_shutdown = io_->fd;
        if (io_->active == 0) {
          fd_to_close = io_->fd;
          io_->fd = -1;
        }
      }
    }
    if (fd_to_shutdown >= 0) ::shutdown(fd_to_shutdown, SHUT_RDWR);
    if (fd_to_close >= 0) ::close(fd_to_close);
  }

  Result<PeerCredential> peer_credential() const override {
    int fd;
    if (!io_begin(&fd)) return Status::closed("pipe closed");
    IoGuard guard{this};
#if defined(SO_PEERCRED)
    struct ucred cred {};
    socklen_t len = sizeof(cred);
    if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) < 0) {
      return Status::io_error("getsockopt(SO_PEERCRED) failed: " +
                              std::string(std::strerror(errno)));
    }
    PeerCredential pc;
    pc.uid = cred.uid;
    pc.gid = cred.gid;
    pc.pid = static_cast<std::int64_t>(cred.pid);
    return pc;
#else
    return Status::not_supported("SO_PEERCRED not available on this platform");
#endif
  }

 private:
  // Register this io on the socket. Returns false once the pipe is closed; the
  // captured fd stays open until io_end (via IoGuard), so close() can
  // shutdown() to wake a blocked syscall without racing fd reuse. Both are
  // const: they only mutate the shared state through the shared_ptr pointee.
  bool io_begin(int* fd_out) const {
    std::lock_guard<std::mutex> lock(io_->mu);
    if (io_->closing || io_->fd < 0) return false;
    ++io_->active;
    *fd_out = io_->fd;
    return true;
  }

  void io_end() const {
    int fd_to_close = -1;
    {
      std::lock_guard<std::mutex> lock(io_->mu);
      --io_->active;
      if (io_->closing && io_->active == 0 && io_->fd >= 0) {
        fd_to_close = io_->fd;
        io_->fd = -1;
      }
    }
    io_->cv.notify_all();
    if (fd_to_close >= 0) ::close(fd_to_close);
  }

  struct IoGuard {
    const UdsPipe* p;
    ~IoGuard() { p->io_end(); }
  };

  std::shared_ptr<PipeIoState> io_;
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

// Listener socket ownership shared with in-flight accept() calls, mirroring
// the shm bootstrap listener: close() cannot wake a blocked ::accept(2), so
// accept() slices its wait and re-checks `closing` every 100ms; the fd is
// only released when no accept is registered against it (close defers the
// release to the last registered accept).
struct UdsListenerState {
  std::mutex mu;
  std::condition_variable cv;
  int fd = -1;
  bool closing = false;
  std::atomic<int> active_accepts{0};
};

class UdsListener final : public Listener {
 public:
  UdsListener(int fd, std::string path)
      : state_(std::make_shared<UdsListenerState>()), path_(std::move(path)) {
    state_->fd = fd;
  }

  ~UdsListener() override {
    close();
    // Wait for already-registered accepts to unwind before the shared state
    // (and possibly the fd) dies.
    std::unique_lock<std::mutex> lock(state_->mu);
    state_->cv.wait_for(lock, std::chrono::seconds(2),
                        [&] { return state_->active_accepts.load() == 0; });
  }

  // Decrement the in-flight accept count and wake the destructor's wait.
  // Every accept() return path calls this exactly once after registering.
  static void finish_accept(const std::shared_ptr<UdsListenerState>& s) {
    int fd_to_close = -1;
    {
      std::lock_guard<std::mutex> lock(s->mu);
      const int prev = s->active_accepts.fetch_sub(1, std::memory_order_acq_rel);
      if (prev == 1 && s->closing && s->fd >= 0) {
        fd_to_close = s->fd;
        s->fd = -1;
      }
    }
    if (fd_to_close >= 0) ::close(fd_to_close);
    s->cv.notify_all();
  }

  Result<std::unique_ptr<Pipe>> accept() override {
    auto state = state_;
    {
      std::lock_guard<std::mutex> lock(state->mu);
      if (state->closing) return Status::closed("listener closed");
      state->active_accepts.fetch_add(1, std::memory_order_relaxed);
    }

    // Slice the listener wait: close() cannot wake a blocked ::accept(2).
    int cfd = -1;
    bool stopped = false;
    for (;;) {
      int lfd;
      {
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->closing) {
          stopped = true;
          break;
        }
        lfd = state->fd;
      }
      if (lfd < 0) {
        stopped = true;
        break;
      }
      struct pollfd p;
      p.fd = lfd;
      p.events = POLLIN;
      p.revents = 0;
      const int rc = ::poll(&p, 1, 100);
      if (rc < 0) {
        if (errno == EINTR) continue;
        finish_accept(state);
        return Status::io_error("poll(uds listener) failed");
      }
      if (rc == 0) continue;  // slice elapsed: re-check closing
      cfd = ::accept(lfd, nullptr, nullptr);
      if (cfd < 0) {
        const int e = errno;
        // A concurrent accept may have consumed the connection between our
        // poll wake and ::accept(2); retry in that case.
        if (e == EINTR || e == ECONNABORTED || e == EAGAIN || e == EWOULDBLOCK) {
          continue;
        }
        finish_accept(state);
        return Status::io_error("accept() failed");
      }
      break;
    }
    if (stopped) {
      finish_accept(state);
      return Status::closed("listener closed during accept");
    }
#if defined(__APPLE__)
    int one = 1;
    (void)::setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    finish_accept(state);
    return std::unique_ptr<Pipe>(new UdsPipe(cfd));
  }

  Result<std::string> local_address() const override {
    std::lock_guard<std::mutex> lock(state_->mu);
    if (state_->fd < 0) return Status::closed("listener closed");
    return std::string("uds://") + path_;
  }

  void close() override {
    int fd_to_close = -1;
    {
      std::lock_guard<std::mutex> lock(state_->mu);
      if (state_->closing) return;
      state_->closing = true;
      if (state_->fd >= 0 && state_->active_accepts.load() == 0) {
        // No accept in flight: this thread owns the fd release. Otherwise the
        // last registered accept releases it in finish_accept.
        fd_to_close = state_->fd;
        state_->fd = -1;
      }
    }
    if (fd_to_close >= 0) ::close(fd_to_close);
    // Remove the socket file to allow rebinding.
    ::unlink(path_.c_str());
    state_->cv.notify_all();
  }

 private:
  std::shared_ptr<UdsListenerState> state_;
  std::string path_;
};

static Result<int> connect_uds(const std::string& path, std::chrono::milliseconds timeout) {
  if (timeout.count() < 0) {
    return Status::invalid_argument("dial timeout must not be negative");
  }
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return Status::io_error("socket(AF_UNIX) failed");
  }
#if defined(__APPLE__)
  int one = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

  struct sockaddr_un addr {};
  addr.sun_family = AF_UNIX;

  // Check path length (sun_path is typically 104 or 108 bytes).
  if (path.size() >= sizeof(addr.sun_path)) {
    ::close(fd);
    return Status::invalid_argument("uds path too long");
  }
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

  // Use non-blocking connect if timeout is specified.
  if (timeout.count() > 0) {
    auto st = socket_utils::set_nonblocking(fd, true);
    if (!st.ok()) {
      ::close(fd);
      return st.status();
    }

    int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (rc == 0) {
      // Connected immediately.
      auto rb = socket_utils::set_nonblocking(fd, false);
      if (!rb.ok()) {
        ::close(fd);
        return rb.status();
      }
      return fd;
    }

    if (errno != EINPROGRESS && errno != EALREADY && errno != EINTR) {
      int err = errno;
      ::close(fd);
      return Status::io_error("connect() failed for uds path: " + path +
                              ": " + std::strerror(err));
    }

    // Wait for connection to complete.
    st = socket_utils::wait_writable(fd, timeout);
    if (!st.ok()) {
      int err = errno;
      ::close(fd);
      if (st.status().code() == StatusCode::kTimeout) {
        return Status::timeout("connect() timed out for uds path: " + path);
      }
      return Status::io_error(st.status().message() + ": " + std::strerror(err));
    }

    // Check if connection succeeded.
    int err = 0;
    socklen_t errlen = sizeof(err);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0 || err != 0) {
      int saved = err;
      ::close(fd);
      return Status::io_error("connect() failed for uds path: " + path +
                              ": SO_ERROR=" + std::to_string(saved) + " " +
                              std::strerror(saved));
    }

    // Restore blocking mode.
    auto rb = socket_utils::set_nonblocking(fd, false);
    if (!rb.ok()) {
      ::close(fd);
      return rb.status();
    }
    return fd;
  }

  // Blocking connect.
  if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
    int err = errno;
    ::close(fd);
    return Status::io_error("connect() failed for uds path: " + path +
                            ": " + std::strerror(err));
  }

  return fd;
}

static Result<int> listen_uds(const std::string& path, int backlog) {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return Status::io_error("socket(AF_UNIX) failed");
  }
#if defined(__APPLE__)
  int one = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

  struct sockaddr_un addr {};
  addr.sun_family = AF_UNIX;

  if (path.size() >= sizeof(addr.sun_path)) {
    ::close(fd);
    return Status::invalid_argument("uds path too long");
  }
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

  // Remove an existing socket file only after the path has been validated.
  // The socket-type, owner and liveness checks live in socket_utils so the
  // shm bootstrap listener shares the same policy.
  auto prep = socket_utils::ensure_stale_uds_removed(addr);
  if (!prep.ok()) {
    ::close(fd);
    return prep.status();
  }

  if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return Status::io_error("bind() failed for uds path: " + path);
  }

  if (::listen(fd, backlog) != 0) {
    ::close(fd);
    ::unlink(path.c_str());
    return Status::io_error("listen() failed for uds path: " + path);
  }

  return fd;
}

#endif  // !_WIN32

}  // namespace

Result<std::unique_ptr<Listener>> uds_listen(const std::string& path, const ListenOptions& opt) {
#if defined(_WIN32)
  (void)path;
  (void)opt;
  return Status::not_supported("uds not available on windows; use pipe:// instead");
#else
  auto fd = listen_uds(path, opt.backlog);
  if (!fd.ok()) return fd.status();
  return std::unique_ptr<Listener>(new UdsListener(fd.value(), path));
#endif
}

Result<std::unique_ptr<Pipe>> uds_dial(const std::string& path, const DialOptions& opt) {
#if defined(_WIN32)
  (void)path;
  (void)opt;
  return Status::not_supported("uds not available on windows; use pipe:// instead");
#else
  auto fd = connect_uds(path, opt.timeout);
  if (!fd.ok()) return fd.status();
  return std::unique_ptr<Pipe>(new UdsPipe(fd.value()));
#endif
}

}  // namespace duct

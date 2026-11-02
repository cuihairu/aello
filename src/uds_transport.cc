#include "duct/duct.h"

#include "duct/protocol.h"
#include "duct/socket_utils.h"
#include "duct/wire.h"

#include <cstring>
#include <cerrno>
#include <memory>
#include <string>

#if defined(_WIN32)
// UDS is not available on Windows; use named pipes instead.
#else
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#endif

namespace duct {
namespace {

#if !defined(_WIN32)

class UdsPipe final : public Pipe {
 public:
  explicit UdsPipe(int fd) : fd_(fd) {}
  ~UdsPipe() override { close(); }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    if (fd_ < 0) return Status::closed("pipe closed");

    // Wait for writable if timeout is specified.
    if (opt.timeout.count() > 0) {
      auto st = socket_utils::wait_writable(fd_, opt.timeout);
      if (!st.ok()) return st;
    }

    return wire::write_frame(fd_, msg, /*flags=*/0, opt.timeout);
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (fd_ < 0) return Status::closed("pipe closed");

    // Wait for readable if timeout is specified.
    if (opt.timeout.count() > 0) {
      auto st = socket_utils::wait_readable(fd_, opt.timeout);
      if (!st.ok()) return st.status();
    }

    return wire::read_frame(fd_, opt.timeout);
  }

  void close() override {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_ = -1;
};

class UdsListener final : public Listener {
 public:
  UdsListener(int fd, std::string path) : fd_(fd), path_(std::move(path)) {}

  ~UdsListener() override { close(); }

  Result<std::unique_ptr<Pipe>> accept() override {
    if (fd_ < 0) return Status::closed("listener closed");

    int cfd = ::accept(fd_, nullptr, nullptr);
    if (cfd < 0) {
      return Status::io_error("accept() failed");
    }
#if defined(__APPLE__)
    int one = 1;
    (void)::setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    return std::unique_ptr<Pipe>(new UdsPipe(cfd));
  }

  Result<std::string> local_address() const override {
    if (fd_ < 0) return Status::closed("listener closed");
    return std::string("uds://") + path_;
  }

  void close() override {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
      // Remove the socket file to allow rebinding.
      ::unlink(path_.c_str());
    }
  }

 private:
  int fd_ = -1;
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
  // Before unlinking, verify it is actually a socket owned by this user; never
  // silently delete an arbitrary file or steal a live peer's socket path.
  struct stat st {};
  if (::lstat(path.c_str(), &st) == 0) {
    if (!S_ISSOCK(st.st_mode) || st.st_uid != ::geteuid()) {
      ::close(fd);
      return Status::invalid_argument("uds path exists and is not a usable socket");
    }
    // Check whether a live endpoint still owns it; if so, do not steal it.
    int probe = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (probe >= 0) {
      // Non-blocking probe: a live listener with a full backlog must not make
      // the listen() caller block while we check ownership.
      int flags = ::fcntl(probe, F_GETFL, 0);
      if (flags >= 0) (void)::fcntl(probe, F_SETFL, flags | O_NONBLOCK);
      int rc = ::connect(probe, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
      ::close(probe);
      if (rc == 0) {
        ::close(fd);
        return Status::invalid_argument("uds path already in use by a live endpoint");
      }
      if (errno != ECONNREFUSED && errno != ENOENT && errno != EAGAIN &&
          errno != EWOULDBLOCK && errno != EINPROGRESS) {
        ::close(fd);
        return Status::io_error("connect probe failed for uds path: " + path);
      }
    }
    if (::unlink(path.c_str()) != 0) {
      ::close(fd);
      return Status::io_error("unlink() failed for uds path: " + path);
    }
  } else if (errno != ENOENT) {
    ::close(fd);
    return Status::io_error("lstat() failed for uds path: " + path);
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

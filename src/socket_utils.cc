#include "duct/socket_utils.h"

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>

#include <mutex>
#else
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace duct::socket_utils {

Result<void> ensure_networking() {
#if defined(_WIN32)
  static std::once_flag once;
  static int init_rc = 0;
  std::call_once(once, [] {
    WSADATA wsa{};
    init_rc = ::WSAStartup(MAKEWORD(2, 2), &wsa);
  });
  if (init_rc != 0) {
    return Status::io_error("WSAStartup failed");
  }
  return {};
#else
  return {};
#endif
}

Result<void> wait_readable(socket_t fd, std::chrono::milliseconds timeout) {
#if defined(_WIN32)
  if (fd == kInvalidSocket) return Status::invalid_argument("invalid socket");
  if (timeout.count() == 0) return {};

  auto st = ensure_networking();
  if (!st.ok()) return st;

  fd_set rfds;
  FD_ZERO(&rfds);
  SOCKET s = static_cast<SOCKET>(fd);
  FD_SET(s, &rfds);
  fd_set efds;
  FD_ZERO(&efds);
  FD_SET(s, &efds);

  timeval tv;
  tv.tv_sec = static_cast<long>(timeout.count() / 1000);
  tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);

  int rc = ::select(0, &rfds, nullptr, &efds, &tv);
  if (rc == SOCKET_ERROR) return Status::io_error("select() failed");
  if (rc == 0) return Status::timeout("read timed out");
  if (FD_ISSET(s, &efds)) return Status::io_error("select() reported error on socket");
  return {};
#else
  if (timeout.count() == 0) {
    // No timeout: caller wants blocking behavior; assume fd is ready.
    return {};
  }

  struct pollfd pfd {};
  pfd.fd = fd;
  pfd.events = POLLIN;

  int rc = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
  if (rc < 0) {
    return Status::io_error("poll() failed");
  }
  if (rc == 0) {
    return Status::timeout("read timed out");
  }
  if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
    // A readable side that has been shut down (POLLHUP) is a normal close, not
    // an error: the subsequent recv() will return 0 and surface kClosed. This
    // keeps UdsPipe::recv from misreporting a peer's normal shutdown as an
    // error (which EventLoop::run() would forward to on_error).
    if (pfd.revents & POLLHUP) {
      return Status::closed("peer closed");
    }
    // POLLERR/POLLNVAL: let the caller read SO_ERROR so the real failure is
    // preserved instead of being masked by a generic poll error.
    return Status::io_error("poll() reported error on fd");
  }
  return {};
#endif
}

Result<void> wait_writable(socket_t fd, std::chrono::milliseconds timeout) {
#if defined(_WIN32)
  if (fd == kInvalidSocket) return Status::invalid_argument("invalid socket");
  if (timeout.count() == 0) return {};

  auto st = ensure_networking();
  if (!st.ok()) return st;

  fd_set wfds;
  FD_ZERO(&wfds);
  SOCKET s = static_cast<SOCKET>(fd);
  FD_SET(s, &wfds);
  fd_set efds;
  FD_ZERO(&efds);
  FD_SET(s, &efds);

  timeval tv;
  tv.tv_sec = static_cast<long>(timeout.count() / 1000);
  tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);

  int rc = ::select(0, nullptr, &wfds, &efds, &tv);
  if (rc == SOCKET_ERROR) return Status::io_error("select() failed");
  if (rc == 0) return Status::timeout("write timed out");
  if (FD_ISSET(s, &efds)) return Status::io_error("select() reported error on socket");
  return {};
#else
  if (timeout.count() == 0) {
    // No timeout: caller wants blocking behavior; assume fd is ready.
    return {};
  }

  struct pollfd pfd {};
  pfd.fd = fd;
  pfd.events = POLLOUT;

  int rc = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
  if (rc < 0) {
    return Status::io_error("poll() failed");
  }
  if (rc == 0) {
    return Status::timeout("write timed out");
  }
  if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
    // A failed non-blocking connect surfaces POLLOUT|POLLERR|POLLHUP together.
    // Reading SO_ERROR first preserves the real cause (e.g. ECONNREFUSED)
    // instead of the POLLHUP branch swallowing it as a generic "peer closed".
    int so_error = 0;
    socklen_t errlen = sizeof(so_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &errlen) != 0) {
      // SO_ERROR itself failed (e.g. invalid fd): report its errno instead of
      // falling through to a misleading "peer closed" or a generic error.
      return Status::io_error(std::string("getsockopt(SO_ERROR) failed: ") +
                              std::strerror(errno));
    }
    if (so_error != 0) {
      return Status::io_error("poll() reported error on fd: SO_ERROR=" +
                             std::to_string(so_error) + " " + std::strerror(so_error));
    }
    if (pfd.revents & POLLHUP) {
      return Status::closed("peer closed");
    }
    return Status::io_error("poll() reported error on fd");
  }
  return {};
#endif
}

Result<void> set_nonblocking(socket_t fd, bool nonblock) {
#if defined(_WIN32)
  if (fd == kInvalidSocket) return Status::invalid_argument("invalid socket");
  auto st = ensure_networking();
  if (!st.ok()) return st;

  u_long mode = nonblock ? 1UL : 0UL;
  SOCKET s = static_cast<SOCKET>(fd);
  if (::ioctlsocket(s, FIONBIO, &mode) != 0) {
    return Status::io_error("ioctlsocket(FIONBIO) failed");
  }
  return {};
#else
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return Status::io_error("fcntl(F_GETFL) failed");
  }

  if (nonblock) {
    flags |= O_NONBLOCK;
  } else {
    flags &= ~O_NONBLOCK;
  }

  if (::fcntl(fd, F_SETFL, flags) < 0) {
    return Status::io_error("fcntl(F_SETFL) failed");
  }
  return {};
#endif
}

Result<void> close_socket(socket_t fd) {
#if defined(_WIN32)
  if (fd == kInvalidSocket) return {};
  auto st = ensure_networking();
  if (!st.ok()) return st;
  SOCKET s = static_cast<SOCKET>(fd);
  if (::closesocket(s) != 0) {
    return Status::io_error("closesocket() failed");
  }
  return {};
#else
  if (fd < 0) return {};
  if (::close(fd) != 0) {
    return Status::io_error("close() failed");
  }
  return {};
#endif
}

#if !defined(_WIN32)
Result<void> ensure_stale_uds_removed(const struct sockaddr_un& addr) {
  const char* path = addr.sun_path;
  // Remove an existing socket file only after the path has been validated.
  // Before unlinking, verify it is actually a socket owned by this user; never
  // silently delete an arbitrary file or steal a live peer's socket path.
  struct stat st {};
  if (::lstat(path, &st) == 0) {
    if (!S_ISSOCK(st.st_mode) || st.st_uid != ::geteuid()) {
      return Status::invalid_argument("uds path exists and is not a usable socket");
    }
    // Check whether a live endpoint still owns it; if so, do not steal it.
    int probe = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (probe >= 0) {
      // Non-blocking probe: a live listener with a full backlog must not make
      // the listen() caller block while we check ownership.
      int flags = ::fcntl(probe, F_GETFL, 0);
      if (flags >= 0) (void)::fcntl(probe, F_SETFL, flags | O_NONBLOCK);
      int rc = ::connect(probe, reinterpret_cast<const struct sockaddr*>(&addr), sizeof(addr));
      ::close(probe);
      if (rc == 0) {
        return Status::invalid_argument("uds path already in use by a live endpoint");
      }
      if (errno != ECONNREFUSED && errno != ENOENT && errno != EAGAIN &&
          errno != EWOULDBLOCK && errno != EINPROGRESS) {
        return Status::io_error("connect probe failed for uds path: " + std::string(path));
      }
    }
    if (::unlink(path) != 0) {
      return Status::io_error("unlink() failed for uds path: " + std::string(path));
    }
  } else if (errno != ENOENT) {
    return Status::io_error("lstat() failed for uds path: " + std::string(path));
  }
  return {};
}
#endif

}  // namespace duct::socket_utils

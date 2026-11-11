#pragma once

#include <chrono>
#include <cstdint>

#include "duct/status.h"

#if !defined(_WIN32)
#include <sys/un.h>  // sockaddr_un for ensure_stale_uds_removed
#endif

namespace duct::socket_utils {

// Cross-platform socket handle type.
#if defined(_WIN32)
using socket_t = std::uintptr_t;  // compatible with WinSock SOCKET (UINT_PTR) without including winsock headers.
inline constexpr socket_t kInvalidSocket = static_cast<socket_t>(static_cast<std::uintptr_t>(-1));
#else
using socket_t = int;
inline constexpr socket_t kInvalidSocket = -1;
#endif

// Initialize platform networking subsystem (WSAStartup on Windows).
Result<void> ensure_networking();

// Wait for fd to become readable within timeout.
// Returns Ok if readable, Timeout if timed out, or IoError on failure.
Result<void> wait_readable(socket_t fd, std::chrono::milliseconds timeout);

// Wait for fd to become writable within timeout.
// Returns Ok if writable, Timeout if timed out, or IoError on failure.
Result<void> wait_writable(socket_t fd, std::chrono::milliseconds timeout);

// Set socket to non-blocking mode.
Result<void> set_nonblocking(socket_t fd, bool nonblock);

// Close a socket handle.
Result<void> close_socket(socket_t fd);

#if !defined(_WIN32)
// Validate an existing UDS path before a listen() binds it: it must be a
// socket owned by the current user and not a live endpoint; otherwise return
// kInvalidArgument without touching the filesystem. On success (or if the
// path does not exist) the stale socket is unlinked so bind() can take it
// over. Shared by uds_listen and the shm bootstrap listener so the two cannot
// drift on the security-relevant unlink policy.
Result<void> ensure_stale_uds_removed(const struct sockaddr_un& addr);
#endif

}  // namespace duct::socket_utils

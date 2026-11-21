#include "tls_stream.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <cerrno>
#include <string>

#include "duct/socket_utils.h"

#if defined(_WIN32)
#include <winsock2.h>
#else
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace duct {
namespace tls {

namespace {

// OpenSSL global init (idempotent). OpenSSL 1.1+ does this lazily on first use,
// but explicit init keeps the error strings loaded for diagnostics.
void ensure_openssl_init() {
  static int done = [] {
    OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS | OPENSSL_INIT_LOAD_CRYPTO_STRINGS,
                     nullptr);
    return 0;
  }();
  (void)done;
}

// Pin the protocol to TLS 1.3 and forbid renegotiation. No cipher-suite knob:
// the library default set for TLS 1.3 is a fixed modern policy (a knob invites
// misconfiguration, per the design).
void apply_modern_policy(SSL_CTX* ctx) {
  (void)SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
  (void)SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION);
  (void)SSL_CTX_set_options(ctx, SSL_OP_NO_RENEGOTIATION);
}

// Loads a PEM leaf certificate chain and its private key into `ctx`. Returns
// kInvalidArgument with a diagnostic when either fails or they mismatch.
Result<void> load_cert_key(SSL_CTX* ctx, const std::string& cert_path,
                           const std::string& key_path) {
  if (SSL_CTX_use_certificate_chain_file(ctx, cert_path.c_str()) != 1) {
    return Status::invalid_argument("failed to load certificate: " + cert_path);
  }
  if (SSL_CTX_use_PrivateKey_file(ctx, key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
    return Status::invalid_argument("failed to load private key: " + key_path);
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    return Status::invalid_argument("certificate and private key do not match");
  }
  return {};
}

// Loads trust anchors (ca_path or the system store). Returns kInvalidArgument
// on failure.
Result<void> load_trust(SSL_CTX* ctx, const std::string& ca_path) {
  if (!ca_path.empty()) {
    if (SSL_CTX_load_verify_locations(ctx, ca_path.c_str(), nullptr) != 1) {
      return Status::invalid_argument("failed to load CA bundle: " + ca_path);
    }
  } else {
    if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
      return Status::invalid_argument("failed to load the system CA store");
    }
  }
  return {};
}

// Waits until `fd` is readable (or writable when `read` is false), bounded by
// `deadline` (a default-constructed time_point blocks forever). Returns
// kTimeout on expiry; the caller retries the SSL op after a ready return.
// The fd is non-blocking, so this is the only place a TLS op ever blocks.
Result<void> wait_fd(wire::SocketHandle fd, bool read,
                     std::chrono::steady_clock::time_point deadline) {
  for (;;) {
    int ms = -1;
    if (deadline.time_since_epoch().count() != 0) {
      const auto remaining = deadline - std::chrono::steady_clock::now();
      if (remaining <= std::chrono::milliseconds::zero()) {
        return Status::timeout("TLS op timed out");
      }
      ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count());
      if (ms <= 0) ms = 1;
    }
#if defined(_WIN32)
    WSAPOLLFD pfd{};
    pfd.fd = static_cast<SOCKET>(fd);
    pfd.events = read ? POLLRDNORM : POLLWRNORM;
    int pr = ::WSAPoll(&pfd, 1, ms);
    if (pr < 0) {
      const int e = WSAGetLastError();
      if (e == WSAEINTR) continue;
      return Status::io_error("WSAPoll failed");
    }
    if (pr == 0) return Status::timeout("TLS op timed out");
    if ((pfd.revents & POLLNVAL) != 0) return Status::closed("fd closed");
#else
    struct pollfd pfd {};
    pfd.fd = fd;
    pfd.events = read ? POLLIN : POLLOUT;
    int pr = ::poll(&pfd, 1, ms);
    if (pr < 0) {
      if (errno == EINTR) continue;
      return Status::io_error("poll failed");
    }
    if (pr == 0) return Status::timeout("TLS op timed out");
    if ((pfd.revents & POLLNVAL) != 0) return Status::closed("fd closed");
#endif
    // POLLHUP / POLLERR are surfaced by the retried SSL op (which sees the
    // real error); report ready so the loop does not spin on a stale event.
    return {};
  }
}

// Classifies a non-WANT SSL error. A certificate verification failure surfaces
// kInvalidArgument with the reason (the verify result is sticky on the SSL);
// everything else is kIoError.
Status ssl_error_status(SSL* ssl, const std::string& what) {
  const long vr = SSL_get_verify_result(ssl);
  if (vr != X509_V_OK) {
    return Status::invalid_argument(
        "certificate verification failed: " +
        std::string(X509_verify_cert_error_string(vr)));
  }
  return Status::io_error(what);
}

}  // namespace

SslContext::~SslContext() {
  if (ctx_) SSL_CTX_free(ctx_);
}

Result<std::shared_ptr<SslContext>> SslContext::make_client(const TlsOptions& opt) {
  ensure_openssl_init();
  SSL_CTX* raw = SSL_CTX_new(TLS_client_method());
  if (raw == nullptr) {
    return Status::io_error("SSL_CTX_new(client) failed");
  }
  auto ctx = std::shared_ptr<SslContext>(new SslContext(raw));
  apply_modern_policy(raw);
  auto trust = load_trust(raw, opt.ca_path);
  if (!trust.ok()) return trust.status();
  if (!opt.cert_path.empty()) {
    if (opt.key_path.empty()) {
      return Status::invalid_argument("client certificate set without key_path");
    }
    auto ck = load_cert_key(raw, opt.cert_path, opt.key_path);
    if (!ck.ok()) return ck.status();
  }
  // The client always verifies the server; verify_peer_name controls the
  // hostname/SAN check at the connection (X509_VERIFY_PARAM) level.
  (void)SSL_CTX_set_verify(raw, SSL_VERIFY_PEER, nullptr);
  return ctx;
}

Result<std::shared_ptr<SslContext>> SslContext::make_server(const TlsOptions& opt) {
  ensure_openssl_init();
  if (opt.cert_path.empty() || opt.key_path.empty()) {
    return Status::invalid_argument(
        "TLS listener requires cert_path and key_path");
  }
  SSL_CTX* raw = SSL_CTX_new(TLS_server_method());
  if (raw == nullptr) {
    return Status::io_error("SSL_CTX_new(server) failed");
  }
  auto ctx = std::shared_ptr<SslContext>(new SslContext(raw));
  apply_modern_policy(raw);
  auto ck = load_cert_key(raw, opt.cert_path, opt.key_path);
  if (!ck.ok()) return ck.status();
  if (!opt.ca_path.empty()) {
    auto trust = load_trust(raw, opt.ca_path);
    if (!trust.ok()) return trust.status();
  }
  if (opt.require_client_cert) {
    // mTLS: require and verify a client certificate against the trust store.
    (void)SSL_CTX_set_verify(raw, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                             nullptr);
  } else {
    (void)SSL_CTX_set_verify(raw, SSL_VERIFY_NONE, nullptr);
  }
  return ctx;
}

Result<void> handshake(SSL* ssl, wire::SocketHandle fd, bool server,
                       std::chrono::milliseconds timeout) {
  // The fd must be non-blocking so SSL_connect/SSL_accept report
  // SSL_ERROR_WANT_READ/WANT_WRITE instead of blocking past the deadline.
  auto nbio = socket_utils::set_nonblocking(fd, true);
  if (!nbio.ok()) return nbio.status();
  const auto deadline = timeout.count() > 0
      ? std::chrono::steady_clock::now() + timeout
      : std::chrono::steady_clock::time_point{};
  for (;;) {
    const int r = server ? SSL_accept(ssl) : SSL_connect(ssl);
    if (r == 1) return {};
    const int err = SSL_get_error(ssl, r);
    if (err == SSL_ERROR_WANT_READ) {
      auto st = wait_fd(fd, /*read=*/true, deadline);
      if (!st.ok()) return st.status();
      continue;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
      auto st = wait_fd(fd, /*read=*/false, deadline);
      if (!st.ok()) return st.status();
      continue;
    }
    return ssl_error_status(ssl, server ? "TLS handshake (accept) failed"
                                        : "TLS handshake (connect) failed");
  }
}

Result<std::size_t> TlsStream::write_some(const void* data, std::size_t len,
                                          std::chrono::milliseconds timeout) {
  const auto deadline = timeout.count() > 0
      ? std::chrono::steady_clock::now() + timeout
      : std::chrono::steady_clock::time_point{};
  for (;;) {
    std::size_t written = 0;
    int r = 0;
    {
      // Hold the op mutex only for the SSL call; release it across the poll
      // wait so the other direction can make progress (no bidirectional
      // backpressure deadlock).
      std::lock_guard<std::mutex> lk(*op_mu_);
      r = SSL_write_ex(ssl_, data, len, &written);
    }
    if (r == 1) return written;
    const int err = SSL_get_error(ssl_, r);
    if (err == SSL_ERROR_WANT_READ) {
      auto st = wait_fd(fd_, /*read=*/true, deadline);
      if (!st.ok()) return st.status();
      continue;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
      auto st = wait_fd(fd_, /*read=*/false, deadline);
      if (!st.ok()) return st.status();
      continue;
    }
    if (err == SSL_ERROR_ZERO_RETURN) return Status::closed("peer closed");
    return ssl_error_status(ssl_, "TLS write failed");
  }
}

Result<std::size_t> TlsStream::read_some(void* buf, std::size_t cap,
                                         std::chrono::milliseconds timeout) {
  const auto deadline = timeout.count() > 0
      ? std::chrono::steady_clock::now() + timeout
      : std::chrono::steady_clock::time_point{};
  for (;;) {
    std::size_t got = 0;
    int r = 0;
    {
      std::lock_guard<std::mutex> lk(*op_mu_);
      r = SSL_read_ex(ssl_, buf, cap, &got);
    }
    if (r == 1) return got;
    const int err = SSL_get_error(ssl_, r);
    if (err == SSL_ERROR_WANT_READ) {
      auto st = wait_fd(fd_, /*read=*/true, deadline);
      if (!st.ok()) return st.status();
      continue;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
      auto st = wait_fd(fd_, /*read=*/false, deadline);
      if (!st.ok()) return st.status();
      continue;
    }
    if (err == SSL_ERROR_ZERO_RETURN) return 0;  // clean close_notify
    return ssl_error_status(ssl_, "TLS read failed");
  }
}

void TlsStream::shutdown() {
  // Wake a blocked poll/SSL op so close() cannot hang. The fd (and the SSL) is
  // owned and released by the transport; here we only interrupt the wait. No
  // SSL_shutdown: it would race an in-flight SSL op on another thread, and the
  // fd shutdown already delivers the wake-up.
#if defined(_WIN32)
  ::shutdown(static_cast<SOCKET>(fd_), SD_BOTH);
#else
  ::shutdown(fd_, SHUT_RDWR);
#endif
}

}  // namespace tls
}  // namespace duct

#pragma once

#include <chrono>
#include <memory>
#include <mutex>

#include "duct/duct.h"
#include "duct/wire.h"

// OpenSSL forward declaration (tls_stream.cc includes the real header).
typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;

namespace duct {
namespace tls {

// RAII SSL_CTX configured for the client or server role from TlsOptions.
// The library only consumes PEM paths; it never generates or stores
// credentials (docs/design/tls-mtls.md).
class SslContext {
 public:
  // Loads trust anchors (ca_path or the system store), the leaf cert/key, and
  // applies a fixed modern policy pinned to TLS 1.3. Certificate verification
  // failure (bad CA, expired, wrong host) is a call-site concern: this only
  // loads the material and sets the policy.
  static Result<std::shared_ptr<SslContext>> make_client(const TlsOptions& opt);
  static Result<std::shared_ptr<SslContext>> make_server(const TlsOptions& opt);

  ~SslContext();
  SslContext(const SslContext&) = delete;
  SslContext& operator=(const SslContext&) = delete;

  SSL_CTX* get() const { return ctx_; }

 private:
  explicit SslContext(SSL_CTX* ctx) : ctx_(ctx) {}
  SSL_CTX* ctx_ = nullptr;
};

// Performs the TLS handshake (SSL_connect or SSL_accept) on `ssl` bound to
// `fd`, bounded by `timeout` (0 = block). The fd is switched to non-blocking
// so a bounded handshake cannot hang; TlsStream assumes it stays non-blocking.
// Returns kIoError on a transport failure and kInvalidArgument with the verify
// reason surfaced on a certificate verification failure.
Result<void> handshake(SSL* ssl, wire::SocketHandle fd, bool server,
                       std::chrono::milliseconds timeout);

// A wire::Stream over an established SSL connection bound to `fd`. The fd must
// be non-blocking; reads/writes wait on the fd (poll) so a timeout still bounds
// a blocked TLS op. The stream does not own the SSL or the fd.
//
// A single SSL object is not safe for concurrent SSL_read/SSL_write, but
// serializing them under one mutex and holding it across a poll wait would
// deadlock under bidirectional backpressure (TLS 1.3 never renegotiates, so a
// write only ever needs writability and a read only readability). The mutex is
// therefore held only around each SSL op and released during the poll wait:
// at most one op runs at a time, yet the other direction can still make
// progress while this one waits for the socket.
class TlsStream final : public wire::Stream {
 public:
  // `ssl` is a fully-established SSL* bound to `fd`. `op_mu` serializes SSL
  // ops across every TlsStream of the same connection (the pipe's mutex).
  TlsStream(SSL* ssl, wire::SocketHandle fd, std::shared_ptr<std::mutex> op_mu)
      : ssl_(ssl), fd_(fd), op_mu_(std::move(op_mu)) {}

  Result<std::size_t> write_some(const void* data, std::size_t len,
                                 std::chrono::milliseconds timeout) override;
  Result<std::size_t> read_some(void* buf, std::size_t cap,
                                std::chrono::milliseconds timeout) override;
  void shutdown() override;

 private:
  SSL* ssl_;
  wire::SocketHandle fd_;
  std::shared_ptr<std::mutex> op_mu_;
};

}  // namespace tls
}  // namespace duct

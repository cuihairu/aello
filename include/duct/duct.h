#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <functional>
#include <memory>
#include <string>

#include "duct/address.h"
#include "duct/message.h"
#include "duct/status.h"
#include "duct/wire.h"

namespace duct {

enum class BackpressurePolicy {
  kBlock = 0,
  kDropNew,
  kDropOld,
  kFailFast,
};

enum class Reliability {
  kAtMostOnce = 0,
  kAtLeastOnce,
};

struct ReconnectPolicy {
  // If enabled, dial() returns a pipe that connects/reconnects automatically.
  bool enabled = false;

  // Initial reconnect delay after a disconnect.
  std::chrono::milliseconds initial_delay{100};
  // Maximum backoff delay between attempts.
  std::chrono::milliseconds max_delay{30'000};
  // Exponential backoff multiplier.
  double backoff_multiplier = 2.0;
  // Maximum reconnect attempts; 0 means retry forever.
  int max_attempts = 0;

  // Heartbeat/keepalive interval. For tcp:// this maps to OS TCP keepalive settings.
  // Zero means disabled.
  std::chrono::milliseconds heartbeat_interval{5'000};
};

enum class ConnectionState {
  kConnecting = 0,
  kConnected,
  kDisconnected,
  kReconnecting,
  kClosed,
};

using ConnectionCallback = std::function<void(ConnectionState, const std::string& reason)>;

struct QosOptions {
  // Explicit opt-in for the QoS wrapper. When false, dial() returns the raw
  // transport pipe even if other QoS fields are set.
  bool enabled = false;

  // Bytes are more stable than msg-count when payload sizes vary.
  // Zero disables the byte watermark (unlimited queue).
  std::size_t snd_hwm_bytes = 4 * 1024 * 1024;
  std::size_t rcv_hwm_bytes = 4 * 1024 * 1024;
  // Item-count cap on the send queue; zero disables it.
  std::size_t snd_hwm_count = 0;
  BackpressurePolicy backpressure = BackpressurePolicy::kBlock;

  // Per-message TTL; zero means disabled.
  std::chrono::milliseconds ttl{0};

  // Close linger/drain time for queued outbound messages; zero means best-effort immediate close.
  std::chrono::milliseconds linger{0};

  Reliability reliability = Reliability::kAtMostOnce;

  // Largest payload the underlying transport can carry in one message. A send
  // above this is rejected synchronously: accepting it and dropping it later in
  // the drain thread would lose data after send() already reported success.
  // With fragmentation enabled the transport can carry arbitrary payloads; this
  // defaults to unlimited (size_t max) so large messages are split into chunks.
  std::size_t max_message_bytes = static_cast<std::size_t>(-1);

  // Token-bucket rate limit. rate_bytes_per_sec == 0 disables the limiter.
  // burst_bytes is the maximum burst allowed (defaults to one second's worth).
  std::size_t rate_bytes_per_sec = 0;
  std::size_t burst_bytes = 0;
};

// Geometry of a shared-memory ring. Both peers must agree on these values;
// the listener validates them against the segment header on accept().
struct ShmOptions {
  // Per-slot payload capacity in bytes. Must be positive and leave the top
  // two bits of the 32-bit length word free for fragmentation flags.
  std::size_t slot_bytes = 64 * 1024;
  // Slots per ring. Each direction (client->server and server->client) has
  // its own ring of this many slots. Must be positive.
  std::uint32_t slot_count = 64;
};

struct SendOptions {
  // If non-zero, send will time out (where supported).
  std::chrono::milliseconds timeout{0};
  // Higher values drain first from the QoS send queue. Zero is the default.
  int priority = 0;
  // Multiplexing channel id; carried on the wire so the receiver can demux.
  std::uint32_t channel_id = 0;
};

struct RecvOptions {
  std::chrono::milliseconds timeout{0};
  // Demux channel id; zero means accept any channel.
  std::uint32_t channel_id = 0;
};

// Peer identity for a connected pipe. The OS populates these on UDS via
// SO_PEERCRED (uid/gid/pid of the connecting process). Other transports return
// kNotSupported until a peer-identity mechanism is wired.
struct PeerCredential {
  std::uint32_t uid = 0;
  std::uint32_t gid = 0;
  std::int64_t pid = 0;
};

class Pipe {
 public:
  virtual ~Pipe() = default;
  virtual Result<void> send(const Message& msg, const SendOptions& opt) = 0;
  virtual Result<Message> recv(const RecvOptions& opt) = 0;
  // Reads a frame together with its wire flags. Transports that carry a channel
  // id on the wire (tcp/uds) return the real flags; others return flags == 0.
  // The default delegates to recv(), so filtering wrappers keep working.
  virtual Result<wire::FrameMessage> recv_frame(const RecvOptions& opt) {
    auto r = recv(opt);
    if (!r.ok()) return r.status();
    return wire::FrameMessage{std::move(r.value()), 0};
  }
  // Largest message the transport accepts in one send() call. The default is
  // unlimited; byte-stream transports use fragmentation and ignore this cap,
  // while non-fragmenting transports expose their exact send-side bound so QoS
  // can reject oversize messages before enqueueing them.
  virtual std::size_t max_message_bytes() const { return std::numeric_limits<std::size_t>::max(); }

  virtual void close() = 0;
  // Returns the peer's credential (uid/gid/pid) when the transport supports it.
  // Default: not supported. TCP/SHM have no OS peer-identity mechanism yet.
  virtual Result<PeerCredential> peer_credential() const {
    return Status::not_supported("peer credential not supported");
  }
};

class Listener {
 public:
  virtual ~Listener() = default;
  virtual Result<std::unique_ptr<Pipe>> accept() = 0;
  // Returns the effective local address. Useful when listening on port 0 (ephemeral).
  // Default implementation returns kNotSupported.
  virtual Result<std::string> local_address() const {
    return Status::not_supported("local_address not supported");
  }
  virtual void close() = 0;
};

struct DialOptions {
  // Bound for the overall connection attempt. A zero timeout uses the transport's
  // default behavior.
  std::chrono::milliseconds timeout{0};
  QosOptions qos{};
  ShmOptions shm{};
  // When enabled, dial() wraps the transport in a ReconnectPipe that redials on
  // disconnect (see src/duct.cc).
  ReconnectPolicy reconnect{};
  // Called on each connection-state transition of a reconnect-enabled dial.
  ConnectionCallback on_state_change{};
};

struct ListenOptions {
  // Reserved: no listener implementation applies it yet.
  QosOptions qos{};
  // Expected shm ring geometry. The listener validates that the segment the
  // dialer created matches these values, rejecting a mismatched peer.
  ShmOptions shm{};
  int backlog = 128;
  // Bound for transport-level handshake completion on accepted sockets.
  // A silent peer is rejected after this interval instead of blocking accept().
  std::chrono::milliseconds handshake_timeout{5000};
};

// Minimal entry points (pattern layer comes later).
Result<std::unique_ptr<Listener>> listen(const std::string& address, const ListenOptions& opt = {});
Result<std::unique_ptr<Pipe>> dial(const std::string& address, const DialOptions& opt = {});

}  // namespace duct

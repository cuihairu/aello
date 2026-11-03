#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "duct/address.h"
#include "duct/message.h"
#include "duct/status.h"

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
  // Bytes are more stable than msg-count when payload sizes vary.
  // Zero disables the byte watermark (unlimited queue).
  std::size_t snd_hwm_bytes = 4 * 1024 * 1024;
  std::size_t rcv_hwm_bytes = 4 * 1024 * 1024;
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

class Pipe {
 public:
  virtual ~Pipe() = default;
  virtual Result<void> send(const Message& msg, const SendOptions& opt) = 0;
  virtual Result<Message> recv(const RecvOptions& opt) = 0;
  virtual void close() = 0;
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
  // default behavior. Reconnect-enabled dials are not wired yet.
  std::chrono::milliseconds timeout{0};
  QosOptions qos{};
  // Reserved: not yet honored by dial() (see src/duct.cc).
  ReconnectPolicy reconnect{};
  // Reserved: not yet honored by dial() (see src/duct.cc).
  ConnectionCallback on_state_change{};
};

struct ListenOptions {
  // Reserved: no listener implementation applies it yet.
  QosOptions qos{};
  int backlog = 128;
};

// Minimal entry points (pattern layer comes later).
Result<std::unique_ptr<Listener>> listen(const std::string& address, const ListenOptions& opt = {});
Result<std::unique_ptr<Pipe>> dial(const std::string& address, const DialOptions& opt = {});

}  // namespace duct

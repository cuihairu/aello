#include "duct/duct.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>

#include "duct/config.h"
#include "duct/qos_pipe.h"

#include "reconnect_pipe.h"
#include "reliable_pipe.h"

namespace duct {

bool tls_available() { return DUCT_HAVE_TLS != 0; }

// Implemented in tcp_transport.cc.
Result<std::unique_ptr<Listener>> tcp_listen(const TcpAddress& addr, const ListenOptions& opt);
// `reliability_session_id` is the stable identity of this logical dial (0 for a
// non-reliable dial); a reliable dial announces it in the handshake so the
// listener can resume the session on every redial.
Result<std::unique_ptr<Pipe>> tcp_dial(const TcpAddress& addr, const DialOptions& opt,
                                       std::uint64_t reliability_session_id = 0);

// Implemented in shm_transport.cc.
Result<std::unique_ptr<Listener>> shm_listen(const std::string& name, const ListenOptions& opt);
Result<std::unique_ptr<Pipe>> shm_dial(const std::string& name, const DialOptions& opt);

// Implemented in pipe_transport.cc (Windows only).
#if defined(_WIN32)
Result<std::unique_ptr<Listener>> pipe_listen(const std::string& name, const ListenOptions& opt);
Result<std::unique_ptr<Pipe>> pipe_dial(const std::string& name, const DialOptions& opt);
#endif

// Implemented in uds_transport.cc (returns not_supported on Windows).
Result<std::unique_ptr<Listener>> uds_listen(const std::string& path, const ListenOptions& opt);
Result<std::unique_ptr<Pipe>> uds_dial(const std::string& path, const DialOptions& opt,
                                       std::uint64_t reliability_session_id = 0);

Result<std::unique_ptr<Listener>> listen(const std::string& address, const ListenOptions& opt) {
  auto parsed = Address::parse(address);
  if (!parsed.ok()) {
    return parsed.status();
  }
  const Address& a = parsed.value();
  if (opt.tls.enabled) {
    if (a.scheme != Scheme::kTcp) {
      return Status::not_supported("tls is only supported on tcp://");
    }
    if (!tls_available()) {
      return Status::not_supported("TLS support not built into this library");
    }
  }
  if (a.scheme == Scheme::kTcp) {
    return tcp_listen(a.tcp, opt);
  }
  if (a.scheme == Scheme::kShm) {
    return shm_listen(a.name, opt);
  }
#if defined(_WIN32)
  if (a.scheme == Scheme::kPipe) {
    return pipe_listen(a.name, opt);
  }
#endif
  if (a.scheme == Scheme::kUds) {
    return uds_listen(a.name, opt);
  }
  return Status::not_supported("listen scheme not supported yet: " + a.scheme_text);
}

namespace {

// One dial attempt: parse, build the transport pipe, and apply the QoS wrapper.
// Does not apply reconnect; dial() wraps this when reconnect is enabled.
// `reliability_session_id` is the reliable dial's stable identity; 0 otherwise.
Result<std::unique_ptr<Pipe>> dial_impl(const std::string& address, const DialOptions& opt,
                                       std::uint64_t reliability_session_id = 0) {
  auto parsed = Address::parse(address);
  if (!parsed.ok()) {
    return parsed.status();
  }
  const Address& a = parsed.value();
  std::unique_ptr<Pipe> base_pipe;

  if (a.scheme == Scheme::kTcp) {
    auto result = tcp_dial(a.tcp, opt, reliability_session_id);
    if (!result.ok()) {
      return result.status();
    }
    base_pipe = std::move(result.value());
  } else if (a.scheme == Scheme::kShm) {
    auto result = shm_dial(a.name, opt);
    if (!result.ok()) {
      return result.status();
    }
    base_pipe = std::move(result.value());
#if defined(_WIN32)
  } else if (a.scheme == Scheme::kPipe) {
    auto result = pipe_dial(a.name, opt);
    if (!result.ok()) {
      return result.status();
    }
    base_pipe = std::move(result.value());
#endif
  } else if (a.scheme == Scheme::kUds) {
    auto result = uds_dial(a.name, opt, reliability_session_id);
    if (!result.ok()) {
      return result.status();
    }
    base_pipe = std::move(result.value());
  } else {
    return Status::not_supported("dial scheme not supported yet: " + a.scheme_text);
  }

  // Apply the QoS wrapper only when explicitly enabled.
  if (opt.qos.enabled) {
    auto qos_pipe = std::make_unique<QosPipe>(std::move(base_pipe), opt.qos);
    return std::unique_ptr<Pipe>(std::move(qos_pipe));
  }

  return base_pipe;
}

// First-connect wait for reconnect-enabled dials: dial() must hand back a live
// pipe or a dial error rather than succeeding while the worker still tries.
constexpr auto kReconnectFirstConnectDefault = std::chrono::milliseconds(5000);

// A session id is the lifetime identity of one reliable dial: it survives every
// redial, so the server can resume the session's counters and so the receiver
// can tell a replay from a new message. Never 0 — 0 is the v1 frame sentinel.
std::uint64_t make_reliability_session_id() {
  static std::atomic<std::uint64_t> counter{0};
  const std::uint64_t tick = static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const std::uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed) + 1;
  std::uint64_t id = tick ^ (0x9E3779B97F4A7C15ull * seq);
  if (id == 0) id = 0x9E3779B97F4A7C15ull;
  return id;
}

// Reliable dial: QoS(ReliablePipe(ReconnectPipe(transport))). The QoS wrapper
// sits outside the reliability layer so its queue bounds the caller-facing
// backpressure while the reliability window bounds what is actually in flight
// and unacked — the innermost bound is the one the wire honors.
//
// The session id and the unacked window both outlive the transport pipe: they
// are the whole reason redials deliver rather than lose.
Result<std::unique_ptr<Pipe>> dial_reliable_reconnect(const std::string& address,
                                                     const DialOptions& opt) {
  DialOptions base = opt;
  base.reconnect.enabled = false;  // Each attempt dials once, no nested reconnect.
  const bool want_qos = base.qos.enabled;
  base.qos.enabled = false;  // dial_impl must not wrap QoS here

  const std::uint64_t session_id = make_reliability_session_id();
  const auto state = std::make_shared<ReliabilitySessionState>();
  const auto reconnect = std::make_shared<ReliabilityReconnect>();

  // The session id is captured by value so every redial announces the SAME
  // session to the listener, which is what lets it resume rather than restart.
  auto dial_once = [address, base, session_id]() -> Result<std::unique_ptr<Pipe>> {
    return dial_impl(address, base, session_id);
  };
  const auto first_wait = opt.timeout.count() > 0 ? opt.timeout
                                                  : kReconnectFirstConnectDefault;
  auto rc_pipe = make_connected_reconnect_pipe(
      std::move(dial_once), opt.reconnect,
      chain_reliability_callback(reconnect, opt.on_state_change), first_wait);
  if (!rc_pipe.ok()) return rc_pipe.status();

  ReliablePipeConfig cfg;
  cfg.session_id = session_id;
  cfg.state = state;
  cfg.reconnect = reconnect;
  cfg.window_bytes = opt.qos.snd_hwm_bytes;  // 0 = unbounded
  cfg.backpressure = opt.qos.backpressure;
  auto reliable = make_reliable_pipe(std::move(rc_pipe.value()), cfg);

  if (want_qos) {
    auto qos_pipe = std::make_unique<QosPipe>(std::move(reliable), opt.qos);
    return std::unique_ptr<Pipe>(std::move(qos_pipe));
  }
  return std::unique_ptr<Pipe>(std::move(reliable));
}

}  // namespace

Result<std::unique_ptr<Pipe>> dial(const std::string& address, const DialOptions& opt) {
  auto parsed = Address::parse(address);
  if (!parsed.ok()) {
    return parsed.status();
  }
  const bool reliable = opt.qos.reliability == Reliability::kAtLeastOnce;
  // Reliability needs a sequenced, redialable byte stream: a session id and a
  // retransmit window mean nothing over shm (a ring, not a stream) or a Windows
  // pipe. Reject loudly at dial instead of silently degrading mid-stream.
  if (reliable && parsed.value().scheme != Scheme::kTcp &&
      parsed.value().scheme != Scheme::kUds) {
    return Status::not_supported("at-least-once reliability is not supported on " +
                                 parsed.value().scheme_text);
  }
  if (opt.tls.enabled && parsed.value().scheme != Scheme::kTcp) {
    return Status::not_supported("tls is only supported on tcp://");
  }
  if (opt.tls.enabled && !tls_available()) {
    return Status::not_supported("TLS support not built into this library");
  }

  DialOptions effective = opt;
  // A reliability dial always reconnects: the replay needs a redial to land on,
  // and the caller's own policy still governs delays and attempt counts.
  if (reliable) {
    effective.reconnect.enabled = true;
    return dial_reliable_reconnect(address, effective);
  }

  if (effective.reconnect.enabled) {
    DialOptions base = effective;
    base.reconnect.enabled = false;
    auto dial_once = [address, base]() -> Result<std::unique_ptr<Pipe>> {
      return dial_impl(address, base);
    };
    const auto first_wait = effective.timeout.count() > 0
        ? effective.timeout : kReconnectFirstConnectDefault;
    return make_connected_reconnect_pipe(std::move(dial_once), effective.reconnect,
                                         effective.on_state_change, first_wait);
  }
  return dial_impl(address, effective);
}

}  // namespace duct

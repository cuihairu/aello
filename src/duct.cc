#include "duct/duct.h"

#include <memory>

#include "duct/qos_pipe.h"

#include "reconnect_pipe.h"

namespace duct {

// Implemented in tcp_transport.cc.
Result<std::unique_ptr<Listener>> tcp_listen(const TcpAddress& addr, const ListenOptions& opt);
Result<std::unique_ptr<Pipe>> tcp_dial(const TcpAddress& addr, const DialOptions& opt);

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
Result<std::unique_ptr<Pipe>> uds_dial(const std::string& path, const DialOptions& opt);

Result<std::unique_ptr<Listener>> listen(const std::string& address, const ListenOptions& opt) {
  auto parsed = Address::parse(address);
  if (!parsed.ok()) {
    return parsed.status();
  }
  const Address& a = parsed.value();
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
Result<std::unique_ptr<Pipe>> dial_impl(const std::string& address, const DialOptions& opt) {
  auto parsed = Address::parse(address);
  if (!parsed.ok()) {
    return parsed.status();
  }
  const Address& a = parsed.value();
  std::unique_ptr<Pipe> base_pipe;

  if (a.scheme == Scheme::kTcp) {
    auto result = tcp_dial(a.tcp, opt);
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
    auto result = uds_dial(a.name, opt);
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

}  // namespace

Result<std::unique_ptr<Pipe>> dial(const std::string& address, const DialOptions& opt) {
  auto parsed = Address::parse(address);
  if (!parsed.ok()) {
    return parsed.status();
  }
  if (opt.reconnect.enabled) {
    DialOptions base = opt;
    base.reconnect.enabled = false;  // Each attempt dials once, no nested reconnect.
    auto dial_once = [address, base]() -> Result<std::unique_ptr<Pipe>> {
      return dial_impl(address, base);
    };
    // dial() must return a connected pipe or a dial error, exactly like a
    // plain dial: wait for the first connect within the caller's deadline
    // (0 = default bound) instead of succeeding vacuously while the background
    // worker is still trying an unreachable peer. Later disconnects still
    // redial through the worker.
    constexpr auto kReconnectFirstConnectDefault = std::chrono::milliseconds(5000);
    const auto first_wait = opt.timeout.count() > 0 ? opt.timeout
                                                    : kReconnectFirstConnectDefault;
    return make_connected_reconnect_pipe(std::move(dial_once), opt.reconnect,
                                         opt.on_state_change, first_wait);
  }
  return dial_impl(address, opt);
}

}  // namespace duct

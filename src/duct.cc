#include "duct/duct.h"

#include <memory>

#include "duct/qos_pipe.h"

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

Result<std::unique_ptr<Pipe>> dial(const std::string& address, const DialOptions& opt) {
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

  // Apply QoS wrapper if any QoS options are configured
  if (opt.qos.snd_hwm_bytes != 0 || opt.qos.backpressure != BackpressurePolicy::kBlock) {
    auto qos_pipe = std::make_unique<QosPipe>(std::move(base_pipe), opt.qos);
    return std::unique_ptr<Pipe>(std::move(qos_pipe));
  }

  return base_pipe;
}

}  // namespace duct

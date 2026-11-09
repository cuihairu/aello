#pragma once

#include <chrono>
#include <functional>
#include <memory>

#include "duct/duct.h"
#include "duct/status.h"

namespace duct {

using DialOnceFn = std::function<Result<std::unique_ptr<Pipe>>()>;

std::unique_ptr<Pipe> make_reconnect_pipe(DialOnceFn dial_once,
                                         ReconnectPolicy policy,
                                         ConnectionCallback on_state_change);

// Like make_reconnect_pipe, but the first connection attempt must complete
// within first_connect_timeout before the factory returns: the caller gets a
// connected pipe or a dial error (kTimeout, or kIoError once the policy's
// attempts are exhausted). This keeps dial()'s "returns connected or failed"
// shape for reconnect-enabled dials instead of succeeding vacuously while the
// background worker is still trying.
Result<std::unique_ptr<Pipe>> make_connected_reconnect_pipe(
    DialOnceFn dial_once, const ReconnectPolicy& policy,
    ConnectionCallback on_state_change,
    std::chrono::milliseconds first_connect_timeout);

}  // namespace duct


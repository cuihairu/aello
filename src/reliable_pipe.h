#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "duct/duct.h"
#include "duct/message.h"
#include "duct/status.h"

namespace duct {

// Shared reliability state for one logical session. A session id is generated
// once by the dialer and announced in the handshake, so both directions of the
// connection and every redial refer to the same state object:
//   - next_expected_in: the recv-side dedup cursor (next contiguous seq).
//   - next_seq_out: the send-side sequence counter.
//   - window / unacked_bytes: the retransmit window (messages sent, not yet
//     cumulatively acknowledged), bounded by the window budget.
// The dialer owns one state for the life of the pipe; the listener's
// ReliabilitySessionTable owns one per session id, shared by every accepted
// connection that resumes it, so a redial continues the counters instead of
// restarting them.
class ReliabilitySessionState {
 public:
  std::mutex mu;
  std::condition_variable cv;
  std::uint64_t next_expected_in = 0;
  std::uint64_t next_seq_out = 0;
  std::uint64_t unacked_bytes = 0;
  std::deque<std::pair<std::uint64_t, Message>> window;  // seq order, unacked
};

// Listener-side map from session id to its shared state. Capped so a peer that
// sprays session ids cannot grow it without limit; a capped-out session is
// rejected at accept() (kQueueFull) rather than silently degrading.
class ReliabilitySessionTable {
 public:
  explicit ReliabilitySessionTable(std::size_t max_sessions);
  // Returns the state for `session_id`, creating a fresh one when unknown.
  Result<std::shared_ptr<ReliabilitySessionState>> resume_or_start(std::uint64_t session_id);

 private:
  std::mutex mu_;
  std::map<std::uint64_t, std::shared_ptr<ReliabilitySessionState>> sessions_;
  std::size_t max_sessions_ = 0;
};

// Shared reconnect trigger for the dialer side. duct.cc chains the dial's
// ConnectionCallback through this so a kConnected-after-disconnect replays the
// unacked window; the ReliablePipe registers the replay hook and clears it on
// destruction, which keeps late callbacks (kClosed during teardown) safe.
class ReliabilityReconnect {
 public:
  std::function<void()> on_reconnect;  // set by the ReliablePipe

  // Called from the reconnect worker with each connection-state transition.
  void on_state(ConnectionState st);

 private:
  std::mutex mu_;
  bool was_disconnected_ = false;
};

struct ReliablePipeConfig {
  std::uint64_t session_id = 0;                    // this session's identity (never 0)
  std::shared_ptr<ReliabilitySessionState> state;  // shared session state
  // Dialer-side reconnect trigger; null on the listener side (an accepted pipe
  // resumes the session from the table instead of reconnecting itself).
  std::shared_ptr<ReliabilityReconnect> reconnect;
  std::size_t window_bytes = 4 * 1024 * 1024;      // retransmit window budget
  BackpressurePolicy backpressure = BackpressurePolicy::kBlock;
  // True when the pipe is bound to a session that already has unacked
  // messages (a server-side resume): the window is replayed as soon as the
  // fresh connection is live.
  bool replay_pending_window_on_bind = false;
};

// Listener-side reliability configuration: a session table shared by every
// accepted connection of one listener, plus the per-pipe window bound. A null
// table means the listener does not offer reliability (accepted pipes are never
// wrapped). The listener creates this in listen() when the caller opted into
// kAtLeastOnce.
struct ReliabilityListenerConfig {
  std::shared_ptr<ReliabilitySessionTable> table;
  std::size_t window_bytes = 4 * 1024 * 1024;  // 0 = unbounded
  BackpressurePolicy backpressure = BackpressurePolicy::kBlock;
};

// Wraps a (typically reconnect-enabled) transport pipe with the reliability
// layer. The caller has already negotiated the handshake; if the negotiated
// capabilities do not carry kCapabilityReliable the wrapper degrades to a
// byte-for-byte passthrough (v1 peer).
std::unique_ptr<Pipe> make_reliable_pipe(std::unique_ptr<Pipe> inner,
                                         const ReliablePipeConfig& cfg);

// Reconnect-hook factory: wraps the dialer's ConnectionCallback so the
// reliability layer replays its window on every kConnected-after-disconnect,
// before the user's callback runs.
ConnectionCallback chain_reliability_callback(std::shared_ptr<ReliabilityReconnect> rc,
                                              ConnectionCallback user_cb);

}  // namespace duct

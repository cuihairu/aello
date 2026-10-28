# duct TODO / Roadmap

This document captures the initial design decisions and the planned feature set.

## Scope
- Message-oriented IPC + networking for distributed game server processes
- Local fast path: `flatProto` + shared memory payload + notification wakeups
- Cross-machine support (same IDC / LAN): TCP (optional TLS/mTLS later)
- Reliability/QoS is opt-in and ideally configurable per channel/message (so realtime traffic can be lossy while critical traffic is reliable)

## Milestones

### M0: Project skeleton (now)
- CMake build, headers, minimal TCP framing transport
- Example binaries to validate compilation and basic send/recv

### M1: Core API + options
- Public API: `listen`/`dial`, `Pipe`, `Message`, `SendOptions`/`RecvOptions`
- Address parsing and scheme dispatch (`shm/uds/pipe/tcp`)
- Timeouts, close semantics, cancellation hooks (if needed)
- `DialOptions.timeout` applied to `tcp/uds/shm` dial path (connect/handshake where possible)
- Error model: retryable vs non-retryable error codes
- Observability hooks: pluggable logger + minimal metrics surface (counters/gauges)

### M2: Connection lifecycle + health (per-connection)
- Implemented:
  - Connection callbacks: `on_state_change` (dial-side)
  - Auto-reconnect (opt-in): exponential backoff + jitter
  - TCP keepalive mapping via `reconnect.heartbeat_interval`
  - Reconnect dial attempts are bounded (so `close()` can join)
- Connection state machine: CONNECTING/CONNECTED/DISCONNECTED/RECONNECTING/CLOSED
- Connection callbacks: `on_error` (todo)
- Heartbeat/keepalive:
  - Detect half-open connections
  - Optional NAT keepalive for TCP
- Auto-reconnect (opt-in):
  - Bounded attempts (or infinite), bounded downtime buffer
  - New `session_id` per successful (re)connect
- Graceful shutdown:
  - `linger`/`drain` options for flushing queued messages
  - Close reason propagation

### M3: QoS + backpressure (per-connection)
- Implemented:
  - `QosPipe`: async send/recv queues + background I/O threads
  - `snd_hwm_bytes` / `rcv_hwm_bytes`, backpressure policy, TTL
  - `linger`: bounded best-effort drain on close
- Queue limits: `snd_hwm_bytes|msgs`, `rcv_hwm_bytes|msgs`
- Backpressure policy (on HWM):
  - `block` (default)
  - `drop_new`
  - `drop_old`
  - `fail_fast` (`EAGAIN`)
- TTL/deadline per message
- Priority scheduling:
  - Reserve `channel_id` in the protocol
  - Start with a single default channel; later add multi-queue WRR/DRR
- Rate limiting (token bucket), per pipe and/or per socket

### M4: at-least-once reliability (per-connection / per-channel opt-in)
- Handshake with `session_id` (changes on reconnect)
- Sequence numbers:
  - `seq` (send)
  - `ack` (cumulative ack; optional SACK bitmap later)
- Retransmit:
  - RTO timer + bounded retry policy
  - Inflight limits bound memory and induce backpressure
- Deduplication:
  - Receiver sliding window for `(session_id, seq)` to prevent duplicate delivery
- NOTE: at-least-once implies duplicates can happen; recommend upper layer idempotency

### M5: Transports
- Implemented:
  - `uds://` (Unix domain socket) with same framing/protocol
  - `shm://` minimal fixed-slot rings + semaphores (bootstrap via UDS + server ACK)
  - `shm://` best-effort `sem_unlink`/`shm_unlink` after accept() to reduce crash-leaks
  - `shm://` keep bootstrap UDS as control channel (detect peer close/crash)
- `pipe://` (Windows named pipe) with same framing/protocol
- `shm://`:
  - Bootstrap/rendezvous: local `uds` socket for exchanging a connection id (initial impl)
  - Per-pipe TX/RX rings storing small descriptors (not raw payload)
  - Payload stored in shared memory pool/slab for true zero-copy
  - Notification:
    - Linux: `eventfd` (or futex)
    - Windows: Event object
    - macOS: kqueue EVFILT_USER or semaphore-based
  - Crash resilience + cleanup strategy for orphaned shm segments

### M6: Performance backends (optional)
- Batch send/recv APIs to reduce syscalls
- Scatter/gather I/O (`writev`/`sendmsg`) for TCP/UDS

### M7: Linux io_uring backend
- TCP/UDS send/recv via io_uring
- Optional busy-spin (spin-then-park) for ultra-low latency (off by default)
- Fallback to epoll if io_uring not available

### M8: Security (optional)
- `uds://`: peer credential checks / filesystem permission guidance
- `tcp://`: TLS/mTLS (configurable), authn/z hooks (if needed)

### M9: Tooling + validation
- Fault injection tests: disconnect/reconnect, packet loss, reordering, delay/jitter (for TCP)
- Compatibility tests: version/capability negotiation
- Microbenchmarks: latency percentiles + throughput + alloc/copy counts

## Protocol (initial plan)
- Message framing with a fixed header (network byte order) and payload
- Reserve fields for:
  - `channel_id` (future multiplexing)
  - `session_id`, `seq`, `ack` (reliability)
  - `frag` fields for fragmentation/reassembly (payload > 64KB)
- Constraints:
  - Default max frame payload: 64KB
  - Larger messages: fragmentation (opt-in, bounded, with reassembly timeouts)

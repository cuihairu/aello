# Aello Roadmap

Requirements ledger for humans. One requirement per line; done items carry one evidence line
(commit hash + test count). Source of truth for what is implemented is the code, not this file.

## Core

- [x] Message model and `Result`/`Status` error handling (no exceptions required). Evidence: d56eb85; 8 test cases green.
- [x] Address parsing and scheme dispatch (tcp/shm/pipe/uds; uds parsed but not wired). Evidence: d56eb85.
- [x] TCP transport with framing, TCP_NODELAY, and port-0 support. Evidence: d56eb85.
- [x] POSIX shm transport: fixed-slot double ring + named semaphores + UDS bootstrap. Evidence: 86e46c6.
- [x] Windows shm and named-pipe transports. Evidence: 155190d.
- [x] Wire framing: 16-byte header, 64 KiB max payload, network byte order. Evidence: d56eb85.
- [x] QosPipe send queue with background drain thread (snd_hwm_bytes / backpressure / TTL). Evidence: 155190d.
- [x] Cross-platform CI (GCC / Clang / MSVC). Evidence: c607b29.
- [x] macOS SIGPIPE suppression in wire send. Evidence: 86f45ab.
- [x] Test suite: address parse, shm echo, shm full-ring timeout, wire boundaries. Evidence: 8 cases; ctest 1/1.

## Transport wiring

- [x] Honor `SendOptions`/`RecvOptions.timeout` on tcp; a mid-frame timeout is fatal. Evidence: bd157c7, cec8067.
- [ ] Wire `DialOptions.timeout` into tcp/shm dial (currently ignored at src/tcp_transport.cc:242, src/shm_transport.cc:500).
- [ ] Wire `uds://` into dial/listen (src/uds_transport.cc implemented but uncompiled and undispatched).
- [ ] Wire auto-reconnect into dial (src/reconnect_pipe.cc exists but uncompiled and uncalled).
- [ ] Wire `on_state_change` connection callback.
- [ ] Implement recv-side watermarks (QosPipe recv currently passes through).
- [ ] Implement `rcv_hwm_bytes` and `linger` consumers (declared, unused).
- [ ] Add recv-side QoS queue and background recv worker.
- [ ] Compile `socket_utils.cc` when UDS is wired.

## Protocol

- [ ] Fragmentation and reassembly for payloads larger than 64 KiB.
- [ ] Reliability (at-least-once): session_id, seq/ack, retransmit, dedup.
- [ ] Priority scheduling and channel_id multiplexing.
- [ ] Rate limiting (token bucket) per pipe or per socket.
- [ ] Capability / version negotiation in the handshake.

## QoS and backpressure

- [x] Reject messages above the transport limit synchronously instead of dropping them later. Evidence: 011e118, b0d4f9e.
- [ ] Item-count cap or per-slot charging (QosPipe limits bytes only, not entries).
- [ ] Distinct queue-full status code (currently borrows kTimeout / kIoError).
- [ ] Make the QoS wrap in dial() an explicit opt-in (currently implicit via non-zero default snd_hwm_bytes).

## Testing

- [ ] Direct QoS tests (send/recv, backpressure policies, TTL, close under load).
- [ ] Concurrent send/recv tests.
- [ ] Multi-process and `kill -9` / restart test matrix.
- [ ] UDS tests (blocked until UDS is wired).
- [ ] Benchmarks (aello-bench): shm round-trip latency and throughput vs UDS/TCP.

## Deferred features

- [ ] RPC request correlation, timeout, and cancellation.
- [ ] Pub/sub with multiple subscribers.
- [ ] Async/event-loop API (async.h is on the design audit delete list; not part of the library).
- [ ] TLS / mTLS for tcp, peer credential checks for uds.
- [ ] Crash recovery for orphaned shm segments.
- [ ] Install / packaging target (cmake templates unreferenced).

## Cleanup (design audit delete/refactor list)

- [ ] Delete or park async.h, utils.h, logging.h, queue.h, convenience.h, raii.h, socket_utils.h, duct_export.h (uncompiled; see docs/design/aello-audit-2026-10-16.md §3).
- [x] Resolve duplicate `QueuedMessage` definition (include/duct/queue.h:16 and include/duct/qos_pipe.h:18). Evidence: 3022436.
- [x] Fix malformed `DUCT Experimental [[deprecated(...)]]` macro (include/duct/duct_export.h:30). Evidence: 3022436.
- [ ] Make shm slot parameters configurable (currently hard-coded 64 slots x 64 KiB).

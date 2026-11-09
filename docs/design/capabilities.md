# Capabilities and Reserved Contracts

This is the single maintained surface for "what exists, what does not, and what
is reserved". Both READMEs link here; update this file instead of the READMEs so
the two languages cannot drift apart.

Last reviewed: 2026-11-09.

## Implemented capabilities

| Area | Capability | Contract |
|---|---|---|
| Message model | `Message` payload + `Result`/`Status` errors; no exceptions | `include/duct/message.h`, `include/duct/status.h` |
| Transports | One `Pipe`/`Listener` API over `tcp://`, `shm://`, `uds://` (POSIX), `pipe://` (Windows named pipes) | `include/duct/duct.h`, `src/duct.cc` dispatch |
| `shm://` fast path | Bounded pre-allocated fixed-slot ring per direction (64 slots × 64 KiB default), semaphore-synchronized, copy-send | `src/shm_transport.cc` (POSIX), `src/win_shm.cc` (Windows) |
| Framing | 16-byte wire header; socket transports cap single frames at 64 KiB and fragment/reassemble larger messages up to 64 MiB; Windows `pipe://` and `shm://` reject oversize synchronously | `include/duct/wire.h`, `src/wire.cc` |
| QoS layer | Send/recv byte watermarks, item-count cap, backpressure policies, per-message TTL, linger drain, priority, rate limit, channel multiplexing | `include/duct/qos_pipe.h`, `src/qos_pipe.cc` |
| Reconnect | Automatic redial with exponential backoff + jitter, bounded attempts, state callbacks | `ReconnectPolicy`, `src/reconnect_pipe.cc`; `dial()` returns connected or fails within the deadline |
| Adapters | Header-only builder/request-reply/batch/loop layer and RAII scoped wrappers; installed with the library, compiled by the test suite | `include/duct/convenience.h`, `include/duct/raii.h` |
| RPC / pub-sub | Correlated request/response and topic broker, header-only | `include/duct/rpc.h`, `include/duct/pubsub.h` |
| Install | CMake package config (`duct::`), pkg-config, CPack TGZ; covered by the `duct_install` smoke test | `CMakeLists.txt:105-146` |
| CI | GCC / Clang / MSVC run the full suite; POSIX suite also runs under wine locally | `.github/workflows/cmake-multi-platform.yml` |

## Explicitly not implemented

- No async/event-loop API on the pipe itself. (When QoS is enabled the send
  side does run an internal queue-drain worker.)
- Delivery is at-most-once. No durability, no retransmit across process death.
- No TLS or other credential layers.
- No versioned releases yet; the Releases page must stay empty until an
  acceptance-tested version is explicitly approved.

## Reserved contracts (declared, not consumed)

Option fields shipped in the public headers that no transport reads yet.
Setting them has no effect today; activating them later is source-compatible.

| Field | Declared at | Current effect |
|---|---|---|
| `ReconnectPolicy::heartbeat_interval` | `include/duct/duct.h:45` | none — keepalives are whatever the OS defaults to; an earlier comment claimed a TCP keepalive mapping that was never implemented (corrected 2026-11-09) |
| `QosOptions::reliability` | `include/duct/duct.h:77` | none — at-most-once is the only delivery semantics; `Reliability::kAtMostOnce` is the only enum value with an implementation |
| `ListenOptions::qos` | `include/duct/duct.h:180` | none — listener-side QoS is reserved |

Note: the standalone `StateCallbackPipe` decorator (`src/state_callback_pipe.*`)
was removed on 2026-11-09; the reconnect dial is the single state-callback path.

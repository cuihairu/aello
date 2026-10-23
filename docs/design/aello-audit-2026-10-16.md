# Aello File-by-File Audit (2026-10-16)

> Input: external analysis `docs/design/aello-analysis-2026-10-16.md` (external analysis, not project fact).
> Method: walk all of `include/`, `src/`, `tests/`, and `examples/` (39 files), classify each file, and verify every concept from the analysis against the code.
> Scope: no implementation changes in this round. This document records conclusions only. The project was renamed from duct to aello (GitHub redirects work), while namespaces and identifiers still say `duct`; renaming is a later implementation task.
> Audit baseline: commit `7646428`, clean worktree.

## 0. Totals

| Classification | Count | Files |
|---|---|---|
| Keep | 18 | See §3–§6 |
| Refactor | 10 | See §3–§6 |
| Delete | 11 | See §3–§6 |

Scope: `include/` 14 + `src/` 16 + `tests/` 1 + `examples/` 8 = 39. Build and support files (CMakeLists, CMake templates, workflows, TODO.md, README.md) are covered separately in §7 and excluded from the totals above.

**Hard build fact:** HEAD fails to compile with local GCC 15.2.0. `include/duct/message.h` uses `std::memcpy`/`std::memcmp` (`message.h:49`, `message.h:57`) and `std::min` (`message.h:47`) without including `<cstring>` or `<algorithm>` (current includes at `message.h:3-9`). Older toolchains passed through transitive includes; newer libstdc++ reports `'memcpy' is not a member of 'std'`. This round records the issue without fixing it.

## 1. Concept verification

The analysis proposes seven concepts. Their current state in code:

| Concept | Code status | Evidence |
|---|---|---|
| `Runtime` | **Not found** | No type with this name |
| `Endpoint` | **Not found** | Same |
| `MessageView` | **No exact type**; closest is `Message::view()`, returning `std::span` | `include/duct/message.h:37-38` |
| `MessageBuilder` | **Not found**; `Message` static factories build messages (`from_bytes`/`from_string`/`with_capacity`) | `include/duct/message.h:18-25` |
| `SendResult` | **No exact type**; played by `Result<void>` + `StatusCode`. There is **no queue-full code**: blocking paths return `kTimeout` (`src/queue.cc:33-34`) and fail-fast paths reuse `kIoError` (`src/queue.cc:58`) | `include/duct/duct.h:84`; `include/duct/status.h:10-18` |
| `Dispatcher` | **No exact type**; closest are callback loops in `convenience::serve`, `echo_server`, and `consume_loop` | `include/duct/convenience.h:279-362` |
| `Transport` | **No abstract type**; the role is handled by `Pipe`/`Listener` plus scheme dispatch, with free functions per transport (`tcp_listen`/`tcp_dial`, `shm_listen`/`shm_dial`, `pipe_listen`/`pipe_dial`, and implemented-but-unwired `uds_listen`/`uds_dial`) | Interfaces `include/duct/duct.h:81-99`; dispatch `src/duct.cc:23-82` |

Note: "not found" means **not present in code**. It does not mean the concept is a new requirement; the user may have undocumented design history.

Verification of factual claims in the analysis:

| Claim | Result |
|---|---|
| "Zero-copy" (also claimed by the old README) | **False.** The shm send path copies into a slot (`src/shm_transport.cc:400`), and receive copies again through `Message::from_bytes` (`src/shm_transport.cc:424`, `src/message.cc:7-16`). The wire path also copies twice (`src/wire.cc:171-175`, `186-191`). This is the analysis's stage 1, "Copy Send" |
| "Bounded, pre-allocated shared-memory channel" | **True.** Each connection has two rings, each with 64 fixed slots × 64 KiB (`src/shm_transport.cc:42-43`, `146-165`), and a full ring blocks according to the timeout (tested at `tests/duct_tests.cc:176-223`) |
| "Start with SPSC" | **Current state is SPSC**: one producer and one consumer per ring, with head/tail advanced by opposite sides (`src/shm_transport.cc:146-149`) |
| Batch send/receive | **No true batching.** `convenience::send_batch`/`recv_batch` are loops over single sends (`include/duct/convenience.h:229-263`), reducing neither atomics nor syscalls |

## 2. Cross-cutting findings

1. **Six source files are not in the build.** `CMakeLists.txt:7-23` compiles only `address`, `duct`, `message`, `qos_pipe`, `shm_transport`, `tcp_transport`, and `wire` (plus `win_shm`/`pipe_transport` on Windows). **Excluded:** `src/uds_transport.cc`, `reconnect_pipe.cc`, `state_callback_pipe.cc`, `socket_utils.cc`, `logging.cc`, and `queue.cc`. With their headers, they form invisible dead code that the compiler never checks.
2. **Automatic reconnect is advertised but not implemented.** `DialOptions::reconnect` and `on_state_change` can be configured (`include/duct/duct.h:101-108`; builder entry `include/duct/convenience.h:50-65`), but `dial()` never reads them (`src/duct.cc:43-82`). `reconnect_pipe.cc` and `state_callback_pipe.cc` are never called and are not compiled. `heartbeat_interval` has no consumer either (`include/duct/duct.h:43`).
3. **QoS is asymmetric and misleading.** `QosPipe` wraps only the dial side and only the send queue; receive is passed through (`src/qos_pipe.cc:158-160`). `rcv_hwm_bytes`, `linger`, and the `Reliability` enum have no consumers (declared at `include/duct/duct.h:59,66,23-26`). Every `listen` ignores `ListenOptions::qos` (for example, `src/shm_transport.cc:488-493` uses only `backlog`). The default non-zero `DialOptions::snd_hwm_bytes=4MB` triggers wrapping at `src/duct.cc:76`, so **every dial gets a QosPipe and a background thread**, idle-polling every 1 ms (`src/qos_pipe.cc:92-98`). TODO.md's M2/M3 "Implemented" markers for `on_state_change`, auto-reconnect, and linger are false.
4. **`uds://` parses but does not work.** `Address::parse` recognizes `uds` (`src/address.cc:37`), but `listen`/`dial` have no uds branch (`src/duct.cc:29-39`, `51-71`) and return `not_supported`. Its dependency `socket_utils.cc` is also not compiled.
5. **The old README contradicted the code.** It advertised zero-copy, automatic reconnect, and connection-state callbacks (all false); documented `-DDUCT_INSTALL=OFF`, install targets, `find_package(duct)`, and pkg-config although there is no install/export logic; listed three example binaries CMake never builds; and said the license was undecided although `LICENSE` is MIT (2025, cuihairu).
6. **ODR hazard:** `struct QueuedMessage` is defined twice in namespace `duct`, in `include/duct/queue.h:16-21` and `include/duct/qos_pipe.h:18-24`. Nothing includes both today, so it has not failed yet.
7. **Test blind spots:** `tests/duct_tests.cc` covers address parsing, shm echo, shm full-ring timeout, wire encoding/decoding, and macOS SIGPIPE (`tests/duct_tests.cc:44-451`). It does not cover the QoS wrapper, reconnect, uds, concurrent send/receive, large messages, or ordering across multiple messages. Several tests disable QosPipe by setting `snd_hwm_bytes` to 0 (`tests/duct_tests.cc:96-98` and three other places), so the QoS path has zero coverage.
8. **`duct_export.h` is unreferenced.** No file includes it and `DUCT_API` is never used. `#define DUCT Experimental [[deprecated(...)]]` at `include/duct/duct_export.h:30` is itself malformed.

## 3. `include/duct/` (14 files)

| File | Classification | Conclusion |
|---|---|---|
| `duct.h` | Keep | Core spine: `Pipe`/`Listener` (`duct.h:81-99`) and all option types, the closest thing to a future Endpoint contract. Debt: reconnect/callback options are unwired; receive watermark, linger, and reliability have no consumers. Either wire them or delete the fields during refactoring |
| `message.h` | Keep | Core message model: `shared_ptr<vector>` backing with a raw-pointer view (`message.h:64-67`). Add the missing standard headers to fix the build; evolve toward Copy → Batch → Claim as described in the adoption document |
| `status.h` | Keep | `Status`/`Result`/`StatusCode` error model (`status.h:62-185`) and the future carrier for `SendResult` semantics. Add a queue-full code rather than borrowing `kTimeout`/`kIoError` |
| `address.h` | Keep | Small, correct address-parse result type (`address.h:16-26`) used by dispatch |
| `protocol.h` | Keep | Frame constants and scheme enum (`protocol.h:11-20`). The magic value is still `'DUCT'` (0x44554354); rename later |
| `wire.h` | Keep | Byte-stream framing: 16-byte header, 64 KiB limit, cross-platform handle (`wire.h:11-23`); shared by TCP, UDS, and pipe |
| `raii.h` | Keep | Header-only RAII wrappers `ScopedPipe`/`ScopedListener` (`raii.h:17-132`); harmless and useful |
| `socket_utils.h` | Keep | Platform socket helpers (`socket_utils.h:20-34`), required when UDS is wired; its `.cc` is not currently compiled |
| `convenience.h` | Refactor | Keep the builder idea but slim it down. `auto_reconnect()` silently does nothing and must be wired or removed. `request()` is bare send+receive with no request correlation (`convenience.h:202-212`). `send_batch` is fake batching. `serve`/`echo_server` can become Dispatcher prototypes in the core |
| `qos_pipe.h` | Refactor | The "QoS" name overpromises: send-side only, byte-bound only, background polling. Keep one of this and `queue.h`, and define the semantics as an explicit bounded send buffer plus backpressure policy. Resolve the duplicate `QueuedMessage` name |
| `async.h` | Delete | Conflicts with the project direction: unbounded `Channel` (`async.h:242-249`), one detached thread per callback operation (`async.h:104-119`), and a 100 ms polling `EventLoop` (`async.h:154-183`) that its own comment says should use epoll. The analysis explicitly excludes a general event loop/async framework from v1 |
| `queue.h` | Delete | `MessageQueue` is uncompiled and unreferenced, `hwm_bytes=0` means unlimited (`queue.cc:221-223`) and violates the bounded-memory principle, and the name collides with `qos_pipe.h`. If an in-process bounded queue is needed later, redesign it around the SPSC ring rather than this deque+mutex implementation |
| `logging.h` | Delete | The bundled logging and metrics framework (Counter/Gauge/Histogram/Registry, `logging.h:239-412`) is outside a messaging runtime. `Histogram::observe` stores every observation in an unbounded vector (`logging.h:354-356`), directly violating bounded memory. Its `.cc` is uncompiled. Performance data belongs to aello-bench; revisit runtime logging later as a minimal hook |
| `utils.h` | Delete | General-purpose grab bag (`Port`/`TcpAddr`/string joiner/`StrongType`/literals at `utils.h:21-100`, `248-310`, `347-376`) unrelated to messaging, untested, and pure API-surface expansion. YAGNI |
| `duct_export.h` | Delete | Entire file is unreferenced, `DUCT_API` marks nothing, and the macro is malformed. Revisit export macros when library packaging is real |

## 4. `src/` (16 files)

| File | Classification | Conclusion |
|---|---|---|
| `duct.cc` | Keep | Scheme dispatch hub (`duct.cc:23-82`). Refactor notes: wire the UDS branch; make the obscure QoS-wrap condition at `duct.cc:76` (`hwm!=0 || policy!=kBlock`) explicit opt-in |
| `address.cc` | Keep | Parsing implementation aligns with tests (`tests/duct_tests.cc:44-76`) |
| `message.cc` | Keep | Fixed together with the missing includes in `message.h` |
| `shm_transport.cc` | Keep | **Primary performance path and core of the project**: POSIX fixed-slot double ring + named semaphores + UDS bootstrap (`shm_transport.cc:146-165`, `305-373`, `488-527`). Known limits: hard-coded slot parameters (`42-43`), crash cleanup only through owner-close unlink (`430-440`) with no abnormal-exit recovery, and a 1 ms polling fallback for semaphore waits on macOS (`119-134`). These are evolution items for aello-shm, not reasons to delete |
| `win_shm.cc` | Keep | Isomorphic Windows implementation: `CreateFileMapping`/`MapViewOfFile`/named semaphores (`win_shm.cc:212-249`), aligned with the POSIX ring parameters |
| `tcp_transport.cc` | Keep | Network backend (P2 in the analysis, but already present and usable): framing + NODELAY + port 0 (`tcp_transport.cc:115-160`, `218-236`). Keep as a reference and local-network path |
| `pipe_transport.cc` | Keep | Windows named-pipe backend (`pipe_transport.cc:250-303`), the local compatibility path for aello-local |
| `socket_utils.cc` | Keep | See `socket_utils.h`; add it to the build when wiring UDS |
| `qos_pipe.cc` | Refactor | Follows `qos_pipe.h`; additionally, `send_worker` handles `kDropNew`/`kDropOld` differently from the branches in `send()` (`qos_pipe.cc:88-99` vs `117-145`). Unify during refactoring |
| `uds_transport.cc` | Refactor | Complete implementation (connect timeout, path cleanup, `uds_transport.cc:104-235`) but not dispatched or compiled. Wire it into `duct.cc`, compile `socket_utils.cc`, add tests, and do not advertise UDS until then |
| `reconnect_pipe.cc` | Refactor | Reasonable implementation (exponential backoff with jitter, state machine, stale-error guard, `reconnect_pipe.cc:132-198`) but uncompiled and unwired. Wire it into `dial()` when `opt.reconnect.enabled` and remove the documentation gap. It is meaningless for shm and applies only to TCP/pipe |
| `reconnect_pipe.h` | Refactor | Same |
| `logging.cc` | Delete | Implements the dead `logging.h` `ConsoleLogger`; not compiled |
| `queue.cc` | Delete | Dead code |
| `state_callback_pipe.cc` | Delete | Never called or compiled; its state-callback role overlaps `ReconnectPipe` and can be subsumed when reconnect is wired |
| `state_callback_pipe.h` | Delete | Same |

## 5. `tests/` (1 file)

| File | Classification | Conclusion |
|---|---|---|
| `duct_tests.cc` | Refactor | Seven useful cases remain, especially shm full-ring timeout and wire boundaries (`tests/duct_tests.cc:176-223`, `298-451`). Problems: hand-rolled EXPECT macros (`30-42`); QoS bypassed rather than tested; no multi-process, stress, or crash cases; UDS untestable because it is unwired. Expand per aello-test P0: direct QoS tests and concurrent send/receive first, then multi-process and `kill -9` restart matrices |

## 6. `examples/` (8 files)

| File | Classification | Conclusion |
|---|---|---|
| `duct_echo_server.cc` | Keep | Minimal server example, in the build (`CMakeLists.txt:43-44`), referenced by nightly packaging |
| `duct_echo_client.cc` | Keep | Same |
| `pipe_test.cc` | Keep | Manual Windows named-pipe tool, in the build |
| `shm_test.cc` | Refactor | Useful for manual testing and in the build, but its output says "Windows shared memory server" (`examples/shm_test.cc:28`) although it also works on POSIX. Fix the text and parameterize it |
| `qos_test.cc` | Refactor | Manual two-terminal tool in the build. Its backpressure demo is exactly the untested area of CI; turn it into automated coverage instead of leaving it in examples |
| `convenience_example.cc` | Refactor | **Not in the build**. It demonstrates `auto_reconnect()` and other features that do not work, teaching users bad practice. Rewrite it alongside the convenience.h cleanup to show only effective APIs, and add it to CMake |
| `async_example.cc` | Delete | Depends on doomed `async.h` and is not built |
| `utils_example.cc` | Delete | Depends on doomed `utils.h` and is not built |

## 7. Build and support files

- `CMakeLists.txt`: **refactor.** Six source files and three examples are missing, there is no install/export, and the project name is still `duct` (`CMakeLists.txt:2`). Fix the build before renaming.
- `cmake/ductConfig.cmake.in`, `cmake/duct.pc.in`, `cmake/duct_version.h.in`: **refactor or delete.** All three are unreferenced; no targets file is generated and the version header is never configured. Either implement packaging or remove the fictional delivery surface.
- `.github/workflows/cmake-multi-platform.yml`: **keep.** GCC/Clang/MSVC matrix plus CTest (`cmake-multi-platform.yml:25-45`). It did not catch the `message.h` break because `ubuntu-latest` still has an older GCC; the failure will surface when the runner updates.
- `.github/workflows/nightly.yml`: **keep.** Schedule/dispatch, rolling nightly tag, and prerelease marker (`nightly.yml:122-140`) fit the rule that nightly builds are automatic but releases remain manual.
- `TODO.md`: **refactor.** M2/M3 "Implemented" markers are false and the document follows the old positioning. Demote it to historical reference or rewrite it once the adoption document owns the roadmap.
- `README.md`: rewritten to the new positioning this round (English primary + `README.zh.md`).

## 8. Recommended order for the next implementation round

1. Fix the build: add `<cstring>`/`<algorithm>` to `message.h`.
2. Apply the delete list: 11 files plus the three CMake templates (or implement packaging).
3. Complete the build: wire `uds_transport.cc`/`socket_utils.cc` and the UDS branch, or explicitly park them.
4. Remove documentation gaps: wire or delete `ReconnectPolicy`/`on_state_change` and `rcv_hwm_bytes`/`linger`/`Reliability`.
5. Add missing tests: direct QoS, concurrent send/receive, abnormal process exit.
6. Only then rename namespaces/branding and split CMake targets (`aello-core`/`shm`/`bench`/`test`).

# Source Layer Analysis (2026-11-07)

This analysis is based on HEAD `f7405a7a6dd726b0552e09294a4303b5cc1cc682` and the current source tree. It records responsibilities, state ownership, communication paths, configuration inputs, lifecycle, and the concrete reason each conceptual layer exists. Paths are repository-relative; line references were captured from that revision.

## Scope and Non-Goals

- The repository is a C++20 in-process and inter-process messaging library, not a deployed service fleet. All runtime state is process-local memory or transport-owned OS state; there is no Redis, MySQL, or external configuration service.
- The analysis does not change code. Existing behavior, including reserved fields that are not currently consumed, is documented as-is.
- Tests, examples, benchmarks, and CI verify the architecture but are not architectural layers. They are called out only when they define an input contract or exercise a lifecycle.

## Layer Model

The library has seven conceptual layers. These are responsibility boundaries rather than directory names; for example, dial/listen dispatch and connection recovery both live in `src/`, while application convenience adapters are header-only.

```text
Application adapters / messaging patterns
        |
Runtime orchestration and dial/listen dispatch
        |
Connection policy and QoS decorators
        |
Transport abstraction
        |
Transport backends
        |
Wire and memory framing
        |
Foundation: result model and platform primitives
```

| Layer | Responsibility | Representative implementation | State storage | Primary inputs |
|---|---|---|---|---|
| 1. Foundation contracts | Provide the `Message` value type, `Status`/`Result<T>` error model, and platform-neutral socket operations. | `include/duct/message.h:15`, `include/duct/status.h:66`, `src/socket_utils.cc:19` | Message backing buffer in memory; socket descriptors in callers | Caller-provided bytes, string views, capacities, and socket handles |
| 2. Wire and memory framing | Normalize byte streams and shared-memory ring records, including version negotiation, fragmentation, and channel flags. | `src/wire.cc:28`, `src/wire.cc:321`, `src/shm_transport.cc:163`, `src/win_shm.cc:76` | In-memory frame buffers; process-shared ring metadata, slots, semaphores, and named-file mappings | `FrameHeader`/`FrameFlags` constants, `kMaxFramePayload`, socket handle or segment mapping |
| 3. Transport backends | Implement `Pipe` and `Listener` lifecycle for TCP, Unix-domain sockets, shared memory, and Windows named pipes. | `src/tcp_transport.cc:57`, `src/uds_transport.cc:31`, `src/shm_transport.cc:540`, `src/pipe_transport.cc:109` | OS sockets; POSIX `/dev/shm` plus bootstrap UDS; Windows file mappings/events; connect state in memory | `Address`, `DialOptions`, `ListenOptions`, `ShmOptions`, `SendOptions`, `RecvOptions` |
| 4. Transport abstraction and dispatch | Hide backend selection behind address schemes and the common `Pipe`/`Listener` contracts. | `include/duct/duct.h:127`, `include/duct/duct.h:154`, `src/duct.cc:29`, `src/duct.cc:56` | Parsed address and option copies on the call stack; decorated pipe instance | Address string, dial/listen options, scheme availability |
| 5. Connection policy and QoS | Add bounded send/recv queues, backpressure, rate limiting, reconnect, and state callbacks without changing a transport. | `src/qos_pipe.cc:11`, `src/reconnect_pipe.cc:18`, `src/state_callback_pipe.cc:10` | STL queues/deques, mutex/condition state, connection state and active pipe; no external store | `QosOptions`, `ReconnectPolicy`, `ConnectionCallback`, per-call options |
| 6. Messaging patterns | Provide reusable semantics above a connected pipe: RPC correlation and in-process topic fan-out. | `include/duct/rpc.h:58`, `include/duct/pubsub.h:27` | Pending RPC map and worker thread; subscriber vector and IDs in shared state | `Pipe`, request payload and timeout, subscription filter and callback, published topic/message |
| 7. Application adapters | Convert the runtime into ergonomic builders, RAII ownership, synchronous batch/request loops, and demonstrable application loops. | `include/duct/convenience.h:22`, `include/duct/raii.h:17`, `examples/convenience_example.cc:23`, `bench/aello_bench.cc:150` | Owned pipe/listener pointer or scoped wrapper; batch vectors and handler state | Address and option builders, application handlers, benchmark arguments |

### Layer 1: Foundation Contracts

- **Implementation:** `Message` owns a shared backing buffer and exposes a view without copying on construction (`include/duct/message.h:15`, `src/message.cc:7`); `Status`, `StatusCode`, `Exception`, and `Result<T>` form the non-throwing error contract (`include/duct/status.h:10`, `include/duct/status.h:66`, `include/duct/status.h:113`); `socket_utils` centralizes readiness waiting, non-blocking mode, and cross-platform close (`include/duct/socket_utils.h`, `src/socket_utils.cc:19`, `src/socket_utils.cc:36`, `src/socket_utils.cc:186`).
- **Configuration schema:** There is no external schema. `Message::from_bytes` and `Message::with_capacity` accept byte ranges/capacities (`include/duct/message.h:20`, `include/duct/message.h:27`); error codes are the contract (`include/duct/status.h:10`).
- **Protocol files:** None. This layer deliberately has no transport protocol.
- **Docs placement:** English `docs/design/source-layer-analysis-2026-11-07.md`; Chinese `docs/zh/design/source-layer-analysis-2026-11-07.md`; catalog `docs/.vitepress/config.mts`.
- **Lifecycle:** No global lifecycle. `ensure_networking()` lazily initializes Winsock on Windows (`src/socket_utils.cc:19`); messages can outlive their originating view because they retain shared storage (`include/duct/message.h:36`).

### Layer 2: Wire and Memory Framing

- **Implementation:** The 16-byte socket-frame codec lives in `src/wire.cc`; `encode_header`/`decode_header` validate magic, version, header length, and payload bound (`src/wire.cc:28`, `src/wire.cc:42`). `write_frame` fragments payloads larger than 64 KiB and marks first/final fragments (`src/wire.cc:283`); `read_frame_with_flags` reassembles up to 64 MiB (`src/wire.cc:327`). POSIX and Windows shared-memory layouts independently define ring metadata, slots, and fragmentation state.
- **State storage:** Stack/buffer-local frame state; pending per-channel reassembly map inside TCP/UDS pipes; process-shared ring metadata/slots and named synchronization objects in shared memory. POSIX naming and bootstrap details begin in `src/shm_transport.cc:244`; Windows equivalents begin in `src/win_shm.cc:144`.
- **Configuration schema:** The on-wire constants and field order are the schema (`include/duct/wire.h:13`, `include/duct/wire.h:27`, `include/duct/protocol.h:14`, `include/duct/protocol.h:22`). Shared-memory geometry is configured by `ShmOptions` and validated on accept (`include/duct/duct.h:92`, `src/shm_transport.cc:163`).
- **Protocol files:** `include/duct/wire.h`, `include/duct/protocol.h`. The RPC request-id extension is documented in `include/duct/rpc.h:22`; pub/sub intentionally reuses the message payload and defines no additional wire frame (`include/duct/pubsub.h:20`).
- **Docs placement:** Same bilingual architecture documents and VitePress registration.
- **Lifecycle:** Header encode/decode is stateless. Socket reads/writes accept a timeout and poison a partially transferred byte stream rather than resending it (`include/duct/wire.h:40`, `src/wire.cc:283`). Shared-memory segments survive process death by design; recovery is layered above in transport backends.

### Layer 3: Transport Backends

- **Implementation:** TCP, UDS, POSIX shared memory, Windows shared memory, and Windows named pipes each provide concrete `Pipe`/`Listener` implementations and free dial/listen entry points: TCP at `src/tcp_transport.cc:57` and `src/tcp_transport.cc:437`; UDS at `src/uds_transport.cc:31` and `src/uds_transport.cc:356`; POSIX SHM at `src/shm_transport.cc:540` and `src/shm_transport.cc:831`; Windows SHM at `src/win_shm.cc:394`; named pipes at `src/pipe_transport.cc:109` and `src/pipe_transport.cc:449`.
- **State storage:** OS socket/listener handles and TCP handshake state (`src/tcp_transport.cc:57`, `src/tcp_transport.cc:199`); UDS handles plus SO_PEERCRED data (`src/uds_transport.cc:31`, `include/duct/duct.h:121`); SHM mappings, ring heads/tails, semaphore/name registries, and owner-PID crash metadata (`src/shm_transport.cc:163`, `src/shm_transport.cc:244`, `src/shm_transport.cc:391`); named-pipe instances and listener shared state (`src/pipe_transport.cc:109`, `src/pipe_transport.cc:299`). No database or cache.
- **Configuration schema:** C++ option structs are the schema: `DialOptions` and `ListenOptions` (`include/duct/duct.h:166`, `include/duct/duct.h:179`), `ShmOptions` (`include/duct/duct.h:94`), `SendOptions`/`RecvOptions` (`include/duct/duct.h:103`, `include/duct/duct.h:112`), and `handshake_timeout` (`include/duct/duct.h:188`). Build/platform inputs are selected by `CMakeLists.txt:18` and `CMakeLists.txt:31`.
- **Protocol files:** Shared transport traits are in `include/duct/duct.h`; wire protocol details are in Layer 2. Backend-specific bootstrap/address forms are established by `src/address.cc:23` and backend factories.
- **Copy/copy:** Same bilingual architecture documents and VitePress registration.
- **Lifecycle:** Dial connects and returns a `Pipe`; listen binds and returns a `Listener`; each destructor invokes `close()` in the observed backends (for example `src/tcp_transport.cc:77`, `src/uds_transport.cc:45`, `src/shm_transport.cc:540`, `src/pipe_transport.cc:109`). `accept()` performs the server handshake (`src/tcp_transport.cc:227`). Shared memory adds orphan discovery/reclaim and explicit unlink-on-close behavior in the backend implementation.

### Layer 4: Transport Abstraction and Dispatch

- **Implementation:** `Pipe` and `Listener` define send/recv, accept/close, frame forwarding, credential and size queries (`include/duct/duct.h:127`, `include/duct/duct.h:154`). `listen()` parses one address and dispatches to the backend (`src/duct.cc:29`); `dial_impl()` does the same for dial and applies optional QoS (`src/duct.cc:56`).
- **State storage:** The layer owns no long-lived state. It passes parsed `Address` and option values through the stack and returns backend-owned objects.
- **Configuration schema:** Same `DialOptions`/`ListenOptions` structs as backends. Address syntax is implemented, not declared in a data file, by `Address::parse` (`src/address.cc:23`), with `Scheme` values at `include/duct/protocol.h:14`.
- **Protocol files:** No dedicated dispatch protocol file; `include/duct/duct.h` is the public contract header.
- **Copy/copy:** Same bilingual architecture documents and VitePress registration.
- **Lifecycle:** No own thread or background process. It composes an object tree and exits when returned objects are destroyed. Reconnect wrapping is selected only when `opt.reconnect.enabled` is true (`src/duct.cc:105`, `src/duct.cc:110`).

### Layer 5: Connection Policy and QoS

- **Implementation:** `QosPipe` wraps any `Pipe` with two worker threads, bounded queues, priority/TTL/rate control, and channel-preserving frame reads (`include/duct/qos_pipe.h:29`, `src/qos_pipe.cc:11`, `src/qos_pipe.cc:313`). `ReconnectPipe` owns dial-once state recovery, backoff with jitter, bounded attempts, and callback publication (`src/reconnect_pipe.cc:18`, `src/reconnect_pipe.cc:167`, `src/reconnect_pipe.cc:222`). `StateCallbackPipe` emits disconnect/closed callbacks from transport results but currently has no active dial wiring (`src/state_callback_pipe.cc:10`, `src/state_callback_pipe.cc:63`).
- **State storage:** STL deques with byte/count watermarks and mutex/CV coordination (`include/duct/qos_pipe.h:48`, `include/duct/qos_pipe.h:59`); token-bucket and error state inside `QosPipe`; reconnect connection state, active inner pipe, last error, and worker thread (`src/reconnect_pipe.cc:249`); atomic closed/disconnected flags in callback adapter (`src/state_callback_pipe.cc:55`).
- **Configuration schema:** `QosOptions` and `ReconnectPolicy` (`include/duct/duct.h:30`, `include/duct/duct.h:58`), plus per-call priority, timeout, and channel (`include/duct/duct.h:103`, `include/duct/duct.h:112`).
- **Protocol files:** None of its own; `recv_frame` explicitly preserves wire channel flags across the decorator (`src/reconnect_pipe.cc:68`).
- **Copy/copy:** Same bilingual architecture documents and VitePress registration.
- **Lifecycle:** `QosPipe` starts workers during construction and drains/joins them in `close()` (`src/qos_pipe.cc:11`, `src/qos_pipe.cc:376`). `ReconnectPipe` starts its worker in construction, publishes connected only after the inner pipe is visible, and joins the worker on close (`src/reconnect_pipe.cc:20`, `src/reconnect_pipe.cc:199`, `src/reconnect_pipe.cc:101`).

### Layer 6: Messaging Patterns

- **Implementation:** `rpc::Client` correlates concurrent request/response calls with a little-endian 32-bit request id and a dispatch thread (`include/duct/rpc.h:22`, `include/duct/rpc.h:54`, `include/duct/rpc.h:157`). `pubsub::Broker` performs in-process prefix topic matching and runs callbacks on the publisher thread (`include/duct/pubsub.h:20`, `include/duct/pubsub.h:76`, `include/duct/pubsub.h:86`).
- **State storage:** RPC pending-call map, atomic IDs, mutex/send-lock state, and worker thread (`include/duct/rpc.h:195`); pub/sub subscriber vector, next ID, and shared liveness state (`include/duct/pubsub.h:35`). No external store.
- **Configuration schema:** No configuration file. RPC uses call timeout and optional explicit cancel (`include/duct/rpc.h:71`, `include/duct/rpc.h:108`); pub/sub uses filter strings and callbacks (`include/duct/pubsub.h:78`).
- **Protocol files:** `include/duct/rpc.h:22` documents the request-id extension. Pub/sub deliberately has no transport ownership or extra wire protocol (`include/duct/pubsub.h:20`).
- **Copy/copy:** Same bilingual architecture documents and VitePress registration.
- **Lifecycle:** RPC starts a dispatch thread in construction and closes/joins it, releasing waiters (`include/duct/rpc.h:60`, `include/duct/rpc.h:123`); late responses are dropped after timeout/cancel (`include/duct/rpc.h:95`, `include/duct/rpc.h:184`). Pub/sub shares reference-counted state so subscriptions can outlive a broker safely (`include/duct/pubsub.h:35`, `include/duct/pubsub.h:45`).

### Layer 7: Application Adapters

- **Implementation:** Builders and raw factories expose dial/listen ergonomics (`include/duct/convenience.h:22`, `include/duct/convenience.h:87`, `include/duct/convenience.h:180`); RAII wrappers own close/accept (`include/duct/raii.h:17`, `include/duct/raii.h:75`, `include/duct/raii.h:141`); helpers implement request/reply, batches, echo/serve, and production/consumption loops (`include/duct/convenience.h:203`, `include/duct/convenience.h:280`, `include/duct/convenience.h:327`). Examples and benchmarks are external consumers: `examples/convenience_example.cc:23`, `bench/aello_bench.cc:150`.
- **State storage:** Owned pointers in builders/wrappers and caller-owned vectors/handler captures; no repository-owned persistent state.
- **Configuration schema:** The same option structs with builder methods for timeout, watermarks, backpressure, reconnect, heartbeat, and callbacks (`include/duct/convenience.h:27` through `include/duct/convenience.h:64`). CMake examples/tests/benchmark toggles are documented at `CMakeLists.txt:15`, `CMakeLists.txt:63`, `CMakeLists.txt:80`, and `CMakeLists.txt:87`.
- **Protocol files:** None. This layer forwards to patterns or the runtime.
- **Copy/copy:** Same bilingual architecture documents and VitePress registration.
- **Lifecycle:** Builders are short-lived factories. RAII wrappers close on destruction and `release()` transfers ownership (`include/duct/raii.h:22`, `include/duct/raii.h:44`). Helper loops end on `kClosed`, propagate other errors, or return after bounded work (`include/duct/convenience.h:280`, `include/duct/convenience.h:303`, `include/duct/convenience.h:327`).

## Communication Map

- **Public call chain:** `convenience`/`raii`/application -> `dial`/`listen` -> scheme dispatch -> optional `ReconnectPipe` -> optional `QosPipe` -> concrete transport -> `wire`/SHM ring -> OS. The dial-side composition points are `src/duct.cc:105` and `src/duct.cc:56`; `QosPipe` wrapping occurs at `src/duct.cc:94`.
- **Backend channels:** TCP/UDS use blocking sockets with framed byte streams; shared memory uses named process-shared mappings/rings plus synchronization primitives; Windows named pipes use overlapped I/O. Dispatch is direct function call, not an event bus or RPC middleware.
- **Internal callbacks:** `ConnectionCallback` is invoked under reconnect state transitions (`src/reconnect_pipe.cc:121`) or state-adapter failures (`src/state_callback_pipe.cc:49`). RPC invokes only caller futures, while pub/sub invokes subscriber callbacks on the publisher thread (`include/duct/pubsub.h:77`).
- **Testing/verification channels:** `tests/duct_tests.cc` calls public APIs and internal decorators (`tests/duct_tests.cc:1`, `tests/duct_tests.cc:4110`); CMake registers `duct_tests`, `duct_install`, and the benchmark (`CMakeLists.txt:74`, `CMakeLists.txt:90`, `CMakeLists.txt:95`); CI executes the CMake matrix and docs deploy (`.github/workflows/cmake-multi-platform.yml`, `.github/workflows/deploy-docs.yml`).

## Configuration and External Input

| Input | Source | Owner | Notes |
|---|---|---|---|
| Build feature switches | `CMakeLists.txt:15`, `CMakeLists.txt:63`, `CMakeLists.txt:80`, `CMakeLists.txt:87` | Build system | Examples, tests, and POSIX benchmark; benchmark only non-Windows. |
| Version | `CMakeLists.txt:3`, `cmake/duct_version.h.in:3` | Packaging | Generates `duct/version.h`; no source code reads generated version values. |
| Package discovery | `CMakeLists.txt:99`, `cmake/ductConfig.cmake.in`, `cmake/duct.pc.in` | Packaging | Install/export, pkg-config, CPack, and install smoke test. |
| Address strings | `src/address.cc:23` | Dispatch | Scheme plus name/path/host-port. |
| Dial/listen/QoS/reconnect/SHM options | `include/duct/duct.h:30`, `include/duct/duct.h:58`, `include/duct/duct.h:94`, `include/duct/duct.h:166`, `include/duct/duct.h:179` | Runtime | C++ structs only; environment variables and config files are not read. |
| Per-call options | `include/duct/duct.h:103`, `include/duct/duct.h:112` | Runtime/patterns | Timeout, priority, channel. |
| CI workflow triggers | `.github/workflows/cmake-multi-platform.yml`, `.github/workflows/deploy-docs.yml` | CI | Repository events; not runtime configuration. |

## State Inventory

| Owner | State | Medium | Persistence/recovery | Evidence |
|---|---|---|---|---|
| `Message` | Payload and extent | Heap-backed shared buffer | Caller lifetime | `include/duct/message.h:67` |
| TCP/UDS pipe | Socket handle, handshake, read locks, per-channel pending frames | Memory/OS socket | None beyond OS socket state | `src/tcp_transport.cc:192`, `src/uds_transport.cc:145` |
| POSIX SHM | Segment, ring metadata, slots, semaphores, names, owner identity | `/dev/shm` + bootstrap UDS + OS semaphores | Owner PID/magic enables orphan reclaim; unlink occurs during backend close | `src/shm_transport.cc:163`, `src/shm_transport.cc:244`, `src/shm_transport.cc:391` |
| Windows SHM | Mapping, events, ring metadata/slots, names, owner identity | Windows named file mapping/kernel events | Same ring contract; platform implementation in `src/win_shm.cc` | `src/win_shm.cc:76`, `src/win_shm.cc:144`, `src/win_shm.cc:198` |
| Windows named pipe | Pipe instance, overlapped operation, listener shared state | OS named pipe/kernel events | Listener state is reference counted to survive concurrent accepts | `src/pipe_transport.cc:109`, `src/pipe_transport.cc:299` |
| QoS | Queues, watermarks, token bucket, worker/error state | Process memory | Not persisted; queues are bounded | `include/duct/qos_pipe.h:48`, `include/duct/qos_pipe.h:59` |
| Reconnect | Inner pipe, state, error, policy, worker | Process memory | Redials on demand; no historical journal | `src/reconnect_pipe.cc:249` |
| RPC | Pending calls, IDs, dispatch worker | Process memory | Not persisted; timeout/cancel releases waiters | `include/duct/rpc.h:195` |
| Pub/sub | Subscriber vector and liveness | Process memory/shared state | Subscriptions may outlive broker destruction | `include/duct/pubsub.h:35` |

## Reserved and Incompletely Wired Contracts

- `DialOptions::on_state_change` is active only when reconnect is enabled because `dial()` passes it to `make_reconnect_pipe` (`include/duct/duct.h:176`, `src/duct.cc:110`). The standalone `make_state_callback_pipe` implementation exists but has no production caller (`src/state_callback_pipe.cc:63`).
- `ReconnectPolicy::heartbeat_interval` is declared and mapped by the builder, but no production transport consumes it (`include/duct/duct.h:43`, `include/duct/convenience.h:57`).
- `QosOptions::reliability` is declared but has no production reader (`include/duct/duct.h:77`).
- `ListenOptions::qos` exists but `listen()` does not construct a QoS wrapper (`include/duct/duct.h:181`, `src/duct.cc:29`). Server-side QoS is therefore explicit construction, as exercised by tests (`tests/duct_tests.cc:4084`).
- Platform behavior differs where documented: socket transports fragment/reassemble to 64 MiB, while Windows SHM/named pipes reject above their own capacity (`README.md:110`). UDS peer credentials are OS-backed; other transports return not-supported (`include/duct/duct.h:118`, `src/uds_transport.cc:31`).

## Split Rationale

1. **Foundation is split from protocol and transport because the error and ownership contract changes at every boundary.** `Message` retains shared backing storage while exposing views (`include/duct/message.h:36`), and `Result<T>`/`Status` force explicit success/error handling across all layers (`include/duct/status.h:113`). Evidence of cross-layer use: backends, decorators, RPC, and convenience helpers all return `Result` (`src/tcp_transport.cc:57`, `src/qos_pipe.cc:29`, `include/duct/rpc.h:58`, `include/duct/convenience.h:203`).
2. **Byte-stream framing is isolated so TCP and UDS share one validated wire implementation.** Both transports encode channel IDs in frame flags and call `wire::write_frame` (`src/tcp_transport.cc:83`, `src/uds_transport.cc:56`), while header validation, fragmentation, reassembly, and timeout behavior are concentrated in `src/wire.cc:42`, `src/wire.cc:283`, and `src/wire.cc:327`. This prevents two socket transports from drifting in protocol compatibility.
3. **Shared-memory ring framing is split from socket wire framing because its synchronization, slot geometry, crash metadata, and platform mappings differ.** POSIX SHM defines ring heads/tails, segment layout, slot control words, and named registries at `src/shm_transport.cc:163`, `src/shm_transport.cc:197`, and `src/shm_transport.cc:244`; Windows has a separate layout/handle implementation at `src/win_shm.cc:76`, `src/win_shm.cc:144`, and `src/win_shm.cc:198`. Its configuration is peer-agreed geometry rather than socket-frame constants (`include/duct/duct.h:92`).
4. **Transport backends are separate because connection establishment and OS resource lifecycle cannot be abstracted into one algorithm.** TCP creates/connects sockets and resolves wildcard/effective ports (`src/tcp_transport.cc:399`, `src/tcp_transport.cc:437`), UDS binds filesystem sockets and captures peer credentials (`src/uds_transport.cc:152`), SHM creates/reclaims process-shared segments (`src/shm_transport.cc:831`), and named pipes create overlapped instances (`src/pipe_transport.cc:449`). A single backend would hide platform-specific failure and recovery semantics.
5. **Scheme dispatch is separated from backends so public addresses remain stable while platforms vary.** One `Address::parse` produces typed schemes (`src/address.cc:23`), then `listen()` and `dial_impl()` route only available schemes (`src/duct.cc:29`, `src/duct.cc:56`). Conditional Windows-only pipe support is isolated at `src/duct.cc:41` and `src/duct.cc:76`, avoiding platform branches throughout callers.
6. **The common `Pipe`/`Listener` boundary exists so policy and pattern layers can compose without knowing the backend.** RPC accepts any `Pipe` (`include/duct/rpc.h:58`); pub/sub explicitly assumes any side can drive it (`include/duct/pubsub.h:20`); QoS and reconnect wrap `std::unique_ptr<Pipe>` (`include/duct/qos_pipe.h:29`, `src/reconnect_pipe.h:11`). This is the concrete seam that permits TCP/UDS/SHM/pipes to share the higher layers.
7. **QoS is separated from transports because bounded memory and delivery policy are cross-transport, queue-time concerns, not OS I/O concerns.** It stores queue bytes/counts and watermarks (`include/duct/qos_pipe.h:48`, `include/duct/qos_pipe.h:55`), applies TTL/backpressure/rate decisions before worker delivery (`src/qos_pipe.cc:32`), and computes an aggregate size bound from both wrapper and transport (`src/qos_pipe.cc:365`). Backends remain focused on actual transmission.
8. **Reconnect is separated from QoS because it owns a different lifecycle failure mode.** QoS drains or drops already-enqueued messages; reconnect closes/replaces a dead inner pipe and redials with exponential backoff/jitter (`src/reconnect_pipe.cc:152`, `src/reconnect_pipe.cc:222`). Keeping it a decorator allows dial to select it independently (`src/duct.cc:110`) and preserve frame channels through replacement (`src/reconnect_pipe.cc:68`).
9. **RPC and pub/sub are separate because request correlation and topic fan-out have incompatible state and threading models.** RPC needs a pending map plus response dispatcher (`include/duct/rpc.h:54`, `include/duct/rpc.h:157`, `include/duct/rpc.h:195`); pub/sub needs a shared subscriber registry and synchronous publisher-thread fan-out (`include/duct/pubsub.h:35`, `include/duct/pubsub.h:86`). Merging them would impose request IDs or callback scheduling on every message path.
10. **Application convenience is kept above the core because ergonomic wrappers have different compatibility and ownership expectations.** Builders capture caller configuration (`include/duct/convenience.h:22`, `include/duct/convenience.h:87`); RAII wrappers convert destruction into deterministic close (`include/duct/raii.h:17`, `include/duct/raii.h:75`); batch/serve helpers encode application loops (`include/duct/convenience.h:230`, `include/duct/convenience.h:280`). They remain header-only add-ons rather than expanding the linked runtime's required surface.
11. **Packaging/build and documentation are separate layers because they define consumption compatibility rather than runtime behavior.** CMake selects active backend sources and links Winsock only on Windows (`CMakeLists.txt:18`, `CMakeLists.txt:31`, `CMakeLists.txt:60`); it also defines install/export/pkg-config/CPack and an install smoke test (`CMakeLists.txt:99`, `CMakeLists.txt:135`, `CMakeLists.txt:142`). Bilingual docs and site navigation make the contract public (`docs/.vitepress/config.mts:17`).
12. **Examples/tests/benchmarks remain outside architectural layers because they verify rather than provide runtime behavior.** The test binary contains the platform regression matrix and public/internal decorator checks (`tests/duct_tests.cc:1`, `tests/duct_tests.cc:4119`); examples demonstrate app loops (`examples/convenience_example.cc:23`); benchmark harnesses fork echo peers and compare transports (`bench/aello_bench.cc:150`, `bench/aello_bench.cc:164`). Keeping them separate prevents sample ergonomics from becoming implicit library dependencies.

## Maintenance Notes

- This is a source-derived snapshot, not a promise that every declared option is implemented. Reserved fields are explicitly listed above.
- Future architecture edits should update the English and Chinese documents together and preserve reciprocal links in `docs/.vitepress/config.mts`.

# Source Glossary (2026-11-07)

This glossary records the source-derived vocabulary of Aello. Terms are grouped by conceptual layer so the table reads as an architecture contract rather than a file inventory. All paths and line numbers refer to HEAD `f7405a7a6dd726b0552e09294a4303b5cc1cc682`; for the layer narrative and split rationale, see [source layer analysis](/design/source-layer-analysis-2026-11-07). Chinese readers should start from the [Chinese glossary](/zh/design/source-glossary-2026-11-07).

## Terms

| Term | English term | Definition | Evidence |
|---|---|---|---|
| 基础契约 | Foundation contract | The `Message`, `Status`, and `Result<T>` vocabulary used across every boundary. | `include/duct/message.h:15`; `include/duct/status.h:66`; `include/duct/status.h:113` |
| 消息 | Message | A bounded byte payload with extent and shared backing storage; views are copied, the model itself is non-copying. | `include/duct/message.h:15`; `include/duct/message.h:36`; `include/duct/message.h:67` |
| 状态码 | Status code | The closed set of success and failure categories used by every callable boundary. | `include/duct/status.h:10` |
| 状态 | Status | A non-throwing code plus human-readable reason; the canonical failure value. | `include/duct/status.h:66`; `include/duct/status.h:71` |
| 结果 | Result | Typed success-or-`Status` return container; optional exceptions remain explicit. | `include/duct/status.h:113`; `include/duct/status.h:131` |
| 平台套接字原语 | Platform socket primitive | Cross-platform readiness, timeout, nonblocking, and close helpers shared by socket transports. | `include/duct/socket_utils.h`; `src/socket_utils.cc:19`; `src/socket_utils.cc:186` |
| 线协议 | Wire protocol | The framed byte-stream contract, including header, flags, version, fragmentation, and reassembly. | `include/duct/wire.h:13`; `include/duct/wire.h:27`; `src/wire.cc:28`; `src/wire.cc:321` |
| 帧头 | Frame header | A validated 16-byte record carrying magic, version, header length, payload length, and flags. | `include/duct/wire.h:13`; `include/duct/wire.h:27`; `src/wire.cc:42` |
| 协议标志 | Protocol flags | Bit-level frame metadata, including fragment boundary and 16-bit channel ID encoding. | `include/duct/protocol.h:22`; `include/duct/protocol.h:39`; `include/duct/protocol.h:43` |
| 分片 | Fragmentation | Splitting a payload larger than one frame into ordered chunks and marking the final chunk. | `src/wire.cc:283`; `src/wire.cc:301`; `README.md:110` |
| 重组 | Reassembly | Accumulating fragments by channel into one message while enforcing a total-size cap. | `src/wire.cc:327`; `src/wire.cc:329`; `src/wire.cc:357` |
| 共享内存环 | Shared-memory ring | A process-shared, fixed-slot ring with head/tail positions, slot control words, and geometry metadata. | `src/shm_transport.cc:163`; `src/shm_transport.cc:168`; `src/shm_transport.cc:197`; `src/win_shm.cc:76` |
| 槽 | Slot | One preallocated ring cell holding a length/control word and payload capacity. | `src/shm_transport.cc:197`; `src/shm_transport.cc:244`; `src/win_shm.cc:144` |
| 共享内存几何 | Shared-memory geometry | Peer-agreed slot byte size and slot count, validated against a self-describing segment header. | `include/duct/duct.h:92`; `include/duct/duct.h:94`; `src/shm_transport.cc:163` |
| 传输抽象 | Transport abstraction | The backend-independent `Pipe`/`Listener` contract through which policies and patterns compose. | `include/duct/duct.h:127`; `include/duct/duct.h:154` |
| 管道 | Pipe | One connected endpoint supporting framed send/receive, channel reads, size limits, credentials, and close. | `include/duct/duct.h:127`; `include/duct/duct.h:130`; `include/duct/duct.h:135` |
| 监听器 | Listener | A bound endpoint that accepts connected pipes, reports its local address, and closes ownership. | `include/duct/duct.h:154`; `include/duct/duct.h:157`; `include/duct/duct.h:163` |
| 地址方案 | Address scheme | The `tcp://`, `uds://`, `shm://`, or `pipe://` selector that routes dial/listen to a backend. | `include/duct/protocol.h:14`; `src/address.cc:23`; `src/duct.cc:29`; `src/duct.cc:56` |
| 分发 | Dispatch | Direct scheme-based selection of a transport's dial/listen factory. | `src/duct.cc:29`; `src/duct.cc:35`; `src/duct.cc:56`; `src/duct.cc:64` |
| 传输后端 | Transport backend | A concrete OS channel implementation for TCP, UDS, shared memory, or Windows named pipes. | `src/tcp_transport.cc:57`; `src/uds_transport.cc:31`; `src/shm_transport.cc:540`; `src/pipe_transport.cc:109` |
| 连接状态 | Connection state | The endpoint lifecycle model: connecting, connected, disconnected, reconnecting, or closed. | `include/duct/duct.h:48`; `src/reconnect_pipe.cc:121`; `src/state_callback_pipe.cc:49` |
| 拨号选项 | Dial options | The client establishment contract containing timeout, QoS, SHM, reconnect, and callback inputs. | `include/duct/duct.h:166`; `include/duct/convenience.h:27`; `src/duct.cc:56` |
| 监听选项 | Listen options | The server establishment contract containing QoS, SHM, backlog, and handshake timeout inputs. | `include/duct/duct.h:179`; `include/duct/convenience.h:92` |
| 连接策略 | Connection policy | The decorator family governing bounded queueing or reconnect behavior around a transport. | `include/duct/qos_pipe.h:29`; `src/reconnect_pipe.cc:18`; `src/state_callback_pipe.cc:10` |
| 服务质量 | QoS | An explicit opt-in pipe decorator with bounded queues, backpressure, priority, TTL, rate limiting, and drain behavior. | `include/duct/duct.h:58`; `include/duct/qos_pipe.h:29`; `src/qos_pipe.cc:32`; `src/duct.cc:94` |
| 背压 | Backpressure | The selected behavior when a bounded queue is full: block, drop new, drop old, or fail fast. | `include/duct/duct.h:18`; `include/duct/duct.h:69`; `src/qos_pipe.cc:168` |
| 高水位 | High-water mark | A byte or item ceiling that bounds send/recv queues before policy intervention. | `include/duct/duct.h:63`; `include/duct/duct.h:67`; `include/duct/qos_pipe.h:48`; `include/duct/qos_pipe.h:59` |
| 令牌桶 | Token bucket | The QoS rate limiter that paces outbound bytes with a configured sustained rate and burst. | `include/duct/duct.h:86`; `src/qos_pipe.cc:32`; `src/qos_pipe.cc:72` |
| 消息过期时间 | Time-to-live | The age threshold after which a queued send is discarded by QoS. | `include/duct/duct.h:71`; `src/qos_pipe.cc:44` |
| 排空宽限期 | Drain linger | The bounded period in which queued outbound messages may complete before close. | `include/duct/duct.h:74`; `src/qos_pipe.cc:376` |
| 重连策略 | Reconnect policy | Dial retry inputs controlling initial/max delay, multiplier, attempt bound, and declared heartbeat interval. | `include/duct/duct.h:30`; `include/duct/duct.h:43`; `src/reconnect_pipe.cc:185` |
| 自动重连 | Automatic reconnect | A decorator that replaces failed inner pipes and retries with exponential backoff plus jitter. | `src/reconnect_pipe.cc:18`; `src/reconnect_pipe.cc:152`; `src/reconnect_pipe.cc:222`; `src/duct.cc:110` |
| 连接回调 | Connection callback | The caller-supplied state-transition handler used by reconnect or a standalone state adapter. | `include/duct/duct.h:48`; `include/duct/duct.h:56`; `src/reconnect_pipe.cc:121`; `src/state_callback_pipe.cc:63` |
| 消息模式 | Messaging pattern | A reusable semantic layer above a connected pipe, represented by RPC and pub/sub. | `include/duct/rpc.h:54`; `include/duct/pubsub.h:20` |
| 请求响应 | Request/response | A synchronous helper that sends one request and waits for one response on the same pipe. | `include/duct/convenience.h:203`; `include/duct/convenience.h:220` |
| 远程过程调用 | RPC | The correlated concurrent request/response pattern using a 32-bit little-endian request-ID frame. | `include/duct/rpc.h:22`; `include/duct/rpc.h:54`; `include/duct/rpc.h:71` |
| 请求标识 | Request identifier | The RPC correlation key prepended to requests and echoed by responses. | `include/duct/rpc.h:22`; `include/duct/rpc.h:28`; `include/duct/rpc.h:37` |
| 待处理调用 | Pending call | RPC state held until its matching response, timeout, cancellation, or close releases the caller. | `include/duct/rpc.h:71`; `include/duct/rpc.h:144`; `include/duct/rpc.h:195` |
| 响应分发 | Response dispatch | The RPC background loop that reads messages and wakes matching pending calls. | `include/duct/rpc.h:54`; `include/duct/rpc.h:157`; `include/duct/rpc.h:179` |
| 发布订阅 | Pub/sub | In-process topic fan-out to independent subscription callbacks. | `include/duct/pubsub.h:20`; `include/duct/pubsub.h:76`; `include/duct/pubsub.h:86` |
| 主题过滤器 | Topic filter | The subscriber prefix used to select topics during publish; callbacks run on the publisher thread. | `include/duct/pubsub.h:76`; `include/duct/pubsub.h:86`; `include/duct/pubsub.h:92` |
| 订阅 | Subscription | A scoped pub/sub handle whose destruction removes its callback and may safely outlive broker destruction. | `include/duct/pubsub.h:35`; `include/duct/pubsub.h:51`; `include/duct/pubsub.h:78` |
| 应用适配器 | Application adapter | The header-only builders, RAII owners, batch helpers, and convenience loops above the runtime. | `include/duct/convenience.h:22`; `include/duct/raii.h:17`; `include/duct/convenience.h:203`; `include/duct/convenience.h:280` |
| 构建器 | Builder | A fluent Dial/Listen option factory that eventually invokes the runtime dial/listen entry point. | `include/duct/convenience.h:22`; `include/duct/convenience.h:70`; `include/duct/convenience.h:87`; `include/duct/convenience.h:116` |
| RAII 包装器 | RAII wrapper | A scoped pipe/listener owner whose destructor closes the resource, while `release()` transfers ownership. | `include/duct/raii.h:17`; `include/duct/raii.h:44`; `include/duct/raii.h:75`; `include/duct/raii.h:107` |
| 通道复用 | Channel multiplexing | Logical stream isolation carried in wire flags and preserved by QoS/reconnect decorators. | `include/duct/duct.h:108`; `src/wire.cc:301`; `src/qos_pipe.cc:356`; `src/reconnect_pipe.cc:68` |
| 包契约 | Packaging contract | Build/install/pkg-config/CPack inputs that make the library consumable without changing runtime semantics. | `CMakeLists.txt:99`; `CMakeLists.txt:135`; `cmake/ductConfig.cmake.in`; `cmake/duct.pc.in` |

## Related Architecture

- English layer narrative: [Source Layer Analysis](/design/source-layer-analysis-2026-11-07)
- Chinese narrative: [源码分层分析](/zh/design/source-layer-analysis-2026-11-07)
- Chinese glossary: [源码术语表](/zh/design/source-glossary-2026-11-07)

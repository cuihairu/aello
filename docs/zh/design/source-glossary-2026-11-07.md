# 源码术语表（2026-11-07）

> 更新 2026-11-09：`StateCallbackPipe`（`src/state_callback_pipe.*`）已删除——其从未参与编译，也未接入任何拨号路径；重连拨号是唯一的连接回调路径。下表引用反映撰写时的 HEAD `f7405a7`。


本表按源码归纳 Aello 术语，并按概念层分组，使其成为架构契约而不是文件清单。所有路径与行号基于 HEAD `f7405a7a6dd726b0552e09294a4303b5cc1cc682`。分层叙述与拆分理由见[源码分层分析](/zh/design/source-layer-analysis-2026-11-07)；英文读者请从 [English glossary](/design/source-glossary-2026-11-07) 开始。

## 术语

| 术语 | 英文 | 定义 | file:line |
|---|---|---|---|
| 基础契约 | Foundation contract | 每个边界共同使用的 `Message`、`Status`、`Result<T>` 词汇。 | `include/duct/message.h:15`; `include/duct/status.h:66`; `include/duct/status.h:113` |
| 消息 | Message | 有界的字节载荷，带长度与共享 backing storage；复制 view，但消息模型自身不复制。 | `include/duct/message.h:15`; `include/duct/message.h:36`; `include/duct/message.h:67` |
| 状态码 | Status code | 所有可调用边界使用的成功与失败类别封闭集合。 | `include/duct/status.h:10` |
| 状态 | Status | 非抛出的代码加人类可读原因，是规范失败值。 | `include/duct/status.h:66`; `include/duct/status.h:71` |
| 结果 | Result | 类型化“成功或 `Status`”返回容器；异常必须显式触发。 | `include/duct/status.h:113`; `include/duct/status.h:131` |
| 平台套接字原语 | Platform socket primitive | socket 传输共享的跨平台就绪、超时、非阻塞和 close 辅助。 | `include/duct/socket_utils.h`; `src/socket_utils.cc:19`; `src/socket_utils.cc:186` |
| 线协议 | Wire protocol | 帧化字节流契约，包含 header、flags、版本、分片和重组。 | `include/duct/wire.h:13`; `include/duct/wire.h:27`; `src/wire.cc:28`; `src/wire.cc:321` |
| 帧头 | Frame header | 经校验的 16 字节记录，携带 magic、版本、头长、载荷长度和 flags。 | `include/duct/wire.h:13`; `include/duct/wire.h:27`; `src/wire.cc:42` |
| 协议标志 | Protocol flags | 位级帧元数据，包括分片边界和 16 位通道 ID 编码。 | `include/duct/protocol.h:22`; `include/duct/protocol.h:39`; `include/duct/protocol.h:43` |
| 分片 | Fragmentation | 把超过单帧的载荷拆成有序块，并标记最后一块。 | `src/wire.cc:283`; `src/wire.cc:301`; `README.md:110` |
| 重组 | Reassembly | 按通道累积分片为一条消息，并限制总尺寸。 | `src/wire.cc:327`; `src/wire.cc:329`; `src/wire.cc:357` |
| 共享内存环 | Shared-memory ring | 进程共享、固定槽的环，包含 head/tail、slot 控制字和几何元数据。 | `src/shm_transport.cc:163`; `src/shm_transport.cc:168`; `src/shm_transport.cc:197`; `src/win_shm.cc:76` |
| 槽 | Slot | 预分配的环形单元，持有长度/控制字和载荷容量。 | `src/shm_transport.cc:197`; `src/shm_transport.cc:244`; `src/win_shm.cc:144` |
| 共享内存几何 | Shared-memory geometry | 对端一致的槽字节大小和槽数量，并与自描述段头校验。 | `include/duct/duct.h:92`; `include/duct/duct.h:94`; `src/shm_transport.cc:163` |
| 传输抽象 | Transport abstraction | 与后端无关的 `Pipe`/`Listener` 契约，策略和模式层通过它组合。 | `include/duct/duct.h:127`; `include/duct/duct.h:154` |
| 管道 | Pipe | 一条已连接端点，支持帧收发、通道读取、尺寸限制、凭据查询和关闭。 | `include/duct/duct.h:127`; `include/duct/duct.h:130`; `include/duct/duct.h:135` |
| 监听器 | Listener | 已绑定端点，接受连接管道、报告本地地址并管理 close 所有权。 | `include/duct/duct.h:154`; `include/duct/duct.h:157`; `include/duct/duct.h:163` |
| 地址方案 | Address scheme | `tcp://`、`uds://`、`shm://` 或 `pipe://`，用于把 dial/listen 路由到后端。 | `include/duct/protocol.h:14`; `src/address.cc:23`; `src/duct.cc:29`; `src/duct.cc:56` |
| 分发 | Dispatch | 基于 scheme 直接选择传输的 dial/listen 工厂。 | `src/duct.cc:29`; `src/duct.cc:35`; `src/duct.cc:56`; `src/duct.cc:64` |
| 传输后端 | Transport backend | TCP、UDS、共享内存或 Windows 命名管道的具体 OS 通道实现。 | `src/tcp_transport.cc:57`; `src/uds_transport.cc:31`; `src/shm_transport.cc:540`; `src/pipe_transport.cc:109` |
| 连接状态 | Connection state | 端点生命周期：connecting、connected、disconnected、reconnecting、closed。 | `include/duct/duct.h:48`; `src/reconnect_pipe.cc:121`; `src/state_callback_pipe.cc:49` |
| 拨号选项 | Dial options | 客户端建立契约，包含 timeout、QoS、SHM、reconnect 和 callback 输入。 | `include/duct/duct.h:166`; `include/duct/convenience.h:27`; `src/duct.cc:56` |
| 监听选项 | Listen options | 服务端建立契约，包含 QoS、SHM、backlog 和 handshake timeout 输入。 | `include/duct/duct.h:179`; `include/duct/convenience.h:92` |
| 连接策略 | Connection policy | 围绕传输治理有界排队或重连行为的装饰器族。 | `include/duct/qos_pipe.h:29`; `src/reconnect_pipe.cc:18`; `src/state_callback_pipe.cc:10` |
| 服务质量 | QoS | 显式启用的 pipe 装饰器，提供有界队列、背压、优先级、TTL、限速和排空行为。 | `include/duct/duct.h:58`; `include/duct/qos_pipe.h:29`; `src/qos_pipe.cc:32`; `src/duct.cc:94` |
| 背压 | Backpressure | 有界队列满时选择的行为：阻塞、丢弃新、丢弃旧或快速失败。 | `include/duct/duct.h:18`; `include/duct/duct.h:69`; `src/qos_pipe.cc:168` |
| 高水位 | High-water mark | 在策略介入前限制发送/接收队列的字节或条目上限。 | `include/duct/duct.h:63`; `include/duct/duct.h:67`; `include/duct/qos_pipe.h:48`; `include/duct/qos_pipe.h:59` |
| 令牌桶 | Token bucket | QoS 限速器，用配置的持续速率和突发容量控制出站字节。 | `include/duct/duct.h:86`; `src/qos_pipe.cc:32`; `src/qos_pipe.cc:72` |
| 消息过期时间 | Time-to-live | QoS 队列中超过该年龄的发送消息会被丢弃。 | `include/duct/duct.h:71`; `src/qos_pipe.cc:44` |
| 排空宽限期 | Drain linger | close 前允许排队出站消息完成的有界等待期。 | `include/duct/duct.h:74`; `src/qos_pipe.cc:376` |
| 重连策略 | Reconnect policy | 拨号重试输入：初始/最大延迟、倍率、尝试上限和已声明的心跳间隔。 | `include/duct/duct.h:30`; `include/duct/duct.h:43`; `src/reconnect_pipe.cc:185` |
| 自动重连 | Automatic reconnect | 替换失败 inner pipe 并以指数退避加抖动重试的装饰器。 | `src/reconnect_pipe.cc:18`; `src/reconnect_pipe.cc:152`; `src/reconnect_pipe.cc:222`; `src/duct.cc:110` |
| 连接回调 | Connection callback | 调用方提供的状态迁移 handler，由 reconnect 或独立状态适配器调用。 | `include/duct/duct.h:48`; `include/duct/duct.h:56`; `src/reconnect_pipe.cc:121`; `src/state_callback_pipe.cc:63` |
| 消息模式 | Messaging pattern | 已连接 pipe 之上的可复用语义层，由 RPC 和 pub/sub 代表。 | `include/duct/rpc.h:54`; `include/duct/pubsub.h:20` |
| 请求响应 | Request/response | 同步 helper，在同一条 pipe 上发送一个请求并等待一个响应。 | `include/duct/convenience.h:203`; `include/duct/convenience.h:220` |
| 远程过程调用 | RPC | 使用 32 位小端请求 ID 帧关联并发请求/响应的模式。 | `include/duct/rpc.h:22`; `include/duct/rpc.h:54`; `include/duct/rpc.h:71` |
| 请求标识 | Request identifier | 前置于请求并由响应回带的 RPC 关联键。 | `include/duct/rpc.h:22`; `include/duct/rpc.h:28`; `include/duct/rpc.h:37` |
| 待处理调用 | Pending call | RPC 状态，直到匹配响应、超时、取消或 close 释放调用方。 | `include/duct/rpc.h:71`; `include/duct/rpc.h:144`; `include/duct/rpc.h:195` |
| 响应分发 | Response dispatch | RPC 后台循环，读取消息并唤醒匹配的 pending call。 | `include/duct/rpc.h:54`; `include/duct/rpc.h:157`; `include/duct/rpc.h:179` |
| 发布订阅 | Pub/sub | 进程内主题向独立订阅回调分发。 | `include/duct/pubsub.h:20`; `include/duct/pubsub.h:76`; `include/duct/pubsub.h:86` |
| 主题过滤器 | Topic filter | 发布时选择主题的订阅前缀；回调在发布者线程运行。 | `include/duct/pubsub.h:76`; `include/duct/pubsub.h:86`; `include/duct/pubsub.h:92` |
| 订阅 | Subscription | 作用域化 pub/sub handle；析构移除回调，并可安全越过 broker 析构。 | `include/duct/pubsub.h:35`; `include/duct/pubsub.h:51`; `include/duct/pubsub.h:78` |
| 应用适配器 | Application adapter | 运行时之上的 header-only builder、RAII 所有者、批量 helper 和便捷循环。 | `include/duct/convenience.h:22`; `include/duct/raii.h:17`; `include/duct/convenience.h:203`; `include/duct/convenience.h:280` |
| 构建器 | Builder | 链式 Dial/Listen 选项工厂，最终调用运行时 dial/listen 入口。 | `include/duct/convenience.h:22`; `include/duct/convenience.h:70`; `include/duct/convenience.h:87`; `include/duct/convenience.h:116` |
| RAII 包装器 | RAII wrapper | scoped pipe/listener 所有者；析构关闭资源，`release()` 转移所有权。 | `include/duct/raii.h:17`; `include/duct/raii.h:44`; `include/duct/raii.h:75`; `include/duct/raii.h:107` |
| 通道复用 | Channel multiplexing | 通过 wire flags 承载的逻辑流隔离，并由 QoS/reconnect 装饰器保留。 | `include/duct/duct.h:108`; `src/wire.cc:301`; `src/qos_pipe.cc:356`; `src/reconnect_pipe.cc:68` |
| 包契约 | Packaging contract | build/install/pkg-config/CPack 输入，使库可消费且不改变运行时语义。 | `CMakeLists.txt:99`; `CMakeLists.txt:135`; `cmake/ductConfig.cmake.in`; `cmake/duct.pc.in` |

## 相关架构

- 英文叙述：[Source Layer Analysis](/design/source-layer-analysis-2026-11-07)
- 中文叙述：[源码分层分析](/zh/design/source-layer-analysis-2026-11-07)
- English glossary: [Source Glossary](/design/source-glossary-2026-11-07)

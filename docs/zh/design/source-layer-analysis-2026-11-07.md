# 源码分层分析（2026-11-07）

本文基于 HEAD `f7405a7a6dd726b0552e09294a4303b5cc1cc682` 与当前源码树，记录职责、状态所有权、调用通道、配置输入、生命周期，以及每一层存在的具体理由。路径均相对仓库根；行号取自该提交。

## 范围与排除

- 本仓库是 C++20 进程内/进程间消息库，不是已部署服务集群。运行时状态只有进程内存或传输拥有的操作系统状态；没有 Redis、MySQL 或外部配置服务。
- 本文不改代码。既有行为按现状记录，包括尚未被消费的预留字段。
- 测试、示例、基准和 CI 验证架构，但不是架构层。只有当它们定义输入契约或验证生命周期时才会列出。

## 分层模型

库共有七个概念层。这些是职责边界，不是目录名；例如拨号/监听分发和连接恢复都在 `src/`，而应用便捷适配器只在头文件中。

```text
应用适配器 / 消息模式
        |
运行时编排与拨号/监听分发
        |
连接策略与 QoS 装饰器
        |
传输抽象
        |
传输后端
        |
线协议与内存帧
        |
基础层：结果模型与平台原语
```

| 层 | 职责 | 代表实现 | 状态存储 | 主要输入 |
|---|---|---|---|---|
| 1. 基础契约 | 提供 `Message` 值类型、`Status`/`Result<T>` 错误模型和平台中立套接字操作。 | `include/duct/message.h:15`、`include/duct/status.h:66`、`src/socket_utils.cc:19` | 内存中的消息 backing buffer；调用方持有 socket 描述符 | 调用方字节、string view、容量、socket 句柄 |
| 2. 线协议与内存帧 | 统一字节流和共享内存环记录，包括版本协商、分片和通道标志。 | `src/wire.cc:28`、`src/wire.cc:321`、`src/shm_transport.cc:163`、`src/win_shm.cc:71` | 内存帧缓冲；进程共享 ring 元数据、slot、信号量和命名文件映射 | `FrameHeader`/`FrameFlags` 常量、`kMaxFramePayload`、socket 句柄或段映射 |
| 3. 传输后端 | 为 TCP、Unix 域套接字、共享内存和 Windows 命名管道实现 `Pipe`/`Listener` 生命周期。 | `src/tcp_transport.cc:57`、`src/uds_transport.cc:31`、`src/shm_transport.cc:540`、`src/pipe_transport.cc:109` | OS socket；POSIX `/dev/shm` 加 bootstrap UDS；Windows 文件映射/事件；内存中的连接状态 | `Address`、`DialOptions`、`ListenOptions`、`ShmOptions`、`SendOptions`、`RecvOptions` |
| 4. 传输抽象与分发 | 通过地址 scheme 和公共 `Pipe`/`Listener` 契约隐藏后端选择。 | `include/duct/duct.h:127`、`include/duct/duct.h:154`、`src/duct.cc:29`、`src/duct.cc:56` | 调用栈上的解析地址和选项副本；被装饰的 pipe 实例 | 地址字符串、拨号/监听选项、scheme 可用性 |
| 5. 连接策略与 QoS | 在不改传输的前提下增加有界收发队列、背压、限速、重连和状态回调。 | `src/qos_pipe.cc:11`、`src/reconnect_pipe.cc:18`、`src/state_callback_pipe.cc:10` | STL 队列/deque、mutex/condition 状态、连接状态与活动 pipe；无外部存储 | `QosOptions`、`ReconnectPolicy`、`ConnectionCallback`、每调用选项 |
| 6. 消息模式 | 在已连接 pipe 之上提供可复用语义：RPC 关联和进程内主题分发。 | `include/duct/rpc.h:58`、`include/duct/pubsub.h:27` | pending RPC map 和工作线程；订阅者 vector 与 ID 的共享状态 | `Pipe`、请求载荷与超时、订阅过滤器和回调、发布主题/消息 |
| 7. 应用适配器 | 把运行时转成符合应用侧习惯的 builder、RAII 所有权、同步批量/请求循环和可演示应用循环。 | `include/duct/convenience.h:22`、`include/duct/raii.h:17`、`examples/convenience_example.cc:23`、`bench/aello_bench.cc:150` | 拥有的 pipe/listener 指针或 scoped wrapper；批量 vector 和 handler 状态 | 地址与选项 builder、应用 handler、基准参数 |

### 第 1 层：基础契约

- **实现：** `Message` 持有共享 backing buffer 并暴露 view，构造时不额外复制（`include/duct/message.h:15`、`src/message.cc:7`）；`Status`、`StatusCode`、`Exception` 和 `Result<T>` 构成非抛出错误契约（`include/duct/status.h:10`、`include/duct/status.h:66`、`include/duct/status.h:113`）；`socket_utils` 集中就绪等待、非阻塞模式和跨平台 close（`include/duct/socket_utils.h`、`src/socket_utils.cc:19`、`src/socket_utils.cc:36`、`src/socket_utils.cc:186`）。
- **配置 schema：** 没有外部 schema。`Message::from_bytes` 与 `Message::with_capacity` 接受字节范围/容量（`include/duct/message.h:20`、`include/duct/message.h:27`）；错误码即契约（`include/duct/status.h:10`）。
- **协议文件：** 无。该层刻意不含传输协议。
- **文档挂载：** 英文 `docs/design/source-layer-analysis-2026-11-07.md`；中文 `docs/zh/design/source-layer-analysis-2026-11-07.md`；站点目录 `docs/.vitepress/config.mts`。
- **生命周期：** 无全局生命周期。Windows 上 `ensure_networking()` 懒初始化 Winsock（`src/socket_utils.cc:19`）；消息因持有共享存储可超过原 view 的生命周期（`include/duct/message.h:36`）。

### 第 2 层：线协议与内存帧

- **实现：** 16 字节 socket 帧编解码在 `src/wire.cc`；`encode_header`/`decode_header` 校验 magic、版本、头长和载荷上限（`src/wire.cc:28`、`src/wire.cc:42`）。`write_frame` 对超过 64 KiB 的载荷分片并标记首/尾片（`src/wire.cc:283`）；`read_frame_with_flags` 重组至多 64 MiB（`src/wire.cc:327`）。POSIX 与 Windows 共享内存布局分别定义 ring 元数据、slot 和分片状态。
- **状态存储：** 栈/缓冲局部的帧状态；TCP/UDS pipe 内的按通道 pending 重组 map；共享内存中的 ring 元数据/slot 和命名同步对象。POSIX 命名与 bootstrap 细节起于 `src/shm_transport.cc:244`；Windows 对应物起于 `src/win_shm.cc:144`。
- **配置 schema：** 线上常量和字段顺序即 schema（`include/duct/wire.h:13`、`include/duct/wire.h:27`、`include/duct/protocol.h:14`、`include/duct/protocol.h:22`）。共享内存几何由 `ShmOptions` 配置并在 accept 校验（`include/duct/duct.h:92`、`src/shm_transport.cc:163`）。
- **协议文件：** `include/duct/wire.h`、`include/duct/protocol.h`。RPC 请求 ID 扩展记录在 `include/duct/rpc.h:22`；pub/sub 刻意复用消息载荷且不定义额外线帧（`include/duct/pubsub.h:20`）。
- **文档挂载：** 同英中架构文档与 VitePress 注册。
- **生命周期：** 头部编解码无状态。socket 读写接受超时；部分传输后的字节流失步必须毒化连接，而不是复用重发（`include/duct/wire.h:40`、`src/wire.cc:283`）。共享内存段按设计可跨进程死亡存续；恢复在传输后端层完成。

### 第 3 层：传输后端

- **实现：** TCP、UDS、POSIX 共享内存、Windows 共享内存和 Windows 命名管道分别提供具体 `Pipe`/`Listener` 实现与自由拨号/监听入口：TCP 在 `src/tcp_transport.cc:57`、`src/tcp_transport.cc:437`；UDS 在 `src/uds_transport.cc:31`、`src/uds_transport.cc:356`；POSIX SHM 在 `src/shm_transport.cc:540`、`src/shm_transport.cc:831`；Windows SHM 在 `src/win_shm.cc:394`；命名管道在 `src/pipe_transport.cc:109`、`src/pipe_transport.cc:449`。
- **状态存储：** OS socket/listener 句柄与 TCP 握手状态（`src/tcp_transport.cc:57`、`src/tcp_transport.cc:199`）；UDS 句柄加 SO_PEERCRED 数据（`src/uds_transport.cc:31`、`include/duct/duct.h:121`）；SHM 映射、ring head/tail、信号量/命名注册表和 owner-PID 崩溃元数据（`src/shm_transport.cc:163`、`src/shm_transport.cc:244`、`src/shm_transport.cc:391`）；命名管道实例和 listener 共享状态（`src/pipe_transport.cc:109`、`src/pipe_transport.cc:299`）。无数据库或缓存。
- **配置 schema：** C++ 选项结构即 schema：`DialOptions` 与 `ListenOptions`（`include/duct/duct.h:166`、`include/duct/duct.h:179`）、`ShmOptions`（`include/duct/duct.h:94`）、`SendOptions`/`RecvOptions`（`include/duct/duct.h:103`、`include/duct/duct.h:112`）、`handshake_timeout`（`include/duct/duct.h:188`）。构建/平台输入由 `CMakeLists.txt:18`、`CMakeLists.txt:31` 选择。
- **协议文件：** 公共传输特征在 `include/duct/duct.h`；线协议细节在第 2 层。后端 bootstrap/地址形态由 `src/address.cc:23` 和后端工厂建立。
- **文案挂载：** 同英中架构文档与 VitePress 注册。
- **生命周期：** dial 连接并返回 `Pipe`；listen 绑定并返回 `Listener`；观察到的后端析构都调用 `close()`（例如 `src/tcp_transport.cc:77`、`src/uds_transport.cc:45`、`src/shm_transport.cc:540`、`src/pipe_transport.cc:109`）。`accept()` 执行服务端握手（`src/tcp_transport.cc:227`）。共享内存额外包含孤儿发现/回收和 close 阶段的 unlink 行为。

### 第 4 层：传输抽象与分发

- **实现：** `Pipe` 和 `Listener` 定义 send/recv、accept/close、帧转发、凭据和尺寸查询（`include/duct/duct.h:127`、`include/duct/duct.h:154`）。`listen()` 解析一个地址并分发后端（`src/duct.cc:29`）；`dial_impl()` 对 dial 做同样分发并应用可选 QoS（`src/duct.cc:56`）。
- **状态存储：** 该层不拥有长期状态。解析后的 `Address` 和选项沿调用栈传递，返回后端拥有的对象。
- **配置 schema：** 与后端相同的 `DialOptions`/`ListenOptions`。地址语法由实现承载而不是数据文件：`Address::parse`（`src/address.cc:23`），`Scheme` 值在 `include/duct/protocol.h:14`。
- **协议文件：** 无专门分发协议文件；`include/duct/duct.h` 是公共契约头。
- **文案挂载：** 同英中架构文档与 VitePress 注册。
- **生命周期：** 没有自己的线程或后台进程。它组合对象树，并在返回对象析构时结束。仅当 `opt.reconnect.enabled` 为真时选择重连包装（`src/duct.cc:105`、`src/duct.cc:110`）。

### 第 5 层：连接策略与 QoS

- **实现：** `QosPipe` 以两个工作线程、有界队列、优先级/TTL/限速和保留通道标志的帧读取包装任意 `Pipe`（`include/duct/qos_pipe.h:29`、`src/qos_pipe.cc:11`、`src/qos_pipe.cc:313`）。`ReconnectPipe` 拥有拨号一次后的恢复状态、带抖动退避、有界尝试和回调发布（`src/reconnect_pipe.cc:18`、`src/reconnect_pipe.cc:167`、`src/reconnect_pipe.cc:222`）。`StateCallbackPipe` 从传输结果发出 disconnect/closed 回调，但当前没有生产侧接线（`src/state_callback_pipe.cc:10`、`src/state_callback_pipe.cc:63`）。
- **状态存储：** STL deque、字节/数量高水位和 mutex/CV 协调（`include/duct/qos_pipe.h:48`、`include/duct/qos_pipe.h:59`）；`QosPipe` 内的 token bucket 和错误状态；重连连接状态、活动 inner pipe、last error 和工作线程（`src/reconnect_pipe.cc:249`）；回调适配器的 atomic closed/disconnected 标志（`src/state_callback_pipe.cc:55`）。
- **配置 schema：** `QosOptions` 与 `ReconnectPolicy`（`include/duct/duct.h:30`、`include/duct/duct.h:58`），加上每调用优先级、超时和通道（`include/duct/duct.h:103`、`include/duct/duct.h:112`）。
- **协议文件：** 无专属协议；`recv_frame` 明确跨装饰器保留线协议通道标志（`src/reconnect_pipe.cc:68`）。
- **文案挂载：** 同英中架构文档与 VitePress 注册。
- **生命周期：** `QosPipe` 构造启动工作线程，`close()` 排空并 join（`src/qos_pipe.cc:11`、`src/qos_pipe.cc:376`）。`ReconnectPipe` 构造启动 worker，仅在 inner pipe 可见后发布 connected，并在 close 时 join worker（`src/reconnect_pipe.cc:20`、`src/reconnect_pipe.cc:199`、`src/reconnect_pipe.cc:101`）。

### 第 6 层：消息模式

- **实现：** `rpc::Client` 用小端 32 位请求 ID 和分发线程关联并发 request/response（`include/duct/rpc.h:22`、`include/duct/rpc.h:54`、`include/duct/rpc.h:157`）。`pubsub::Broker` 做进程内主题前缀匹配，并在发布者线程执行回调（`include/duct/pubsub.h:20`、`include/duct/pubsub.h:76`、`include/duct/pubsub.h:86`）。
- **状态存储：** RPC pending-call map、atomic ID、mutex/send-lock 状态和工作线程（`include/duct/rpc.h:195`）；pub/sub 订阅者 vector、next ID 和共享存活状态（`include/duct/pubsub.h:35`）。无外部存储。
- **配置 schema：** 无配置文件。RPC 使用调用超时和可选显式取消（`include/duct/rpc.h:71`、`include/duct/rpc.h:108`）；pub/sub 使用过滤字符串和回调（`include/duct/pubsub.h:78`）。
- **协议文件：** `include/duct/rpc.h:22` 记录请求 ID 扩展。pub/sub 刻意不拥有传输，也没有额外线协议（`include/duct/pubsub.h:20`）。
- **文案挂载：** 同英中架构文档与 VitePress 注册。
- **生命周期：** RPC 构造启动分发线程，close/join 并释放等待者（`include/duct/rpc.h:60`、`include/duct/rpc.h:123`）；超时/取消后丢弃迟到响应（`include/duct/rpc.h:95`、`include/duct/rpc.h:184`）。Pub/sub 通过引用计数共享状态，使订阅可安全越过 broker 析构（`include/duct/pubsub.h:35`、`include/duct/pubsub.h:45`）。

### 第 7 层：应用适配器

- **实现：** builder 和 raw 工厂暴露 dial/listen 易用入口（`include/duct/convenience.h:22`、`include/duct/convenience.h:87`、`include/duct/convenience.h:180`）；RAII wrapper 拥有 close/accept（`include/duct/raii.h:17`、`include/duct/raii.h:75`、`include/duct/raii.h:141`）；辅助函数实现 request/reply、批量、echo/serve和生产/消费循环（`include/duct/convenience.h:203`、`include/duct/convenience.h:280`、`include/duct/convenience.h:327`）。示例与基准是外部消费者：`examples/convenience_example.cc:23`、`bench/aello_bench.cc:150`。
- **状态存储：** builder/wrapper 中的 owned 指针以及调用方 vector/handler 捕获；不拥有持久状态。
- **配置 schema：** 同一组选项结构，配合 timeout、水位、背压、重连、心跳和回调的 builder 方法（`include/duct/convenience.h:27` 至 `include/duct/convenience.h:64`）。CMake 示例/测试/基准开关记录在 `CMakeLists.txt:15`、`CMakeLists.txt:63`、`CMakeLists.txt:80`、`CMakeLists.txt:87`。
- **协议文件：** 无。该层转发到模式层或运行时。
- **文案挂载：** 同英中架构文档与 VitePress 注册。
- **生命周期：** builder 是短期工厂。RAII wrapper 析构关闭，`release()` 转移所有权（`include/duct/raii.h:22`、`include/duct/raii.h:44`）。辅助循环在 `kClosed` 结束、传播其他错误，或在有界工作后返回（`include/duct/convenience.h:280`、`include/duct/convenience.h:303`、`include/duct/convenience.h:327`）。

## 调用面

- **公共调用链：** `convenience`/`raii`/应用 -> `dial`/`listen` -> scheme 分发 -> 可选 `ReconnectPipe` -> 可选 `QosPipe` -> 具体传输 -> `wire`/SHM ring -> OS。拨号侧组合点是 `src/duct.cc:105`、`src/duct.cc:56`；`QosPipe` 包装在 `src/duct.cc:94`。
- **后端通道：** TCP/UDS 使用阻塞 socket 和帧字节流；共享内存使用命名进程共享映射/ring 加同步原语；Windows 命名管道使用 overlapped I/O。分发是直接函数调用，不是事件总线或 RPC 中间件。
- **内部回调：** `ConnectionCallback` 在重连状态迁移（`src/reconnect_pipe.cc:121`）或状态适配器失败（`src/state_callback_pipe.cc:49`）时调用。RPC 只唤醒调用方 future；pub/sub 在发布者线程调用订阅者回调（`include/duct/pubsub.h:77`）。
- **验证通道：** `tests/duct_tests.cc` 调用公共 API 与内部装饰器（`tests/duct_tests.cc:1`、`tests/duct_tests.cc:4110`）；CMake 注册 `duct_tests`、`duct_install` 和基准（`CMakeLists.txt:74`、`CMakeLists.txt:90`、`CMakeLists.txt:95`）；CI 执行 CMake 矩阵与文档部署（`.github/workflows/cmake-multi-platform.yml`、`.github/workflows/deploy-docs.yml`）。

## 配置与外部输入

| 输入 | 来源 | 所有层 | 说明 |
|---|---|---|---|
| 构建特性开关 | `CMakeLists.txt:15`、`CMakeLists.txt:63`、`CMakeLists.txt:80`、`CMakeLists.txt:87` | 构建系统 | 示例、测试和 POSIX 基准；基准仅非 Windows。 |
| 版本 | `CMakeLists.txt:3`、`cmake/duct_version.h.in:3` | 打包 | 生成 `duct/version.h`；无源码读取生成后的版本值。 |
| 包发现 | `CMakeLists.txt:99`、`cmake/ductConfig.cmake.in`、`cmake/duct.pc.in` | 打包 | install/export、pkg-config、CPack 和安装冒烟测试。 |
| 地址字符串 | `src/address.cc:23` | 分发 | scheme 加 name/path/host-port。 |
| dial/listen/QoS/重连/SHM 选项 | `include/duct/duct.h:30`、`include/duct/duct.h:58`、`include/duct/duct.h:94`、`include/duct/duct.h:166`、`include/duct/duct.h:179` | 运行时 | 只有 C++ 结构；不读环境变量或配置文件。 |
| 每调用选项 | `include/duct/duct.h:103`、`include/duct/duct.h:112` | 运行时/模式 | 超时、优先级、通道。 |
| CI workflow 触发 | `.github/workflows/cmake-multi-platform.yml`、`.github/workflows/deploy-docs.yml` | CI | 仓库事件；不是运行时配置。 |

## 状态清单

| 所有者 | 状态 | 介质 | 持久化/恢复 | 证据 |
|---|---|---|---|---|
| `Message` | 载荷与长度 | 堆共享缓冲 | 随调用方生命周期 | `include/duct/message.h:67` |
| TCP/UDS pipe | socket 句柄、握手、读锁、按通道 pending 帧 | 内存/OS socket | 只随 OS socket 状态 | `src/tcp_transport.cc:192`、`src/uds_transport.cc:145` |
| POSIX SHM | 段、ring 元数据、slot、信号量、命名、owner 身份 | `/dev/shm` + bootstrap UDS + OS 信号量 | owner PID/magic 支持孤儿回收；unlink 发生在后端 close | `src/shm_transport.cc:163`、`src/shm_transport.cc:244`、`src/shm_transport.cc:391` |
| Windows SHM | 映射、事件、ring 元数据/slot、命名、owner 身份 | Windows 命名文件映射/内核事件 | 同一 ring 契约；平台实现在 `src/win_shm.cc` | `src/win_shm.cc:76`、`src/win_shm.cc:144`、`src/win_shm.cc:198` |
| Windows 命名管道 | pipe 实例、overlapped 操作、listener 共享状态 | OS 命名管道/内核事件 | listener 状态引用计数以支持并发 accept | `src/pipe_transport.cc:109`、`src/pipe_transport.cc:299` |
| QoS | 队列、水位、token bucket、worker/错误状态 | 进程内存 | 不持久化；队列有界 | `include/duct/qos_pipe.h:48`、`include/duct/qos_pipe.h:59` |
| 重连 | inner pipe、状态、错误、策略、worker | 进程内存 | 按需重拨；无历史日志 | `src/reconnect_pipe.cc:249` |
| RPC | pending 调用、ID、分发 worker | 进程内存 | 不持久化；超时/取消释放等待者 | `include/duct/rpc.h:195` |
| Pub/sub | 订阅者 vector 与存活标志 | 进程内存/共享状态 | 订阅可越过 broker 析构 | `include/duct/pubsub.h:35` |

## 预留与未完全接线契约

- `DialOptions::on_state_change` 仅在启用 reconnect 时生效，因为 `dial()` 将其传给 `make_reconnect_pipe`（`include/duct/duct.h:176`、`src/duct.cc:110`）。独立 `make_state_callback_pipe` 实现存在但没有生产调用方（`src/state_callback_pipe.cc:63`）。
- `ReconnectPolicy::heartbeat_interval` 已声明且 builder 可设置，但没有生产传输消费（`include/duct/duct.h:43`、`include/duct/convenience.h:57`）。
- `QosOptions::reliability` 已声明但没有生产读取方（`include/duct/duct.h:77`）。
- `ListenOptions::qos` 存在，但 `listen()` 不构造 QoS wrapper（`include/duct/duct.h:181`、`src/duct.cc:29`）。因此服务端 QoS 只能显式构造，测试即如此使用（`tests/duct_tests.cc:4084`）。
- 平台差异按文档保留：socket 传输分片/重组至 64 MiB，Windows SHM/命名管道超过自身容量则拒绝（`README.md:110`）。UDS peer credentials 由 OS 支持；其他传输返回 not-supported（`include/duct/duct.h:118`、`src/uds_transport.cc:31`）。

## 拆分理由

1. **基础层与协议/传输分离，因为错误与所有权契约跨越每个边界。** `Message` 在暴露 view 的同时保留共享 backing（`include/duct/message.h:36`），`Result<T>`/`Status` 迫使所有层显式处理成功/错误（`include/duct/status.h:113`)。跨层使用证据：后端、装饰器、RPC 和便捷层都返回 `Result`（`src/tcp_transport.cc:57`、`src/qos_pipe.cc:29`、`include/duct/rpc.h:58`、`include/duct/convenience.h:203`）。
2. **字节流帧协议独立抽取，使 TCP 和 UDS 共用同一套已验证实现。** 两种传输都把通道 ID 编码进帧 flags 并调用 `wire::write_frame`（`src/tcp_transport.cc:83`、`src/uds_transport.cc:56`）；头部校验、分片、重组和超时集中在 `src/wire.cc:42`、`src/wire.cc:283`、`src/wire.cc:327`。这防止两个 socket 传输在协议兼容性上漂移。
3. **共享内存 ring 帧与 socket 线协议分离，因为同步、slot 几何、崩溃元数据和平台映射不同。** POSIX SHM 在 `src/shm_transport.cc:163`、`src/shm_transport.cc:197`、`src/shm_transport.cc:244` 定义 ring head/tail、段布局、slot 控制字和命名注册表；Windows 在 `src/win_shm.cc:76`、`src/win_shm.cc:144`、`src/win_shm.cc:198` 有独立布局/句柄实现。其配置是对端一致的几何参数，而不是 socket 帧常量（`include/duct/duct.h:92`）。
4. **传输后端必须拆分，因为连接建立和 OS 资源生命周期无法抽象成同一算法。** TCP 创建/连接 socket 并解析 wildcard/effective port（`src/tcp_transport.cc:399`、`src/tcp_transport.cc:437`）；UDS 绑定文件系统 socket 并捕获 peer credentials（`src/uds_transport.cc:152`）；SHM 创建/回收进程共享段（`src/shm_transport.cc:831`）；命名管道创建 overlapped 实例（`src/pipe_transport.cc:449`）。单一后端会掩盖平台专属失败与恢复语义。
5. **scheme 分发与后端分离，使公共地址稳定而平台可变。** 一个 `Address::parse` 产生类型化 scheme（`src/address.cc:23`），随后 `listen()` 和 `dial_impl()` 只路由可用 scheme（`src/duct.cc:29`、`src/duct.cc:56`）。Windows-only pipe 支持的条件被隔离在 `src/duct.cc:41`、`src/duct.cc:76`，调用方不再散落平台分支。
6. **公共 `Pipe`/`Listener` 边界使策略和模式层无需认识后端。** RPC 接受任意 `Pipe`（`include/duct/rpc.h:58`）；pub/sub 明确假设任意一侧可驱动（`include/duct/pubsub.h:20`）；QoS 和重连包装 `std::unique_ptr<Pipe>`（`include/duct/qos_pipe.h:29`、`src/reconnect_pipe.h:11`）。这就是 TCP/UDS/SHM/pipe 共享高层组件的具体接缝。
7. **QoS 与传输分离，因为有界内存和投递策略是跨传输的入队期问题，不是 OS I/O 问题。** 它保存队列字节/数量与水位（`include/duct/qos_pipe.h:48`、`include/duct/qos_pipe.h:55`），在 worker 投递前应用 TTL/背压/限速（`src/qos_pipe.cc:32`），并从 wrapper 与传输共同推导尺寸上限（`src/qos_pipe.cc:365`）。后端保持专注实际传输。
8. **重连与 QoS 分离，因为二者处理不同生命周期故障。** QoS 排空或丢弃已入队消息；重连关闭/替换死掉的 inner pipe，并以指数退避/抖动重新拨号（`src/reconnect_pipe.cc:152`、`src/reconnect_pipe.cc:222`)。保持独立装饰器使 dial 可单独选择（`src/duct.cc:110`），且替换连接时保留帧通道（`src/reconnect_pipe.cc:68`）。
9. **RPC 与 pub/sub 分离，因为请求关联和主题分发拥有不兼容的状态与线程模型。** RPC 需要 pending map 加响应分发线程（`include/duct/rpc.h:54`、`include/duct/rpc.h:157`、`include/duct/rpc.h:195`）；pub/sub 需要共享订阅注册表和发布者线程同步分发（`include/duct/pubsub.h:35`、`include/duct/pubsub.h:86`）。合并会给每条消息路径强加请求 ID 或回调调度。
10. **应用便捷层留在核心之上，因为易用包装的所有权与兼容性预期不同。** builder 捕获调用方配置（`include/duct/convenience.h:22`、`include/duct/convenience.h:87`）；RAII wrapper 把析构转成确定性 close（`include/duct/raii.h:17`、`include/duct/raii.h:75`）；批量/serve helper 编码应用循环（`include/duct/convenience.h:230`、`include/duct/convenience.h:280`）。它们保持 header-only 增补，而不扩大链接库必需面。
11. **打包/构建与文档是独立层，因为它们定义消费兼容性而非运行时行为。** CMake 选择活跃后端源并只在 Windows 链接 Winsock（`CMakeLists.txt:18`、`CMakeLists.txt:31`、`CMakeLists.txt:60`）；同时定义 install/export/pkg-config/CPack 和安装冒烟测试（`CMakeLists.txt:99`、`CMakeLists.txt:135`、`CMakeLists.txt:142`）。双语文档和站点导航使契约公开（`docs/.vitepress/config.mts:17`）。
12. **示例/测试/基准不属于架构层，因为它们验证而不是提供运行时行为。** 测试二进制承载平台回归矩阵和公共/内部装饰器检查（`tests/duct_tests.cc:1`、`tests/duct_tests.cc:4119`）；示例演示应用循环（`examples/convenience_example.cc:23`）；基准 fork echo 对端并比较传输（`bench/aello_bench.cc:150`、`bench/aello_bench.cc:164`）。分离避免示例易用性变成隐式库依赖。

## 维护说明

- 这是源码推导快照，不是所有声明选项都已实现的承诺。预留字段已在上方明确列出。
- 后续架构修改应同步更新英中文档，并在 `docs/.vitepress/config.mts` 保持互链。

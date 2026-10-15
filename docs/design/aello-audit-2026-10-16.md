# Aello 逐文件审计（2026-10-16）

> 输入：外部分析稿 `docs/design/aello-analysis-2026-10-16.md`（性质：外部分析，不是本仓事实）。
> 方法：全量遍历 `include/` `src/` `tests/` `examples/`（共 39 个文件），逐文件三分类；分析稿提到的概念逐条对代码核验。
> 口径：本轮零实现代码；本文只落定性结论。改名历史：本仓前身 duct 已改名 aello（GitHub 重定向有效），代码内 namespace/标识符仍是 `duct`，改名属后续实现轮。
> 审计基线：commit `7646428`，工作区干净。

## 0. 总量

| 分类 | 数量 | 文件 |
|---|---|---|
| 保留 | 18 | 见 §3–§6 |
| 重构 | 10 | 见 §3–§6 |
| 删除 | 11 | 见 §3–§6 |

范围：`include/` 14 + `src/` 16 + `tests/` 1 + `examples/` 8 = 39。构建与配套文件（CMakeLists、cmake 模板、workflows、TODO.md、README.md）在 §7 单独说明，不计入上表。

**构建事实（先说最硬的一条）**：HEAD 在本机 GCC 15.2.0 下编译失败——`include/duct/message.h` 使用 `std::memcpy`/`std::memcmp`（`include/duct/message.h:49`、`include/duct/message.h:57`）与 `std::min`（`include/duct/message.h:47`），但未包含 `<cstring>`/`<algorithm>`（现有 include 列表见 `include/duct/message.h:3-9`）。旧工具链靠传递包含侥幸通过，新 libstdc++ 收紧后报 `'memcpy' is not a member of 'std'`。本轮不修，仅记录。

## 1. 分析稿概念核验

分析稿第三节给出七个"建议先定义"的概念。逐一对代码核验：

| 概念 | 代码层面 | 证据 |
|---|---|---|
| `Runtime` | **查无** | 无任何同名类型（全仓 grep 无命中） |
| `Endpoint` | **查无** | 同上 |
| `MessageView` | **查无同名类型**；最近似物是 `Message::view()`，返回 `std::span` | `include/duct/message.h:37-38` |
| `MessageBuilder` | **查无**；消息构建由 `Message` 静态工厂承担（`from_bytes`/`from_string`/`with_capacity`） | `include/duct/message.h:18-25` |
| `SendResult` | **查无同名类型**；该角色由 `Result<void>` + `StatusCode` 扮演。注意：**没有"队列满"专用码**，队列满在阻塞路径表现为 `kTimeout`（`src/queue.cc:33-34`），在 fail-fast 路径借用 `kIoError`（`src/queue.cc:58`） | `include/duct/duct.h:84`；`include/duct/status.h:10-18` |
| `Dispatcher` | **查无同名类型**；最近似物是 `convenience::serve`/`echo_server`/`consume_loop` 的回调循环 | `include/duct/convenience.h:279-362` |
| `Transport` | **查无抽象类型**；该角色由 `Pipe`/`Listener` 接口 + scheme 分发承担，各传输是自由函数（`tcp_listen`/`tcp_dial`、`shm_listen`/`shm_dial`、`pipe_listen`/`pipe_dial`，另有已实现未接线的 `uds_listen`/`uds_dial`） | 接口 `include/duct/duct.h:81-99`；分发 `src/duct.cc:23-82` |

按用户口径注明：以上「查无」只说明**代码里没有**，不构成"全新需求"的定性——用户握有未落文档的设计历史，若这些概念曾在历史设计中出现，以用户口径为准。

另核验分析稿的几条事实性断言：

| 分析稿断言 | 核验结果 |
|---|---|
| 「零拷贝」（旧 README 也这么宣传） | **不成立**。shm 发送是 memcpy 进槽（`src/shm_transport.cc:400`），接收是 `Message::from_bytes` 再复制一次（`src/shm_transport.cc:424`、`src/message.cc:7-16`）；wire 路径同样两次复制（`src/wire.cc:171-175`、`src/wire.cc:186-191`）。现状即分析稿说的阶段 1「Copy Send」 |
| 「有界、预分配的共享内存通道」 | **成立**。每连接两环，每环 64 槽 × 64 KiB 固定（`src/shm_transport.cc:42-43`、`src/shm_transport.cc:146-165`），满则按超时阻塞（有测试 `tests/duct_tests.cc:176-223`） |
| 「SPSC 起步」 | **现状即 SPSC**：每环单生产者单消费者，head/tail 各一方推进（`src/shm_transport.cc:146-149`） |
| 批量收发 | **查无真批量**。`convenience::send_batch`/`recv_batch` 只是单发循环（`include/duct/convenience.h:229-263`），不减少原子操作或系统调用 |

## 2. 横切发现（比单文件结论更重要）

1. **六个源文件不在构建里。** `CMakeLists.txt:7-23` 只编译 `address/duct/message/qos_pipe/shm_transport/tcp_transport/wire`（Windows 加 `win_shm/pipe_transport`）。**未编译**：`src/uds_transport.cc`、`src/reconnect_pipe.cc`、`src/state_callback_pipe.cc`、`src/socket_utils.cc`、`src/logging.cc`、`src/queue.cc`。它们与配套头文件（`logging.h`、`queue.h`、`socket_utils.h`）一起构成"看不见的死代码"——编译器从不检查它们。
2. **自动重连是广告功能。** `DialOptions::reconnect` 与 `on_state_change` 可以设置（`include/duct/duct.h:101-108`，builder 入口 `include/duct/convenience.h:50-65`），但 `dial()` 全程不读取（`src/duct.cc:43-82` 无一处引用 `opt.reconnect`/`opt.on_state_change`）。实现文件 `reconnect_pipe.cc`、`state_callback_pipe.cc` 写完了却从未被调用（`make_reconnect_pipe`/`make_state_callback_pipe` 全仓零调用方），且不在构建里。`heartbeat_interval` 同样无人消费（仅定义于 `include/duct/duct.h:43`）。
3. **QoS 名不副实且不对称。** `QosPipe` 只在 dial 侧包裹、只管发送队列（recv 直接透传，`src/qos_pipe.cc:158-160`）；`rcv_hwm_bytes`、`linger`、`Reliability` 枚举全仓无消费者（仅定义于 `include/duct/duct.h:59,66,23-26`）；`ListenOptions::qos` 被各 listen 忽略（如 `src/shm_transport.cc:488-493` 只用 `backlog`）。默认 `DialOptions` 的 `snd_hwm_bytes=4MB` 非零，触发 `src/duct.cc:76` 的包裹条件——**默认每个 dial 都套 QosPipe 并起后台线程**，空闲时靠 1ms 轮询睡眠（`src/qos_pipe.cc:92-98`）。TODO.md M2/M3 标注的 "Implemented: on_state_change / auto-reconnect / linger" 与事实不符。
4. **uds:// 解析得了、用不了。** `Address::parse` 认识 `uds`（`src/address.cc:37`），但 `listen`/`dial` 的分发没有 uds 分支（`src/duct.cc:29-39`、`src/duct.cc:51-71`），运行时返回 `not_supported`。且 `uds_transport.cc` 依赖的 `socket_utils.cc` 也不在构建里，接上线即断链。
5. **README 与代码不符（旧 README 的清单）**：宣传"零拷贝/自动重连/连接状态回调"（皆不成立，见上）；文档给出 `-DDUCT_INSTALL=OFF`、install 目标、`find_package(duct)`、pkg-config（`CMakeLists.txt` 无任何 `install()`/导出逻辑，`cmake/*.in` 模板从未被引用生成）；列出 `duct_convenience_example`/`duct_async_example`/`duct_utils_example` 三个二进制（CMake 从不构建它们，`CMakeLists.txt:42-57`）；许可证写"待定"（实际 `LICENSE` 为 MIT，2025 cuihairu）。
6. **ODR 隐患**：`struct QueuedMessage` 在 `include/duct/queue.h:16-21` 与 `include/duct/qos_pipe.h:18-24` 同名同 namespace（`duct`）重复定义。当前 queue.h 无人包含所以未爆，一旦同 TU 引入即重定义错误。
7. **测试盲区**：`tests/duct_tests.cc` 覆盖地址解析、shm echo、shm 满环超时、wire 编解码与 macOS SIGPIPE（`tests/duct_tests.cc:44-451`），但 QoS 包装层、重连、uds、并发收发、大消息、多消息顺序均无覆盖；且测试通过把 `snd_hwm_bytes` 置 0 **绕过** QosPipe（`tests/duct_tests.cc:96-98` 等四处）——QoS 路径实际零测试。
8. **`duct_export.h` 整文件无引用**：无任何文件包含它，`DUCT_API` 零使用；其中 `#define DUCT Experimental [[deprecated(...)]]`（`include/duct/duct_export.h:30`）本身是写坏的宏。

## 3. include/duct/（14 文件）

| 文件 | 分类 | 结论 |
|---|---|---|
| `duct.h` | 保留 | 核心脊柱：`Pipe`/`Listener`（`duct.h:81-99`）+ 全部选项类型，是未来 Endpoint 契约的最近似物。遗留债务记档：`ReconnectPolicy`/`ConnectionCallback` 广告未接线（§2.2），`rcv_hwm_bytes`/`linger`/`Reliability` 无消费者（§2.3），重构轮要么接线要么先删字段 |
| `message.h` | 保留 | 消息模型核心，`shared_ptr<vector>` 背书 + 裸指针视图（`message.h:64-67`）。修构建即补 `<cstring>`/`<algorithm>`；远期按 Copy→Batch→Claim 路线演化，方向见 adoption 文档 |
| `status.h` | 保留 | `Status`/`Result`/`StatusCode` 错误模型（`status.h:62-185`），是未来 `SendResult` 语义的载体。重构轮需补"队列满"专用码（现状借用 `kTimeout`/`kIoError`，§1） |
| `address.h` | 保留 | 地址解析结果类型（`address.h:16-26`），小而正确，被分发依赖 |
| `protocol.h` | 保留 | 帧常量与 scheme 枚举（`protocol.h:11-20`）。改名注记：magic 仍是 `'DUCT'`（0x44554354），属后续实现轮 |
| `wire.h` | 保留 | 字节流帧协议：16 字节头、64 KiB 上限、跨平台句柄（`wire.h:11-23`），tcp/uds/pipe 共用 |
| `raii.h` | 保留 | `ScopedPipe`/`ScopedListener` 纯头文件 RAII 薄封装（`raii.h:17-132`），无害且实用 |
| `socket_utils.h` | 保留 | 平台 socket 辅助（poll/select/FIONBIO，`socket_utils.h:20-34`），uds 接线时必需；注记：其 .cc 当前不在构建（§2.1） |
| `convenience.h` | 重构 | builder 思路可留，内容要瘦身：`auto_reconnect()` 链式设置后静默无效（§2.2）必须在重构轮消除（接线或移除）；`request()` 是裸 send+recv 无请求关联（`convenience.h:202-212`）；`send_batch` 是伪批量（§1）；`serve`/`echo_server` 可作为 Dispatcher 雏形收编进核心 |
| `qos_pipe.h` | 重构 | "QoS"名不副实（§2.3）：仅发送侧、仅字节界、后台线程轮询。改法方向：与 `queue.h` 二存一，语义改为明确的"有界发送缓冲 + 背压策略"，`QueuedMessage` 撞名（§2.6）必须处理 |
| `async.h` | 删除 | 与定位正面冲突：`Channel` 无界增长（`async.h:242-249` push 无任何上限）、回调风格每次操作 detach 一个线程（`async.h:104-119`）、`EventLoop` 是 100ms 轮询（`async.h:154-183`，注释自认应换 epoll）。分析稿明确"第一版不要通用事件循环/异步框架" |
| `queue.h` | 删除 | `MessageQueue` 未编译、零引用（§2.1），`hwm_bytes=0` 即无限（`queue.cc:221-223`）违背有界原则，且与 qos_pipe.h 撞名（§2.6）。进程内有界队列如需，按 SPSC 环重设计，不复用这份 deque+mutex 实现 |
| `logging.h` | 删除 | 自带日志框架 + 指标体系（Counter/Gauge/Histogram/Registry 单例，`logging.h:239-412`）超出消息运行时定位；`Histogram::observe` 把所有观测值压进无界 vector（`logging.h:354-356`）直接违反有界内存原则；配套 .cc 未编译。性能数字归 aello-bench，运行时日志后续按最小 hook 重议 |
| `utils.h` | 删除 | 通用工具杂物箱：`Port`/`TcpAddr`/字符串拼接器/`StrongType`/字面量（`utils.h:21-100`、`248-310`、`347-376`），与消息运行时无关，零测试，纯 API 面膨胀。YAGNI |
| `duct_export.h` | 删除 | 零引用整文件（§2.8），`DUCT_API` 从未标注任何符号，宏本体写坏；导出宏等真做库打包时重来 |

## 4. src/（16 文件）

| 文件 | 分类 | 结论 |
|---|---|---|
| `duct.cc` | 保留 | scheme 分发中枢（`duct.cc:23-82`）。重构注记：接线 uds 分支；`duct.cc:76` 的 QoS 包裹条件（`hwm!=0 || policy!=kBlock`）语义晦涩，重构轮改为显式 opt-in |
| `address.cc` | 保留 | 解析实现与测试对齐（`tests/duct_tests.cc:44-76`） |
| `message.cc` | 保留 | 随 `message.h` 补头文件即修复 |
| `shm_transport.cc` | 保留 | **主性能路径，定位核心**：POSIX 侧固定槽双环 + 命名信号量 + UDS 引导（`shm_transport.cc:146-165`、`305-373`、`488-527`）。已知边界记档：槽参数硬编码（`shm_transport.cc:42-43`）、崩溃残留靠 owner close 时 unlink（`shm_transport.cc:430-440`）无进程异常退出恢复、信号量等待 macOS 走 1ms 轮询回退（`shm_transport.cc:119-134`）。这些是 aello-shm P0 的演进项，不是删除项 |
| `win_shm.cc` | 保留 | Windows 侧同构实现：`CreateFileMapping`/`MapViewOfFile`/命名信号量（`win_shm.cc:212-249`），与 POSIX 侧同环同参数，保持双平台对齐 |
| `tcp_transport.cc` | 保留 | 网络后端（分析稿 P2 但已存在且可用）：帧收发 + NODELAY + 端口 0 支持（`tcp_transport.cc:115-160`、`218-236`）。保留作参照与本地网联调用 |
| `pipe_transport.cc` | 保留 | Windows 命名管道后端（`pipe_transport.cc:250-303`），对应 aello-local 定位的本地兼容路径 |
| `socket_utils.cc` | 保留 | 见 `socket_utils.h`；重构轮随 uds 接线一并入构建 |
| `qos_pipe.cc` | 重构 | 随 `qos_pipe.h`（§2.3 全部条目）；另 `send_worker` 对 `kDropNew`/`kDropOld` 的处理与 `send()` 内分支重复且不一致（`qos_pipe.cc:88-99` vs `qos_pipe.cc:117-145`），重构时统一 |
| `uds_transport.cc` | 重构 | 实现完整（超时连接、路径清理，`uds_transport.cc:104-235`）但未分发未编译（§2.4）。改法方向：接线进 `duct.cc` 分发 + `socket_utils.cc` 入构建 + 补测试；接线前 README 不宣传 uds |
| `reconnect_pipe.cc` | 重构 | 实现质量尚可（指数退避+抖动、状态机、stale error 防护，`reconnect_pipe.cc:132-198`），但未编译未接线。改法方向：接线进 `dial()` 的 `opt.reconnect.enabled` 路径并删掉字段广告落差；shm 场景无意义，仅 tcp/pipe 生效 |
| `reconnect_pipe.h` | 重构 | 同上 |
| `logging.cc` | 删除 | 实现死头文件 `logging.h` 的 `ConsoleLogger`，本身不在构建（§2.1） |
| `queue.cc` | 删除 | 死代码（§2.1、§2.6） |
| `state_callback_pipe.cc` | 删除 | 从未调用未编译（§2.2）；职责与 `ReconnectPipe` 的状态回调重叠，接线重连时由后者统一承担 |
| `state_callback_pipe.h` | 删除 | 同上 |

## 5. tests/（1 文件）

| 文件 | 分类 | 结论 |
|---|---|---|
| `duct_tests.cc` | 重构 | 现有 7 个用例有价值（尤其 shm 满环超时与 wire 边界，`tests/duct_tests.cc:176-223`、`298-451`），但：手搓 EXPECT 宏无框架（`duct_tests.cc:30-42`）；QoS 靠置 0 绕过而非直测（§2.7）；无多进程/压力/崩溃用例（分析稿 aello-test 的核心内容全缺）；uds 无法测（未接线）。改法方向：按 aello-test P0 扩展——先补 QoS 直测与并发收发，再多进程与 kill -9 重启矩阵 |

## 6. examples/（8 文件）

| 文件 | 分类 | 结论 |
|---|---|---|
| `duct_echo_server.cc` | 保留 | 最小服务端样例，在构建内（`CMakeLists.txt:43-44`），nightly 打包引用它 |
| `duct_echo_client.cc` | 保留 | 同上 |
| `pipe_test.cc` | 保留 | Windows 命名管道手动联调用，在构建内 |
| `shm_test.cc` | 重构 | 手动联调有价值，在构建内；但输出文案写死 "Windows shared memory server"（`examples/shm_test.cc:28`）而它对 POSIX 同样工作，误导；随重构轮改文案并参数化 |
| `qos_test.cc` | 重构 | 手动双终端工具，在构建内；其演示的背压行为恰是 CI 零覆盖区（§2.7），重构轮应转成自动化用例而非留在 examples |
| `convenience_example.cc` | 重构 | **不在构建内**（§2.5）；演示 `auto_reconnect()` 等实际无效的功能等于教用户踩坑。改法方向：随 convenience.h 瘦身重写，只演示真实生效的 API，并入 CMake |
| `async_example.cc` | 删除 | 依赖判死的 `async.h`，且不在构建内 |
| `utils_example.cc` | 删除 | 依赖判死的 `utils.h`，且不在构建内 |

## 7. 构建与配套（不计入三分类总量）

- `CMakeLists.txt`：**重构对象**。六个源文件缺席（§2.1）；三个 example 缺席（§2.5）；无 install/export；project 名仍是 `duct`（`CMakeLists.txt:2`）。重构轮先补齐构建再谈改名。
- `cmake/ductConfig.cmake.in`、`cmake/duct.pc.in`、`cmake/duct_version.h.in`：**重构对象**。三份模板零引用——`ductTargets.cmake` 从不生成，版本头从不配置。要么接线 install 真做打包，要么删除；现状是"仓库里躺着的虚构交付面"。
- `.github/workflows/cmake-multi-platform.yml`：保留。GCC/Clang/MSVC 矩阵 + ctest，符合多平台验收需要（`cmake-multi-platform.yml:25-45`）。注记：它目前没拦住 message.h 的构建破损（ubuntu-latest 的 GCC 版本较旧），等 runner 升级即暴露。
- `.github/workflows/nightly.yml`：保留。schedule + dispatch、滚动 nightly tag、prerelease 标记（`nightly.yml:122-140`），符合"nightly 允许、正式版本只由用户手动触发"的红线。
- `TODO.md`：**重构对象**。M2/M3 的 "Implemented" 标注与代码不符（§2.3）；整体按旧定位写就。由 adoption 文档接管路线图后降级为历史参考或重写。
- `README.md`：本轮已按新定位重写（英文主文档 + `README.zh.md`），旧版失实清单见 §2.5。

## 8. 建议的处理顺序（供下一实现轮，本轮不动代码）

1. 修构建：`message.h` 补 `<cstring>`/`<algorithm>`——一行改动解锁一切。
2. 删除名单落地：§3/§4/§6 的 11 个删除项 + `cmake/` 三模板（或接线打包，二选一）。
3. 构建补齐：`uds_transport.cc`/`socket_utils.cc` 入构建并接线 uds 分支，或明确 park（删分发外的部分）。
4. 消除广告落差：`ReconnectPolicy`/`on_state_change` 接线或删字段；`rcv_hwm_bytes`/`linger`/`Reliability` 同理。
5. 测试补盲：QoS 直测、并发收发、进程异常退出矩阵。
6. 之后才谈 namespace/品牌改名与模块目标拆分（aello-core/shm/bench/test 的 CMake 化）。

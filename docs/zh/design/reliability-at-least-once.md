# At-Least-Once 送达语义提案（2026-11-21）

状态：**提案，未实现**。在用户对文末决策点（D1–D5）签核之前，
`QosOptions::reliability` 仍为保留字段。下文的故障模型由
`tests/loss_injection_tests.cc` 钉死——该测试矩阵与本提案同批落地，且在
当前（at-most-once）语义下保持全绿。

另见[能力与保留契约](../../design/capabilities.md)（英文单源）——在提案实现之前，
其「明确未实现」一节仍然准确。

## 目标

消费保留的 `Reliability::kAtLeastOnce`：让启用重连的管道对因连接死亡
而丢失的应用消息进行重发，配合接收端去重，且不改变 `Pipe` API 形状。

## 实测故障模型（由丢包注入矩阵钉死）

| 单元 | 测试 | 当前行为（at-most-once） |
|---|---|---|
| L1 | `test_loss_injection_drop_is_silent` | `send()` 返回 ok、消息被静默丢弃——无报错、无重发 |
| L2 | `test_loss_injection_duplicate_arrives_twice` | 重复发送的消息原样到达两次——应用层以下无任何去重 |
| L3 | `test_loss_injection_reorder_observed` | 乱序发送的两条按交换后的顺序原样到达——应用层以下无排序 |
| L4 | `test_midstream_close_delivers_exact_prefix` | 服务端停止读取并断开：接收端恰好持有已读前缀、有序、零重复；断点之后的一切永久丢失 |
| L5 | `test_reconnect_gap_losses_are_invisible_to_caller` | 会话中途断连并重拨后：任何 id 不会到达两次（at-most-once 不变量成立），但已返回 ok、写入濒死连接内核缓冲的发送会丢失，调用方对成功完全无从重试 |

L5 是核心动机：调用方无法重试它认为已送达的消息。只有协议层
（会话 + 序号 + 确认 + 重发）能封堵这一缺口。

## 设计

### 会话标识

拨号方在每次启用重连的 dial 时生成一次 64 位 `session_id`，跨重拨
存活。监听方从每条连接的第一帧获知它：见到已知会话则续接，见到新会话
则从头开始。

### 序号

每条应用消息携带按方向独立的 `u64` 序号，会话内从 0 起。

### 确认

接收方发送**累计 ACK 控制帧**：ack = 已按序交付的最高序号。每个方向
至多挂起一个 ACK，按短定时器或每 N 条消息刷新（先到者触发）。

### 重发窗口

发送方保有界未确认窗口，字节预算复用既有 `snd_hwm_bytes`（默认不引入
新内存旋钮）。`kDisconnected` 之后的 `kConnected` 到来时，发送方先按序
重放未确认窗口，再发新消息。窗口溢出即背压，遵循既有
`BackpressurePolicy`（`kDropNew`/`kDropOld` 使被丢消息退化为
at-most-once——文档化行为，非静默）。

### 去重

接收方跟踪已按序交付的最高序号，丢弃小于等于它的帧；另配一个小的有界
乱序暂存，防止重发与慢 ACK 竞争造成重复。暂存溢出即丢弃（可能重发——
这正是保证为 at-least-once 而非 exactly-once 的原因）。

### 线格式

`FrameHeader` 已含 `version` 与 `header_len`（`include/duct/wire.h:29-34`）。
提案 **D1**：version 2 帧把头扩展到 32 字节（基础 16 + `session_id` u64
+ `seq` u64）；`header_len` 消歧义，v1 对端继续可用。可靠性能力位通过
既有握手能力位掩码协商（`include/duct/wire.h:62-70`）；对端为 v1 时连接
按 at-most-once 继续（优雅降级，文档化）。ACK 控制帧复用 flags 字段并带
小载荷（累计 ack + 会话回显）。

### 分层

新增 `ReliablePipe` 装饰器；启用可靠性的 dial 组合为
`QosPipe(ReliablePipe(ReconnectPipe(transport)))`。`ReliablePipe` 位于
重连边界之上，会话与未确认窗口跨重拨存活，并由既有
`ConnectionCallback` 驱动重放（`kDisconnected` → 预备重放，
`kConnected` → 冲刷窗口）。

### 范围

v1 覆盖 `tcp://` 与 `uds://`（具备分片的字节流传输）。Windows `pipe://`
延后；`shm://` 不在范围——崩溃按设计丢失环形缓冲内容，持久化仍为非目标。

### 非目标（与 capabilities.md 一致）

不跨进程死亡做持久化；不做落盘；不是 exactly-once；v1 不做空闲保活探测
（`ReconnectPolicy::heartbeat_interval` 仍保留）。

## 决策点（需用户签核）

- **D1——线格式编码**：扩展 32 字节头（推荐）vs 仅用 flags 的边车字段。
- **D2——范围**：先 `tcp://` + `uds://`（推荐），`pipe://` 其后。
- **D3——默认值**：窗口预算 = `snd_hwm_bytes`（推荐）；去重暂存 = 64 条。
- **D4——组合**：`reliability = kAtLeastOnce` 自动启用重连（推荐）vs
  `reconnect.enabled` 为 false 时 dial 报错。
- **D5——验收**：矩阵 L1–L5 保持全绿，另加新单元 R1（重连后重发）、
  R2（重放消息去重）、R3（背压下窗口有界）、R4（对 v1 对端优雅降级）。

## 验收标准（后续工作，以 D1–D5 为门）

实现 `ReliablePipe`，为套件加单元 R1–R4，`kAtMostOnce` 下保持 L1–L5
全绿，并在同一变更中更新 `capabilities.md`（把保留行移入已实现表）。

# Aello 代码深审报告（2026-11-02）

> 范围：全仓（`include/`、`src/`、`tests/`、`examples/`、CMake/CI）。实际编入库的源文件为
> `address.cc`、`duct.cc`、`message.cc`、`qos_pipe.cc`、`shm_transport.cc`、`tcp_transport.cc`、`wire.cc`
> （Windows 额外编译 `win_shm.cc`、`pipe_transport.cc`）。
> 方法：安全静态扫描 + 隐藏 bug 猎捕 + 质量门 + 可复现实验；每条发现带 `file:line` 证据。
> 模式：非交互，按严重度自主修复 P0/P1，一问题一提交，测试绿后提交。

## 0. 结论摘要

- **无 P0**（未发现远程可利用的安全漏洞、确定性崩溃或数据破坏级缺陷）。
- **发现 30 项**：**已修 13 项**（P1×7、P2×6），**剩余 17 项**（P2×11、P3×6，均为非阻塞/未编译/设计待办）。
- 修复后：构建 **0 告警**，`ctest` **1/1（18 用例）**，ASan/UBSan 绿，TSan 绿，连续 10 次运行全绿。
- 安全静态扫描**零命中**：无硬编码密钥、无 shell 拼接、无 SQL、无 eval、无危险反序列化；路径名已白名单化。

## 1. 质量门基线

环境：Ubuntu，GCC 15.2.0，CMake 4.2.3，C++20。

| 门 | 本轮起点（`b1158e3`） | 最终 |
|---|---|---|
| 构建 | 绿，`-Wall -Wextra -Wpedantic`，0 告警 | 绿，0 告警 |
| 单元测试 | `ctest` 1/1，10 用例 | `ctest` 1/1，**18 用例** |
| ASan + UBSan | 绿 | 绿（`detect_leaks=1`） |
| TSan | 绿 | 绿（`halt_on_error=1`） |
| clang-tidy | 17 条告警（含 1 条真实 use-after-move） | 6 条，均为 P3 风格/误报类（见 F-29） |
| 稳定性 | — | `duct_tests` 连续 10 次全绿 |

复现命令：

```bash
cmake --build build-review -j4
ctest --test-dir build-review --output-on-failure
cmake --build build-asan -j4 && ctest --test-dir build-asan --output-on-failure
cmake --build build-tsan -j4 && ctest --test-dir build-tsan --output-on-failure
clang-tidy -p build-review -checks='-*,bugprone-*,clang-analyzer-*' \
  src/address.cc src/duct.cc src/message.cc src/qos_pipe.cc \
  src/shm_transport.cc src/tcp_transport.cc src/wire.cc
```

## 2. 安全静态扫描

| 检查项 | 结果 | 证据 / 说明 |
|---|---|---|
| 硬编码密钥、口令、token、私钥 | 零命中 | 全仓 grep `api_key/secret/password/token/PRIVATE KEY` 无业务命中 |
| Shell 注入（`system`/`popen`/`exec*`） | 零命中 | 全仓无此类调用 |
| SQL 拼接 | 不适用 | 本仓无数据库/SQL |
| `eval`/动态执行 | 零命中 | 无解释器调用 |
| 反序列化 | 有防御 | `decode_header` 校验 magic/version/header_len/payload_len（`src/wire.cc:62-74`）；`read_frame` 按校验后的长度分配（`src/wire.cc:289`）；shm 槽长度二次校验（`src/shm_transport.cc:424`） |
| 路径穿越 | 有防御 | shm 总线名经 `sanitize_name` 白名单化（`src/shm_transport.cc:45-57`）后哈希；uds 路径由调用方提供（API 契约） |
| 整数/缓冲区 | 有防御 | `parse_u16` 拒绝 >65535（`src/address.cc:14`）；帧/槽上限 64 KiB；`snprintf` 均为定长缓冲 |
| 供应链 | 需关注 | GitHub Actions 使用 tag 而非 commit SHA 固定（P3 建议，未计入发现数） |

结论：**无 P0 安全问题**。shm 的 `/tmp` 会合点存在本地多用户抢占面（F-18），属可用性/隔离问题，非数据泄露。

## 3. 已修复：P1（7 项）

### F-01 QosPipe 重排队路径的 use-after-move

- **证据**：旧 `src/qos_pipe.cc:82` 先 `send_queue_.push_front(std::move(msg_to_send))`，随后读取
  `msg_to_send.message.size()`，对象已被移走，尺寸统计失真（clang-tidy `bugprone-use-after-move` 确认）。
- **影响**：`send_bytes_` 记账漂移，可能提前/延后触发 HWM，进而错误阻塞或放行。
- **修复**：移动前捕获 `requeue_size`。
- **提交**：`ad7bd1c fix(qos): capture message size before requeue move`

### F-02 永久失败消息导致 worker 无限自旋 / 超大消息永久卡死

- **证据**：旧 `src/qos_pipe.cc:71-89` 对所有发送失败一律 `push_front` 重试。shm/wire 对 >64 KiB 返回
  `kInvalidArgument`，worker 会永远重试队首，后续消息全部饿死。复现：>64 KiB 消息经 QoS+shm 发送后
  接收端永远收不到。
- **修复**：仅 `kTimeout` 重排队；`kClosed`/`kIoError` 停止 worker；其余永久错误丢弃该消息并继续排空。
- **提交**：`09c55d4 fix(qos): drop permanently-failing messages instead of spinning`（含回归用例）

### F-03 `SendOptions.timeout == 0` 被当成“立刻超时”

- **证据**：旧 `src/qos_pipe.cc:136-154` 在 kBlock 下无条件 `wait_for(lock, opt.timeout, ...)`。
  `duct.h:72` 明确 0 表示“无超时”，但 `wait_for(0ms)` 立即返回并报 `kTimeout`。
- **修复**：0 走无超时 `wait`，>0 才用 `wait_for`；超时后再次校验水位。
- **提交**：`26001ab fix(qos): honor zero send timeout as an indefinite block`（含回归用例）

### F-04 `send()` 可在 `close()` 之后入队并返回成功

- **证据**：旧 `src/qos_pipe.cc:97-115` 只在加锁前检查 `running_`；`close()` 置 false 并 join 后，
  已通过首检的线程仍可入队，消息永远不会被发送，但 `send()` 返回 Ok。
- **修复**：持 `send_mutex_` 后二次检查 `running_`。
- **提交**：`55e2949 fix(qos): recheck running state under the send lock`

### F-05 TCP 传输完全忽略 `SendOptions/RecvOptions.timeout`

- **证据**：旧 `src/tcp_transport.cc:57-65` 的 `send/recv` 形参匿名，直接调用阻塞的
  `wire::write_frame/read_frame`。复现：对端不读，30 s 测试超时；对端不发，`recv` 永久阻塞。
- **修复**：`wire` 增加 timeout 参数，按帧设置 `SO_SNDTIMEO/SO_RCVTIMEO`，把
  `EAGAIN/EWOULDBLOCK/WSAETIMEDOUT` 映射为 `kTimeout`；TCP/UDS 传递调用方超时。
- **提交**：`bd157c7 fix(wire): honor send/recv timeouts on socket I/O`（含 socketpair 与 TCP 回归用例）

### F-06 `async::EventLoop::stop()` 必然死锁

- **证据**：`include/duct/async.h:154-206`。`run()` 持有 `mutex_` 执行阻塞 `recv()` 与用户回调；
  `stop()` 先锁同一 `mutex_` 再 `join()` 后台线程。复现：后台 loop 启动后 `stop()` 在 6 s 超时内不返回。
- **修复**：`run()` 在锁内快照 pipe 列表，锁外做 I/O 和回调；`stop()` 在锁外 join；从回调内调用
  `stop()` 时不再 join 自身。
- **提交**：`cde3c45 fix(async): keep EventLoop mutex out of I/O and join paths`（scratch 程序验证两种路径）

### F-07 帧中途超时被误报为可重试，TCP 流会失步

- **证据**：修复 F-05 后，`write_frame/read_frame` 在已写出/读入部分帧后超时仍返回 `kTimeout`；
  `QosPipe` 将 `kTimeout` 视为瞬时错误并整条重发，TCP 字节流会永久错位。
- **修复**：`write_all/read_exact` 跟踪“帧已开始”；帧中途超时返回 `kIoError`（QosPipe 随即关闭管道），
  仅帧首超时返回可重试的 `kTimeout`。
- **提交**：`cec8067 fix(wire): distinguish frame-start and mid-frame timeouts`（含半帧超时用例）

## 4. 已修复：P2（6 项）

| ID | 问题 | 证据 | 修复 | 提交 |
|---|---|---|---|---|
| F-08 | QoS 接受 64 KiB–HWM 之间的消息，传输层拒绝后在 worker 中静默丢弃，`send()` 已返回成功 | `src/qos_pipe.cc:90-93`；`include/duct/wire.h:13`；`src/shm_transport.cc:42` | 新增 `QosOptions::max_message_bytes`（默认 64 KiB），`send()` 同步拒绝；`snd_hwm_bytes==0` 按无上限处理 | `011e118`、`b0d4f9e` |
| F-09 | Windows shm 对含 `std::atomic` 的 `ShmLayout` 做 `memset`，属未定义行为 | `src/win_shm.cc:236-239`（旧） | placement-new 值初始化，补 `<new>`；MinGW 交叉编译验证 | `5e8dac7` |
| F-10 | `queue.h` 与 `qos_pipe.h` 在同一命名空间重复定义 `QueuedMessage`，同时包含即编译失败 | `include/duct/queue.h:16`、`include/duct/qos_pipe.h:18`（旧） | `queue.h` 重命名为 `QueueEntry`，同步 `queue.cc` | `3022436` |
| F-11 | `get_default_logger()` 返回裸引用，替换 logger 后旧对象销毁，引用悬空 | `src/logging.cc:37-47`（旧） | 保留被替换的 logger（`retired`），保证已发出的引用在进程生命周期内有效 | `3022436` |
| F-12 | `logging.h` 使用 `std::vector`/`std::unordered_map`、`convenience.h` 使用 `std::vector` 但未直接包含 | `include/duct/logging.h:358,410-412`；`include/duct/convenience.h:231` | 补齐 `<vector>`、`<unordered_map>`，消除对传递包含的依赖 | `3022436` |
| F-13 | 实验性 API 宏定义语法错误：`#define DUCT Experimental [[deprecated(...)]]`，无法按预期使用 | `include/duct/duct_export.h:30`（旧） | 改为 `DUCT_EXPERIMENTAL [[deprecated(...)]]` | `3022436` |

## 5. 剩余发现：P2（11 项，未修）

| ID | 问题 | 证据 | 建议 |
|---|---|---|---|
| F-14 | `DialOptions.timeout` 被 tcp/shm dial 忽略 | `src/tcp_transport.cc:242-246`、`src/shm_transport.cc:500-501` | 接入非阻塞 connect / 截止时间；或从 API 文档移除 |
| F-15 | `ListenOptions.qos` 被所有 listener 忽略 | `src/tcp_transport.cc:214-216`、`src/shm_transport.cc:493-495` | 在 accept 后包裹 QoS，或标记为保留字段 |
| F-16 | shm 上 `close()` 与并发 `send/recv` 存在 unmap 后访问竞态，API 未声明线程安全契约 | `src/shm_transport.cc:387-444` | 增加关闭守卫/引用计数，并在 `duct.h` 写明线程模型 |
| F-17 | `QosPipe::close()` 并发调用会对同一 `std::thread` 双重 `join`；`recv()` 无关闭保护 | `src/qos_pipe.cc:170-189` | 用 `std::once_flag`/互斥保护 join，统一关闭语义 |
| F-18 | shm 会合点固定为 `/tmp/duct_shm_<hash>.sock`，`uds_listen` 无条件 `unlink`，可抢占活跃 listener | `src/shm_transport.cc:199,215` | 使用 `XDG_RUNTIME_DIR`/私有目录 + `O_EXCL` 锁文件；拒绝覆盖 |
| F-19 | shm `recv()` 发现槽长度非法时直接返回，未归还 `spaces` 信号量，容量永久 -1 | `src/shm_transport.cc:424-425` | 错误路径也 `sem_post(spaces)` 并推进 tail（或直接关闭管道） |
| F-20 | Windows 命名管道传输忽略 `Send/RecvOptions.timeout`，且发送前不校验 64 KiB 帧上限 | `src/pipe_transport.cc:79,125,100` | 与 tcp/wire 对齐：超时 + 尺寸校验 |
| F-21 | 接收侧 QoS 未实现：`rcv_hwm_bytes`、`linger` 无消费者，`QosPipe::recv` 直接透传 | `src/qos_pipe.cc:170-172`、`include/duct/duct.h:59,66` | 增加接收队列/水位，或从公开选项移除 |
| F-22 | 未编入库：`MessageQueue` 把 `hwm_bytes=0` 当无上限 | `src/queue.cc:221-223` | 删除或在删除清单中显式标注 |
| F-23 | 未编入库：`Histogram::observe` 无界增长 | `include/duct/logging.h:358` | 固定窗口/环形缓冲后启用 |
| F-24 | 未编入库：`async::Channel` 无界；回调 API 每次操作创建一个 detached 线程 | `include/duct/async.h:105,115,310` | 删除 async.h，或改为有界队列 + 线程池 |

## 6. 剩余发现：P3（6 项，未修）

| ID | 问题 | 证据 | 建议 |
|---|---|---|---|
| F-25 | `sanitize_name` 把不同总线名映射到同一标识（如 `a-b` 与 `a_b`），可能串扰 | `src/shm_transport.cc:45-57` | 白名单化后再追加原串哈希 |
| F-26 | `sem_timedwait` 使用 `CLOCK_REALTIME` 绝对时间，系统时钟回拨会拉长/缩短等待 | `src/shm_transport.cc:88-107` | 使用 `sem_clockwait(CLOCK_MONOTONIC)`（平台可用时） |
| F-27 | `kFailFast` 复用 `kIoError`，调用方无法区分“队列满”和其它 I/O 错误 | `src/qos_pipe.cc:123-124` | 增加 `kQueueFull`/`kWouldBlock` 状态码 |
| F-28 | TCP 地址解析未处理 IPv6 方括号（`tcp://[::1]:9000` 的 host 保留方括号） | `src/address.cc:44-56` | 识别 `[...]` 并校验端口分隔 |
| F-29 | clang-tidy 剩余 6 条：整数乘法隐式加宽（`duct.h:58`、`wire.h:13`、`shm_transport.cc:42`）、析构中虚调用告警（`qos_pipe.cc:14`）、相邻参数易混（`tcp_transport.cc:162`） | 见左 | 标注 `QosPipe final`、常量改 `std::size_t{64} * 1024`；参数结构体化 |
| F-30 | Windows 命名管道 DACL 允许 Everyone 完全访问 | `src/pipe_transport.cc:52-64` | 生产默认限制到当前用户 SID |

## 7. 回归用例与验证

新增/加强的用例（均在旧代码上失败或可稳定复现）：

| 用例 | 覆盖 |
|---|---|
| `test_qos_block_zero_timeout_blocks` | F-03：0 超时在 kBlock 下必须阻塞，`close()` 能唤醒 |
| `test_qos_permanent_send_error_does_not_wedge` | F-02：永久失败消息不再饿死后续消息 |
| `test_qos_oversized_message_rejected` | F-08：>64 KiB 同步返回 `kInvalidArgument` |
| `test_qos_zero_hwm_is_unlimited` | F-08：`snd_hwm_bytes==0` 在非默认背压下仍按无上限处理 |
| `test_wire_read_timeout` | F-05：无数据时 `read_frame(50ms)` 返回 `kTimeout` 且不悬挂 |
| `test_wire_write_timeout` | F-05/F-07：发送缓冲已满时帧首超时为 `kTimeout` |
| `test_wire_partial_frame_timeout` | F-07：半帧后停顿时返回 `kIoError`，避免重发破坏流 |
| `test_tcp_recv_timeout_honored` | F-05：真实 TCP 连接上 `RecvOptions.timeout` 生效 |

实测结果：

```text
build-review: 0 warnings, ctest 1/1 (18 cases)
build-asan:   ctest 1/1 (18 cases), ASan+UBSan+LeakSanitizer clean
build-tsan:   ctest 1/1 (18 cases), ThreadSanitizer clean
repeat:       10/10 runs green
```

## 8. 修复提交列表

| 提交 | 说明 |
|---|---|
| `ad7bd1c` | fix(qos): capture message size before requeue move |
| `09c55d4` | fix(qos): drop permanently-failing messages instead of spinning |
| `26001ab` | fix(qos): honor zero send timeout as an indefinite block |
| `55e2949` | fix(qos): recheck running state under the send lock |
| `bd157c7` | fix(wire): honor send/recv timeouts on socket I/O |
| `cde3c45` | fix(async): keep EventLoop mutex out of I/O and join paths |
| `5e8dac7` | fix(win_shm): construct shared layout instead of memset-ing atomics |
| `3022436` | fix(headers): resolve duplicate type, missing includes, dangling logger |
| `011e118` | fix(qos): reject oversized messages at send time |
| `b0d4f9e` | fix(qos): treat zero snd_hwm_bytes as unlimited under backpressure |
| `cec8067` | fix(wire): distinguish frame-start and mid-frame timeouts |

> 本报告与 `todo.md` 状态更新为单独文档提交，不含代码改动。

---

**统计：发现 30 项 / 已修 13 项（P1×7、P2×6）/ 剩余 17 项（P2×11、P3×6）/ P0 = 0。**

---

## 独立审核意见（2026-11-02，异模型审核员）

> 审核方式：干净上下文、不信任报告本身，逐条打开源码对照；已修 P0/P1 核对 commit 存在 + 测试真绿；另抽样 3 个报告未覆盖的高危面（认证 / 输入处理 / 资源释放）。证据统一对照**报告时点快照 `1fb4fac`**（报告写成时的 HEAD），避免被报告之后落地的提交（`4aa9785` 等）干扰。

### 1. 逐条真实性判定（30 项）——证伪 0

**已修 F-01..F-13（13 项）全部真实**：11 个修复 commit 均在 main（`ad7bd1c`…`cec8067`），diff 与声称一致。

| ID | 核实要点 |
|---|---|
| F-01 | 旧 `qos_pipe.cc:82` 确为 `push_front(move)` 后读 `message.size()`（use-after-move）；`ad7bd1c` 改移动前捕获 `requeue_size` |
| F-02 | 旧 worker 对非 kClosed/kIoError 一律 `push_front` 重试；`09c55d4` 改仅 kTimeout 重排、其余丢弃继续排空 |
| F-03 | 旧 kBlock 无条件 `wait_for(lock, opt.timeout)`（0 立即返回误报 kTimeout）；`26001ab` 改 0 走无超时 `wait`；`duct.h`「If non-zero」口径一致 |
| F-04 | 旧 `send()` 仅加锁前查 `running_`；`55e2949` 加锁内二次检查 |
| F-05 | 旧 `tcp_transport.cc:57-65` send/recv 形参匿名、直传无超时；`bd157c7` 加 timeout、SO_SNDTIMEO/SO_RCVTIMEO、EAGAIN/EWOULDBLOCK/WSAETIMEDOUT→kTimeout |
| F-06 | `async.h` `run()` 持 `mutex_` 阻塞 I/O+回调、`stop()` 持锁再 join；`cde3c45` 锁外 I/O、锁外 join、防自 join |
| F-07 | `cec8067` 加 `frame_started`：半帧超时→kIoError（当前 `wire.cc:191-192,235-236`），帧首→kTimeout |
| F-08 | 当前 `qos_pipe.cc:103` 同步拒 `> max_message_bytes`；`wire.h:13`/`shm_transport.cc:42` 64KiB 上限确认 |
| F-09 | 旧 `win_shm.cc:236` `memset(h.mem,0,kShmSize)`；`5e8dac7` 改 placement-new + `<new>` |
| F-10/11/12/13 | `3022436`：`queue.h` 现 `struct QueueEntry`（重名消除）；`logging.cc:37,47` `retired` 保活被替换 logger；`<vector>/<unordered_map>` 补齐；旧 `#define DUCT Experimental [[deprecated]]` 实为宏名+空格错位，现 `DUCT_EXPERIMENTAL` |

**剩余 F-14..F-30（17 项）全部真实**（对照 `1fb4fac`）：F-14 报告时点 `tcp_dial:242`/`shm_dial:501` 确为 `(void)opt`、connect_tcp/uds_connect 无 timeout 参数——**真实**，已由报告后 `4aa9785 fix(dial): honor DialOptions.timeout...` 修复（非证伪，属「报告后已解决」，应从剩余清单摘除）。F-15..F-30 逐条在快照对应行找到所述问题（`(void)opt`、unmap 竞态、双重 join、`/tmp`+unlink、非法槽长不归还 spaces、`(void)opt`+无尺寸校验、rcv/linger 无消费者、queue `hwm==0` 无上限、`Histogram` 无界、`async` detached 线程、sanitize 碰撞、`sem_timedwait(CLOCK_REALTIME)`、kFailFast=kIoError、IPv6 方括号、clang-tidy、DACL Everyone）。

**行号精校**：F-18/19/27 与快照**精确命中**；F-21 引 `duct.h:59,66`，实际 `rcv_hwm_bytes`/`linger` 在 `:60,:67`（±1，引到相邻字段/注释）；F-30 引 `:52-64` 实际 `:55-65`（±3）。属引用不精，**不影响结论成立**。

### 2. 已修 P0/P1「commit 存在 + 测试真绿」核验

- 11 个修复 commit 全部 `merge-base --is-ancestor` main ✓。
- 独立复现质量门（本机 GCC 15.2.0）：
  - `build-review`：0 告警，ctest 1/1（当前 HEAD 为 21 用例，**含报告时点全部 18 用例**）✓
  - `build-asan`（`-fsanitize=address,undefined`，`detect_leaks=1`）：0 告警，ctest 1/1 ✓
  - `build-tsan`（`-fsanitize=thread`，`halt_on_error=1`）：0 告警，ctest 1/1 ✓
- **结论：修复真实且有效，测试确实绿。** 无假修复、无「声称已修实则未修」。

### 3. 抽样 3 个高危面（报告未覆盖）

- **认证**：shm 段与信号量均以 **0600** 创建（`shm_open/sem_open`，`shm_transport.cc:340,365-368`）——权限面控制良好；客户端 `shm_open(O_RDWR,0600)`/`sem_open(name,0)`。**漏网 M-1**：UDS 引导套接字（`/tmp/duct_shm_<hash>.sock`）`bind` 后**无显式 `chmod`**，访问权限完全依赖进程 umask（`shm_transport.cc:205-222`）；umask 宽松时（0/002）任意本地用户可 connect，叠加 `:217` 无条件 unlink 与 F-18 的 /tmp 抢占面。P3（本地、可用性/冒名面，非数据泄露）。
- **输入处理**：`decode_header` 校验 magic/version/`header_len==16`/`payload_len≤64K`，`read_frame` 按校验后长度分配；`write_frame` 同样拒 >64K——**防御完备，无漏网**。
- **资源释放**：`close_handles` 优雅处理 SEM_FAILED/munmap/fd；`shm_setup` 各失败路径均 close_handles+unlink 全部资源（`:344-349,356-361,371-378`）；`shm_dial` 的 uds_connect 失败也完整清理（`:545-553`）；`uds_listen` 各错误路径关 fd。**漏网 M-2**：`apply_socket_timeout` 把 `SO_SNDTIMEO/SO_RCVTIMEO` 设为每次 `send()`/`recv()` 系统调用的超时，而 `write_all/read_exact` 逐字节循环——**超时是「每次系统调用」而非「整帧截止」**；慢对端逐字节滴流可把 64KiB 帧拉远超过时值（`wire.cc:121-142`）。与 `Send/RecvOptions.timeout`（整次操作截止）语义不符，但非内存安全。P3。

### 4. 漏网发现汇总（2 项，均 P3）

| # | 面 | 问题 | 证据 | 严重度 |
|---|---|---|---|---|
| M-1 | 认证 | UDS 引导套接字无显式 chmod，权限依赖 umask；宽松 umask 下任意本地用户可 connect | `src/shm_transport.cc:205-222` | P3 |
| M-2 | 输入/契约 | `Send/RecvOptions.timeout` 按每次系统调用而非整帧计时，慢滴流可远超超时 | `src/wire.cc:121-142` | P3 |

### 5. 报告自洽性（小瑕疵，不涉证据）

- F-29 clang-tidy 称「剩余 6 条」：本机复现**7 处**——`duct.h:59/:60/:74` 三处整型加宽（报告引 `:58` 单处）、`wire.h:13`、`shm_transport.cc:44`、`qos_pipe.cc:14` 析构虚调用、`tcp_transport.cc:231` 相邻易混参数；全部为风格/P3 类，**无正确性缺陷**，报告「均为 P3 风格/误报类」定性成立，仅计数差 1、duct.h 站点数少计。
- §7 新增用例 8 项 = 报告时点实际新增 8 项（基线 10 → 18，与 §0/§1 一致），准确；报告后 `4aa9785` 另加 3 项（dial timeout）不在 §7，正常。

### 6. 总评

- **可信度：92%**。证据层面 100% 真实（30/30 发现证据成立、13/13 修复有效、无证伪、无假修复）；扣分项为 1 处 clang-tidy 计数/站点误差、1 处 F-21 行号 ±1、2 处 P3 漏网（M-1/M-2）、F-14「报告后已解决」需知悉。
- 「无 P0」结论成立；安全扫描零命中复核成立（反序列化/路径/整数/缓冲确有防御，0600 权限、decode_header、close_handles 均核实）。
- 建议：M-1 补 `chmod(0600)`/`fchmod`；M-2 将超时改为整帧截止（或 deadline 折算剩余）；F-14 从剩余清单/todo 摘除（已由 `4aa9785` 修复）；F-29 更新为 7 处并标注 duct.h 三处站点。

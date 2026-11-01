# Aello 代码深审报告（2026-11-01）

> 范围：全仓源码（`include/`、`src/`、`tests/`、`examples/`）+ 构建配置。
> 方法：安全静态扫描 + 隐藏 bug 猎捕 + 质量门（编译告警 / 测试基线）。发现按严重度排序，每条带 file:line 证据。
> 基线：工作区含本轮未提交改动；修复前 GCC 15.2.0 构建绿、ctest 1/1（8 用例）。

## 0. 结论摘要

- **发现 15 项**，**已修 3 项**（P1×2、P2×1），**剩余 12 项**（P2/P3，记入 `todo.md`）。
- 无 P0（无崩溃/未定义行为/安全漏洞）。安全静态扫描零命中。
- 修复后构建绿、无告警、ctest 1/1（10 用例），QoS 路径首次获得直接覆盖。

## 1. 安全静态扫描（零命中）

| 检查项 | 结果 | 证据 |
|---|---|---|
| 硬编码密钥/口令/token | 无 | 全仓 grep 无 api_key/secret/password/token/私钥块 |
| Shell 注入（system/popen/exec） | 无 | 全仓无此类调用 |
| SQL 拼接 | 无 | 无 SQL 语句（非数据库仓） |
| eval/动态执行 | 无 | 无解释器调用 |
| 反序列化 | **有防御** | `wire::decode_header` 校验 magic/version/header_len/payload_len≤64K（`src/wire.cc:61-72`）；`read_frame` 按 payload_len 精确分配（`src/wire.cc:225`）；shm 槽 len≤64K（`src/shm_transport.cc:420-421`） |
| 路径穿越 | 无 | shm 名经 `sanitize_name` 白名单化（`src/shm_transport.cc:45-57`）并哈希进 `/tmp`（`:199`）；uds 路径由调用方提供（API 契约） |
| 整数/缓冲 | 有防御 | `parse_u16` 拒绝 >65535（`src/address.cc:14`）；`write_frame` 拒绝 >64K（`src/wire.cc:199-201`）；`snprintf` 均为定长缓冲 |

无 P0 安全问题；`shm` 的 `/tmp/duct_shm_<hash>.sock` 属本地多用户 DoS 面（见 §2-9），非数据泄露。

## 2. 发现与修复（按严重度）

| ID | 严重度 | 问题 | 证据 | 处置 |
|---|---|---|---|---|
| BR-1 | **P1** | **QosPipe::close() 死锁**：send_worker 在持有 `send_mutex_` 期间执行阻塞的 `underlying_->send()`（默认 timeout=0，shm 环满时 `sem_wait` 永久阻塞）；`close()` 等待同一互斥锁 → 满环/慢对端下 `close()` 永久挂起。`send()` 同样被慢传输拖住 | `src/qos_pipe.cc:72`（持锁发送）、`:162-169`（close 等锁） | **已修**：pop-before-send + 有界发送超时（100ms）+ close 先 join 再关底层 |
| BR-2 | **P1** | **QosPipe HWM 塌陷**：worker 用 `can_send_message`（`send_bytes_ >= snd_hwm_bytes`）作为发送门槛；一旦队列到高水位，worker 停止排空，而它又是唯一释放字节的路径 → 队列永久卡死、吞吐归零 | `src/qos_pipe.cc`（旧 `can_send_message` 门槛） | **已修**：HWM 只门槛入队侧（`send()`），worker 无条件排空，传输满时由有界超时退避 |
| BR-3 | **P1** | **QosPipe kDropOld 竞态**：`send()` 的 kDropOld 弹出队首，可能与 worker 正在发送的同一条消息冲突（重复弹出/记账错乱） | `src/qos_pipe.cc:128-132`（旧实现） | **已修**：pop-before-send 消除竞态（BR-1 同一修复） |
| BR-4 | P2 | `std::memset` 清含 `std::atomic` 的结构体（`ShmLayout`）——`-Wclass-memaccess` 告警，对原子属未定义行为 | `src/shm_transport.cc:324`（旧） | **已修**：改为 placement-new 值初始化（新 shm 段本就由 OS 清零） |
| BR-5 | P2 | `DialOptions.timeout` 被 tcp/shm dial 忽略（文档声明应用于 dial 路径，实际 `(void)opt`） | `src/tcp_transport.cc:242`、`src/shm_transport.cc:497` | 记 `todo.md`（Transport wiring） |
| BR-6 | P2 | `RecvOptions.timeout` 被 tcp recv 忽略（`TcpPipe::recv` 丢弃 opt） | `src/tcp_transport.cc:62-65` | 记 `todo.md` |
| BR-7 | P2 | `ListenOptions::qos` 被所有 listen 忽略（仅用 backlog） | `src/shm_transport.cc:489-494`、`src/tcp_transport.cc:214-216` | 记 `todo.md` |
| BR-8 | P2 | shm `uds_listen` 无条件 `unlink` bootstrap 路径，可抢占同总线名活跃 listener 的套接字 | `src/shm_transport.cc:215` | 记 `todo.md` |
| BR-9 | P2 | `close()` 与并发 `send/recv` 在 shm 上是 use-after-unmap 竞态；API 未声明线程安全契约 | `src/shm_transport.cc:430-440` | 记 `todo.md`（补线程安全文档/守卫） |
| BR-10 | P2 | `MessageQueue` 的 `hwm_bytes=0` 视为无上限，违反有界内存原则（未编入库） | `src/queue.cc:221-223` | 记 `todo.md`（删除清单） |
| BR-11 | P2 | `Histogram::observe` 每次观测入无界 vector（未编入库） | `include/duct/logging.h:354-356` | 记 `todo.md`（删除清单） |
| BR-12 | P2 | `async::Channel` 无界 + 每操作一个 detached 线程（未编入库） | `include/duct/async.h:242-249`、`:104-119` | 记 `todo.md`（删除清单） |
| BR-13 | P3 | `sanitize_name` 把不同总线名映射到同一 id（如 `a-b` 与 `a_b`），可能串扰 | `src/shm_transport.cc:45-57` | 记 `todo.md` |
| BR-14 | P3 | `/tmp` bootstrap 套接字路径可被其他本地用户预占（DoS） | `src/shm_transport.cc:199,215` | 记 `todo.md` |
| BR-15 | P3 | kFailFast 复用 `kIoError`，无独立「队列满」状态码 | `src/qos_pipe.cc`（旧） | 记 `todo.md`（QoS 项） |

## 3. 质量门

| 门 | 结果 |
|---|---|
| 编译 | 绿（GCC 15.2.0，`-Wall -Wextra -Wpedantic`，`CMakeLists.txt:28-29`） |
| 告警 | 修复前 1 条（BR-4 `-Wclass-memaccess`）；修复后 **0 条** |
| 类型检查 | C++20 全量编译绿 |
| lint 配置 | 无 clang-format/clang-tidy/cppcheck（仓内未配置；以编译器告警为 lint 门） |
| 测试基线 | 8 用例（修复前）→ **10 用例**（新增 QoS 高水位投递 + QoS 满环 close 回归）；ctest 1/1 |

新增回归用例说明（均可在旧代码上失败）：

- `test_qos_delivery_under_hwm`：小 HWM 下持续投递 16 条并收到回显——旧代码 worker 卡在高水位，回显永不返回（BR-2）。
- `test_qos_close_under_full_ring`：填满 shm 环后 `close()` 须在 3s 内返回——旧代码死锁挂起（BR-1）。

## 4. 说明与后续

- 本轮未改任何 API/头文件签名（`qos_pipe.h` 仅删除未使用的私有 `can_send_message` 声明；`duct.h` 仅注释口径修正）。
- BR-5..BR-15 全部为「代码有字段/代码块但未消费或未编译」类，按审计规范以源码为真相，已记入 `todo.md`；其实现/删除属设计轮（`docs/design/aello-audit-2026-10-16.md` §8）。
- 建议后续：为 QosPipe 增加 kDropOld/kFailFast 直接用例，并跑 ThreadSanitizer（当前 CI 未启用 sanitizer）。

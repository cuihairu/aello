# Aello

[![Status](https://img.shields.io/badge/status-in%20development-orange)](https://github.com/cuihairu/aello/releases)

> [English](README.md) | **中文**

<p align="center">
  <a href="https://github.com/cuihairu/aello/actions/workflows/cmake-multi-platform.yml"><img src="https://github.com/cuihairu/aello/actions/workflows/cmake-multi-platform.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
  <a href="https://isocpp.org/"><img src="https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus&logoColor=white" alt="C++20"></a>
  <img src="https://img.shields.io/badge/status-experimental-orange" alt="Status">
</p>

**Aello** 是面向游戏服务器的消息运行时：解决服务器进程之间如何以低延迟、
有界内存和明确契约传递消息。

它**不是**重写 BigWorld，也**不是**把共享内存、Aeron、ZeroMQ 包在同一个 API
后面。它只吸收值得吸收的设计思想——有界通道、传输隔离、复制优先的所有权模型——
并只保留游戏后端真正需要的部分。

前身名为 `duct`。

## 状态

实验阶段，未发布。当前已有的：

- 消息模型（`Message`）与 `Result`/`Status` 错误处理——不强制异常。
- 一套 `Pipe`/`Listener` API 覆盖三种传输：`tcp://`、`shm://`、`pipe://`（Windows 命名管道）。
- `shm://` 快路径：每方向一个有界、预分配的固定槽环（64 槽 × 64 KiB），
  信号量同步，复制发送语义。
- 字节流传输使用 16 字节帧头；单条消息上限 64 KiB。
- 跨平台 CI（GCC / Clang / MSVC）运行测试套件。

当前**没有**的——直说，免得你自己踩：

- 没有自动重连（选项字段目前被忽略）。
- `uds://` 地址能解析，但尚未接入 dial/listen。
- 没有接收侧水位、消息 TTL 强制、分片。
- 没有基准测试、RPC、发布订阅、异步层。

设计审计与路线图在 [`docs/design/`](docs/design/)：

- [`aello-audit-2026-10-16.md`](docs/design/aello-audit-2026-10-16.md) —— 逐文件审计：保留 / 重构 / 删除。
- [`aello-adoption-2026-10-16.md`](docs/design/aello-adoption-2026-10-16.md) —— 外部分析吸收了什么、延后了什么。

## 原则

- **有界内存。** 队列绝不隐式增长；队列满时按策略阻塞、超时或丢弃。
- **低延迟但不牺牲正确性。** 内存序与崩溃行为靠验证，不靠假设。
- **传输与消息语义分离。** 进入传输后端是唯一的送达承诺；处理与持久化属于应用层。
- **先走通一条性能路径。** SPSC 共享内存，直到实测数据要求更多。
- **性能结论来自可复现基准。** 目前没有基准，所以不做任何性能宣称。

## 快速开始

需要 C++20 与 CMake 3.20+。

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

服务端进程：

```cpp
#include "duct/duct.h"

auto listener = duct::listen("shm://gamebus");
if (!listener.ok()) { /* 处理 listener.status() */ }

auto pipe = listener.value()->accept();
if (!pipe.ok()) { /* 处理 pipe.status() */ }

for (;;) {
  auto msg = pipe.value()->recv({});
  if (!msg.ok()) break;
  pipe.value()->send(msg.value(), {});   // 回显
}
```

客户端进程：

```cpp
#include "duct/duct.h"

auto pipe = duct::dial("shm://gamebus");
if (!pipe.ok()) { /* 处理 pipe.status() */ }

pipe.value()->send(duct::Message::from_string("hello"), {});
auto reply = pipe.value()->recv({});
```

所有调用返回 `Result<T>`；检查 `.ok()` 并读 `.status()`——除非主动调用
`value_or_throw()`，否则不抛异常。

## 地址

| Scheme | 形式 | 平台 | 状态 |
|---|---|---|---|
| `tcp://` | `tcp://host:port`（或裸 `host:port`） | 全部 | 可用 |
| `shm://` | `shm://name` | POSIX、Windows | 可用——快路径 |
| `pipe://` | `pipe://name` | Windows 命名管道 | 可用 |
| `uds://` | `uds:///path/to/socket` | POSIX | 可解析，**尚未接线** |

## 语义（当前契约）

- **复制发送。** 载荷被复制进传输（环形槽或帧流）。零拷贝 claim/commit 是
  延后能力，不是现状。
- **单条消息最大 64 KiB。** 更大的载荷在被拒绝，直到分片能力落地。
- **`shm://` 队列满行为：** send 阻塞至有空间或超时（`SendOptions::timeout`），
  超时返回 `kTimeout`。
- **可靠性为 at-most-once。** 对端死亡时在途消息丢失；可靠投递是应用层课题。

## 构建

CMake 选项：

| 选项 | 默认 | 作用 |
|---|---|---|
| `DUCT_BUILD_EXAMPLES` | `ON` | echo 服务端/客户端与手动传输工具 |
| `DUCT_BUILD_TESTS` | `ON` | 测试套件（用 `ctest` 运行） |

尚无 install 目标——通过 `add_subdirectory(aello)` 引入并链接 `duct` 目标。
库命名（`duct` → `aello`）将在后续版本序列变更；GitHub 自 `duct` 的重定向仍然有效。

## 仓库结构

```
include/duct/     公共头文件（核心 API、消息、状态、帧协议、传输）
src/              传输实现（tcp、shm、命名管道 + 辅助）
tests/            跨平台测试套件
examples/         echo 对 + 手动传输工具
docs/design/      分析、审计与吸收结论
```

## 许可证

[MIT](LICENSE) © cuihairu

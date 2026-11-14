# Aello

<p align="center"><a href="https://github.com/cuihairu/aello/releases"><img src="https://img.shields.io/badge/status-in%20development-orange" alt="status"/></a> <a href="https://github.com/cuihairu/aello/actions/workflows/cmake-multi-platform.yml"><img src="https://github.com/cuihairu/aello/actions/workflows/cmake-multi-platform.yml/badge.svg" alt="CI"></a> <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a> <a href="https://isocpp.org/"><img src="https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus&logoColor=white" alt="C++20"></a> <a href="https://cuihairu.github.io/aello/"><img src="https://img.shields.io/badge/docs-blue" alt="docs"></a></p>

> **English** | [中文](README.zh.md)

**Aello** is a messaging runtime for game servers: how server processes exchange
messages with low latency, bounded memory, and explicit contracts.

It is **not** a rewrite of BigWorld, and **not** a wrapper that hides shared
memory, Aeron, or ZeroMQ behind one API. It borrows the ideas worth borrowing —
bounded channels, transport isolation, copy-first ownership — and keeps only
what a game backend actually needs.

Formerly known as `duct`.

## Status

Experimental, pre-release. One `Pipe`/`Listener` API over `tcp://`, `shm://`,
`uds://` (POSIX) and `pipe://` (Windows named pipes), with a bounded `shm://`
fast path, a QoS layer (watermarks, backpressure, per-message TTL), automatic
reconnect with exponential backoff, and header-only RPC and pub/sub layers
on top.

The capability matrix — what exists, what does **not** exist yet, and which
option fields are reserved but not consumed — is maintained in one place,
[`docs/design/capabilities.md`](docs/design/capabilities.md); this section and
its Chinese twin only summarize it. Notable absences: no async/event-loop API
on the pipe itself, delivery is at-most-once, and there is no TLS layer.

Design audit and roadmap live in [`docs/design/`](docs/design/):

- [`aello-audit-2026-10-16.md`](docs/design/aello-audit-2026-10-16.md) — per-file audit: keep / refactor / delete.
- [`aello-adoption-2026-10-16.md`](docs/design/aello-adoption-2026-10-16.md) — what was adopted from the external analysis, what was deferred.

## Principles

- **Bounded memory.** Queues never grow implicitly; a full queue blocks, times out, or drops — by policy.
- **Low latency without sacrificing correctness.** Memory ordering and crash behavior are verified, not assumed.
- **Transport and message semantics are separate.** Entering the transport backend is the only delivery promise; processing and durability belong to the application.
- **One performance path first.** SPSC shared memory until measurements demand more.
- **Performance claims come from reproducible benchmarks** (`bench/aello_bench.cc`);
  no published numbers yet, so none are made.

## Quick start

Requires C++20 and CMake 3.20+.

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Server process:

```cpp
#include "duct/duct.h"

auto listener = duct::listen("shm://gamebus");
if (!listener.ok()) { /* handle listener.status() */ }

auto pipe = listener.value()->accept();
if (!pipe.ok()) { /* handle pipe.status() */ }

for (;;) {
  auto msg = pipe.value()->recv({});
  if (!msg.ok()) break;
  pipe.value()->send(msg.value(), {});   // echo
}
```

Client process:

```cpp
#include "duct/duct.h"

auto pipe = duct::dial("shm://gamebus");
if (!pipe.ok()) { /* handle pipe.status() */ }

pipe.value()->send(duct::Message::from_string("hello"), {});
auto reply = pipe.value()->recv({});
```

Every call returns a `Result<T>`; check `.ok()` and read `.status()` — no exceptions unless you ask for them (`value_or_throw()`).

## Addresses

| Scheme | Form | Platforms | Status |
|---|---|---|---|
| `tcp://` | `tcp://host:port` (or bare `host:port`) | all | working |
| `shm://` | `shm://name` | POSIX, Windows | working — the fast path |
| `pipe://` | `pipe://name` | Windows named pipes | working |
| `uds://` | `uds:///path/to/socket` | POSIX | working |

## Semantics (current contract)

- **Copy send.** Payloads are copied into the transport (ring slot or framed
  stream). Zero-copy claim/commit is a deferred capability, not a current one.
- **Per-frame 64 KiB cap on socket byte-stream transports (`tcp://`, `uds://`).**
  Larger messages are fragmented and reassembled, up to a 64 MiB limit. The POSIX
  `shm://` transport reassembles across ring slots up to the same limit. `pipe://`
  and Windows `shm://` do not fragment and reject messages over their per-frame
  or per-slot limit on the send side (`kInvalidArgument`).
- **Full queue behavior on `shm://`:** send blocks until space or timeout
  (`SendOptions::timeout`), then returns `kTimeout`. This is the transport-level
  behavior (with QoS disabled); with QoS enabled the in-memory send queue's
  `snd_hwm_bytes` water mark applies first.
- **Reliability is at-most-once.** Messages in flight are lost if a peer dies;
  durable delivery is an application-level concern.

## Building

CMake options:

| Option | Default | Effect |
|---|---|---|
| `DUCT_BUILD_EXAMPLES` | `ON` | echo server/client and manual transport tools |
| `DUCT_BUILD_TESTS` | `ON` | test suite (run via `ctest`) |
| `DUCT_BUILD_BENCH` | `ON` | benchmark harness and its `ctest` smoke run (not built on Windows) |

There is no versioned release yet — consume via `add_subdirectory(aello)` and
link the `duct` target, or install it: `cmake --install` exports a CMake
package config (`duct::`, `ductTargets.cmake` / `ductConfig.cmake`), a
pkg-config file (`duct.pc`), and CPack TGZ packaging. The install rules are
covered by the `duct_install` smoke test in `ctest`. Library naming
(`duct` → `aello`) will change in a later release series; the GitHub redirect
from `duct` remains valid.

## Nightly builds

`.github/workflows/nightly.yml` runs daily at 00:00 UTC and on manual
dispatch on four legs — `linux-x64` (GCC), `linux-x64-clang`, `windows-x64`
(MSVC), and `macos-arm64` (Clang). Each leg builds the library and examples,
runs the full acceptance suite under `ctest`, then packages
`duct-linux-x64.tar.gz`, `duct-linux-x64-clang.tar.gz`,
`duct-windows-x64.zip`, and `duct-macos-arm64.tar.gz` (headers, static
library, echo pair, `README.md`, `LICENSE`) and publishes them under a single
rolling `nightly` pre-release tag, so only the latest build is kept. macOS is
exercised only on that schedule; `cmake-multi-platform.yml` covers Ubuntu and
Windows.

## Repository layout

```
include/duct/     public headers (core API, message, status, wire, transports)
src/              transport implementations (tcp, uds, shm, named pipe + helpers)
tests/            cross-platform test suite (one source per layer under test)
examples/         echo pair + manual transport tools
bench/            benchmarks
docs/design/      analysis, audit, and adoption decisions
```

`socket_utils.h` backs the built library. `convenience.h` (builders and
request/reply/batch/loop helpers) and `raii.h` (scoped ownership wrappers) are
header-only application adapters: they are installed with the library and
compiled by the test suite, but add no binary artifacts to `duct` itself.

## License

[MIT](LICENSE) © cuihairu

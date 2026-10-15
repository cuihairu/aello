# Aello

> **English** | [中文](README.zh.md)

<p align="center">
  <a href="https://github.com/cuihairu/aello/actions/workflows/cmake-multi-platform.yml"><img src="https://github.com/cuihairu/aello/actions/workflows/cmake-multi-platform.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
  <a href="https://isocpp.org/"><img src="https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus&logoColor=white" alt="C++20"></a>
  <img src="https://img.shields.io/badge/status-experimental-orange" alt="Status">
</p>

**Aello** is a messaging runtime for game servers: how server processes exchange
messages with low latency, bounded memory, and explicit contracts.

It is **not** a rewrite of BigWorld, and **not** a wrapper that hides shared
memory, Aeron, or ZeroMQ behind one API. It borrows the ideas worth borrowing —
bounded channels, transport isolation, copy-first ownership — and keeps only
what a game backend actually needs.

Formerly known as `duct`.

## Status

Experimental, pre-release. What exists today:

- Message model (`Message`) and `Result`/`Status` error handling — no exceptions required.
- One `Pipe`/`Listener` API over three transports: `tcp://`, `shm://`, `pipe://` (Windows named pipes).
- The `shm://` fast path: a bounded, pre-allocated fixed-slot ring per direction
  (64 slots × 64 KiB), semaphore-synchronized, with copy-send semantics.
- A 16-byte wire header for byte-stream transports; single messages are capped at 64 KiB.
- Cross-platform CI (GCC / Clang / MSVC) running the test suite.

What does **not** exist yet — stated plainly so nobody has to discover it:

- No automatic reconnect (the option fields are currently ignored).
- `uds://` addresses parse but are not wired into dial/listen.
- No receive-side watermarks, no message TTL enforcement, no fragmentation.
- No benchmarks, no RPC, no pub/sub, no async layer.

Design audit and roadmap live in [`docs/design/`](docs/design/):

- [`aello-audit-2026-10-16.md`](docs/design/aello-audit-2026-10-16.md) — per-file audit: keep / refactor / delete.
- [`aello-adoption-2026-10-16.md`](docs/design/aello-adoption-2026-10-16.md) — what was adopted from the external analysis, what was deferred.

## Principles

- **Bounded memory.** Queues never grow implicitly; a full queue blocks, times out, or drops — by policy.
- **Low latency without sacrificing correctness.** Memory ordering and crash behavior are verified, not assumed.
- **Transport and message semantics are separate.** Entering the transport backend is the only delivery promise; processing and durability belong to the application.
- **One performance path first.** SPSC shared memory until measurements demand more.
- **Performance claims come from reproducible benchmarks.** None exist yet, so none are made.

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
| `uds://` | `uds:///path/to/socket` | POSIX | parsed, **not wired yet** |

## Semantics (current contract)

- **Copy send.** Payloads are copied into the transport (ring slot or framed
  stream). Zero-copy claim/commit is a deferred capability, not a current one.
- **Single messages up to 64 KiB.** Larger payloads are rejected until
  fragmentation exists.
- **Full queue behavior on `shm://`:** send blocks until space or timeout
  (`SendOptions::timeout`), then returns `kTimeout`.
- **Reliability is at-most-once.** Messages in flight are lost if a peer dies;
  durable delivery is an application-level concern.

## Building

CMake options:

| Option | Default | Effect |
|---|---|---|
| `DUCT_BUILD_EXAMPLES` | `ON` | echo server/client and manual transport tools |
| `DUCT_BUILD_TESTS` | `ON` | test suite (run via `ctest`) |

There is no install target yet — consume via `add_subdirectory(aello)` and link
the `duct` target. Library naming (`duct` → `aello`) will change in a later
release series; the GitHub redirect from `duct` remains valid.

## Repository layout

```
include/duct/     public headers (core API, message, status, wire, transports)
src/              transport implementations (tcp, shm, named pipe + helpers)
tests/            cross-platform test suite
examples/         echo pair + manual transport tools
docs/design/      analysis, audit, and adoption decisions
```

## License

[MIT](LICENSE) © cuihairu

# Aello Analysis Report (External Draft)

> Source: <https://chatgpt.com/s/t_6ad1057e6b4081918ad731e17983e85f>
> Compiled: 2026-10-16
> **Status: external analysis, not established project fact.** Verify every claim against the code before acting. References to "existing implementation" or "earlier design" should be treated as **undocumented** until checked.
> The project formerly known as **duct has been renamed to aello** (GitHub redirects still work).

---

## 1. Establish what Aello is for

I recommend positioning Aello as:

> **Aello — A high-performance messaging runtime for game servers.**
>
> A low-latency, predictable, observable runtime for passing messages between server processes.

It is not a reimplementation of BigWorld, nor a thin wrapper that hides shared memory, Aeron, and ZeroMQ behind one API.

Aello should absorb the most valuable design ideas from each system:

- **BigWorld Mercury**: message organization, channels, reliability, and communication lifecycles in game servers.
- **Aeron**: low-latency transport, shared-memory IPC, buffer management, flow control, and performance testing.
- **ZeroMQ**: a unified messaging abstraction, transport isolation, queue policies, and communication patterns.
- **Folly ProducerConsumerQueue**: low-synchronization design for fixed-capacity SPSC queues.
- **Boost.Interprocess**: shared memory, process-shared synchronization, memory layout, and cross-process object lifetimes.

The goal is not to accumulate every feature from these projects. It is to select the mechanisms that fit real game-backend workloads.

## 2. What Aello should learn from mature projects

High performance is not just putting messages in shared memory. It also involves cache locality, thread contention, allocation, consumer wake-up, backpressure, and the effect of slow consumers on unrelated message flows.

**Aeron — the main transport reference.** Aeron provides high-performance IPC and UDP transport, with Media Driver, Publication, and Subscription concepts. Its flow control bounds how far a producer may run ahead of a consumer. Aello should study these mechanisms rather than copy the API.

**LMAX Disruptor — queue and scheduling model.** Worth studying for pre-allocated ring buffers, sequence numbers, consumer dependency graphs, and the trade-off between busy polling and blocking waits. Disruptor's event-processing model is not a complete cross-process protocol.

**ZeroMQ — messaging abstraction and patterns.** Its unified message API and backend isolation are useful references. Automatic reconnect, brokers, pub/sub, and every routing pattern do not all belong in the first release.

**Folly / Moodycamel — concurrent queues.** Folly's SPSC queue uses fixed capacity and separate producer and consumer endpoints. Moodycamel splits queues per producer. Aello should begin with the simple SPSC path and introduce multi-producer structures only when real workloads justify them.

**Boost.Interprocess — shared-memory infrastructure.** Study creation and destruction, process-shared synchronization primitives, permissions, relative-offset pointers, and resource lifetimes. Shared memory cannot safely contain ordinary process-local pointers or unverified object layouts.

### Key conclusion

Do not start by building a complete Aeron replacement. **Own the game-specific message runtime and IPC core, keep network transport pluggable, and integrate mature network libraries such as Aeron when necessary.** This avoids becoming another broad communication framework that is shallow in every area.

## 3. Overall architecture

Split Aello into three clear boundaries: **message runtime, transport backends, and optional game-communication extensions**.

### Game communication extensions (optional)

RPC · request/response · service addressing · sessions

Game-process calls, request correlation, and service discovery. None of these are forced into the core library.

### Aello Runtime

**Endpoint** — a process communication endpoint

**Message** — message boundaries and metadata

**Dispatcher** — receive and dispatch

**Flow Control** — queue capacity and backpressure

### Transport backends

Shared memory (primary performance path), local sockets (compatibility path), and network transport (later extension).

The diagram describes a target architecture. It does not claim that the current repository already implements these modules.

### 3.1 Runtime Core

Begin with a minimal, stable public vocabulary:

| Concept | Responsibility |
|---|---|
| `Runtime` | Initialization, resource management, startup, and shutdown |
| `Endpoint` | An endpoint that can send or receive messages |
| `MessageView` | Read-only message view that does not own the underlying memory by default |
| `MessageBuilder` | Build a message for sending |
| `SendResult` | Distinguish sent, queue full, endpoint closed, and related outcomes |
| `Dispatcher` | Route received messages to the appropriate handler |
| `Transport` | Hide differences between transport mechanisms |

The first version does not need `Future`, a complex async framework, a general event loop, or timers. First design synchronous send, batch send/receive, and receiver polling. Decide later whether an async wrapper is warranted.

### 3.2 Shared memory as the primary performance path

Start with a **bounded, pre-allocated shared-memory message channel**, not a universal queue.

- Fixed-capacity ring buffers, so the normal path does not allocate and free memory repeatedly.
- Fixed headers plus bounded payloads, or descriptors backed by shared-memory buffers.
- Fixed layouts, explicit alignment, and relative offsets across processes. Never store ordinary pointers in shared memory.
- Explicit atomic synchronization and memory ordering, so consumers never observe a partially published message.
- Isolate frequently written producer and consumer state to reduce false sharing.
- Define queue-full behavior explicitly: immediate return, bounded wait, or blocking wait. Never allow implicit unbounded growth.
- Make the wait strategy configurable: busy polling, brief CPU yield, or OS wait primitives.

**Do not assume that using `std::atomic` makes shared memory, crash recovery, or cross-platform behavior automatically safe.** Verify the implementation conditions for each target OS and compiler, and add abnormal-process-exit tests.

### 3.3 Message ownership and memory management

Treat zero-copy as an optional capability, not a first-version slogan. Separate three stages:

1. **Copy Send**: the caller supplies a payload and the library copies it into the queue.
2. **Batch Send**: submit multiple messages at once to reduce atomic operations and call overhead.
3. **Claim / Commit**: the caller claims a shared buffer, writes to it, explicitly commits, and the receiver releases it after processing.

The third stage reduces copies, but introduces buffer lifetimes, failed commits, leaked uncommitted buffers, and abnormal process termination. It requires an explicit ownership protocol before implementation.

## 4. Message semantics for game servers

Game servers differ from ordinary business messaging middleware: messages are not merely data delivery. They can affect entity state, frame logic, request/response timing, and cross-process call ordering.

For example, a game backend may exchange messages between **BaseApp** (player sessions, entities, business requests) and **CellApp** (spatial entities, scene logic, simulation updates), as well as DB or other service processes. This is an illustrative relationship, not a universal deployment topology.

Aello should deliberately layer its message semantics:

| Message category | Example | First-stage policy |
|---|---|---|
| One-way messages | State updates, notifications, events | Supported |
| Request / response | Query an entity, load data | Optional upper-layer module |
| Batch messages | A group of entity updates or events | Core batch send/receive support |
| Sequenced messages | Frame number, state version, event sequence | Metadata extension; do not decide game semantics |
| Broadcast / multiple subscribers | Regional state, public events | Later |
| Scheduled / delayed messages | Delayed execution, timeout jobs | Not in the first core |

### Do not conflate reliability, ordering, and transport

A message transported over shared memory is not automatically reliable after the receiving process crashes. Likewise, FIFO ordering does not guarantee that messages from multiple producers match the global order required by game logic.

Define separate stages:

- **Transport success**: the message entered the transport backend.
- **Receive success**: the receiver obtained the message.
- **Processing complete**: application logic handled it.
- **Persistence complete**: durable state was written when the application requires it.

The first version should promise only transport behavior with a precise contract. Do not claim end-to-end exactly-once. Reliable retry belongs with request IDs, idempotency, and application-level acknowledgements.

## 5. How to set performance goals

Do not start with a marketing number such as "ten million messages per second" and design backwards. Build repeatable benchmarks first.

Cover four dimensions:

**Latency** — p50, p95, p99, and p99.9 one-way latency, plus round-trip latency. Tail latency and jitter matter more than the mean.

**Throughput** — messages per second and payload bytes per second for each message-size bucket. Do not report only one metric.

**Resource cost** — CPU utilization, CPU cycles per message, memory footprint, context switches, and the cost of different wait strategies.

**Stress and failure** — slow consumers, full queues, producer bursts, forced process termination, restart, and resource cleanup. Performance results are valid only alongside correctness tests.

Use the same machine and comparable workloads to test at least:

1. Aello shared memory.
2. Aello's local socket backend.
3. Aeron IPC.
4. A simple SPSC queue baseline.
5. Aeron UDP or another network option when investigating cross-machine transport.

Aeron's own benchmarks and documentation are important reference material.

## 6. Modules and implementation order

| Module | Responsibility | Priority |
|---|---|---|
| `aello-core` | Message model, error types, endpoint contracts | P0 |
| `aello-shm` | Shared memory, ring queues, synchronization | P0 |
| `aello-bench` | Throughput, latency, resource, and contention benchmarks | P0 |
| `aello-test` | Multi-process, stress, crash, and restart tests | P0 |
| `aello-local` | UDS / Windows local-transport adapter | P1 |
| `aello-rpc` | Request correlation, response, timeout, cancellation | P1 |
| `aello-network` | Pluggable network backend or mature-library adapter | P2 |
| `aello-pubsub` | Multiple subscribers, broadcast, dispatch policy | P2 |

P0/P1/P2 describe recommended order, not modules that already exist.

### Recommended stages

1. **Benchmarks and contracts** — define operating systems, message-size distribution, producer/consumer counts, ordering requirements, queue-full behavior, and measurement method. Establish a reproducible baseline.
2. **Minimal shared-memory engine** — implement a bounded SPSC queue, publish/consume, inter-process wake-up, shutdown, and cleanup. Verify memory ordering, process termination, and restart.
3. **Game-server integration** — use simulated BaseApp/CellApp workloads to test one-way messages, batch state updates, and request/response with realistic sizes and bursts.
4. **Concurrency model** — use benchmark evidence to decide whether MPSC, MPMC, multiple channels, batch commit, zero-copy, or more complex wait strategies are justified.
5. **Network and upper-layer semantics** — then evaluate UDS, TCP, Aeron, RPC, service addressing, and broadcast. Keep transport separate from the game protocol.

## 7. Final positioning and name

Adopt:

# Aello

High-Performance Messaging Runtime for Game Servers

**Core positioning**

A high-performance process-communication and messaging runtime for game servers: bounded, low-latency, observable local message channels first, with clear extension boundaries for network transport and game communication semantics.

**Core principles**

- Low latency without sacrificing correctness.
- Bounded memory; queues never grow without limit.
- Separate transport from message semantics.
- Validate one performance path before expanding the concurrency model.
- Every performance claim must be supported by a reproducible benchmark.

My conclusion is that Aello should not become a project that merely aggregates fast communication libraries. It should be infrastructure **with shared-memory IPC as its first performance target, the game-server message runtime as its core abstraction, and mature network adapters as extensions**.

The next step is not to implement every module. It is to audit the existing `cuihairu/duct` code file by file: identify what to keep, what API to refactor, and what to delete, then turn that into an Aello technical design and migration plan that a coding agent can execute.

---

Possible follow-ups:

- Detailed design of Aello's message transport and synchronization mechanisms.
- A concrete benchmark plan and test-case design.
- A scenario-based module roadmap.

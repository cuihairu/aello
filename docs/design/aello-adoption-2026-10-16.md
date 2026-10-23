# Aello Analysis Adoption Decisions (2026-10-16)

> Inputs: external analysis `docs/design/aello-analysis-2026-10-16.md` and file audit `docs/design/aello-audit-2026-10-16.md`.
> User direction: "review and adopt part of it." This document records **adopt/defer decisions and rationale** only. No implementation code is included.
> Source of truth: current code behavior is defined by file:line evidence in the audit, not by the external analysis.

## 1. Positioning: adopted

**"High-Performance Messaging Runtime for Game Servers"** is adopted and reflected in the README (English primary + `README.zh.md`).

Two non-goals are also recorded:

- It is not a BigWorld rewrite. It borrows Mercury's thinking about game-message organization (channels, reliability, lifecycle), not engine-level replication.
- It is not shared memory / Aeron / ZeroMQ hidden behind one API. Transport backends remain pluggable, and mature network libraries may be integrated directly instead of building a full replacement.

## 2. Modules and priorities

The analysis's module table was checked against current code. **The priority ordering is adopted as the roadmap**; splitting CMake targets is deferred to an implementation round (the repository currently has one `duct` target, `CMakeLists.txt:7-15`).

| Analysis module | Priority | Decision | Current state and rationale |
|---|---|---|---|
| `aello-core` message model / errors / endpoint contracts | P0 | **Adopt** | Seeds already exist: `Message`/`Status`/`Result`/`Pipe`/`Listener`/wire framing. The first step is the audit §8 cleanup (remove utils/logging/async/export), not adding concepts. Whether to introduce `Runtime`/`Endpoint` is left to the implementation round |
| `aello-shm` shared memory / ring queues / synchronization | P0 | **Adopt** | The primary path already exists in the right shape (fixed-slot double ring + semaphores, `shm_transport.cc:42-43`). Evolution items (descriptors + memory pool, eventfd/futex wake-up, crash recovery) wait for benchmark evidence; see §3.1 |
| `aello-bench` throughput / latency / resource benchmarks | P0 | **Adopt — currently the largest gap** | The repository has no benchmarks; examples are manual test tools. The analysis's stage 1, "benchmark before optimizing," is the first task from a zero baseline |
| `aello-test` multi-process / stress / crash tests | P0 | **Adopt** | `duct_tests.cc` is one single-process file and lacks the crash/restart matrix (audit §5). The expansion path is defined |
| `aello-local` UDS/Windows local adapter | P1 | **Adopt direction, defer implementation** | The audit found `uds_transport.cc` implemented but unwired and uncompiled. Wiring and tests are the first P1 task, not a rewrite |
| `aello-rpc` request correlation / timeout / cancellation | P1 | **Defer** | `convenience::request` is bare send+receive with no request-ID correlation (`convenience.h:202-212`). Stabilize core contracts first |
| `aello-network` pluggable network backend / Aeron adapter | P2 | **Defer** | `tcp_transport.cc` is sufficient for now. Decide on Aeron when a cross-machine requirement appears |
| `aello-pubsub` multiple subscribers / broadcast | P2 | **Defer** | Current connections use a dual SPSC ring; broadcast is a separate dispatch-layer problem and should not be mixed into the core |

## 3. Four specific rulings

### 3.1 Zero-copy as an optional capability (Copy → Batch → Claim/Commit)

**Ruling: adopt as a roadmap; implement in stages. The project is at stage 1; Batch and Claim/Commit are explicitly deferred.**

- The current implementation is stage 1 (Copy Send): shm send copies into a slot (`shm_transport.cc:400`) and receive copies again (`shm_transport.cc:424`). This is not a regression; it is the correct starting point recommended by the analysis.
- **Batch deferred:** true batching (multiple messages per submission, amortizing atomics and semaphore posts) requires a ring-queue API. Existing `convenience::send_batch` is a fake batch that loops over single sends (`convenience.h:229-240`) and must not be described as batch capability.
- **Claim/Commit deferred:** a cross-process buffer-ownership protocol needs ownership documentation and crash tests before it can handle failed commits, uncommitted leaks, and abnormal process termination. Until aello-bench proves copying is a bottleneck, the complexity is not justified. The README no longer advertises zero-copy.

### 3.2 Four success states (transport / receive / processing / persistence)

**Ruling: adopt the vocabulary in design documents; defer API changes. The v1 contract promises only "transport success."**

- The current error model exposes transport-level results only (`Result<void>` + `StatusCode`, `status.h:10-18`), and has no queue-full code. Blocking paths borrow `kTimeout`; fail-fast paths borrow `kIoError` (`queue.cc:33-58`).
- Receive acknowledgement, business completion, and durable persistence are upper-layer semantics and the core library must not pretend to provide them. The analysis's rule that v1 guarantees only explicitly contracted transport behavior, without claiming end-to-end exactly-once, is adopted for the README.
- Implementation action: add a queue-full code to `StatusCode`; keep the four-state distinction as design vocabulary rather than a v1 API.

### 3.3 Bounded memory; no implicit queue growth

**Ruling: adopt now, promote to a core principle, and state it in the README.**

- The main path already complies: the shm ring is fixed at 64 slots × 64 KiB (`shm_transport.cc:42-43`), and QosPipe applies a byte high-water mark (`qos_pipe.cc:117`). The principle confirms existing behavior rather than imposing a new one.
- Violations (all already on the audit delete/refactor list): unbounded `async::Channel` (`async.h:242-249`), unbounded `Histogram` observations (`logging.h:354-356`), and `MessageQueue` treating `hwm=0` as unlimited (`queue.cc:221-223`).
- Refactor note: QosPipe limits bytes but not item count, so tiny messages can accumulate excessive entries (`qos_pipe.cc:117` compares only bytes). Add an item cap or charge by slot.

### 3.4 Start with SPSC; decide MPSC/MPMC from real load

**Ruling: adopt now (it describes the current implementation); defer MPSC/MPMC until benchmark evidence exists.**

- Each connection has two rings, one producer, one consumer, with head/tail exclusively advanced by opposite sides (`shm_transport.cc:146-149`). This is SPSC today and needs no change.
- Multi-producer support requires redesigning enqueue arbitration and full-ring semantics. That is pure risk before aello-bench produces single-path data. The analysis's "do not implement early for a feature checklist" is adopted as discipline.

## 4. Explicitly not adopted for now

| Item | Reason |
|---|---|
| General event loop / timers / Future framework in the core | v1 explicitly excludes them; `async.h` is slated for deletion in the audit |
| Runtime metrics framework (Counter/Gauge/Registry) | Performance numbers belong to aello-bench; keep only minimal runtime hooks and revisit later |
| Advertising automatic reconnect as an out-of-the-box feature | The code is unwired (audit §2.2); no document may claim it until wired |
| End-to-end exactly-once promise | Consistent with §3.2; never claim it |
| Promotional performance numbers ("ten million per second") | Wait for reproducible aello-bench data; until then the README states structural facts only |

## 5. Next work order (implementation round only)

1. Fix the `message.h` build (one line).
2. Apply the audit delete list (11 files plus the three CMake templates decision).
3. Create `bench/`: first a minimal reproducible shm round-trip-latency and one-way-throughput benchmark, compared with UDS/TCP (analysis stage 1).
4. Wire UDS and reconnect (or remove the advertised fields) to eliminate the documentation gap.
5. Fill test blind spots: direct QoS tests, concurrency, and process `kill -9`/restart matrices.

---
layout: home

hero:
  name: Aello
  text: High-Performance Messaging Runtime for Game Servers
  tagline: Bounded, low-latency, observable process messaging with explicit contracts and pluggable transports.
  actions:
    - theme: brand
      text: Read the design
      link: /design/aello-adoption-2026-10-16
    - theme: alt
      text: Source code
      link: https://github.com/cuihairu/aello

features:
  - title: Bounded memory
    details: Queues never grow implicitly. A full queue blocks, times out, or drops according to an explicit policy.
  - title: One performance path first
    details: SPSC shared memory is the first fast path; broader concurrency models wait for benchmark evidence.
  - title: Explicit contracts
    details: Transport success, receive success, processing completion, and persistence are separate states.
  - title: Pluggable transports
    details: TCP, shared memory, and Windows named pipes share one Pipe/Listener model, with UDS wiring in progress.
---

![Aello messaging runtime architecture](/images/architecture-en.svg)

## Current status

Aello is an experimental, pre-release messaging runtime. It currently provides a message model, `Result`/`Status` error handling, a `Pipe`/`Listener` API over `tcp://`, `shm://`, and `pipe://`, bounded fixed-slot shared-memory rings, a 16-byte wire header, and cross-platform CI.

The site records the design decisions and the file-by-file audit that drive the next implementation round:

- [Adoption decisions](/design/aello-adoption-2026-10-16) — what was adopted, what was deferred, and why.
- [External analysis](/design/aello-analysis-2026-10-16) — the architecture and performance direction that informed the decisions.
- [File-by-file audit](/design/aello-audit-2026-10-16) — current code findings, with file:line evidence.

See the [repository README](https://github.com/cuihairu/aello) for build and usage instructions.

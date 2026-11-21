# At-Least-Once Delivery Proposal (2026-11-21)

Status: **proposal, not implemented**. `QosOptions::reliability` stays a
reserved field until the user signs off on the decision points (D1–D5) at the
end of this document. The fault model below is pinned by
`tests/loss_injection_tests.cc`, which lands together with this proposal and
stays green on current (at-most-once) semantics.

See also [capabilities and reserved contracts](capabilities.md) — the
"explicitly not implemented" section remains accurate until this proposal is
implemented.

## Goal

Consume the reserved `Reliability::kAtLeastOnce` so a reconnect-enabled pipe
redelivers application messages lost to a dead connection, with receiver-side
dedup, without changing the `Pipe` API shape.

## Observed fault model (pinned by the loss-injection matrix)

| Cell | Test | Current behavior (at-most-once) |
|---|---|---|
| L1 | `test_loss_injection_drop_is_silent` | `send()` returns ok, message silently discarded — no error, no redelivery |
| L2 | `test_loss_injection_duplicate_arrives_twice` | a duplicated send arrives twice verbatim — no dedup anywhere below the application |
| L3 | `test_loss_injection_reorder_observed` | a reordered pair arrives swapped verbatim — no sequencing below the application |
| L4 | `test_midstream_close_delivers_exact_prefix` | server stops reading and closes: receiver holds exactly the prefix it read, in order, zero duplicates; everything past the cut is gone forever |
| L5 | `test_reconnect_gap_losses_are_invisible_to_caller` | after a mid-conversation disconnect and redial: no id ever arrives twice (at-most-once invariant holds), but sends that returned ok into the dying connection's kernel buffer are lost although the caller saw success |

L5 is the core motivation: the caller cannot retry what it believes was
delivered. Only a protocol layer (session + seq + ack + retransmit) can close
it.

## Design

### Session identity

The dialer generates a 64-bit `session_id` once per reconnect-enabled dial; it
survives redials. The listener learns it from the first frame of every
connection. A server that sees a session it already knows resumes it; a new
session starts fresh.

### Sequencing

Every application message carries a per-direction `u64` sequence number,
starting at 0 for the session.

### Acknowledgement

The receiver emits **cumulative ACK control frames**: ack = highest in-order
sequence delivered. At most one ACK is pending per direction; it is flushed on
a short timer or every N messages, whichever first.

### Retransmit window

The sender keeps a bounded window of unacknowledged messages, byte-bounded by
the existing `snd_hwm_bytes` budget (no new memory knob by default). On
`kConnected` after a `kDisconnected`, the sender replays the unacked window in
order before new sends. Window overflow is backpressure and follows the
existing `BackpressurePolicy` (`kDropNew`/`kDropOld` degrade that message to
at-most-once — documented behavior, not silent).

### Dedup

The receiver tracks the highest in-order sequence delivered and drops frames
at or below it, plus a small bounded out-of-order holdback so a retransmit
racing a slow ACK cannot duplicate. Holdback overflow is dropped (possible
redelivery — this is why the guarantee is at-least-once, not exactly-once).

### Wire format

`FrameHeader` already carries `version` and `header_len`
(`include/duct/wire.h:29-34`). Proposal: **D1** — version 2 frames extend the
header to 32 bytes (base 16 + `session_id` u64 + `seq` u64); `header_len`
disambiguates, so v1 peers keep working. A reliability capability bit is
negotiated through the existing handshake capability bitmask
(`include/duct/wire.h:62-70`); a v1 peer means the connection proceeds at
at-most-once (graceful degradation, documented). ACK control frames reuse the
flags field with a small payload (cumulative ack + session echo).

### Layering

New `ReliablePipe` decorator; composition for a reliability-enabled dial
becomes `QosPipe(ReliablePipe(ReconnectPipe(transport)))`. `ReliablePipe`
sits above the reconnect boundary so the session and unacked window survive
redials, and drives replay from the existing `ConnectionCallback`
(`kDisconnected` → arm replay, `kConnected` → flush window).

### Scope

v1 covers `tcp://` and `uds://` (byte-stream transports with fragmentation).
Windows `pipe://` deferred; `shm://` out of scope — a crash loses ring
contents by design, and durability stays a non-goal.

### Non-goals (unchanged from capabilities.md)

No durability across process death; no persistence; not exactly-once; no idle
keepalive probing in v1 (`ReconnectPolicy::heartbeat_interval` stays
reserved).

## Decision points (user sign-off required)

- **D1 — wire encoding**: extended 32-byte header (recommended) vs flags-only
  sidecar fields.
- **D2 — scope**: `tcp://` + `uds://` first (recommended), `pipe://` later.
- **D3 — defaults**: window budget = `snd_hwm_bytes` (recommended); dedup
  holdback = 64 messages.
- **D4 — composition**: `reliability = kAtLeastOnce` auto-enables reconnect
  (recommended) vs dial-time error when `reconnect.enabled` is false.
- **D5 — acceptance**: the matrix L1–L5 stays green, plus new cells R1
  (redelivery after reconnect), R2 (dedup of replayed messages), R3 (window
  bound under backpressure), R4 (graceful v1-peer degradation).

## Acceptance criteria (future work, gated on D1–D5)

Implement `ReliablePipe`, add cells R1–R4 to the suite, keep L1–L5 green for
`kAtMostOnce`, and update `capabilities.md` (move the reserved row to the
implemented table) in the same change.

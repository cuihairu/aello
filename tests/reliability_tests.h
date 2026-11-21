#pragma once

// Reliability acceptance cells R1-R4 from docs/design/reliability-at-least-once.md
// (decision point D5). The loss-injection matrix L1-L5 in loss_injection_tests.cc
// pins the at-most-once baseline these cells build on.
// Definitions in reliability_tests.cc.

// R1: a reconnecting reliable dial redelivers every message the dying
// connection swallowed, in order, after the redial.
void test_reliable_redelivers_after_reconnect();

// R2: the replayed window may contain messages the receiver already delivered
// (their ACKs were never processed by the dialer); the receiver deduplicates
// them so every id arrives exactly once.
void test_reliable_replay_is_deduplicated();

// R3: the retransmit window is byte-bounded by snd_hwm_bytes; overflow follows
// the backpressure policy instead of growing without limit.
void test_reliable_window_bounded_by_snd_hwm();

// R4: a v1 peer (reliability not negotiated) degrades to at-most-once on a
// healthy connection instead of failing the protocol.
void test_reliable_degrades_to_at_most_once_against_v1_peer();

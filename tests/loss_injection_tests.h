#pragma once

// Loss-injection matrix: characterizes the current at-most-once semantics
// under deterministic faults, as the acceptance base for the future
// kAtLeastOnce layer (proposal: docs/design/reliability-at-least-once.md).
// Definitions in loss_injection_tests.cc.

void test_loss_injection_drop_is_silent();
void test_loss_injection_duplicate_arrives_twice();
void test_loss_injection_reorder_observed();
void test_midstream_close_delivers_exact_prefix();
void test_reconnect_gap_losses_are_invisible_to_caller();

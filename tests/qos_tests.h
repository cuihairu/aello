#pragma once

// qos-layer regression tests. Definitions in qos_tests.cc.

void test_qos_block_zero_timeout_blocks();
void test_qos_permanent_send_error_does_not_wedge();
void test_qos_oversized_message_rejected();
void test_qos_hwm_not_a_message_cap();
void test_qos_zero_hwm_is_unlimited();
void test_qos_delivery_under_hwm();
void test_qos_close_under_full_ring();
void test_reconnect_dial_connects_and_echoes();
void test_reconnect_dial_fails_after_attempts();
void test_reconnect_dial_unreachable_fails_within_deadline();
void test_reconnect_dial_reports_bound_on_return();
void test_reconnect_callback_ops_pipe();
void test_qos_recv_watermark_and_worker();
void test_qos_linger_drains_before_close();
void test_qos_priority_scheduling();
void test_qos_rate_limit_paces_delivery();
void test_qos_rate_limit_default_burst_drains();
void test_reconnect_channel_preserved();
void test_qos_shm_recv_midfragment_close_completes();
void test_qos_and_reconnect_forward_max_message_bytes();
void test_qos_channel_multiplexing();
void test_qos_item_count_cap();
void test_qos_failfast_returns_queue_full();
void test_qos_dial_opt_in_disabled_by_default();
void test_qos_ttl_expires_queued_message();
void test_qos_send_failure_does_not_kill_recv();
void test_reconnect_ioerror_send_not_resent();
void test_qos_drop_old_evicts_oldest_queued();

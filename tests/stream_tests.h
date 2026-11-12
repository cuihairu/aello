#pragma once

// stream-layer regression tests. Definitions in stream_tests.cc.

void test_uds_echo_one();
void test_uds_multi_message_echo();
void test_uds_channel_recv_subms_deadline_bounded();
void test_uds_concurrent_channel_recv();
void test_uds_peer_credential();
void test_tcp_recv_timeout_honored();
void test_tcp_dial_timeout_happy_path();
void test_tcp_dial_timeout_honored();
void test_tcp_connect_refused_is_io_error();
void test_uds_dial_rejects_negative_timeout();
void test_uds_listen_rejects_non_socket_path();
void test_uds_positive_connect_timeout_is_preserved();
void test_tcp_channel_multiplexing();
void test_tcp_channel_recv_does_not_drop_other_channel();
void test_tcp_channel_pending_capped();
void test_tcp_close_unblocks_blocked_recv();
void test_uds_close_unblocks_blocked_recv();
void test_channel_pending_cap_is_shared_across_channels();
void test_tcp_channel_recv_deadline_bounded();
void test_tcp_channel_recv_subms_deadline_bounded();
void test_tcp_concurrent_channel_recv();
void test_tcp_handshake_negotiates_version();
void test_tcp_accept_rejects_silent_peer();
void test_concurrent_send_recv_channels();
void test_wait_writable_getsockopt_failure_reports_errno();
void test_ensure_stale_uds_removed_policy();

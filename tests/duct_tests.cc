// Regression suite entry point. Test bodies live in tests/<layer>_tests.cc;
// this file only fixes the execution order so failures and hangs stay
// attributable via the [RUN]/[DONE] markers.

#include "test_support.h"

#include "core_tests.h"
#include "pipe_tests.h"
#include "shm_tests.h"
#include "stream_tests.h"
#include "qos_tests.h"
#include "wire_tests.h"
#include "session_tests.h"
#include "adapter_tests.h"

int main() {
#if defined(SIGPIPE)
  // The channel-flood tests keep a peer sending on another channel while the
  // client times out and closes. A send racing that close can hit a broken
  // pipe; on Linux the library only suppresses SIGPIPE on macOS, so ignore it
  // here to keep the harness from dying mid-suite. Library send() semantics are
  // unchanged; this only quiets the signal the write would otherwise raise.
  std::signal(SIGPIPE, SIG_IGN);
#endif
  RUN_TEST(test_address_parse);
  RUN_TEST(test_shm_echo_one);
  RUN_TEST(test_uds_echo_one);
  RUN_TEST(test_uds_multi_message_echo);
  RUN_TEST(test_uds_channel_recv_subms_deadline_bounded);
  RUN_TEST(test_uds_concurrent_channel_recv);
  RUN_TEST(test_uds_peer_credential);
  RUN_TEST(test_shm_crash_recovery_reclaims_orphan);
  RUN_TEST(test_shm_reclaims_legacy_orphan);
  RUN_TEST(test_pipe_echo_one);
  RUN_TEST(test_pipe_rejects_oversize_message);
  RUN_TEST(test_pipe_dial_failure_unblocks_accept);
  RUN_TEST(test_pipe_concurrent_accepts_unblock_on_close);
  RUN_TEST(test_pipe_dial_absent_reports_timeout);
  RUN_TEST(test_pipe_timeout_completion_preserves_message);
  RUN_TEST(test_pipe_midframe_timeout_poisons_connection);
  RUN_TEST(test_shm_backpressure_timeout);
  RUN_TEST(test_shm_burst_without_receiver);
  RUN_TEST(test_wire_decode_rejects_bad_magic);
  RUN_TEST(test_wire_socketpair_frames);
  RUN_TEST(test_wire_fragment_stream_size_capped);
  RUN_TEST(test_wire_read_timeout);
  RUN_TEST(test_wire_write_timeout);
  RUN_TEST(test_wire_partial_frame_timeout);
  RUN_TEST(test_tcp_recv_timeout_honored);
  RUN_TEST(test_tcp_dial_timeout_happy_path);
  RUN_TEST(test_tcp_dial_timeout_honored);
  RUN_TEST(test_tcp_connect_refused_is_io_error);
  RUN_TEST(test_uds_dial_rejects_negative_timeout);
  RUN_TEST(test_uds_listen_rejects_non_socket_path);
  RUN_TEST(test_uds_positive_connect_timeout_is_preserved);
  RUN_TEST(test_shm_dial_timeout_honored);
  RUN_TEST(test_shm_dial_positive_timeout_path);
  RUN_TEST(test_dial_rejects_negative_timeout);
  RUN_TEST(test_wire_clears_send_timeout_for_blocking_write);
  RUN_TEST(test_wire_write_no_sigpipe_on_macos);
  RUN_TEST(test_qos_block_zero_timeout_blocks);
  RUN_TEST(test_qos_permanent_send_error_does_not_wedge);
  RUN_TEST(test_qos_oversized_message_rejected);
  RUN_TEST(test_qos_zero_hwm_is_unlimited);
  RUN_TEST(test_qos_delivery_under_hwm);
  RUN_TEST(test_qos_close_under_full_ring);
  RUN_TEST(test_reconnect_dial_connects_and_echoes);
  RUN_TEST(test_reconnect_dial_fails_after_attempts);
  RUN_TEST(test_reconnect_dial_unreachable_fails_within_deadline);
  RUN_TEST(test_reconnect_dial_reports_bound_on_return);
  RUN_TEST(test_reconnect_callback_ops_pipe);
  RUN_TEST(test_qos_recv_watermark_and_worker);
  RUN_TEST(test_qos_linger_drains_before_close);
  RUN_TEST(test_wire_fragmentation_and_reassembly);
  RUN_TEST(test_shm_fragmentation_and_reassembly);
  RUN_TEST(test_shm_reassembly_size_capped);
  RUN_TEST(test_shm_first_chunk_size_capped);
  RUN_TEST(test_shm_fragmentation_midway_timeout_keeps_stream_synced);
  RUN_TEST(test_shm_configurable_slot_geometry);
  RUN_TEST(test_shm_geometry_mismatch_rejected);
  RUN_TEST(test_shm_rejects_invalid_geometry);
  RUN_TEST(test_qos_priority_scheduling);
  RUN_TEST(test_tcp_channel_multiplexing);
  RUN_TEST(test_tcp_channel_recv_does_not_drop_other_channel);
  RUN_TEST(test_tcp_channel_pending_capped);
  RUN_TEST(test_shm_rejects_oversize_message);
  RUN_TEST(test_shm_recv_unblocks_on_peer_close);
  RUN_TEST(test_tcp_channel_recv_deadline_bounded);
  RUN_TEST(test_tcp_channel_recv_subms_deadline_bounded);
  RUN_TEST(test_reconnect_channel_preserved);
  RUN_TEST(test_tcp_concurrent_channel_recv);
  RUN_TEST(test_qos_shm_recv_midfragment_close_completes);
  RUN_TEST(test_qos_rate_limit_paces_delivery);
  RUN_TEST(test_qos_rate_limit_default_burst_drains);
  RUN_TEST(test_qos_and_reconnect_forward_max_message_bytes);
  RUN_TEST(test_qos_channel_multiplexing);
  RUN_TEST(test_tcp_handshake_negotiates_version);
  RUN_TEST(test_tcp_accept_rejects_silent_peer);
  RUN_TEST(test_qos_item_count_cap);
  RUN_TEST(test_qos_failfast_returns_queue_full);
  RUN_TEST(test_qos_dial_opt_in_disabled_by_default);
  RUN_TEST(test_qos_ttl_expires_queued_message);
  RUN_TEST(test_qos_drop_old_evicts_oldest_queued);
  RUN_TEST(test_concurrent_send_recv_channels);
  RUN_TEST(test_multi_process_kill9_restart);
  RUN_TEST(test_rpc_request_correlation);
  RUN_TEST(test_rpc_request_timeout);
  RUN_TEST(test_rpc_close_joins_without_transport_wakeup);
  RUN_TEST(test_pubsub_multiple_subscribers);
  RUN_TEST(test_pubsub_subscription_outlives_broker);
  RUN_TEST(test_pubsub_concurrent_broker_subscription_destruction);

  RUN_TEST(test_adapter_dial_builder_propagates_options);
  RUN_TEST(test_adapter_listen_builder_binds_and_accepts);
  RUN_TEST(test_adapter_request_reply_roundtrip);
  RUN_TEST(test_adapter_batch_roundtrip);
  RUN_TEST(test_adapter_raii_scoped_roundtrip);
  RUN_TEST(test_adapter_echo_server_loop);

  if (g_failures != 0) {
    std::cerr << "FAIL (" << g_failures << ")\n";
    return 1;
  }
  std::cerr << "OK\n";
  return 0;
}

#pragma once

// shm-layer regression tests. Definitions in shm_tests.cc.

void test_shm_echo_one();
void test_shm_crash_recovery_reclaims_orphan();
void test_shm_reclaims_legacy_orphan();
void test_shm_backpressure_timeout();
void test_shm_burst_without_receiver();
void test_shm_dial_timeout_honored();
void test_shm_dial_positive_timeout_path();
void test_shm_fragmentation_and_reassembly();
void test_shm_reassembly_size_capped();
void test_shm_first_chunk_size_capped();
void test_shm_fragmentation_midway_timeout_keeps_stream_synced();
void test_shm_configurable_slot_geometry();
void test_shm_geometry_mismatch_rejected();
void test_shm_rejects_invalid_geometry();
void test_shm_rejects_oversize_message();
void test_shm_recv_unblocks_on_peer_close();
void test_multi_process_kill9_restart();

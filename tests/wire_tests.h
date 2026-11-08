#pragma once

// wire-layer regression tests. Definitions in wire_tests.cc.

void test_wire_decode_rejects_bad_magic();
void test_wire_socketpair_frames();
void test_wire_fragment_stream_size_capped();
void test_wire_read_timeout();
void test_wire_write_timeout();
void test_wire_partial_frame_timeout();
void test_wire_clears_send_timeout_for_blocking_write();
void test_wire_write_no_sigpipe_on_macos();
void test_wire_fragmentation_and_reassembly();

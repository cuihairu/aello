#pragma once

// pipe-layer regression tests. Definitions in pipe_tests.cc.

void test_pipe_echo_one();
void test_pipe_rejects_oversize_message();
void test_pipe_dial_failure_unblocks_accept();
void test_pipe_concurrent_accepts_unblock_on_close();
void test_pipe_dial_absent_reports_timeout();
void test_pipe_timeout_completion_preserves_message();
void test_pipe_midframe_timeout_poisons_connection();

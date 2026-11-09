#pragma once

// Application-adapter regression tests (convenience.h builders/ops, raii.h
// wrappers). Definitions in adapter_tests.cc.

void test_adapter_dial_builder_propagates_options();
void test_adapter_listen_builder_binds_and_accepts();
void test_adapter_request_reply_roundtrip();
void test_adapter_batch_roundtrip();
void test_adapter_raii_scoped_roundtrip();
void test_adapter_echo_server_loop();

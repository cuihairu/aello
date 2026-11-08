#pragma once

// session-layer regression tests. Definitions in session_tests.cc.

void test_rpc_request_correlation();
void test_rpc_request_timeout();
void test_rpc_close_joins_without_transport_wakeup();
void test_pubsub_multiple_subscribers();
void test_pubsub_subscription_outlives_broker();
void test_pubsub_concurrent_broker_subscription_destruction();

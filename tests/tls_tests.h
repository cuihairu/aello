#pragma once

// TLS transport tests. Definitions in tls_tests.cc. The bodies are compiled
// only when DUCT_HAVE_TLS (the library was built with OpenSSL); on a bare
// build the symbols are absent and duct_tests.cc skips the RUN_TEST calls.

void test_tls_only_on_tcp();
void test_tls_echo_roundtrip();
void test_tls_reliable_echo_roundtrip();
void test_tls_expired_cert_rejected();
void test_tls_wrong_host_rejected();
void test_tls_wrong_host_verify_name_skipped();
void test_tls_untrusted_ca_rejected();
void test_tls_listener_rejects_wrong_key();
void test_tls_listener_requires_cert_and_key();
void test_tls_mtls_requires_client_cert();
void test_tls_mtls_client_without_cert_rejected();

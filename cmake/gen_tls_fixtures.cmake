# Generates test-only TLS fixtures (test CA + leaf pairs) into FIXTURE_DIR.
# Invoked by the build (cmake -P), never run by hand. The material is test
# scaffolding only — clearly named, never installed, and regenerated on every
# configure so no key ever persists as a build artifact across runs.
#
# Usage: cmake -DFIXTURE_DIR=<dir> -DOPENSSL_EXE=<openssl-cli> -P gen_tls_fixtures.cmake
#
# Fixture matrix (all under FIXTURE_DIR):
#   ca.crt / ca.key            self-signed test CA (10y), the only trust anchor
#   server.crt / server.key    CA-signed leaf, CN=localhost,
#                              SAN=DNS:localhost,IP:127.0.0.1, valid 1y
#   wronghost.crt / wronghost.key  CA-signed leaf, CN/SAN=evil.example.com
#   expired.crt / expired.key  CA-signed leaf with notAfter == generation time
#                              (the only failure is expiry, not the chain)
#   untrusted.crt / untrusted.key  self-signed leaf (not from the CA)
#   wrongkey.key               an unrelated key, to pair with server.crt
#   client.crt / client.key    CA-signed leaf for mTLS clients

if(NOT DEFINED FIXTURE_DIR OR NOT DEFINED OPENSSL_EXE)
  message(FATAL_ERROR "FIXTURE_DIR and OPENSSL_EXE are required")
endif()

set(OSSL "${OPENSSL_EXE}")
set(OUT "${FIXTURE_DIR}")
file(REMOVE_RECURSE "${OUT}")
file(MAKE_DIRECTORY "${OUT}")

# Run an openssl step; any nonzero exit aborts the build (a broken fixture is a
# broken test, better loud than silently skipped).
function(run_ossl)
  execute_process(COMMAND ${ARGN} RESULT_VARIABLE rc
                  OUTPUT_QUIET ERROR_QUIET)
  if(NOT rc EQUAL 0)
    message(FATAL_ERROR "fixture step failed (${rc}): ${ARGN}")
  endif()
endfunction()

# CA-signed leaf: name is the stem, cn the subject, days the validity (0 means
# expired-at-generation), ext the subjectAltName text (may be empty).
function(sign_leaf name cn days ext)
  set(extfile "${OUT}/${name}.ext")
  if(ext STREQUAL "")
    file(WRITE "${extfile}" "basicConstraints=CA:FALSE\n")
  else()
    file(WRITE "${extfile}" "${ext}\n")
  endif()
  run_ossl(${OSSL} req -newkey rsa:2048 -nodes
           -keyout "${OUT}/${name}.key" -out "${OUT}/${name}.csr"
           -subj "/CN=${cn}")
  run_ossl(${OSSL} x509 -req -in "${OUT}/${name}.csr"
           -CA "${OUT}/ca.crt" -CAkey "${OUT}/ca.key" -CAcreateserial
           -out "${OUT}/${name}.crt" -days ${days} -extfile "${extfile}")
  file(REMOVE "${OUT}/${name}.csr" "${extfile}")
endfunction()

# Test CA.
run_ossl(${OSSL} req -x509 -newkey rsa:2048 -nodes
         -keyout "${OUT}/ca.key" -out "${OUT}/ca.crt" -days 3650
         -subj "/CN=duct-test-ca")

# Server leaf for the positive cases (name verification against localhost).
sign_leaf(server localhost 365 "subjectAltName=DNS:localhost,IP:127.0.0.1")
# Negative: chain is valid, but the hostname does not match (no IP SAN, so an
# IP-literal dial to 127.0.0.1 fails name verification).
sign_leaf(wronghost evil.example.com 365
          "subjectAltName=DNS:evil.example.com")
# Negative: CA-signed but expired at generation time (tests run after the
# build, so notAfter is always in the past by then).
sign_leaf(expired localhost 0 "subjectAltName=DNS:localhost,IP:127.0.0.1")
# Negative: not from the CA at all.
run_ossl(${OSSL} req -x509 -newkey rsa:2048 -nodes
         -keyout "${OUT}/untrusted.key" -out "${OUT}/untrusted.crt"
         -days 365 -subj "/CN=localhost"
         -addext "subjectAltName=DNS:localhost,IP:127.0.0.1")
# Negative: a key that does not match server.crt.
run_ossl(${OSSL} genrsa -out "${OUT}/wrongkey.key" 2048)
# mTLS client leaf.
sign_leaf(client duct-test-client 365 "")

message(STATUS "TLS test fixtures generated in ${OUT}")

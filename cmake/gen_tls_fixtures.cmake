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
#   expired.crt / expired.key  CA-signed leaf whose validity ended in the
#                              past (the only failure is expiry, not the chain)
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

# CA-signed leaf: name is the stem, cn the subject, validity the x509 validity
# flags as a ;-list (e.g. "-days;365"), ext the subjectAltName text (may be
# empty). The list form keeps each flag and its value as separate argv entries
# after the ${validity} expansion.
function(sign_leaf name cn validity ext)
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
           -out "${OUT}/${name}.crt" ${validity} -extfile "${extfile}")
  file(REMOVE "${OUT}/${name}.csr" "${extfile}")
endfunction()

# Expired-fixture validity, per openssl family:
#   < 4.0: "-days 0" signs with notAfter == generation time (expires at once).
#   >= 4.0: option parsing rejects non-positive -days outright, but 4.x always
#     has -not_before/-not_after, so pin a fixed past validity window instead
#     (deterministic and always already expired — no clock math needed).
execute_process(COMMAND ${OSSL} version OUTPUT_VARIABLE ossl_version
                OUTPUT_STRIP_TRAILING_WHITESPACE RESULT_VARIABLE ossl_rc)
if(NOT ossl_rc EQUAL 0)
  message(FATAL_ERROR "cannot run ${OSSL} version")
endif()
string(REGEX MATCH "^OpenSSL [0-9]+" ossl_head "${ossl_version}")
string(REGEX REPLACE "^OpenSSL " "" ossl_major "${ossl_head}")
if(ossl_major GREATER_EQUAL 4)
  set(EXPIRED_VALIDITY "-not_before;20190101000000Z;-not_after;20200101000000Z")
else()
  set(EXPIRED_VALIDITY "-days;0")
endif()

# Test CA.
run_ossl(${OSSL} req -x509 -newkey rsa:2048 -nodes
         -keyout "${OUT}/ca.key" -out "${OUT}/ca.crt" -days 3650
         -subj "/CN=duct-test-ca")

# Server leaf for the positive cases (name verification against localhost).
sign_leaf(server localhost "-days;365" "subjectAltName=DNS:localhost,IP:127.0.0.1")
# Negative: chain is valid, but the hostname does not match (no IP SAN, so an
# IP-literal dial to 127.0.0.1 fails name verification).
sign_leaf(wronghost evil.example.com "-days;365"
          "subjectAltName=DNS:evil.example.com")
# Negative: CA-signed but expired (tests run long after the validity window,
# so notAfter is always in the past by then).
sign_leaf(expired localhost "${EXPIRED_VALIDITY}"
          "subjectAltName=DNS:localhost,IP:127.0.0.1")
# Negative: not from the CA at all.
run_ossl(${OSSL} req -x509 -newkey rsa:2048 -nodes
         -keyout "${OUT}/untrusted.key" -out "${OUT}/untrusted.crt"
         -days 365 -subj "/CN=localhost"
         -addext "subjectAltName=DNS:localhost,IP:127.0.0.1")
# Negative: a key that does not match server.crt.
run_ossl(${OSSL} genrsa -out "${OUT}/wrongkey.key" 2048)
# mTLS client leaf.
sign_leaf(client duct-test-client "-days;365" "")

message(STATUS "TLS test fixtures generated in ${OUT}")

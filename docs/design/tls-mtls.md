# TLS / mTLS Proposal (2026-11-21)

Status: **implemented 2026-11-22** (the recommended option was signed off at
every decision point T1–T5). TLS 1.3 over OpenSSL 3 is available on `tcp://`
via opt-in `TlsOptions` on `dial()`/`listen()`; a build without OpenSSL
(`DUCT_BUILD_TLS` off) reports `tls_available() == false` and rejects
`TlsOptions.enabled` with `kNotSupported`. `tls://` addresses remain unknown
at the address layer (`Address::parse` reports `kInvalidArgument`, pinned by
`test_tls_scheme_rejected_as_unsupported`) — TLS is a `tcp://` option, not a
scheme. Test fixtures are build-time generated (never committed as production
material); the one manual smoke with real credentials stays a user task.

See also [capabilities and reserved contracts](capabilities.md).

## Goal

Encrypt and authenticate `tcp://` connections with TLS 1.3, with optional
mutual authentication (mTLS) for server-to-server game backends.

## Scope

`tcp://` only in v1. `uds://` already carries process identity via
`SO_PEERCRED` (`duct.h` peer credentials) and local links are commonly
considered trusted; `shm://`/`pipe://` are same-host by construction. TLS for
`uds://` can follow later without protocol impact.

## Design

### Layering

TLS wraps the raw socket **before** wire framing: TCP connect → TLS handshake
→ existing `wire::handshake` capability negotiation (rides encrypted) →
frames. The reconnect wrapper re-runs the full TLS handshake on every redial;
session resumption is a v2 concern.

### Dependency

**T1** — OpenSSL 3.x (recommended: one API across Linux / macOS / mingw; MSVC
consumers take it via vcpkg; CMake `find_package(OpenSSL)` + exported target)
vs platform TLS backends (SChannel + SecureTransport: zero external
dependency, but three backends to keep in lockstep).

### Addressing

**T2** — keep `tcp://` and opt in per dial/listen via `DialOptions.tls` /
`ListenOptions.tls` (recommended: addresses stay identical between plain and
encrypted deployments) vs a new `tls://` scheme (explicit in the address, but
splits URLs across deployments and doubles the scheme matrix).

### Configuration API

**T3** — a `TlsOptions` struct on both `DialOptions` and `ListenOptions`,
source-compatible like the existing reserved fields:

- `enabled` (default false)
- `ca_path` — trust anchors; empty = system store
- `cert_path` + `key_path` — client optional, required on the server
- `verify_peer_name` (client, default true; hostname/SAN check)
- `require_client_cert` (server, default false — **T4**: flipped true it is mTLS)
- minimum version pinned to TLS 1.3; no cipher-suites knob (fixed modern
  policy; a knob invites misconfiguration)

### Certificate provisioning (user-owned)

Real key/cert material and trust-store layout are a deployment decision:
whether the fleet runs an internal CA, how keys are distributed to hosts, and
rotation policy. The library only consumes paths — it never generates or
stores production credentials.

### Failure modes

Handshake failure maps to `kIoError` on dial (connection refused class);
certificate verification failure maps to `kInvalidArgument` with the verify
error surfaced. A failed TLS handshake never reaches wire framing, so the
existing capability handshake and framing code stay untouched.

## Decision points — signed off 2026-11-22 (all recommended options)

- **T1 — dependency**: **OpenSSL 3** (over platform backends).
- **T2 — addressing**: **`tcp://` + `TlsOptions` opt-in** (over a `tls://`
  scheme).
- **T3 — config shape**: **`TlsOptions` as listed above** (accepted as
  proposed).
- **T4 — mTLS default**: **`require_client_cert` default false** (explicit
  opt-in per listener, over true fleet-wide).
- **T5 — acceptance**: **in-repo self-signed test fixtures** (test-only CA +
  leaf pair, clearly marked, generated at build time — never committed as
  production material), negative tests (expired / wrong-host / untrusted /
  wrong-key), plus one manual smoke with real credentials supplied by the
  user.

## Non-goals

No TLS on `uds://`/`shm://`/`pipe://` in v1; no certificate rotation or
management; no OCSP/CRL checking in v1; no session resumption in v1.

## Acceptance criteria

Met 2026-11-22 (T1–T5 signed off earlier that day):

- TLS 1.3 on `tcp://` via `TlsOptions` (client-side verification, optional
  mTLS), layered before the wire handshake, build-gated by `DUCT_BUILD_TLS`.
- `test_tls_scheme_rejected_as_unsupported` stays: the `tls://` scheme is still
  unknown because TLS is a `tcp://` option, not a scheme. The positive and
  negative handshake cases live in `tests/tls_tests.cc` against build-time
  generated fixtures (`cmake/gen_tls_fixtures.cmake`): echo round-trip,
  reliable echo, expired cert, wrong host (rejected, plus accepted when the
  name check is skipped), untrusted CA, mismatched server key, missing server
  cert/key, mTLS, and mTLS with a client that presents no certificate.
- Full suite green (134 tests) on GCC/Linux, and green under wine on the
  mingw gate with TLS compiled out (the intended degradation, not a gap).
- The "no TLS" line moved to the implemented table in
  [capabilities](capabilities.md) in the same change.

Remaining: the single manual smoke with real credentials (user-provided).

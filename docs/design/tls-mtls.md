# TLS / mTLS Proposal (2026-11-21)

Status: **proposal, not implemented**. There is no TLS dependency in the build
(no OpenSSL in CMake) and no certificate configuration API; `tls://` addresses
are rejected at the address layer: `Address::parse` reports `kInvalidArgument`
("unknown scheme") before any scheme dispatch (pinned by
`test_tls_scheme_rejected_as_unsupported`). Implementation is gated on the
user signing off the decision points T1–T5 and on the user provisioning real
credentials — test fixtures are not production certificates.

See also [capabilities and reserved contracts](capabilities.md) — "no TLS or
other credential layers" stays accurate until this proposal is implemented.

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

## Decision points (user sign-off required)

- **T1 — dependency**: OpenSSL 3 (recommended) vs platform backends.
- **T2 — addressing**: `tcp://` + `TlsOptions` opt-in (recommended) vs
  `tls://` scheme.
- **T3 — config shape**: `TlsOptions` as listed above.
- **T4 — mTLS default**: `require_client_cert` default false (recommended;
  explicit opt-in per listener) vs true fleet-wide.
- **T5 — acceptance**: in-repo self-signed test fixtures (test-only CA +
  leaf pair, clearly marked, generated at build time — never committed as
  production material), negative tests (expired / wrong-host / untrusted /
  wrong-key), plus one manual smoke with real credentials supplied by the
  user.

## Non-goals

No TLS on `uds://`/`shm://`/`pipe://` in v1; no certificate rotation or
management; no OCSP/CRL checking in v1; no session resumption in v1.

## Acceptance criteria (future work, gated on T1–T5)

Implement the TLS transport wrapper, replace
`test_tls_scheme_rejected_as_unsupported` with positive and negative
handshake tests, keep the full existing suite green, and move the
"no TLS" line in [capabilities](capabilities.md) to the implemented table in
the same change.

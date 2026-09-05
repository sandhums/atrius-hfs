---
name: work-with-auth
description: Work on HFS authentication & authorization. Use for SMART-on-FHIR/OAuth2, JWT bearer validation, JWKS, scopes/permissions, token replay semantics, SMART discovery, and HFS_AUTH_* configuration.
---

# Authentication & Authorization

Use this when working in `helios-auth` or HFS auth behavior. The crate validates SMART-on-FHIR / OAuth2 JWT bearer tokens and enforces scopes. It is consumed by `helios-rest`, `helios-hfs`, and `helios-subscriptions`.

## Behavior

- Disabled by default. Set `HFS_AUTH_ENABLED=true` to require bearer tokens.
- Validates JWTs against a JWKS endpoint (`HFS_AUTH_JWKS_URL`), checking issuer, audience, and signing algorithm.
- Enforces SMART v2 scopes (e.g. `system/Patient.rs`) via `SmartScopePolicy` / `ScopeSet` / `SmartPermissions`.
- Derives the tenant from a JWT claim (`HFS_AUTH_TENANT_CLAIM`, default `tenant_id`).
- Serves `/.well-known/smart-configuration` (SMART discovery), populated from the `HFS_SMART_*` endpoints.
- Holds no cross-instance state: validation is local, so instances behind a load balancer need no shared infrastructure.

## Environment

| Variable | Default | Description |
|---|---|---|
| `HFS_AUTH_ENABLED` | `false` | Require bearer-token auth |
| `HFS_AUTH_JWKS_URL` | none | JWKS endpoint for token signature verification |
| `HFS_AUTH_ISSUER` | none | Expected `iss` |
| `HFS_AUTH_AUDIENCE` | none | Expected `aud` |
| `HFS_AUTH_ALGORITHMS` | `RS256,RS384,ES256,ES384` | Allowed signing algorithms |
| `HFS_AUTH_TENANT_CLAIM` | `tenant_id` | JWT claim used to resolve the tenant |
| `HFS_AUTH_JWKS_MIN_REFRESH_INTERVAL` | `10` | Minimum seconds between JWKS refreshes |

SMART discovery passthrough (advertised in `/.well-known/smart-configuration`): `HFS_SMART_TOKEN_ENDPOINT`, `HFS_SMART_AUTHORIZE_ENDPOINT`, `HFS_SMART_JWKS_URL`, `HFS_SMART_INTROSPECTION_ENDPOINT`, `HFS_SMART_MANAGEMENT_ENDPOINT`, `HFS_SMART_REGISTRATION_ENDPOINT`, `HFS_SMART_REVOCATION_ENDPOINT`. Outbound calls can carry `HFS_OUTBOUND_BEARER_TOKEN`.

## Token replay

There is **no `jti` replay cache** — bearer access tokens are accepted as many
times as the client presents them until they expire. Do not reintroduce one
(#205): single-use `jti` semantics belong to `private_key_jwt` client assertions
(RFC 7523 §3), which the authorization server's token endpoint enforces. HFS is
a resource server with no token endpoint, so every `jti` it sees belongs to a
*reusable* access token (Keycloak and most IdPs emit one on all of them), and
replay-checking it rejects the second and every later use of a perfectly valid
token.

A replay cache cannot defend a bearer token anyway: a replayed one is
byte-identical to a legitimate one. Real anti-replay needs sender-constrained
tokens (DPoP/mTLS), where a fresh per-request proof — not the access token — is
what gets checked. `crates/auth/tests/bearer_token_reuse.rs` pins this behavior.

## JTI revocation (deny-list)

Separate from replay: after BFF logout/refresh, the access token is still valid
until `exp`. The BFF writes `SET hfs:revoked:jti:<jti> EX <ttl>` on the shared
Redis; HFS/HIS reject that `jti` when `HFS_AUTH_JTI_REVOCATION=true`.

- Requires `HFS_AUTH_REDIS_URL` and the `redis` feature on `helios-auth`.
- Tokens with no usable `jti` are rejected (they cannot be named on the list).
- Each `EXISTS` is bounded by `HFS_AUTH_JTI_REVOCATION_TIMEOUT_MS` (default 500).
  Timeout and Redis errors fail **closed** (`AuthError::RevocationUnavailable` →
  HTTP 503). Do not fail open: a partition after a successful logout would
  otherwise keep serving the revoked token.

| Variable | Default | Description |
|---|---|---|
| `HFS_AUTH_JTI_REVOCATION` | `false` | Consult the Redis deny-list |
| `HFS_AUTH_REDIS_URL` | none | Redis URL (required when revocation is enabled) |
| `HFS_AUTH_JTI_REVOCATION_TIMEOUT_MS` | `500` | Per-request Redis `EXISTS` budget |

## Key API

`AuthConfig`, `AuthProvider` / `JwksBearerAuthProvider`, `Principal`, `ScopeSet` / `SmartPermissions`, `SmartScopePolicy`, `JwksCache`, `SmartConfiguration`.

## Code map

`config.rs`, `provider/`, `jwks/` (cache, fetcher), `jti/` (deny-list), `scope/` (smart_v2, permissions), `policy/`, `principal.rs`, `discovery.rs`, `outbound.rs`. Auth scopes are consumed by Bulk Data Submit (`system/bulk-submit`). Unit tests are inline in `src/`; integration tests live under `crates/auth/tests/`.

//! JTI revocation (deny-list) support.
//!
//! HFS is a resource server, so it never sees the single-use `private_key_jwt`
//! client assertions that JTI *replay caches* protect — those are consumed by
//! the IdP's token endpoint. What remains useful here is the opposite concern:
//! rejecting bearer access tokens whose `jti` was explicitly *revoked* before
//! expiry (e.g. after logout), via a Redis blocklist shared with the BFF.
//!
//! When the Redis checker is enabled, tokens without a `jti` are rejected:
//! they cannot be named on the blocklist, so logout would be a no-op. Redis
//! `EXISTS` is bounded by [`crate::config::DEFAULT_JTI_REVOCATION_TIMEOUT_MS`]
//! and fails closed (`AuthError::RevocationUnavailable`) on timeout or error.

pub mod revocation;

pub use crate::config::DEFAULT_JTI_REVOCATION_TIMEOUT_MS;
pub use revocation::{
    JtiRevocation, NoOpJtiRevocation, REVOKED_JTI_KEY_PREFIX, build_jti_revocation,
};

#[cfg(feature = "redis")]
pub use revocation::RedisJtiRevocation;

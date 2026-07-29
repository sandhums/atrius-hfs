//! JTI revocation (deny-list) support.
//!
//! HFS is a resource server, so it never sees the single-use `private_key_jwt`
//! client assertions that JTI *replay caches* protect — those are consumed by
//! the IdP's token endpoint. What remains useful here is the opposite concern:
//! rejecting bearer access tokens whose `jti` was explicitly *revoked* before
//! expiry (e.g. after logout), via a Redis blocklist shared with the BFF.

pub mod revocation;

pub use revocation::{
    JtiRevocation, NoOpJtiRevocation, REVOKED_JTI_KEY_PREFIX, build_jti_revocation,
};

#[cfg(feature = "redis")]
pub use revocation::RedisJtiRevocation;

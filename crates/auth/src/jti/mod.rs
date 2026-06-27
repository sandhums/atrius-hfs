pub mod memory;
pub mod revocation;

pub use revocation::{
    NoOpJtiRevocation, REVOKED_JTI_KEY_PREFIX, build_jti_revocation, JtiRevocation,
};
#[cfg(feature = "redis")]
pub mod redis;


#[cfg(feature = "redis")]
pub use revocation::RedisJtiRevocation;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::AuthError;

/// Trait for JWT ID (jti) tracking.
///
/// For OAuth **access tokens**, the same `jti` appears on every request until
/// expiry — implementations must **not** treat a duplicate as a replay attack.
/// Use [`DisabledJtiCache`] when no tracking is needed.
#[async_trait]
pub trait JtiCache: Send + Sync + 'static {
    /// Record a JTI if new. Always returns `false` (not a replay) for valid
    /// reusable bearer access tokens.
    async fn check_and_store(
        &self,
        jti: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, AuthError>;
}

/// JTI cache implementation which never treats tokens as replays.
///
/// This is intended for deployments where JWT IDs identify reusable bearer
/// access tokens rather than one-time client assertions.
#[derive(Debug, Clone, Copy, Default)]
pub struct DisabledJtiCache;

#[async_trait]
impl JtiCache for DisabledJtiCache {
    async fn check_and_store(
        &self,
        _jti: &str,
        _expires_at: DateTime<Utc>,
    ) -> Result<bool, AuthError> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_cache_never_reports_replay() {
        let cache = DisabledJtiCache;
        let expires = Utc::now();

        assert!(!cache.check_and_store("same-jti", expires).await.unwrap());
        assert!(!cache.check_and_store("same-jti", expires).await.unwrap());
    }
}

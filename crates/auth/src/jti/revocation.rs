use std::sync::Arc;

use async_trait::async_trait;

use crate::config::AuthConfig;
use crate::error::AuthError;

/// Redis key prefix for revoked access-token JTIs.
///
/// The BFF writes `SET hfs:revoked:jti:<jti> EX <ttl>` on logout and token refresh;
/// HFS/HIS check this set before accepting a bearer token.
pub const REVOKED_JTI_KEY_PREFIX: &str = "hfs:revoked:jti:";

/// Checks whether an access token JTI has been explicitly revoked (e.g. after logout).
#[async_trait]
pub trait JtiRevocation: Send + Sync + 'static {
    async fn is_revoked(&self, jti: &str) -> Result<bool, AuthError>;
}

/// No-op revocation checker (default when revocation is disabled).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpJtiRevocation;

#[async_trait]
impl JtiRevocation for NoOpJtiRevocation {
    async fn is_revoked(&self, _jti: &str) -> Result<bool, AuthError> {
        Ok(false)
    }
}

#[cfg(feature = "redis")]
mod redis_impl {
    use super::*;
    use redis::AsyncCommands;

    /// Redis-backed JTI revocation blocklist (shared with BFF).
    pub struct RedisJtiRevocation {
        client: redis::Client,
    }

    impl RedisJtiRevocation {
        pub fn new(redis_url: &str) -> Result<Self, AuthError> {
            let client = redis::Client::open(redis_url).map_err(|e| {
                AuthError::InternalError(format!("Redis connection error: {}", e))
            })?;
            Ok(Self { client })
        }

        fn key(jti: &str) -> String {
            format!("{}{}", REVOKED_JTI_KEY_PREFIX, jti)
        }
    }

    #[async_trait]
    impl JtiRevocation for RedisJtiRevocation {
        async fn is_revoked(&self, jti: &str) -> Result<bool, AuthError> {
            let mut conn = self.client.get_multiplexed_async_connection().await.map_err(
                |e| AuthError::InternalError(format!("Redis connection error: {}", e)),
            )?;
            let exists: bool = conn.exists(Self::key(jti)).await.map_err(|e| {
                AuthError::InternalError(format!("Redis EXISTS error: {}", e))
            })?;
            Ok(exists)
        }
    }
}

#[cfg(feature = "redis")]
pub use redis_impl::RedisJtiRevocation;

/// Build a JTI revocation checker from [`AuthConfig`].
pub fn build_jti_revocation(config: &AuthConfig) -> Result<Arc<dyn JtiRevocation>, AuthError> {
    if !config.jti_revocation_enabled {
        return Ok(Arc::new(NoOpJtiRevocation));
    }

    #[cfg(feature = "redis")]
    {
        let url = config.redis_url.as_ref().ok_or_else(|| {
            AuthError::InternalError(
                "HFS_AUTH_REDIS_URL is required when HFS_AUTH_JTI_REVOCATION=true".into(),
            )
        })?;
        Ok(Arc::new(RedisJtiRevocation::new(url)?))
    }

    #[cfg(not(feature = "redis"))]
    {
        let _ = config;
        Err(AuthError::InternalError(
            "JTI revocation requires the 'redis' feature on helios-auth".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_never_revokes() {
        let checker = NoOpJtiRevocation;
        assert!(!checker.is_revoked("any-jti").await.unwrap());
    }
}

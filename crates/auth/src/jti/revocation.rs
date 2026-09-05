use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::config::AuthConfig;
use crate::error::AuthError;

/// Redis key prefix for revoked access-token JTIs.
///
/// The BFF writes `SET hfs:revoked:jti:<jti> EX <ttl>` on logout and token refresh;
/// HFS/HIS check this set before accepting a bearer token.
pub const REVOKED_JTI_KEY_PREFIX: &str = "hfs:revoked:jti:";

/// How long to wait for Redis itself when opening the connection manager at boot.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Checks whether an access token JTI has been explicitly revoked (e.g. after logout).
#[async_trait]
pub trait JtiRevocation: Send + Sync + 'static {
    /// When true, tokens without a usable `jti` are rejected.
    ///
    /// A deny-list cannot cover a token it cannot name. `NoOpJtiRevocation`
    /// leaves this false so local/dev tokens without `jti` still authenticate.
    fn requires_jti(&self) -> bool {
        false
    }

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

/// Run `fut` and map elapsed time to [`AuthError::RevocationUnavailable`].
async fn with_timeout<T>(
    timeout: Duration,
    fut: impl Future<Output = Result<T, AuthError>>,
) -> Result<T, AuthError> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_elapsed) => {
            tracing::warn!(
                timeout_ms = timeout.as_millis() as u64,
                "JTI revocation check timed out; failing closed"
            );
            Err(AuthError::RevocationUnavailable)
        }
    }
}

#[cfg(feature = "redis")]
mod redis_impl {
    use super::*;
    use redis::AsyncCommands;
    use redis::aio::ConnectionManager;

    /// Redis-backed JTI revocation blocklist (shared with BFF).
    pub struct RedisJtiRevocation {
        conn: ConnectionManager,
        timeout: Duration,
    }

    impl RedisJtiRevocation {
        /// Connect with a multiplexed [`ConnectionManager`] (reconnects; clones
        /// share the underlying connection — same pattern as the BFF store).
        pub async fn connect(redis_url: &str, timeout: Duration) -> Result<Self, AuthError> {
            let client = redis::Client::open(redis_url)
                .map_err(|e| AuthError::InternalError(format!("Redis connection error: {e}")))?;
            let conn = tokio::time::timeout(CONNECT_TIMEOUT, ConnectionManager::new(client))
                .await
                .map_err(|_elapsed| {
                    AuthError::InternalError(
                        "Redis connection timed out while enabling JTI revocation".into(),
                    )
                })?
                .map_err(|e| AuthError::InternalError(format!("Redis connection error: {e}")))?;
            Ok(Self { conn, timeout })
        }

        fn key(jti: &str) -> String {
            format!("{}{}", REVOKED_JTI_KEY_PREFIX, jti)
        }
    }

    #[async_trait]
    impl JtiRevocation for RedisJtiRevocation {
        fn requires_jti(&self) -> bool {
            true
        }

        async fn is_revoked(&self, jti: &str) -> Result<bool, AuthError> {
            let mut conn = self.conn.clone();
            let key = Self::key(jti);
            with_timeout(self.timeout, async move {
                let exists: bool = conn.exists(key).await.map_err(|e| {
                    tracing::warn!(error = %e, "Redis JTI EXISTS failed; failing closed");
                    AuthError::RevocationUnavailable
                })?;
                Ok(exists)
            })
            .await
        }
    }
}

#[cfg(feature = "redis")]
pub use redis_impl::RedisJtiRevocation;

/// Build a JTI revocation checker from [`AuthConfig`].
pub async fn build_jti_revocation(
    config: &AuthConfig,
) -> Result<Arc<dyn JtiRevocation>, AuthError> {
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
        let timeout = Duration::from_millis(config.jti_revocation_timeout_ms.max(1));
        Ok(Arc::new(RedisJtiRevocation::connect(url, timeout).await?))
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
        assert!(!checker.requires_jti());
    }

    #[tokio::test]
    async fn timeout_fails_closed() {
        let result = with_timeout(
            Duration::from_millis(20),
            std::future::pending::<Result<bool, AuthError>>(),
        )
        .await;
        assert!(matches!(result, Err(AuthError::RevocationUnavailable)));
    }

    #[tokio::test]
    async fn build_disabled_returns_noop() {
        let checker = match build_jti_revocation(&AuthConfig::default()).await {
            Ok(c) => c,
            Err(e) => panic!("noop: {e}"),
        };
        assert!(!checker.requires_jti());
        assert!(!checker.is_revoked("x").await.unwrap());
    }

    #[tokio::test]
    async fn build_enabled_without_redis_url_fails() {
        let config = AuthConfig {
            jti_revocation_enabled: true,
            redis_url: None,
            ..AuthConfig::default()
        };
        let err = match build_jti_revocation(&config).await {
            Ok(_) => panic!("must fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("REDIS") || err.to_string().contains("redis"),
            "got {err}"
        );
    }
}

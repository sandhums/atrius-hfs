//! Outbound auth for cds-server → clinical HFS writes (GuidanceResponse / Flag).
//!
//! Precedence:
//! 1. `CDS_FEEDBACK_FHIR_BEARER_TOKEN` — static bearer (smoke / short-lived override)
//! 2. `CDS_FEEDBACK_OAUTH_*` — OAuth2 `client_credentials` with cached access token
//! 3. none — writes go unauthenticated (401 when HFS auth is on)

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Resolves a bearer access token for FHIR write requests.
#[async_trait]
pub trait FhirWriteAuth: Send + Sync {
    /// Human-readable mode for startup logs (`static-bearer`, `client-credentials`, `none`).
    fn mode(&self) -> &'static str;

    /// Current access token, or `None` when writes are unauthenticated.
    async fn bearer_token(&self) -> Result<Option<String>, String>;
}

/// No credentials attached.
#[derive(Debug, Default)]
pub struct NoFhirWriteAuth;

#[async_trait]
impl FhirWriteAuth for NoFhirWriteAuth {
    fn mode(&self) -> &'static str {
        "none"
    }

    async fn bearer_token(&self) -> Result<Option<String>, String> {
        Ok(None)
    }
}

/// Fixed bearer from `CDS_FEEDBACK_FHIR_BEARER_TOKEN`.
#[derive(Debug)]
pub struct StaticBearerAuth {
    token: String,
}

impl StaticBearerAuth {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait]
impl FhirWriteAuth for StaticBearerAuth {
    fn mode(&self) -> &'static str {
        "static-bearer"
    }

    async fn bearer_token(&self) -> Result<Option<String>, String> {
        Ok(Some(self.token.clone()))
    }
}

/// OAuth2 client-credentials grant with in-memory cache.
pub struct ClientCredentialsAuth {
    http: reqwest::Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
    cache: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: u64,
}

impl ClientCredentialsAuth {
    pub fn new(
        http: reqwest::Client,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        scope: Option<String>,
    ) -> Self {
        Self {
            http,
            token_url: token_url.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            scope: scope.filter(|s| !s.trim().is_empty()),
            cache: Mutex::new(None),
        }
    }

    async fn fetch_token(&self) -> Result<CachedToken, String> {
        let mut form = vec![
            ("grant_type", "client_credentials".to_string()),
            ("client_id", self.client_id.clone()),
            ("client_secret", self.client_secret.clone()),
        ];
        if let Some(ref scope) = self.scope {
            form.push(("scope", scope.clone()));
        }

        let resp = self
            .http
            .post(&self.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| format!("client_credentials token request failed: {e}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| format!("client_credentials token body: {e}"))?;
        if !status.is_success() {
            return Err(format!(
                "client_credentials → {status}: {}",
                body.chars().take(300).collect::<String>()
            ));
        }

        let parsed: TokenResponse = serde_json::from_str(&body).map_err(|e| {
            format!(
                "client_credentials JSON: {e}: {}",
                body.chars().take(200).collect::<String>()
            )
        })?;

        let expires_in = if parsed.expires_in == 0 {
            300
        } else {
            parsed.expires_in
        };
        // Refresh ~30s before expiry (floor 1s).
        let ttl = expires_in.saturating_sub(30).max(1);
        debug!(
            client_id = %self.client_id,
            expires_in,
            cache_ttl_secs = ttl,
            "cds feedback client_credentials token minted"
        );
        Ok(CachedToken {
            access_token: parsed.access_token,
            expires_at: Instant::now() + Duration::from_secs(ttl),
        })
    }
}

#[async_trait]
impl FhirWriteAuth for ClientCredentialsAuth {
    fn mode(&self) -> &'static str {
        "client-credentials"
    }

    async fn bearer_token(&self) -> Result<Option<String>, String> {
        {
            let guard = self.cache.lock().await;
            if let Some(ref cached) = *guard
                && Instant::now() < cached.expires_at
            {
                return Ok(Some(cached.access_token.clone()));
            }
        }

        let minted = self.fetch_token().await?;
        let token = minted.access_token.clone();
        *self.cache.lock().await = Some(minted);
        Ok(Some(token))
    }
}

/// Build write-auth from env-derived pieces.
///
/// Static bearer wins over client-credentials when both are set.
pub fn build_fhir_write_auth(
    http: reqwest::Client,
    static_bearer: Option<&str>,
    token_url: Option<&str>,
    client_id: Option<&str>,
    client_secret: Option<&str>,
    scope: Option<&str>,
) -> Arc<dyn FhirWriteAuth> {
    if let Some(token) = static_bearer.map(str::trim).filter(|s| !s.is_empty()) {
        return Arc::new(StaticBearerAuth::new(token));
    }

    let token_url = token_url.map(str::trim).filter(|s| !s.is_empty());
    let client_id = client_id.map(str::trim).filter(|s| !s.is_empty());
    let client_secret = client_secret.map(str::trim).filter(|s| !s.is_empty());

    match (token_url, client_id, client_secret) {
        (Some(url), Some(id), Some(secret)) => Arc::new(ClientCredentialsAuth::new(
            http,
            url,
            id,
            secret,
            scope
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        )),
        (None, None, None) => Arc::new(NoFhirWriteAuth),
        _ => {
            warn!(
                "incomplete CDS_FEEDBACK_OAUTH_* (need TOKEN_URL + CLIENT_ID + CLIENT_SECRET); \
                 FHIR writes will be unauthenticated"
            );
            Arc::new(NoFhirWriteAuth)
        }
    }
}

/// Attach bearer (when present) to a request builder.
pub async fn authorize_request(
    auth: &dyn FhirWriteAuth,
    mut req: reqwest::RequestBuilder,
) -> Result<reqwest::RequestBuilder, String> {
    if let Some(token) = auth.bearer_token().await? {
        req = req.bearer_auth(token);
    }
    Ok(req)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn static_bearer_returns_token() {
        let auth = StaticBearerAuth::new("tok-123");
        assert_eq!(auth.mode(), "static-bearer");
        assert_eq!(
            auth.bearer_token().await.unwrap().as_deref(),
            Some("tok-123")
        );
    }

    #[tokio::test]
    async fn client_credentials_mints_and_caches() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("client_id=cds-backend-client"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "access-1",
                "expires_in": 300,
                "token_type": "Bearer"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let auth = ClientCredentialsAuth::new(
            reqwest::Client::new(),
            format!("{}/token", server.uri()),
            "cds-backend-client",
            "cds-backend-secret",
            None,
        );

        assert_eq!(
            auth.bearer_token().await.unwrap().as_deref(),
            Some("access-1")
        );
        // Second call hits cache — mock expects only one POST.
        assert_eq!(
            auth.bearer_token().await.unwrap().as_deref(),
            Some("access-1")
        );
    }

    #[tokio::test]
    async fn build_prefers_static_over_oauth() {
        let auth = build_fhir_write_auth(
            reqwest::Client::new(),
            Some("static-tok"),
            Some("http://example/token"),
            Some("id"),
            Some("secret"),
            None,
        );
        assert_eq!(auth.mode(), "static-bearer");
        assert_eq!(
            auth.bearer_token().await.unwrap().as_deref(),
            Some("static-tok")
        );
    }

    #[tokio::test]
    async fn build_oauth_when_complete() {
        let auth = build_fhir_write_auth(
            reqwest::Client::new(),
            None,
            Some("http://example/token"),
            Some("cds-backend-client"),
            Some("secret"),
            Some("system/*.cruds"),
        );
        assert_eq!(auth.mode(), "client-credentials");
    }

    #[tokio::test]
    async fn build_none_when_incomplete_oauth() {
        let auth = build_fhir_write_auth(
            reqwest::Client::new(),
            None,
            Some("http://example/token"),
            Some("cds-backend-client"),
            None,
            None,
        );
        assert_eq!(auth.mode(), "none");
    }
}

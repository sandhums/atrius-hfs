//! Shared HTTP client for a FHIR terminology server (HTS or compatible).
//!
//! Used by the REST search `:in` path, write-path/`$validate` bindings,
//! FHIRPath `%terminologies`, and the UI ValueSet picker. A process-wide TTL
//! cache is shared across those callers so a miss in one is a hit in another.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use reqwest::{Client, RequestBuilder, Response};
use serde_json::{Value, json};

const VALIDATE_TTL: Duration = Duration::from_secs(300);
const EXPAND_TTL: Duration = Duration::from_secs(300);
const FHIR_JSON: &str = "application/fhir+json";

/// Three retries after the initial request, with 1s, 2s and 4s backoff.
const GATEWAY_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

/// Errors talking to a terminology server.
#[derive(Debug, thiserror::Error)]
pub enum TerminologyError {
    /// The HTTP request itself failed (network, DNS, timeout).
    #[error("Terminology server request failed: {0}")]
    Network(String),

    /// The server responded with a non-2xx status code.
    #[error("Terminology server returned HTTP {status}: {body}")]
    ServerError {
        /// HTTP status code.
        status: u16,
        /// Response body (optionally truncated).
        body: String,
    },

    /// The response body could not be parsed as expected.
    #[error("Failed to parse terminology server response: {0}")]
    Parse(String),
}

/// A single expanded code from a FHIR ValueSet expansion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandedCode {
    /// Code system URL. May be empty.
    pub system: String,
    /// The code value.
    pub code: String,
    /// Optional human-readable display text.
    pub display: Option<String>,
}

impl ExpandedCode {
    /// Formats the code as `system|code` if a system is present, or just `code`.
    pub fn as_token(&self) -> String {
        if self.system.is_empty() {
            self.code.clone()
        } else {
            format!("{}|{}", self.system, self.code)
        }
    }
}

/// Construction options for [`TerminologyClient`].
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Request timeout. `None` means no timeout (reqwest default).
    pub timeout: Option<Duration>,
    /// TCP connect timeout.
    pub connect_timeout: Option<Duration>,
    /// Bypass HTTP(S)_PROXY. Search uses this so an internal HTS fails fast.
    pub no_proxy: bool,
    /// Truncate non-success response bodies to this many chars. `None` keeps all.
    pub truncate_error_body: Option<usize>,
}

impl ClientOptions {
    /// REST search `:in` / `:below` / `:above` — fail fast, skip proxy.
    pub fn rest_search() -> Self {
        Self {
            timeout: Some(Duration::from_secs(10)),
            connect_timeout: Some(Duration::from_secs(2)),
            no_proxy: true,
            truncate_error_body: Some(512),
        }
    }

    /// FHIRPath `%terminologies` — 30s unless the caller overrides `timeout`.
    pub fn fhirpath() -> Self {
        Self {
            timeout: Some(Duration::from_secs(30)),
            connect_timeout: None,
            no_proxy: false,
            truncate_error_body: None,
        }
    }

    /// Binding `$validate-code` (write path / `$validate`).
    pub fn validation(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            connect_timeout: None,
            no_proxy: false,
            truncate_error_body: None,
        }
    }

    /// UI ValueSet picker — sub-3s UX timeout.
    pub fn ui_expand() -> Self {
        Self {
            timeout: Some(Duration::from_millis(2500)),
            connect_timeout: None,
            no_proxy: false,
            truncate_error_body: None,
        }
    }
}

/// Async HTTP client for FHIR terminology operations.
#[derive(Clone)]
pub struct TerminologyClient {
    http: Client,
    base_url: String,
    truncate_error_body: Option<usize>,
}

impl TerminologyClient {
    /// Build a client targeting `base_url` (trailing slash is stripped).
    pub fn new(base_url: impl Into<String>, options: ClientOptions) -> Self {
        let mut builder = Client::builder();
        if let Some(timeout) = options.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(connect) = options.connect_timeout {
            builder = builder.connect_timeout(connect);
        }
        if options.no_proxy {
            builder = builder.no_proxy();
        }
        let http = builder.build().expect("terminology HTTP client");
        Self::with_http(http, base_url, options.truncate_error_body)
    }

    /// These terminology operations only read data, including those sent as POST.
    /// Retry transient gateway/service failures, but preserve transport, parsing and
    /// other HTTP errors. Each attempt retains the configured HTTP client timeout.
    async fn send_with_retry(
        &self,
        request: impl Fn() -> RequestBuilder,
    ) -> Result<Response, TerminologyError> {
        let mut delays = GATEWAY_RETRY_DELAYS.into_iter();
        loop {
            let response = request()
                .send()
                .await
                .map_err(|e| TerminologyError::Network(e.to_string()))?;
            // Cloudflare uses HTTP 530 for tunnel failures, including error 1033
            // when no healthy cloudflared instance can receive the request.
            if !matches!(response.status().as_u16(), 502 | 503 | 504 | 530) {
                return Ok(response);
            }
            let Some(delay) = delays.next() else {
                return Ok(response);
            };
            drop(response);
            tokio::time::sleep(delay).await;
        }
    }

    /// Wrap an existing reqwest client (tests, custom auth).
    pub fn with_http(
        http: Client,
        base_url: impl Into<String>,
        truncate_error_body: Option<usize>,
    ) -> Self {
        Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            truncate_error_body,
        }
    }

    /// Configured terminology-server root (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `POST {path}` with a FHIR Parameters (or other JSON) body.
    pub async fn post_json(&self, path: &str, body: &Value) -> Result<Value, TerminologyError> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .send_with_retry(|| {
                self.http
                    .post(&url)
                    .json(body)
                    .header("Content-Type", FHIR_JSON)
                    .header("Accept", FHIR_JSON)
            })
            .await?;
        self.read_json(response).await
    }

    /// `GET {path}` with query pairs.
    pub async fn get_query(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<Value, TerminologyError> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .send_with_retry(|| self.http.get(&url).query(query).header("Accept", FHIR_JSON))
            .await?;
        self.read_json(response).await
    }

    /// `POST /ValueSet/$expand` by canonical URL. Cached process-wide for [`EXPAND_TTL`].
    pub async fn expand_value_set(
        &self,
        value_set_url: &str,
    ) -> Result<Vec<ExpandedCode>, TerminologyError> {
        let key = format!("post|{value_set_url}");
        if let Some(hit) = cache_get(expand_cache(), &key, EXPAND_TTL) {
            return Ok(hit);
        }
        let body = json!({
            "resourceType": "Parameters",
            "parameter": [{ "name": "url", "valueUri": value_set_url }]
        });
        let value = self.post_json("/ValueSet/$expand", &body).await?;
        let codes = extract_expansion_codes(&value)?;
        expand_cache().insert(key, (codes.clone(), Instant::now()));
        Ok(codes)
    }

    /// Inline compose `$expand` for `:below` (`is-a`) / `:above` (`generalizes`).
    pub async fn expand_subsumption(
        &self,
        system: &str,
        code: &str,
        op: &str,
    ) -> Result<Vec<ExpandedCode>, TerminologyError> {
        let body = json!({
            "resourceType": "Parameters",
            "parameter": [{
                "name": "valueSet",
                "resource": {
                    "resourceType": "ValueSet",
                    "compose": {
                        "include": [{
                            "system": system,
                            "filter": [{ "property": "concept", "op": op, "value": code }]
                        }]
                    }
                }
            }]
        });
        let value = self.post_json("/ValueSet/$expand", &body).await?;
        extract_expansion_codes(&value)
    }

    /// `GET /ValueSet/$expand`.
    pub async fn expand_get(
        &self,
        value_set_url: &str,
        extra: &[(&str, &str)],
    ) -> Result<Value, TerminologyError> {
        let mut query: Vec<(&str, &str)> = vec![("url", value_set_url)];
        query.extend_from_slice(extra);
        self.get_query("/ValueSet/$expand", &query).await
    }

    /// `POST /ValueSet/$validate-code`, returning the Parameters `result` boolean.
    ///
    /// Cached process-wide under `cache_key` for [`VALIDATE_TTL`].
    pub async fn validate_code_bool(
        &self,
        cache_key: &str,
        body: &Value,
    ) -> Result<bool, TerminologyError> {
        if let Some(hit) = cache_get(validate_cache(), cache_key, VALIDATE_TTL) {
            return Ok(hit);
        }
        let value = self.post_json("/ValueSet/$validate-code", body).await?;
        let verdict = parameters_result_bool(&value)?;
        validate_cache().insert(cache_key.to_string(), (verdict, Instant::now()));
        Ok(verdict)
    }

    async fn read_json(&self, response: reqwest::Response) -> Result<Value, TerminologyError> {
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let mut body = response.text().await.unwrap_or_default();
            if let Some(max) = self.truncate_error_body {
                body = body.chars().take(max).collect();
            }
            return Err(TerminologyError::ServerError { status, body });
        }
        response
            .json()
            .await
            .map_err(|e| TerminologyError::Parse(e.to_string()))
    }
}

/// Pull `ExpandedCode` rows from a ValueSet `expansion.contains` array.
pub fn extract_expansion_codes(value: &Value) -> Result<Vec<ExpandedCode>, TerminologyError> {
    let contains = value
        .pointer("/expansion/contains")
        .and_then(|c| c.as_array())
        .ok_or_else(|| {
            TerminologyError::Parse(
                "ValueSet expansion response is missing /expansion/contains".to_string(),
            )
        })?;

    let mut codes = Vec::with_capacity(contains.len());
    for entry in contains {
        let code = entry
            .get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if code.is_empty() {
            continue;
        }
        let system = entry
            .get("system")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let display = entry
            .get("display")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        codes.push(ExpandedCode {
            system,
            code,
            display,
        });
    }
    Ok(codes)
}

/// `Parameters.parameter[name=result].valueBoolean`.
pub fn parameters_result_bool(body: &Value) -> Result<bool, TerminologyError> {
    body.get("parameter")
        .and_then(Value::as_array)
        .and_then(|params| {
            params
                .iter()
                .find(|p| p.get("name").and_then(Value::as_str) == Some("result"))
        })
        .and_then(|p| p.get("valueBoolean"))
        .and_then(Value::as_bool)
        .ok_or_else(|| TerminologyError::Parse("no boolean 'result' parameter in response".into()))
}

fn expand_cache() -> &'static DashMap<String, (Vec<ExpandedCode>, Instant)> {
    static CACHE: OnceLock<DashMap<String, (Vec<ExpandedCode>, Instant)>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn validate_cache() -> &'static DashMap<String, (bool, Instant)> {
    static CACHE: OnceLock<DashMap<String, (bool, Instant)>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

fn cache_get<T: Clone>(
    cache: &DashMap<String, (T, Instant)>,
    key: &str,
    ttl: Duration,
) -> Option<T> {
    let entry = cache.get(key)?;
    let (value, at) = entry.clone();
    if at.elapsed() < ttl {
        Some(value)
    } else {
        drop(entry);
        cache.remove(key);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_two_codes() {
        let response = json!({
            "resourceType": "ValueSet",
            "expansion": {
                "total": 2,
                "contains": [
                    {"system": "http://example.org/cs", "code": "A", "display": "Alpha"},
                    {"system": "http://example.org/cs", "code": "B", "display": "Beta"}
                ]
            }
        });
        let codes = extract_expansion_codes(&response).unwrap();
        assert_eq!(codes.len(), 2);
        assert_eq!(codes[0].code, "A");
        assert_eq!(codes[0].display.as_deref(), Some("Alpha"));
        assert_eq!(codes[1].code, "B");
    }

    #[test]
    fn extract_empty_expansion() {
        let response = json!({
            "resourceType": "ValueSet",
            "expansion": { "total": 0, "contains": [] }
        });
        assert!(extract_expansion_codes(&response).unwrap().is_empty());
    }

    #[test]
    fn extract_missing_expansion() {
        let response = json!({"resourceType": "ValueSet"});
        assert!(matches!(
            extract_expansion_codes(&response),
            Err(TerminologyError::Parse(_))
        ));
    }

    #[test]
    fn extract_skips_empty_codes() {
        let response = json!({
            "expansion": {
                "contains": [
                    {"system": "http://example.org/cs", "code": ""},
                    {"system": "http://example.org/cs", "code": "X"}
                ]
            }
        });
        let codes = extract_expansion_codes(&response).unwrap();
        assert_eq!(codes.len(), 1);
        assert_eq!(codes[0].code, "X");
    }

    #[test]
    fn as_token_with_and_without_system() {
        let with = ExpandedCode {
            system: "http://snomed.info/sct".into(),
            code: "73211009".into(),
            display: None,
        };
        assert_eq!(with.as_token(), "http://snomed.info/sct|73211009");
        let bare = ExpandedCode {
            system: String::new(),
            code: "active".into(),
            display: None,
        };
        assert_eq!(bare.as_token(), "active");
    }

    #[test]
    fn client_trims_trailing_slash() {
        let client = TerminologyClient::new("http://localhost:9091/", ClientOptions::rest_search());
        assert_eq!(client.base_url(), "http://localhost:9091");
    }

    #[test]
    fn parameters_result_bool_reads_result() {
        let body = json!({
            "resourceType": "Parameters",
            "parameter": [
                { "name": "result", "valueBoolean": true },
                { "name": "message", "valueString": "ok" }
            ]
        });
        assert!(parameters_result_bool(&body).unwrap());
    }
}

//! Reverse-proxy handlers and upstream forwarding for [`cr-fhir-bridge`].
//!
//! # Request routing
//!
//! Every FHIR REST call hits [`proxy_fhir`], which selects an upstream base via
//! [`resolve_upstream_base`]:
//!
//! | Path pattern | Upstream | Project response? |
//! |--------------|----------|-------------------|
//! | `/Library`, `/Library/*` | `CR_FHIR_BRIDGE_KR_URL` (KR HFS) | No — pass-through ELM artifacts |
//! | All other FHIR paths | `CR_FHIR_BRIDGE_UPSTREAM_URL` (clinical HFS) | Yes — Atrius→QI-Core when JSON |
//!
//! # Sidecar contract
//!
//! [`is_library_fhir_path`] implements the routing rule required because the JVM sidecar loads
//! the **primary** CQL library from `libraryBaseUrl` but every CQL **`include`** (e.g. `FHIRHelpers`)
//! from **`hfsBaseUrl`**. Without KR routing on `/Library/*`, includes 404 even when libraries
//! exist on the Knowledge Repository.
//!
//! Tenant isolation headers (`X-Tenant-ID`, `Authorization`, conditional headers) are forwarded
//! unchanged so clinical and KR upstreams see the same request context as a direct client.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode},
    response::IntoResponse,
};
use http_body_util::BodyExt as _;
use reqwest::Client;
use tracing::{debug, warn};

use atrius_runtime_mapper::RuntimeMapper;

use atrius_clinical_reasoning::{ClinicalReasoningClient, ClinicalReasoningConfig};

use crate::apply::ClinicalReasoningEndpoints;
use crate::transform::{TransformStats, is_fhir_json_content_type, transform_fhir_value};

const FORWARD_REQUEST_HEADERS: &[&str] = &[
    "accept",
    "accept-language",
    "authorization",
    "content-type",
    "if-match",
    "if-none-match",
    "prefer",
    "x-tenant-id",
];

const SKIP_RESPONSE_HEADERS: &[&str] = &[
    "connection",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

/// Shared bridge state.
#[derive(Clone)]
pub struct BridgeState {
    pub upstream_base: String,
    pub kr_base: Option<String>,
    /// When set, `$apply` routes invoke the JVM sidecar (PlanDefinition / ActivityDefinition).
    pub cr: Option<ClinicalReasoningEndpoints>,
    pub http: Client,
    pub mapper: Arc<RuntimeMapper>,
    pub max_body_size: usize,
    /// Injected as `X-Tenant-ID` when the inbound request omits it.
    pub default_tenant: Option<String>,
}

impl BridgeState {
    pub fn new(
        upstream_base: impl Into<String>,
        kr_base: Option<String>,
        http: Client,
        mapper: RuntimeMapper,
        max_body_size: usize,
        default_tenant: Option<String>,
    ) -> Self {
        Self {
            upstream_base: upstream_base.into(),
            kr_base,
            cr: None,
            http,
            mapper: Arc::new(mapper),
            max_body_size,
            default_tenant: default_tenant
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty()),
        }
    }

    pub fn with_clinical_reasoning(
        mut self,
        bridge_base: String,
        hts_base: String,
        sidecar_url: String,
        request_timeout: Duration,
    ) -> anyhow::Result<Self> {
        let library_base = self
            .kr_base
            .clone()
            .unwrap_or_else(|| self.upstream_base.clone());
        let config = ClinicalReasoningConfig::new(sidecar_url).request_timeout(request_timeout);
        let client = ClinicalReasoningClient::new(config)?;
        self.cr = Some(ClinicalReasoningEndpoints::new(
            bridge_base,
            library_base,
            hts_base,
            client,
        ));
        Ok(self)
    }
}

/// JVM sidecar resolves CQL `include` libraries via `hfsBaseUrl`, not `libraryBaseUrl`.
pub fn is_library_fhir_path(path_and_query: &str) -> bool {
    let path = path_and_query.split('?').next().unwrap_or(path_and_query);
    path == "/Library" || path.starts_with("/Library/")
}

fn resolve_upstream_base(state: &BridgeState, path_and_query: &str) -> (String, bool) {
    if let Some(kr) = state.kr_base.as_deref()
        && is_library_fhir_path(path_and_query)
    {
        return (kr.to_string(), false);
    }
    (state.upstream_base.clone(), true)
}

pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

pub async fn proxy_fhir(
    State(state): State<Arc<BridgeState>>,
    request: Request<Body>,
) -> Result<Response<Body>, BridgeProxyError> {
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let (upstream_base, project_response) = resolve_upstream_base(&state, path_and_query);
    let upstream_url = format!("{upstream_base}{path_and_query}");

    debug!(%method, %upstream_url, project_response, "proxy upstream");

    let body_bytes = read_body(body, state.max_body_size).await?;

    let mut upstream = state.http.request(method.clone(), &upstream_url);
    for (name, value) in forward_request_headers(&parts.headers, state.default_tenant.as_deref())
    {
        upstream = upstream.header(name, value);
    }
    if !body_bytes.is_empty()
        || method == Method::POST
        || method == Method::PUT
        || method == Method::PATCH
    {
        upstream = upstream.body(body_bytes.clone());
    }

    let upstream_resp = upstream.send().await?;
    let status = upstream_resp.status();
    let headers = upstream_resp.headers().clone();
    let bytes = upstream_resp.bytes().await?;

    let (out_body, stats) = if project_response {
        maybe_transform_response(&state.mapper, status, &headers, &bytes)?
    } else {
        (bytes.clone(), TransformStats::default())
    };
    if stats.projected > 0 {
        debug!(?stats, "projected upstream FHIR response");
    }

    build_proxy_response(status, &headers, out_body)
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeProxyError {
    #[error("request body too large (max {max} bytes)")]
    BodyTooLarge { max: usize },
    #[error("failed to read request body")]
    BodyRead,
    #[error("upstream request failed: {0}")]
    Upstream(#[from] reqwest::Error),
    #[error("invalid response header: {0}")]
    InvalidHeader(#[from] http::header::ToStrError),
    #[error("response build failed: {0}")]
    ResponseBuild(#[from] http::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl IntoResponse for BridgeProxyError {
    fn into_response(self) -> Response<Body> {
        let status = match &self {
            Self::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::BAD_GATEWAY,
        };
        (status, self.to_string()).into_response()
    }
}

async fn read_body(body: Body, max: usize) -> Result<Bytes, BridgeProxyError> {
    let collected = body
        .collect()
        .await
        .map_err(|_| BridgeProxyError::BodyRead)?;
    let bytes = collected.to_bytes();
    if bytes.len() > max {
        return Err(BridgeProxyError::BodyTooLarge { max });
    }
    Ok(bytes)
}

fn forward_request_headers(
    headers: &HeaderMap,
    default_tenant: Option<&str>,
) -> Vec<(HeaderName, HeaderValue)> {
    let mut out = Vec::new();
    for name in FORWARD_REQUEST_HEADERS {
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        for value in headers.get_all(&header_name) {
            out.push((header_name.clone(), value.clone()));
        }
    }
    if default_tenant.is_some() && !headers.contains_key("x-tenant-id") {
        let Ok(header_name) = HeaderName::from_bytes(b"x-tenant-id") else {
            return out;
        };
        if let Ok(value) = HeaderValue::from_str(default_tenant.unwrap()) {
            out.push((header_name, value));
        }
    }
    out
}

fn maybe_transform_response(
    mapper: &RuntimeMapper,
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    bytes: &Bytes,
) -> Result<(Bytes, TransformStats), BridgeProxyError> {
    if !status.is_success() || bytes.is_empty() {
        return Ok((bytes.clone(), TransformStats::default()));
    }

    let content_type = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !is_fhir_json_content_type(content_type) {
        return Ok((bytes.clone(), TransformStats::default()));
    }

    let mut value: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "upstream returned non-JSON body with FHIR content-type; passing through");
            return Ok((bytes.clone(), TransformStats::default()));
        }
    };

    let stats = match transform_fhir_value(mapper, &mut value) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "FHIR projection failed; passing through upstream body");
            return Ok((bytes.clone(), TransformStats::default()));
        }
    };

    if stats.projected == 0 {
        return Ok((bytes.clone(), stats));
    }

    let projected = serde_json::to_vec(&value)?;
    Ok((Bytes::from(projected), stats))
}

fn build_proxy_response(
    status: reqwest::StatusCode,
    upstream_headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, BridgeProxyError> {
    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in upstream_headers.iter() {
        if SKIP_RESPONSE_HEADERS
            .iter()
            .any(|skip| name.as_str().eq_ignore_ascii_case(skip))
        {
            continue;
        }
        builder = builder.header(name, value);
    }
    builder = builder.header(http::header::CONTENT_LENGTH, body.len());
    Ok(builder.body(Body::from(body))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwards_tenant_header_name() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant-id", HeaderValue::from_static("clinic-a"));
        let forwarded = forward_request_headers(&headers, None);
        assert_eq!(forwarded.len(), 1);
        assert_eq!(forwarded[0].0, "x-tenant-id");
    }

    #[test]
    fn injects_default_tenant_when_missing() {
        let headers = HeaderMap::new();
        let forwarded = forward_request_headers(&headers, Some("atrius-hospitals"));
        assert_eq!(forwarded.len(), 1);
        assert_eq!(forwarded[0].0, "x-tenant-id");
        assert_eq!(forwarded[0].1, "atrius-hospitals");
    }

    #[test]
    fn library_paths_match_fhir_library_routes() {
        assert!(is_library_fhir_path("/Library"));
        assert!(is_library_fhir_path("/Library/FHIRHelpers"));
        assert!(is_library_fhir_path(
            "/Library?name=FHIRHelpers&version=4.4.000"
        ));
        assert!(!is_library_fhir_path("/Patient/p1"));
        assert!(!is_library_fhir_path("/metadata"));
    }
}

//! Persist analytics artifacts (MeasureReport, Group) to clinical HFS.

use std::sync::Arc;

use reqwest::header::LOCATION;
use serde_json::Value;
use tracing::debug;

use crate::fhir_write_auth::{FhirWriteAuth, authorize_request};

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("analytics FHIR persist auth failed: {0}")]
    Auth(String),
    #[error("analytics FHIR persist HTTP error: {0}")]
    Http(String),
    #[error("analytics FHIR persist rejected (HTTP {status}): {body}")]
    Rejected { status: u16, body: String },
}

#[derive(Debug, Clone)]
pub struct PersistResult {
    pub resource_type: String,
    pub id: String,
    pub location: String,
}

pub struct FhirPersister {
    http: reqwest::Client,
    fhir_base_url: String,
    auth: Arc<dyn FhirWriteAuth>,
    tenant_id: Option<String>,
}

impl std::fmt::Debug for FhirPersister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FhirPersister")
            .field("fhir_base_url", &self.fhir_base_url)
            .field("auth_mode", &self.auth.mode())
            .field("tenant_id", &self.tenant_id)
            .finish_non_exhaustive()
    }
}

impl FhirPersister {
    pub fn new(
        http: reqwest::Client,
        fhir_base_url: impl Into<String>,
        auth: Arc<dyn FhirWriteAuth>,
        tenant_id: Option<String>,
    ) -> Self {
        Self {
            http,
            fhir_base_url: fhir_base_url.into().trim_end_matches('/').to_string(),
            auth,
            tenant_id: tenant_id.filter(|t| !t.trim().is_empty()),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.fhir_base_url
    }

    /// Create (POST) when `id` is `None`, otherwise PUT `{type}/{id}`.
    pub async fn write(
        &self,
        resource_type: &str,
        id: Option<&str>,
        resource: &Value,
    ) -> Result<PersistResult, PersistError> {
        let (method, url) = match id {
            Some(id) if !id.trim().is_empty() => (
                reqwest::Method::PUT,
                format!("{}/{resource_type}/{}", self.fhir_base_url, id.trim()),
            ),
            _ => (
                reqwest::Method::POST,
                format!("{}/{resource_type}", self.fhir_base_url),
            ),
        };

        let req = self
            .http
            .request(method, &url)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(resource);
        let mut req = authorize_request(self.auth.as_ref(), req)
            .await
            .map_err(PersistError::Auth)?;
        if let Some(ref tenant) = self.tenant_id {
            req = req.header("X-Tenant-ID", tenant);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| PersistError::Http(e.to_string()))?;
        let status = resp.status();
        let location = resp
            .headers()
            .get(LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let text = resp
            .text()
            .await
            .map_err(|e| PersistError::Http(e.to_string()))?;

        if !status.is_success() {
            return Err(PersistError::Rejected {
                status: status.as_u16(),
                body: text.chars().take(800).collect(),
            });
        }

        let body: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let resolved_id = body
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| id.map(str::to_string))
            .or_else(|| {
                location
                    .as_deref()
                    .and_then(|loc| id_from_location(loc, resource_type))
            })
            .unwrap_or_else(|| "unknown".into());
        let resolved_location = location
            .unwrap_or_else(|| format!("{}/{resource_type}/{resolved_id}", self.fhir_base_url));

        debug!(
            resource_type,
            id = %resolved_id,
            "analytics resource persisted to clinical HFS"
        );

        Ok(PersistResult {
            resource_type: resource_type.to_string(),
            id: resolved_id,
            location: resolved_location,
        })
    }
}

pub fn persist_result_json(result: &PersistResult) -> Value {
    serde_json::json!({
        "resourceType": result.resource_type,
        "id": result.id,
        "location": result.location
    })
}

fn id_from_location(loc: &str, resource_type: &str) -> Option<String> {
    let marker = format!("/{resource_type}/");
    let rest = loc.split(&marker).nth(1)?;
    let id = rest.split('/').next()?.trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_history_location() {
        assert_eq!(
            id_from_location(
                "http://127.0.0.1:8082/MeasureReport/mr-1/_history/2",
                "MeasureReport"
            )
            .as_deref(),
            Some("mr-1")
        );
    }
}

//! CDS Hooks `fhirAuthorization` forwarded to the JVM sidecar for SMART bearer access to clinical FHIR.

use serde::{Deserialize, Serialize};

/// OAuth 2.0 bearer credentials for clinical FHIR REST (`hfsBaseUrl` retrieves).
///
/// Populated by **cds-server** from CDS Hooks `fhirAuthorization` on invoke. The sidecar
/// attaches `access_token` to outbound clinical FHIR calls only (not HTS / KR).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SidecarFhirAuthorization {
    pub access_token: String,
    #[serde(default = "default_bearer")]
    pub token_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patient: Option<String>,
}

fn default_bearer() -> String {
    "Bearer".to_string()
}

impl SidecarFhirAuthorization {
    #[must_use]
    pub fn access_token_only(access_token: impl Into<String>) -> Self {
        Self {
            access_token: access_token.into(),
            token_type: default_bearer(),
            expires_in: None,
            scope: None,
            subject: None,
            patient: None,
        }
    }
}

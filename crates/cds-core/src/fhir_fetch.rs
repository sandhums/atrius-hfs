//! Read FHIR **JSON** from the EHR using `fhirServer` + `fhirAuthorization` on a [`CdsRequest`](helios_cds_hooks::CdsRequest).
//!
//! For [FHIR Clinical Reasoning](https://www.hl7.org/fhir/clinicalreasoning-module.html) `$evaluate`
//! on a `PlanDefinition`, add a similar helper that POSTs a `Parameters` body to the
//! `/{ResourceType}/$evaluate` URL you standardize on.

use std::sync::OnceLock;
use std::time::Duration;

use helios_cds_hooks::FhirAuthorization;
use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use thiserror::Error;
use url::Url;

/// Errors from a FHIR `http://` / `https://` read.
#[derive(Error, Debug)]
pub enum FhirFetchError {
    /// Bad `fhirServer` URL.
    #[error("invalid fhir base url: {0}")]
    InvalidBase(#[from] url::ParseError),
    /// Transport / timeout / TLS.
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    /// Non-success HTTP status from the FHIR server.
    #[error("http status {0} from FHIR server")]
    HttpStatus(reqwest::StatusCode),
    /// Response was not valid JSON.
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
}

fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(3))
            .build()
            .expect("build reqwest Client")
    })
}

fn patient_read_url(fhir_base: &str, patient_id: &str) -> Result<Url, url::ParseError> {
    let id = patient_id.trim_start_matches("Patient/");
    let s = format!("{}/Patient/{}", fhir_base.trim_end_matches('/'), id);
    Url::parse(&s)
}

/// `GET {fhirBase}/Patient/{id}` with `Authorization: {tokenType} {accessToken}`.
pub async fn get_patient_json(
    fhir_base: &str,
    auth: &FhirAuthorization,
    patient_id: &str,
) -> Result<Value, FhirFetchError> {
    let url = patient_read_url(fhir_base, patient_id)?;
    let bearer = format!("{} {}", auth.token_type, auth.access_token);
    let response = http_client()
        .get(url)
        .header(AUTHORIZATION, bearer)
        .header(reqwest::header::ACCEPT, "application/fhir+json")
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(FhirFetchError::HttpStatus(status));
    }
    let v = response.json().await?;
    Ok(v)
}

/// Best-effort display string from a FHIR JSON `Patient` (R4-style `name`).
pub fn patient_display_name(patient: &Value) -> Option<String> {
    let names = patient.get("name")?.as_array()?;
    let n = names.first()?;
    if let Some(t) = n.get("text").and_then(|x| x.as_str()) {
        return Some(t.trim().to_string());
    }
    let fam = n.get("family").and_then(|f| f.as_str());
    let given0 = n
        .get("given")
        .and_then(|g| g.as_array())
        .and_then(|a| a.first())
        .and_then(|g| g.as_str());
    match (given0, fam) {
        (Some(g), Some(f)) if !f.is_empty() => Some(format!("{g} {f}")),
        (None, Some(f)) if !f.is_empty() => Some(f.to_string()),
        (Some(g), _) if !g.is_empty() => Some(g.to_string()),
        _ => None,
    }
}

/// Load display name from the EHR; on failure returns `None` and logs a warning.
pub async fn try_patient_display_name(
    fhir_base: &str,
    auth: &FhirAuthorization,
    patient_id: &str,
) -> Option<String> {
    match get_patient_json(fhir_base, auth, patient_id).await {
        Ok(p) => {
            if p.get("resourceType").and_then(|r| r.as_str()) == Some("Patient") {
                patient_display_name(&p)
            } else {
                tracing::warn!("FHIR GET Patient: unexpected resourceType");
                None
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "FHIR GET Patient failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_from_patient_fhir_name() {
        let p = serde_json::json!({
            "resourceType": "Patient",
            "id": "1",
            "name": [{ "family": "Doe", "given": ["Jane"] }]
        });
        assert_eq!(patient_display_name(&p).as_deref(), Some("Jane Doe"));
    }
}

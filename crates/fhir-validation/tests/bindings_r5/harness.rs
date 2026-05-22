//! Shared helpers for the `bindings_r5` integration test crate.

use fhir_validation::R5FhirPathEvaluator;
use fhir_validation::terminology::service::RemoteTerminologyService;
use helios_fhir::{FhirResource, FhirVersion};
use reqwest::Client;
use std::time::Duration;

pub fn r5_evaluator_for(resource: &FhirResource) -> R5FhirPathEvaluator {
    let FhirResource::R5(r) = resource else {
        panic!("expected R5 FhirResource");
    };
    R5FhirPathEvaluator::new((**r).clone())
}

/// Base URL for a FHIR terminology server (trailing slash optional), if set in the environment.
pub fn terminology_base_url_from_env() -> Option<String> {
    std::env::var("FHIR_TERMINOLOGY_BASE_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_owned())
        .filter(|s| !s.is_empty())
}

fn default_terminology_base_url() -> String {
    terminology_base_url_from_env().unwrap_or_else(|| "http://localhost:9091".to_string())
}

/// Client for async binding tests that need `$validate-code` against [HTS](https://github.com/) (Helios Terminology Server).
///
/// Uses `FHIR_TERMINOLOGY_BASE_URL` when set; otherwise defaults to `http://localhost:9091`.
/// Expects **FHIR core** ValueSets/CodeSystems on HTS (e.g. `common-tags`, `service-type`, `identifier-type`).
/// R4 ABDM packages on HTS are for **`r4_suite`** / `HFS_PROFILE_MANIFEST`, not for these R5 binding tests.
pub fn remote_terminology_for_tests() -> RemoteTerminologyService {
    let base = default_terminology_base_url();
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(8)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client");
    RemoteTerminologyService::with_client(client, base, FhirVersion::R5)
}

/// Fail fast when the configured terminology server is not a working FHIR `$validate-code` endpoint.
pub async fn assert_remote_terminology_reachable() {
    let base = default_terminology_base_url();
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client");
    let health_url = format!("{base}/health");
    let health = client
        .get(&health_url)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {health_url}: {e}"));
    assert!(
        health.status().is_success(),
        "terminology server health check failed: GET {health_url} -> {}",
        health.status()
    );

    let validate_url = format!("{base}/ValueSet/$validate-code");
    let body = serde_json::json!({
        "resourceType": "Parameters",
        "parameter": [
            {"name": "url", "valueUri": "http://hl7.org/fhir/ValueSet/common-tags"},
            {"name": "code", "valueCode": "HTEST"}
        ]
    });
    let resp = client
        .post(&validate_url)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {validate_url}: {e}"));
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "terminology server must accept POST {validate_url} (got {status}): {text}"
    );
}

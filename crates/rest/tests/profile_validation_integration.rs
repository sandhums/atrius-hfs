//! Integration tests for NDHM/ABDM profile validation on the REST API.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helios_fhir::FhirVersion;
use helios_persistence::backends::sqlite::SqliteBackend;
use helios_rest::{ProfileValidationMode, ServerConfig};
use serde_json::json;
use tower::ServiceExt;

const NDHM_PATIENT_PROFILE: &str = "https://nrces.in/ndhm/fhir/r4/StructureDefinition/Patient";

fn write_test_manifest() -> PathBuf {
    let profiles = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../fhir-validation/tests/fixtures/r4/profiles");
    let manifest = serde_json::json!({
        "structure_definition_files": [
            profiles.join("StructureDefinition-Patient.json"),
            profiles.join("StructureDefinition-AtriusPatient.json"),
        ]
    });
    let path = std::env::temp_dir().join("helios-rest-ndhm-test-manifest.json");
    std::fs::write(&path, serde_json::to_string(&manifest).unwrap()).unwrap();
    path
}

fn abdm_test_config() -> ServerConfig {
    ServerConfig {
        profile_manifest: Some(write_test_manifest()),
        profile_validation_mode: ProfileValidationMode::Warn,
        profile_validation_addons: false,
        default_fhir_version: FhirVersion::R4,
        ..ServerConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn validate_operation_returns_operation_outcome() {
    let backend = SqliteBackend::in_memory().expect("sqlite");
    backend.init_schema().expect("schema");
    let config = abdm_test_config();
    let app = helios_rest::create_app_with_auth(
        backend,
        config,
        helios_auth::AuthConfig::default(),
        None,
        None,
    );

    let body = json!({
        "resourceType": "Patient",
        "meta": { "profile": ["http://atrius.in/StructureDefinition/AtriusPatient"] },
        "name": [{ "text": "Test" }]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/Patient/$validate")
                .header("Content-Type", "application/fhir+json")
                .header("X-Tenant-ID", "default")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let outcome: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(outcome["resourceType"], "OperationOutcome");
    assert!(outcome["issue"].as_array().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn create_persists_under_warn_mode_despite_profile_issues() {
    let backend = SqliteBackend::in_memory().expect("sqlite");
    backend.init_schema().expect("schema");
    let config = abdm_test_config();
    let app = helios_rest::create_app_with_auth(
        backend,
        config,
        helios_auth::AuthConfig::default(),
        None,
        None,
    );

    let body = json!({
        "resourceType": "Patient",
        "meta": { "profile": ["http://atrius.in/StructureDefinition/AtriusPatient"] },
        "name": [{ "text": "Incomplete" }]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/Patient")
                .header("Content-Type", "application/fhir+json")
                .header("X-Tenant-ID", "default")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
}

fn ndhm_manifest_config(terminology_server: Option<String>) -> ServerConfig {
    let profiles = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../fhir-validation/tests/fixtures/r4/profiles");
    let manifest = serde_json::json!({
        "structure_definition_files": [
            profiles.join("StructureDefinition-Patient.json"),
        ]
    });
    let path = std::env::temp_dir().join("helios-rest-ndhm-patient-manifest.json");
    std::fs::write(&path, serde_json::to_string(&manifest).unwrap()).unwrap();
    ServerConfig {
        profile_manifest: Some(path),
        profile_validation_mode: ProfileValidationMode::Warn,
        profile_validation_addons: false,
        terminology_server,
        default_fhir_version: FhirVersion::R4,
        ..ServerConfig::default()
    }
}

/// Richer NDHM Patient with ADN identifier type — `$validate` uses HTS for profile bindings.
///
/// Requires HTS at `HFS_TERMINOLOGY_SERVER` / `HTS_TERMINOLOGY_BASE_URL` (default `http://localhost:9091`).
#[tokio::test(flavor = "multi_thread")]
async fn validate_ndhm_patient_with_hts_terminology() {
    let hts_url = std::env::var("HFS_TERMINOLOGY_SERVER")
        .or_else(|_| std::env::var("HTS_TERMINOLOGY_BASE_URL"))
        .unwrap_or_else(|_| "http://localhost:9091".to_string());

    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("reqwest");
    let health = client
        .get(format!("{hts_url}/health"))
        .send()
        .await
        .expect("HTS health GET");
    assert!(
        health.status().is_success(),
        "HTS not reachable at {hts_url}/health"
    );

    let backend = SqliteBackend::in_memory().expect("sqlite");
    backend.init_schema().expect("schema");
    let config = ndhm_manifest_config(Some(hts_url));
    let pv = helios_rest::ProfileValidationService::try_from_config(&config)
        .expect("load")
        .expect("profiles");
    assert!(pv.has_terminology_server());

    let app = helios_rest::create_app_with_auth(
        backend,
        config,
        helios_auth::AuthConfig::default(),
        None,
        None,
    );

    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../fhir-validation/tests/fixtures/r4/examples/ndhm-richer-patient.json");
    let body: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(fixture).unwrap()).unwrap();
    assert_eq!(
        body["meta"]["profile"].as_array().and_then(|a| a.first()),
        Some(&json!(NDHM_PATIENT_PROFILE))
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/Patient/$validate")
                .header("Content-Type", "application/fhir+json")
                .header("X-Tenant-ID", "default")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let outcome: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let issues = outcome["issue"].as_array().expect("issues");
    let binding_errors: Vec<_> = issues
        .iter()
        .filter(|i| {
            i["code"].as_str() == Some("value")
                && i["expression"]
                    .as_str()
                    .is_some_and(|e| e.contains("ndhm-identifier-type-code"))
        })
        .collect();
    assert!(
        binding_errors.is_empty(),
        "ADN should pass NDHM identifier.type binding via HTS; binding errors: {binding_errors:#?}"
    );
}

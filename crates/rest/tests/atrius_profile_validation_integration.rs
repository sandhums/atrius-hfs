//! Integration tests for Atrius IG profile validation (P0 #3 / #5).
//!
//! Uses the generated `manifests/atrius-r4-profile-manifest-core.json` when present.
//! Run after `./scripts/setup-atrius-profile-registry.sh` (fetches published package.tgz).

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use helios_fhir::FhirVersion;
use helios_persistence::backends::sqlite::SqliteBackend;
use helios_rest::{ProfileValidationMode, ServerConfig};
use serde_json::json;
use tower::ServiceExt;

const ATRIUS_SCHEDULE: &str =
    "https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-schedule";
const ATRIUS_EPISODE: &str =
    "https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-episode-of-care";

fn core_manifest_path() -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../manifests/atrius-r4-profile-manifest-core.json");
    path.exists().then_some(path)
}

fn atrius_strict_config() -> Option<ServerConfig> {
    let manifest = core_manifest_path()?;
    Some(ServerConfig {
        profile_manifest: Some(manifest),
        profile_validation_mode: ProfileValidationMode::Strict,
        profile_validation_addons: false,
        default_fhir_version: FhirVersion::R4,
        ..ServerConfig::default()
    })
}

#[test]
fn core_manifest_exists_with_atrius_profiles() {
    let Some(path) = core_manifest_path() else {
        panic!(
            "missing {}; run ./scripts/build-atrius-profile-manifest.sh",
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../manifests/atrius-r4-profile-manifest-core.json")
                .display()
        );
    };
    let raw = std::fs::read_to_string(&path).expect("read manifest");
    let manifest: serde_json::Value = serde_json::from_str(&raw).expect("parse manifest");
    let count = manifest["structure_definition_files"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert!(
        count >= 100,
        "expected ≥100 StructureDefinitions in core manifest, got {count}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn strict_mode_rejects_schedule_missing_planning_horizon() {
    let Some(config) = atrius_strict_config() else {
        eprintln!("skip: core manifest not found");
        return;
    };

    let backend = SqliteBackend::in_memory().expect("sqlite");
    backend.init_schema().expect("schema");
    let app = helios_rest::create_app_with_auth(
        backend,
        config,
        helios_auth::AuthConfig::default(),
        None,
        None,
    );

    let body = json!({
        "resourceType": "Schedule",
        "id": "sched-bad",
        "meta": { "profile": [ATRIUS_SCHEDULE] },
        "actor": [{ "reference": "Practitioner/dr-1" }]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/Schedule")
                .header("Content-Type", "application/fhir+json")
                .header("X-Tenant-ID", "default")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread")]
async fn strict_mode_accepts_minimal_schedule() {
    let Some(config) = atrius_strict_config() else {
        eprintln!("skip: core manifest not found");
        return;
    };

    let backend = SqliteBackend::in_memory().expect("sqlite");
    backend.init_schema().expect("schema");
    let app = helios_rest::create_app_with_auth(
        backend,
        config,
        helios_auth::AuthConfig::default(),
        None,
        None,
    );

    let body = json!({
        "resourceType": "Schedule",
        "id": "sched-ok",
        "meta": { "profile": [ATRIUS_SCHEDULE] },
        "actor": [{ "reference": "Practitioner/dr-1" }],
        "planningHorizon": {
            "start": "2026-07-07T09:00:00+05:30",
            "end": "2026-12-31T23:59:59+05:30"
        }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/Schedule")
                .header("Content-Type", "application/fhir+json")
                .header("X-Tenant-ID", "default")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test(flavor = "multi_thread")]
async fn strict_mode_rejects_episode_missing_patient() {
    let Some(config) = atrius_strict_config() else {
        eprintln!("skip: core manifest not found");
        return;
    };

    let backend = SqliteBackend::in_memory().expect("sqlite");
    backend.init_schema().expect("schema");
    let app = helios_rest::create_app_with_auth(
        backend,
        config,
        helios_auth::AuthConfig::default(),
        None,
        None,
    );

    let body = json!({
        "resourceType": "EpisodeOfCare",
        "id": "eoc-bad",
        "meta": { "profile": [ATRIUS_EPISODE] },
        "status": "active",
        "type": [{ "coding": [{ "system": "http://terminology.hl7.org/CodeSystem/episodeofcare-type", "code": "inp" }] }],
        "managingOrganization": { "reference": "Organization/hosp-1" },
        "period": { "start": "2026-07-07" }
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/EpisodeOfCare")
                .header("Content-Type", "application/fhir+json")
                .header("X-Tenant-ID", "default")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

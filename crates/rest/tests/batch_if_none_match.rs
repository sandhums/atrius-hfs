//! `ifNoneMatch` on batch PUT: create-if-absent, fail only that entry.

use std::path::PathBuf;
use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue};
use axum_test::TestServer;
use helios_fhir::FhirVersion;
use helios_persistence::backends::sqlite::{SqliteBackend, SqliteBackendConfig};
use helios_persistence::core::ResourceStorage;
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_rest::ServerConfig;
use helios_rest::config::{MultitenancyConfig, TenantRoutingMode};
use serde_json::{Value, json};

const X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");
const CONTENT_TYPE: HeaderName = HeaderName::from_static("content-type");

async fn create_test_server() -> (TestServer, Arc<SqliteBackend>) {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("data"))
        .unwrap_or_else(|| PathBuf::from("data"));

    let backend_config = SqliteBackendConfig {
        data_dir: Some(data_dir),
        ..Default::default()
    };
    let backend = SqliteBackend::with_config(":memory:", backend_config)
        .expect("Failed to create SQLite backend");
    backend.init_schema().expect("Failed to init schema");
    let backend = Arc::new(backend);

    let config = ServerConfig {
        multitenancy: MultitenancyConfig {
            routing_mode: TenantRoutingMode::HeaderOnly,
            ..Default::default()
        },
        base_url: "http://localhost:8080".to_string(),
        default_tenant: "test-tenant".to_string(),
        ..ServerConfig::for_testing()
    };

    let state = helios_rest::AppState::new(Arc::clone(&backend), config);
    let app = helios_rest::routing::fhir_routes::create_routes(state);
    let server = TestServer::new(app).expect("Failed to create test server");

    (server, backend)
}

fn test_tenant() -> TenantContext {
    TenantContext::new(
        TenantId::new("test-tenant"),
        TenantPermissions::full_access(),
    )
}

async fn seed_patient(backend: &SqliteBackend, id: &str, family: &str) {
    let tenant = test_tenant();
    let patient = json!({
        "resourceType": "Patient",
        "id": id,
        "name": [{"family": family}],
        "active": true
    });
    backend
        .create(&tenant, "Patient", patient, FhirVersion::R4)
        .await
        .expect("Failed to seed patient");
}

async fn post_batch(server: &TestServer, bundle: Value) -> Value {
    let response = server
        .post("/")
        .add_header(X_TENANT_ID, HeaderValue::from_static("test-tenant"))
        .add_header(
            CONTENT_TYPE,
            HeaderValue::from_static("application/fhir+json"),
        )
        .json(&bundle)
        .await;
    response.assert_status_ok();
    response.json()
}

fn batch(entries: Vec<Value>) -> Value {
    json!({
        "resourceType": "Bundle",
        "type": "batch",
        "entry": entries,
    })
}

fn put_entry(id: &str, family: &str, if_none_match: Option<&str>) -> Value {
    let mut request = json!({ "method": "PUT", "url": format!("Patient/{id}") });
    if let Some(if_none_match) = if_none_match {
        request["ifNoneMatch"] = json!(if_none_match);
    }
    json!({
        "request": request,
        "resource": {
            "resourceType": "Patient",
            "id": id,
            "name": [{"family": family}],
            "active": true
        }
    })
}

fn entry_status(body: &Value, index: usize) -> String {
    body["entry"][index]["response"]["status"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

fn assert_precondition_failed(body: &Value, index: usize) {
    let status = entry_status(body, index);
    assert!(
        status.starts_with("412"),
        "entry {index} should have failed its precondition, got {status}"
    );
    let code = body["entry"][index]["response"]["outcome"]["issue"][0]["code"].as_str();
    assert_eq!(code, Some("conflict"), "entry {index} outcome code");
}

async fn read_family(server: &TestServer, id: &str) -> Option<String> {
    let response = server
        .get(&format!("/Patient/{id}"))
        .add_header(X_TENANT_ID, HeaderValue::from_static("test-tenant"))
        .await;
    if !response.status_code().is_success() {
        return None;
    }
    let body: Value = response.json();
    body["name"][0]["family"].as_str().map(str::to_string)
}

#[tokio::test]
async fn put_with_star_if_none_match_creates_when_absent() {
    let (server, _backend) = create_test_server().await;

    let body = post_batch(
        &server,
        batch(vec![put_entry("fresh", "Created", Some("*"))]),
    )
    .await;

    assert!(
        entry_status(&body, 0).starts_with("201"),
        "got {}",
        entry_status(&body, 0)
    );
    assert_eq!(
        read_family(&server, "fresh").await.as_deref(),
        Some("Created")
    );
}

#[tokio::test]
async fn put_with_star_if_none_match_is_412_when_present_and_leaves_siblings() {
    let (server, backend) = create_test_server().await;
    seed_patient(&backend, "p1", "Original").await;

    let body = post_batch(
        &server,
        batch(vec![
            put_entry("p1", "Overwritten", Some("*")),
            put_entry("p2", "Sibling", Some("*")),
        ]),
    )
    .await;

    assert_precondition_failed(&body, 0);
    assert!(
        entry_status(&body, 1).starts_with("201"),
        "batch must fail only the colliding entry, got {}",
        entry_status(&body, 1)
    );
    assert_eq!(
        read_family(&server, "p1").await.as_deref(),
        Some("Original"),
        "a rejected ifNoneMatch must not overwrite"
    );
    assert_eq!(read_family(&server, "p2").await.as_deref(), Some("Sibling"));
}

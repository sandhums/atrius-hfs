//! SNOMED `system|code` token search on ServiceRequest.category.
//!
//! HIS lab/imaging worklists search `category=http://snomed.info/sct|{code}`.
//! This SQLite REST test confirms the token matches a freshly written
//! resource (no Elasticsearch refresh lag).

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
const SNOMED: &str = "http://snomed.info/sct";
const LABORATORY: &str = "108252007";

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

fn bundle_ids(body: &Value) -> Vec<String> {
    body["entry"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|entry| entry["resource"]["id"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn snomed_system_code_category_finds_fresh_service_request() {
    let (server, backend) = create_test_server().await;
    let tenant = test_tenant();
    let sr = json!({
        "resourceType": "ServiceRequest",
        "id": "sr-lab-snomed",
        "status": "active",
        "intent": "order",
        "subject": { "reference": "Patient/p1" },
        "category": [{
            "coding": [{
                "system": SNOMED,
                "code": LABORATORY,
                "display": "Laboratory procedure"
            }]
        }]
    });
    backend
        .create(&tenant, "ServiceRequest", sr, FhirVersion::R4)
        .await
        .expect("create ServiceRequest");

    let token = format!("{SNOMED}|{LABORATORY}");
    let matched = server
        .get(&format!("/ServiceRequest?category={token}"))
        .add_header(X_TENANT_ID, HeaderValue::from_static("test-tenant"))
        .await;
    matched.assert_status_ok();
    let body: Value = matched.json();
    assert!(
        bundle_ids(&body).contains(&"sr-lab-snomed".to_string()),
        "SNOMED system|code category search missed the written ServiceRequest: {body}"
    );

    let legacy = server
        .get("/ServiceRequest?category=laboratory")
        .add_header(X_TENANT_ID, HeaderValue::from_static("test-tenant"))
        .await;
    legacy.assert_status_ok();
    let legacy_body: Value = legacy.json();
    assert!(
        !bundle_ids(&legacy_body).contains(&"sr-lab-snomed".to_string()),
        "bare 'laboratory' must not match a SNOMED category: {legacy_body}"
    );
}

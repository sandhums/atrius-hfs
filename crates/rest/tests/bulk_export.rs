//! Integration tests for the FHIR Bulk Data Export (`$export`) endpoints.
//!
//! Exercises the kick-off → poll → manifest → download → delete lifecycle for
//! all three export levels, plus parameter validation, the `ExportStatus` →
//! HTTP mapping, and the unsupported-parameter behavior.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::middleware::Next;
use axum_test::TestServer;
use chrono::Utc;
use helios_auth::{Principal, ScopeSet};
use helios_fhir::FhirVersion;
use helios_persistence::backends::local_fs::LocalFsOutputStore;
use helios_persistence::backends::sqlite::{SqliteBackend, SqliteBackendConfig};
use helios_persistence::core::{
    BulkExportJobStore, BulkExportStorage, DefaultExportWorker, ExportClaimStrategy,
    ExportOutputStore, ExportRequest, ResourceStorage, StartExportInput, WorkerId,
};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_rest::ServerConfig;
use helios_rest::bulk_export_auth::BearerScopeAuth;
use helios_rest::config::{MultitenancyConfig, TenantRoutingMode};
use serde_json::{Value, json};

/// Builds a test server with the bulk-export subsystem wired in, plus the
/// SQLite backend and the local-FS output store (for driving a worker).
async fn create_bulk_export_server() -> (
    TestServer,
    Arc<SqliteBackend>,
    Arc<LocalFsOutputStore>,
    tempfile::TempDir,
) {
    create_bulk_export_server_with(
        TenantRoutingMode::HeaderOnly,
        "http://localhost:8080",
        "test-tenant",
        None,
    )
    .await
}

async fn create_bulk_export_server_with(
    routing_mode: TenantRoutingMode,
    base_url: &str,
    default_tenant: &str,
    principal_tenant: Option<&str>,
) -> (
    TestServer,
    Arc<SqliteBackend>,
    Arc<LocalFsOutputStore>,
    tempfile::TempDir,
) {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("data"))
        .unwrap_or_else(|| PathBuf::from("data"));

    let backend_config = SqliteBackendConfig {
        data_dir: Some(data_dir),
        ..Default::default()
    };
    let backend = Arc::new(
        SqliteBackend::with_config(":memory:", backend_config).expect("create SQLite backend"),
    );
    backend.init_schema().expect("init schema");

    let tmp = tempfile::tempdir().expect("tempdir");
    let output = Arc::new(LocalFsOutputStore::new(
        tmp.path(),
        "https://wrong-internal.example",
    ));
    let file_auth = Arc::new(BearerScopeAuth);

    let config = ServerConfig {
        multitenancy: MultitenancyConfig {
            routing_mode,
            strict_validation: principal_tenant.is_some(),
            ..Default::default()
        },
        base_url: base_url.to_string(),
        default_tenant: default_tenant.to_string(),
        ..ServerConfig::for_testing()
    };

    let state = helios_rest::AppState::new(Arc::clone(&backend), config).with_bulk_export(
        backend.clone() as Arc<dyn BulkExportJobStore>,
        output.clone() as Arc<dyn ExportOutputStore>,
        file_auth,
    );
    let app = helios_rest::routing::fhir_routes::create_routes(state);
    let app = if let Some(tenant_id) = principal_tenant {
        let principal = Principal::stub("bulk-export-client", ScopeSet::parse("system/Patient.rs"))
            .with_issuer("https://issuer.example")
            .with_tenant_id(tenant_id);
        app.layer(axum::middleware::from_fn(
            move |mut request: axum::extract::Request, next: Next| {
                let principal = principal.clone();
                async move {
                    request.extensions_mut().insert(principal);
                    next.run(request).await
                }
            },
        ))
    } else {
        app
    };
    let server = TestServer::new(app).expect("create test server");

    (server, backend, output, tmp)
}

fn inject_principal(app: axum::Router, subject: &str, scopes: &str) -> axum::Router {
    let principal =
        Principal::stub(subject, ScopeSet::parse(scopes)).with_issuer("https://issuer.example");
    app.layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: Next| {
            let principal = principal.clone();
            async move {
                request.extensions_mut().insert(principal);
                next.run(request).await
            }
        },
    ))
}

async fn create_bulk_export_server_as(
    subject: &str,
    scopes: &str,
) -> (
    TestServer,
    Arc<SqliteBackend>,
    Arc<LocalFsOutputStore>,
    tempfile::TempDir,
) {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("data"))
        .unwrap_or_else(|| PathBuf::from("data"));

    let backend_config = SqliteBackendConfig {
        data_dir: Some(data_dir),
        ..Default::default()
    };
    let backend = Arc::new(
        SqliteBackend::with_config(":memory:", backend_config).expect("create SQLite backend"),
    );
    backend.init_schema().expect("init schema");

    let tmp = tempfile::tempdir().expect("tempdir");
    let output = Arc::new(LocalFsOutputStore::new(
        tmp.path(),
        "https://wrong-internal.example",
    ));
    let file_auth = Arc::new(BearerScopeAuth);

    let config = ServerConfig {
        default_tenant: "test-tenant".to_string(),
        ..ServerConfig::for_testing()
    };

    let state = helios_rest::AppState::new(Arc::clone(&backend), config).with_bulk_export(
        backend.clone() as Arc<dyn BulkExportJobStore>,
        output.clone() as Arc<dyn ExportOutputStore>,
        file_auth,
    );
    let app = inject_principal(
        helios_rest::routing::fhir_routes::create_routes(state),
        subject,
        scopes,
    );
    let server = TestServer::new(app).expect("create test server");
    (server, backend, output, tmp)
}

fn test_export_input(owner: &str, request: ExportRequest) -> StartExportInput {
    StartExportInput {
        request,
        transaction_time: Utc::now(),
        request_url: "http://localhost:8080/$export?_type=Patient".to_string(),
        owner_subject: Some(owner.to_string()),
        fhir_version: FhirVersion::default(),
    }
}

fn test_tenant() -> TenantContext {
    TenantContext::new(
        TenantId::new("test-tenant"),
        TenantPermissions::full_access(),
    )
}

/// Drains all currently-claimable export jobs by running a worker synchronously.
async fn drain_workers(backend: &Arc<SqliteBackend>, output: &Arc<LocalFsOutputStore>) {
    let worker_id = WorkerId::new("test-worker");
    let worker = DefaultExportWorker::new(
        backend.clone(),
        backend.clone(),
        output.clone(),
        worker_id.clone(),
    );
    while let Some(lease) = backend
        .claim_next(&worker_id, Duration::from_secs(60))
        .await
        .expect("claim_next")
    {
        worker.run_job(lease).await.expect("run_job");
    }
}

/// Seeds N Patient resources.
async fn seed_patients(backend: &Arc<SqliteBackend>, n: usize) {
    seed_patients_for(backend, "test-tenant", n).await;
}

async fn seed_patients_for(backend: &Arc<SqliteBackend>, tenant_id: &str, n: usize) {
    let tenant = TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access());
    for i in 0..n {
        backend
            .create(
                &tenant,
                "Patient",
                json!({"resourceType": "Patient", "id": format!("p{i}")}),
                FhirVersion::default(),
            )
            .await
            .expect("seed patient");
    }
}

#[tokio::test]
async fn authenticated_path_tenant_and_public_prefix_flow_through_bulk_export_urls() {
    let (server, backend, output, _tmp) = create_bulk_export_server_with(
        TenantRoutingMode::UrlPath,
        "https://public.example/fhir/",
        "default",
        Some("acme"),
    )
    .await;
    seed_patients_for(&backend, "acme", 1).await;

    let kickoff = server
        .get("/acme/$export")
        .add_header("prefer", "respond-async")
        .add_query_param("_type", "Patient")
        .await;
    assert_eq!(kickoff.status_code(), StatusCode::ACCEPTED);
    let status_url = kickoff
        .headers()
        .get("content-location")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(status_url.starts_with("https://public.example/fhir/acme/export-status/"));
    let status_path = status_url
        .strip_prefix("https://public.example/fhir")
        .unwrap();

    drain_workers(&backend, &output).await;
    let done = server.get(status_path).await;
    assert_eq!(done.status_code(), StatusCode::OK);
    let manifest: Value = done.json();
    assert_eq!(
        manifest["request"],
        "https://public.example/fhir/acme/$export?_type=Patient"
    );
    assert!(
        manifest["output"][0]["url"]
            .as_str()
            .unwrap()
            .starts_with("https://public.example/fhir/acme/export-file/")
    );
}

#[tokio::test]
async fn both_mode_header_tenant_can_follow_unprefixed_bulk_export_urls() {
    let public_base = "https://public.example/fhir";
    let (server, backend, output, _tmp) =
        create_bulk_export_server_with(TenantRoutingMode::Both, public_base, "default", None).await;
    seed_patients_for(&backend, "acme", 1).await;

    let kickoff = server
        .get("/$export")
        .add_header("x-tenant-id", "acme")
        .add_header("prefer", "respond-async")
        .add_query_param("_type", "Patient")
        .await;
    assert_eq!(kickoff.status_code(), StatusCode::ACCEPTED);
    let status_url = kickoff
        .headers()
        .get("content-location")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(status_url.starts_with("https://public.example/fhir/export-status/"));
    assert!(!status_url.contains("/acme/"));
    let status_path = status_url.strip_prefix(public_base).unwrap();

    assert_eq!(
        server
            .get(status_path)
            .add_header("x-tenant-id", "acme")
            .await
            .status_code(),
        StatusCode::ACCEPTED
    );
    drain_workers(&backend, &output).await;

    let done = server
        .get(status_path)
        .add_header("x-tenant-id", "acme")
        .await;
    assert_eq!(done.status_code(), StatusCode::OK);
    let manifest: Value = done.json();
    assert_eq!(
        manifest["request"],
        "https://public.example/fhir/$export?_type=Patient"
    );
    let file_url = manifest["output"][0]["url"].as_str().unwrap();
    assert!(file_url.starts_with("https://public.example/fhir/export-file/"));
    let file_path = file_url.strip_prefix(public_base).unwrap();
    assert_eq!(
        server
            .get(file_path)
            .add_header("x-tenant-id", "acme")
            .await
            .status_code(),
        StatusCode::OK
    );

    // `both` still accepts the canonical URL-path form for the same job.
    assert_eq!(
        server
            .get(&format!("/acme{status_path}"))
            .await
            .status_code(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_system_export_full_lifecycle() {
    let (server, backend, output, _tmp) = create_bulk_export_server().await;
    seed_patients(&backend, 3).await;

    // Kick-off — requires Prefer: respond-async.
    let resp = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_type", "Patient")
        .await;
    assert_eq!(resp.status_code(), StatusCode::ACCEPTED);
    let status_url = resp
        .headers()
        .get("content-location")
        .expect("Content-Location header")
        .to_str()
        .unwrap()
        .to_string();
    let status_path = status_url.strip_prefix("http://localhost:8080").unwrap();

    // Poll before the worker runs — still 202.
    let polling = server
        .get(status_path)
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(polling.status_code(), StatusCode::ACCEPTED);
    assert!(polling.headers().get("retry-after").is_some());

    // Run the worker.
    drain_workers(&backend, &output).await;

    // Poll again — now 200 + manifest.
    let done = server
        .get(status_path)
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(done.status_code(), StatusCode::OK);
    let manifest: Value = done.json();
    assert!(manifest["transactionTime"].is_string());
    assert_eq!(
        manifest["request"],
        "http://localhost:8080/$export?_type=Patient"
    );
    assert_eq!(manifest["requiresAccessToken"], true);
    let output_files = manifest["output"].as_array().expect("output array");
    let total: u64 = output_files
        .iter()
        .map(|f| f["count"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 3);

    // Download the first output file.
    let file_url = output_files[0]["url"].as_str().unwrap();
    let file_path = file_url.strip_prefix("http://localhost:8080").unwrap();
    let download = server
        .get(file_path)
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(download.status_code(), StatusCode::OK);
    assert_eq!(
        download.headers().get("content-type").unwrap(),
        "application/fhir+ndjson"
    );
    assert_eq!(download.text().lines().count(), 3);

    // Delete the job.
    let deleted = server
        .delete(status_path)
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(deleted.status_code(), StatusCode::ACCEPTED);

    // Status URL is now gone.
    let gone = server
        .get(status_path)
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(gone.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_patient_and_group_export_levels() {
    let (server, backend, output, _tmp) = create_bulk_export_server().await;
    seed_patients(&backend, 2).await;

    // Patient-level kick-off.
    let resp = server
        .get("/Patient/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .await;
    assert_eq!(resp.status_code(), StatusCode::ACCEPTED);

    // Group-level kick-off.
    let tenant = test_tenant();
    backend
        .create(
            &tenant,
            "Group",
            json!({"resourceType": "Group", "id": "g1", "member": []}),
            FhirVersion::default(),
        )
        .await
        .unwrap();
    let resp = server
        .get("/Group/g1/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .await;
    assert_eq!(resp.status_code(), StatusCode::ACCEPTED);

    drain_workers(&backend, &output).await;
}

#[tokio::test]
async fn test_kickoff_requires_respond_async() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;
    let resp = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_unsupported_output_format_rejected() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;
    let resp = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_outputFormat", "text/csv")
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_unsupported_param_strict_vs_lenient() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;

    // strict → 400
    let strict = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async, handling=strict")
        .add_query_param("includeAssociatedData", "LatestProvenanceResources")
        .await;
    assert_eq!(strict.status_code(), StatusCode::BAD_REQUEST);

    // no handling directive (lenient default) → accepted
    let lenient = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("includeAssociatedData", "LatestProvenanceResources")
        .await;
    assert_eq!(lenient.status_code(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn test_type_filter_validation() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;

    // _typeFilter whose resource type is not in _type → 400
    let mismatch = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_type", "Patient")
        .add_query_param("_typeFilter", "Observation?status=final")
        .await;
    assert_eq!(mismatch.status_code(), StatusCode::BAD_REQUEST);

    // _typeFilter carrying a result-control param → 400
    let bad_param = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_type", "Observation")
        .add_query_param("_typeFilter", "Observation?_sort=date")
        .await;
    assert_eq!(bad_param.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_status_and_download_unknown_job() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;

    let status = server
        .get("/export-status/nonexistent")
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(status.status_code(), StatusCode::NOT_FOUND);

    let download = server
        .get("/export-file/nonexistent/Patient-0")
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(download.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_post_kickoff_with_parameters_body() {
    let (server, backend, output, _tmp) = create_bulk_export_server().await;
    seed_patients(&backend, 2).await;

    // POST kickoff using a FHIR Parameters resource body.
    let body = json!({
        "resourceType": "Parameters",
        "parameter": [
            {"name": "_type", "valueString": "Patient"}
        ]
    });
    let resp = server
        .post("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .json(&body)
        .await;
    assert_eq!(resp.status_code(), StatusCode::ACCEPTED);
    assert!(resp.headers().get("content-location").is_some());

    drain_workers(&backend, &output).await;
}

#[tokio::test]
async fn test_since_parameter_accepted() {
    let (server, backend, output, _tmp) = create_bulk_export_server().await;
    seed_patients(&backend, 1).await;

    let resp = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_since", "2020-01-01T00:00:00Z")
        .await;
    assert_eq!(resp.status_code(), StatusCode::ACCEPTED);

    drain_workers(&backend, &output).await;
}

#[tokio::test]
async fn test_invalid_since_rejected() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;

    let resp = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_since", "not-a-date")
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_elements_parameter_accepted() {
    let (server, backend, output, _tmp) = create_bulk_export_server().await;
    seed_patients(&backend, 1).await;

    let resp = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_type", "Patient")
        .add_query_param("_elements", "id,name")
        .await;
    assert_eq!(resp.status_code(), StatusCode::ACCEPTED);
    let status_url = resp
        .headers()
        .get("content-location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let status_path = status_url.strip_prefix("http://localhost:8080").unwrap();

    drain_workers(&backend, &output).await;

    let done = server
        .get(status_path)
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(done.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn test_valid_type_filter_accepted() {
    let (server, backend, output, _tmp) = create_bulk_export_server().await;
    seed_patients(&backend, 1).await;

    // _typeFilter with valid resource type (in _type) and allowed search param.
    let resp = server
        .get("/$export")
        .add_header("x-tenant-id", "test-tenant")
        .add_header("prefer", "respond-async")
        .add_query_param("_type", "Patient")
        .add_query_param("_typeFilter", "Patient?active=true")
        .await;
    assert_eq!(resp.status_code(), StatusCode::ACCEPTED);

    drain_workers(&backend, &output).await;
}

#[tokio::test]
async fn test_capability_statement_advertises_export() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;
    let resp = server
        .get("/metadata")
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let cs: Value = resp.json();
    let ops = cs["rest"][0]["operation"]
        .as_array()
        .expect("operation array");
    let names: Vec<&str> = ops.iter().filter_map(|o| o["name"].as_str()).collect();
    assert!(names.contains(&"export"));
    assert!(names.contains(&"patient-export"));
    assert!(names.contains(&"group-export"));
    assert_eq!(
        cs["instantiates"][0],
        "http://hl7.org/fhir/uv/bulkdata/CapabilityStatement/bulk-data"
    );
}

#[tokio::test]
async fn export_jobs_list_includes_owner_and_request_url() {
    let (server, backend, _output, _tmp) = create_bulk_export_server().await;
    let job_id = backend
        .start_export(
            &test_tenant(),
            test_export_input("alice", ExportRequest::system()),
        )
        .await
        .expect("start");

    let resp = server
        .get("/export-jobs")
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: Value = resp.json();
    let jobs = body["jobs"].as_array().expect("jobs");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["job_id"], job_id.as_str());
    assert_eq!(jobs[0]["owner_subject"], "alice");
    assert_eq!(jobs[0]["level"], "system");
    assert_eq!(
        jobs[0]["request_url"],
        "http://localhost:8080/$export?_type=Patient"
    );
    assert_eq!(jobs[0]["status"], "accepted");
}

#[tokio::test]
async fn export_jobs_list_scopes_to_caller_without_system_wildcard() {
    let (server, backend, _output, _tmp) =
        create_bulk_export_server_as("alice", "system/Patient.rs").await;
    backend
        .start_export(
            &test_tenant(),
            test_export_input("alice", ExportRequest::system()),
        )
        .await
        .expect("alice job");
    backend
        .start_export(
            &test_tenant(),
            test_export_input("bob", ExportRequest::patient()),
        )
        .await
        .expect("bob job");

    let resp = server
        .get("/export-jobs")
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: Value = resp.json();
    let jobs = body["jobs"].as_array().expect("jobs");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["owner_subject"], "alice");
}

#[tokio::test]
async fn export_jobs_list_system_wildcard_sees_all_owners() {
    let (server, backend, _output, _tmp) =
        create_bulk_export_server_as("alice", "system/*.rs").await;
    backend
        .start_export(
            &test_tenant(),
            test_export_input("alice", ExportRequest::system()),
        )
        .await
        .expect("alice job");
    backend
        .start_export(
            &test_tenant(),
            test_export_input("bob", ExportRequest::patient()),
        )
        .await
        .expect("bob job");

    let resp = server
        .get("/export-jobs")
        .add_header("x-tenant-id", "test-tenant")
        .await;
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body: Value = resp.json();
    assert_eq!(body["jobs"].as_array().expect("jobs").len(), 2);
}

#[tokio::test]
async fn export_jobs_list_paginates_and_filters_status() {
    let (server, backend, _output, _tmp) = create_bulk_export_server().await;
    for _ in 0..3 {
        backend
            .start_export(
                &test_tenant(),
                test_export_input("alice", ExportRequest::system()),
            )
            .await
            .expect("start");
    }

    let page1 = server
        .get("/export-jobs")
        .add_header("x-tenant-id", "test-tenant")
        .add_query_param("_count", "2")
        .await;
    assert_eq!(page1.status_code(), StatusCode::OK);
    let body1: Value = page1.json();
    assert_eq!(body1["jobs"].as_array().expect("jobs").len(), 2);
    let cursor = body1["next_cursor"].as_str().expect("next_cursor");

    let page2 = server
        .get("/export-jobs")
        .add_header("x-tenant-id", "test-tenant")
        .add_query_param("_count", "2")
        .add_query_param("_cursor", cursor)
        .await;
    assert_eq!(page2.status_code(), StatusCode::OK);
    let body2: Value = page2.json();
    assert_eq!(body2["jobs"].as_array().expect("jobs").len(), 1);
    assert!(body2["next_cursor"].is_null());

    let filtered = server
        .get("/export-jobs")
        .add_header("x-tenant-id", "test-tenant")
        .add_query_param("status", "complete")
        .await;
    assert_eq!(filtered.status_code(), StatusCode::OK);
    let body: Value = filtered.json();
    assert!(body["jobs"].as_array().expect("jobs").is_empty());
}

#[tokio::test]
async fn export_jobs_list_rejects_bad_cursor() {
    let (server, _backend, _output, _tmp) = create_bulk_export_server().await;
    let resp = server
        .get("/export-jobs")
        .add_header("x-tenant-id", "test-tenant")
        .add_query_param("_cursor", "not-a-cursor")
        .await;
    assert_eq!(resp.status_code(), StatusCode::BAD_REQUEST);
}

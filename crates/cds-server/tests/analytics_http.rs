//! Analytics façade HTTP tests (measure persist, CQL cohort, catalog-grounded nl-views).

use std::sync::{Arc, Once};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cds_server::{
    AppState, analytics, build_router,
    clinical_reasoning::{ClinicalReasoningClient, ClinicalReasoningConfig, FhirServiceEndpoints},
    fhir_write_auth::NoFhirWriteAuth,
    kr_manifest::demo_manifest,
    services::{CdsEvalBackend, registry_from_manifest},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ensure_test_metrics() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        helios_observability::metrics::init("cds-server-analytics-test");
    });
}

fn analytics_router(engine: analytics::AnalyticsEngine) -> axum::Router {
    ensure_test_metrics();
    build_router(
        AppState {
            registry: registry_from_manifest(&demo_manifest(), CdsEvalBackend::Demo, None),
            kr_readiness: None,
            subscription_notify: None,
            analytics: analytics::AnalyticsState {
                catalog: Arc::new(analytics::catalog::ViewCatalog::embedded()),
                engine: Some(Arc::new(engine)),
            },
        },
        false,
    )
}

async fn engine(sidecar: &MockServer, hfs: &MockServer) -> analytics::AnalyticsEngine {
    let client = ClinicalReasoningClient::new(ClinicalReasoningConfig::new(sidecar.uri()))
        .expect("sidecar client");
    let endpoints = FhirServiceEndpoints::new(hfs.uri(), "http://hts.example")
        .with_library_base_url("http://kr.example");
    let persist = analytics::persist::FhirPersister::new(
        reqwest::Client::new(),
        hfs.uri(),
        Arc::new(NoFhirWriteAuth),
        None,
    );
    analytics::AnalyticsEngine {
        client: Arc::new(client),
        endpoints: Arc::new(endpoints),
        persist: Some(Arc::new(persist)),
        measurement_period: None,
        cohort_concurrency: 2,
    }
}

async fn json_body(res: axum::response::Response) -> (StatusCode, Value) {
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

#[tokio::test]
async fn nl_views_suggests_hba1c_sqlquery_without_sidecar() {
    ensure_test_metrics();
    let app = build_router(
        AppState {
            registry: registry_from_manifest(&demo_manifest(), CdsEvalBackend::Demo, None),
            kr_readiness: None,
            subscription_notify: None,
            analytics: analytics::AnalyticsState::default(),
        },
        false,
    );
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/nl-views")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"text": "latest hba1c per patient"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, v) = json_body(res).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["supported"], true);
    assert_eq!(
        v["suggestions"][0]["id"],
        "atrius-in-patient-latest-observation"
    );
    assert_eq!(v["suggestions"][0]["kind"], "sql-query");
}

#[tokio::test]
async fn measure_evaluate_demo_mode_is_unavailable() {
    ensure_test_metrics();
    let app = build_router(
        AppState {
            registry: registry_from_manifest(&demo_manifest(), CdsEvalBackend::Demo, None),
            kr_readiness: None,
            subscription_notify: None,
            analytics: analytics::AnalyticsState::default(),
        },
        false,
    );
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/measure/evaluate")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "measureId": "AtriusCMS165ControllingHighBP",
                        "patientId": "cms165-demo",
                        "persist": false
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn measure_evaluate_persists_measure_report() {
    let sidecar = MockServer::start().await;
    let hfs = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/measure/evaluate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "measureId": "AtriusCMS165ControllingHighBP",
            "measureReport": {
                "resourceType": "MeasureReport",
                "status": "complete",
                "type": "individual",
                "measure": "https://atrius.in/fhir/r4/atrius-in/Measure/AtriusCMS165ControllingHighBP",
                "subject": { "reference": "Patient/cms165-demo" },
                "period": { "start": "2026-01-01", "end": "2026-12-31" }
            }
        })))
        .mount(&sidecar)
        .await;
    Mock::given(method("POST"))
        .and(path("/MeasureReport"))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header("Location", format!("{}/MeasureReport/mr-cms165", hfs.uri()))
                .set_body_json(json!({
                    "resourceType": "MeasureReport",
                    "id": "mr-cms165"
                })),
        )
        .mount(&hfs)
        .await;

    let app = analytics_router(engine(&sidecar, &hfs).await);
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/measure/evaluate")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "measureId": "AtriusCMS165ControllingHighBP",
                        "patientId": "cms165-demo",
                        "periodStart": "2026-01-01",
                        "periodEnd": "2026-12-31"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, v) = json_body(res).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["measureReport"]["resourceType"], "MeasureReport");
    assert_eq!(v["persisted"]["id"], "mr-cms165");
    assert_eq!(v["patientId"], "cms165-demo");
}

#[tokio::test]
async fn cohort_keeps_true_members_and_puts_group() {
    let sidecar = MockServer::start().await;
    let hfs = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/evaluate/expression"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "expression": "Initial Population",
            "resultType": "java.lang.Boolean",
            "result": true
        })))
        .mount(&sidecar)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex(r"^/Group/"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "resourceType": "Group",
            "id": "cql-test"
        })))
        .mount(&hfs)
        .await;

    let app = analytics_router(engine(&sidecar, &hfs).await);
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cohorts")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "libraryId": "AtriusCMS165ControllingHighBP",
                        "libraryVersion": "0.1.0",
                        "expression": "Initial Population",
                        "patientIds": ["Patient/p1", "p2"],
                        "name": "CMS165 IP"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, v) = json_body(res).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["memberCount"], 2);
    assert_eq!(v["group"]["resourceType"], "Group");
    assert_eq!(v["group"]["actual"], true);
    assert_eq!(
        v["exportHints"]["group"]
            .as_str()
            .unwrap()
            .starts_with("Group/"),
        true
    );
}

#[tokio::test]
async fn cohort_excludes_false_results() {
    let sidecar = MockServer::start().await;
    let hfs = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/evaluate/expression"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "expression": "Numerator",
            "result": false
        })))
        .mount(&sidecar)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex(r"^/Group/"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "resourceType": "Group",
            "id": "empty"
        })))
        .mount(&hfs)
        .await;

    let app = analytics_router(engine(&sidecar, &hfs).await);
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cohorts")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "libraryId": "AtriusCMS165ControllingHighBP",
                        "expression": "Numerator",
                        "patientIds": ["p1"],
                        "persist": true
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, v) = json_body(res).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["memberCount"], 0);
    assert_eq!(v["excluded"][0], "p1");
}

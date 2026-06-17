//! Integration tests: bridge proxies upstream HFS and projects Condition search bundles.

use std::sync::Arc;
use std::time::Duration;

use atrius_runtime_mapper::{MapperManifest, QICORE_CONDITION_ENCOUNTER_DIAGNOSIS, RuntimeMapper};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cr_fhir_bridge::{BridgeState, build_router, upstream_http_client};
use serde_json::json;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn health_ok() {
    let state = test_state("http://example.invalid").await;
    let app = build_router(state, false);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn proxies_and_projects_condition_search() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/Condition"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 1,
            "entry": [{
                "resource": {
                    "resourceType": "Condition",
                    "id": "c1",
                    "meta": { "profile": ["https://atrius.in/fhir/r4/atrius-core/StructureDefinition/atrius-condition-encounter-diagnosis"] },
                    "category": [{ "coding": [{ "system": "http://terminology.hl7.org/CodeSystem/condition-category", "code": "encounter-diagnosis" }] }],
                    "code": { "coding": [{ "system": "http://hl7.org/fhir/sid/icd-10", "code": "I10" }] },
                    "subject": { "reference": "Patient/p1" }
                }
            }]
        })))
        .mount(&mock)
        .await;

    let state = test_state(&mock.uri()).await;
    let app = build_router(state, false);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/Condition?patient=Patient/p1")
                .header("accept", "application/fhir+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v["entry"][0]["resource"]["meta"]["profile"][0],
        QICORE_CONDITION_ENCOUNTER_DIAGNOSIS
    );
}

#[tokio::test]
async fn passes_through_upstream_errors() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/Patient/unknown"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "resourceType": "OperationOutcome",
            "issue": [{ "severity": "error", "code": "not-found" }]
        })))
        .mount(&mock)
        .await;

    let state = test_state(&mock.uri()).await;
    let app = build_router(state, false);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/Patient/unknown")
                .header("accept", "application/fhir+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["resourceType"], "OperationOutcome");
}

async fn test_state(upstream: &str) -> Arc<BridgeState> {
    test_state_with_kr(upstream, None).await
}

async fn test_state_with_kr(upstream: &str, kr: Option<&str>) -> Arc<BridgeState> {
    let http = upstream_http_client(Duration::from_secs(5)).unwrap();
    Arc::new(BridgeState::new(
        upstream,
        kr.map(str::to_owned),
        http,
        RuntimeMapper::new(MapperManifest::default_v0_1()),
        10 * 1024 * 1024,
    ))
}

#[tokio::test]
async fn proxies_library_reads_to_kr_when_configured() {
    let clinical = MockServer::start().await;
    let kr = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/Library/FHIRHelpers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "resourceType": "Library",
            "id": "FHIRHelpers",
            "version": "4.4.000"
        })))
        .mount(&kr)
        .await;

    let state = test_state_with_kr(&clinical.uri(), Some(&kr.uri())).await;
    let app = build_router(state, false);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/Library/FHIRHelpers")
                .header("accept", "application/fhir+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["id"], "FHIRHelpers");
}

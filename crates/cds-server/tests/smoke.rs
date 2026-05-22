//! Integration smoke tests for CDS router.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cds_server::{
    build_router,
    kr_manifest::{demo_manifest, parse_manifest_json},
    services::{CdsEvalBackend, ServiceRegistry, registry_from_manifest},
};
use serde_json::json;
use tower::ServiceExt;

fn demo_registry() -> ServiceRegistry {
    registry_from_manifest(&demo_manifest(), CdsEvalBackend::Demo)
}

#[tokio::test]
async fn health_ok() {
    let app = build_router(demo_registry(), false);
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
async fn discovery_includes_service_id() {
    let app = build_router(demo_registry(), false);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/cds-services")
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
    let services = v["services"].as_array().expect("services array");
    assert!(
        services.iter().any(|s| s["id"] == "atrius-patient-view"),
        "{services:?}"
    );
}

#[tokio::test]
async fn patient_view_hook_returns_cards() {
    let app = build_router(demo_registry(), false);
    let payload = json!({
        "hook": "patient-view",
        "hookInstance": "550e8400-e29b-41d4-a716-446655440000",
        "context": {
            "userId": "Practitioner/1",
            "patientId": "1288992"
        }
    });
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/cds-services/atrius-patient-view")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let cards = v["cards"].as_array().expect("cards");
    assert!(!cards.is_empty());
}

#[tokio::test]
async fn discovery_lists_multiple_kr_manifest_services() {
    let json = r#"{"services":[
        {"id":"svc-a","hook":"patient-view","description":"A","libraryId":"L1","expression":"E1"},
        {"id":"svc-b","hook":"patient-view","description":"B","libraryId":"L2","expression":"E2"}
    ]}"#;
    let m = parse_manifest_json(json.as_bytes()).unwrap();
    let reg = registry_from_manifest(&m, CdsEvalBackend::Demo);
    let app = build_router(reg, false);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/cds-services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let ids: Vec<_> = v["services"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["id"].as_str())
        .collect();
    assert!(ids.contains(&"svc-a"));
    assert!(ids.contains(&"svc-b"));
}

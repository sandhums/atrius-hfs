//! Integration tests for FHIR `$apply` proxy routes.

use std::sync::Arc;
use std::time::Duration;

use atrius_clinical_reasoning::{ClinicalReasoningClient, ClinicalReasoningConfig};
use atrius_runtime_mapper::{MapperManifest, RuntimeMapper};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cr_fhir_bridge::{BridgeState, ClinicalReasoningEndpoints, build_router, upstream_http_client};
use serde_json::json;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn metadata_advertises_apply_when_sidecar_configured() {
    let state = apply_test_state("http://bridge.example").await;
    let app = build_router(state, false);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/metadata")
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
    let resources = v["rest"][0]["resource"].as_array().unwrap();
    let pd = resources
        .iter()
        .find(|r| r["type"] == "PlanDefinition")
        .unwrap();
    assert_eq!(pd["operation"][0]["name"], "apply");
}

#[tokio::test]
async fn plan_definition_instance_apply_returns_parameters() {
    let sidecar = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/plandefinition/apply"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "planDefinitionId": "cms165",
            "carePlan": {
                "resourceType": "CarePlan",
                "id": "cp-1",
                "status": "active",
                "intent": "proposal",
                "subject": { "reference": "Patient/p1" }
            },
            "requestGroup": {
                "resourceType": "RequestGroup",
                "id": "rg-1",
                "status": "active",
                "intent": "proposal"
            }
        })))
        .mount(&sidecar)
        .await;

    let state = apply_test_state_with_sidecar(&sidecar.uri()).await;
    let app = build_router(state, false);

    let req_body = json!({
        "resourceType": "Parameters",
        "parameter": [
            { "name": "subject", "valueString": "Patient/p1" },
            { "name": "encounter", "valueString": "Encounter/e1" }
        ]
    });

    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/PlanDefinition/cms165/$apply")
                .header("content-type", "application/fhir+json")
                .body(Body::from(req_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["resourceType"], "Parameters");
    assert_eq!(v["parameter"][0]["name"], "return");
    assert_eq!(v["parameter"][0]["resource"]["resourceType"], "CarePlan");
}

#[tokio::test]
async fn activity_definition_type_apply_returns_request_resource() {
    let sidecar = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/activitydefinition/apply"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "activityDefinitionId": "order-ecg",
            "resource": {
                "resourceType": "ServiceRequest",
                "status": "draft",
                "intent": "proposal",
                "subject": { "reference": "Patient/p1" }
            }
        })))
        .mount(&sidecar)
        .await;

    let state = apply_test_state_with_sidecar(&sidecar.uri()).await;
    let app = build_router(state, false);

    let req_body = json!({
        "resourceType": "Parameters",
        "parameter": [
            { "name": "subject", "valueString": "Patient/p1" },
            { "name": "activityDefinition", "resource": {
                "resourceType": "ActivityDefinition",
                "id": "order-ecg"
            }}
        ]
    });

    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ActivityDefinition/$apply")
                .header("content-type", "application/fhir+json")
                .body(Body::from(req_body.to_string()))
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
        v["parameter"][0]["resource"]["resourceType"],
        "ServiceRequest"
    );
}

async fn apply_test_state(bridge_base: &str) -> Arc<BridgeState> {
    build_apply_state("http://sidecar.invalid", bridge_base).await
}

async fn apply_test_state_with_sidecar(sidecar_url: &str) -> Arc<BridgeState> {
    build_apply_state(sidecar_url, "http://bridge.example").await
}

async fn build_apply_state(sidecar_url: &str, bridge_base: &str) -> Arc<BridgeState> {
    let http = upstream_http_client(Duration::from_secs(5)).unwrap();
    let client = ClinicalReasoningClient::new(ClinicalReasoningConfig::new(sidecar_url)).unwrap();
    let mut state = BridgeState::new(
        "http://clinical.invalid",
        Some("http://kr.invalid".into()),
        http,
        RuntimeMapper::new(MapperManifest::default_v0_1()),
        10 * 1024 * 1024,
    );
    state.cr = Some(ClinicalReasoningEndpoints::new(
        bridge_base.to_string(),
        "http://kr.invalid".into(),
        "http://hts.invalid".into(),
        client,
    ));
    Arc::new(state)
}

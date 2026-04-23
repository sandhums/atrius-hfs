use std::sync::Arc;

use axum::Router;
use axum_test::TestServer;
use cds_server::{CdsServiceDispatch, CdsServiceRegistry, ServiceWrapper, cds_hooks_router};
use helios_cds_hooks::hooks::PatientViewContext;
use helios_cds_hooks::{
    Card, CdsHooksError, CdsHooksService, CdsRequest, CdsResponse, CdsService, Feedback,
    FeedbackRequest,
};

struct Greeter;

#[async_trait::async_trait]
impl CdsHooksService for Greeter {
    type Context = PatientViewContext;

    fn definition(&self) -> CdsService {
        CdsService {
            hook: "patient-view".to_string(),
            title: None,
            description: "greet".to_string(),
            id: "greeter-1".to_string(),
            prefetch: None,
            usage_requirements: None,
            version: None,
            extension: None,
        }
    }

    async fn call(
        &self,
        _r: &CdsRequest,
        c: &PatientViewContext,
    ) -> Result<CdsResponse, CdsHooksError> {
        Ok(CdsResponse::with_cards(vec![Card::info(
            format!("id {}", c.patient_id),
            "G",
        )]))
    }
}

fn test_app() -> Router {
    let s: Arc<dyn CdsServiceDispatch> = Arc::new(ServiceWrapper::new(Arc::new(Greeter)));
    let registry = CdsServiceRegistry::try_from_services([s]).unwrap();
    cds_hooks_router(registry)
}

#[tokio::test]
async fn discovery_lists_service() {
    let server = TestServer::new(test_app()).unwrap();
    let r = server.get("/cds-services").await;
    r.assert_status_ok();
    let v: serde_json::Value = r.json();
    assert_eq!(v["services"][0]["id"], "greeter-1");
}

#[tokio::test]
async fn invoke_unknown_id_404() {
    let server = TestServer::new(test_app()).unwrap();
    let r = server
        .post("/cds-services/nope")
        .json(&serde_json::json!({
            "hook": "patient-view",
            "hookInstance": "00000000-0000-0000-0000-000000000001",
            "context": { "userId": "P/a", "patientId": "1" }
        }))
        .await;
    r.assert_status_not_found();
}

#[tokio::test]
async fn invoke_returns_cards() {
    let server = TestServer::new(test_app()).unwrap();
    let r = server
        .post("/cds-services/greeter-1")
        .json(&serde_json::json!({
            "hook": "patient-view",
            "hookInstance": "00000000-0000-0000-0000-000000000001",
            "context": { "userId": "Practitioner/1", "patientId": "99" }
        }))
        .await;
    r.assert_status_ok();
    let v: serde_json::Value = r.json();
    assert!(v["cards"][0]["summary"].as_str().unwrap().contains("99"));
}

#[tokio::test]
async fn feedback_no_content() {
    let server = TestServer::new(test_app()).unwrap();
    use chrono::Utc;
    let r = server
        .post("/cds-services/greeter-1/feedback")
        .json(&FeedbackRequest {
            feedback: vec![Feedback {
                card: "c1".to_string(),
                outcome: helios_cds_hooks::FeedbackOutcome::Accepted,
                accepted_suggestions: None,
                override_reason: None,
                outcome_timestamp: Utc::now(),
            }],
        })
        .await;
    r.assert_status(axum::http::StatusCode::NO_CONTENT);
}

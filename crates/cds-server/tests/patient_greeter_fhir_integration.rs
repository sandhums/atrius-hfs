//! Full stack: Axum [`cds_server::cds_hooks_router`], [`cds_core::PatientGreeterService`],
//! [`cds_core::PatientViewQualityGapsService`], and wiremock EHR for `GET Patient/{id}`.

use std::sync::Arc;

use axum::Router;
use axum_test::TestServer;
use cds_core::{PatientGreeterService, PatientViewQualityGapsService};
use cds_server::{CdsServiceDispatch, CdsServiceRegistry, ServiceWrapper, cds_hooks_router};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app_greeter_only() -> Router {
    let s: Arc<dyn CdsServiceDispatch> =
        Arc::new(ServiceWrapper::new(Arc::new(PatientGreeterService)));
    let registry = CdsServiceRegistry::try_from_services([s]).unwrap();
    cds_hooks_router(registry)
}

fn app_greeter_and_quality_gaps() -> Router {
    let greeter: Arc<dyn CdsServiceDispatch> =
        Arc::new(ServiceWrapper::new(Arc::new(PatientGreeterService)));
    let gaps: Arc<dyn CdsServiceDispatch> =
        Arc::new(ServiceWrapper::new(Arc::new(PatientViewQualityGapsService)));
    let registry = CdsServiceRegistry::try_from_services([greeter, gaps]).unwrap();
    cds_hooks_router(registry)
}

#[tokio::test]
async fn discovery_lists_separate_greeter_and_quality_services() {
    let server = TestServer::new(app_greeter_and_quality_gaps()).unwrap();
    let r = server.get("/cds-services").await;
    r.assert_status_ok();
    let v: serde_json::Value = r.json();
    let ids: Vec<String> = v["services"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"patient-greeter".to_string()));
    assert!(ids.contains(&"patient-quality-gaps".to_string()));
}

#[tokio::test]
async fn invoke_patient_greeter_fetches_patient_name_from_ehr_through_http() {
    let mock_ehr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/Patient/1288992"))
        .and(header("Accept", "application/fhir+json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "resourceType": "Patient",
            "id": "1288992",
            "name": [{ "family": "Doe", "given": ["Jane"] }]
        })))
        .mount(&mock_ehr)
        .await;

    let server = TestServer::new(app_greeter_only()).unwrap();
    let r = server
        .post("/cds-services/patient-greeter")
        .json(&serde_json::json!({
            "hook": "patient-view",
            "hookInstance": "00000000-0000-0000-0000-000000000042",
            "fhirServer": mock_ehr.uri(),
            "fhirAuthorization": {
                "access_token": "integration-test-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": "patient/*.read",
                "subject": "cds-server-test"
            },
            "context": {
                "userId": "Practitioner/1",
                "patientId": "1288992"
            }
        }))
        .await;

    r.assert_status_ok();
    let v: serde_json::Value = r.json();
    let cards = v["cards"].as_array().expect("cards");
    assert_eq!(cards.len(), 1, "greeter service returns greeting only");
    let summary0 = cards[0]["summary"].as_str().expect("card 0 summary");
    assert!(
        summary0.contains("EHR: Jane Doe"),
        "expected EHR name; got: {summary0}"
    );
    assert!(
        summary0.contains("1288992"),
        "expected patient id; got: {summary0}"
    );
}

#[tokio::test]
async fn invoke_patient_quality_gaps_returns_colorectal_card_for_50_plus_prefetch() {
    let server = TestServer::new(app_greeter_and_quality_gaps()).unwrap();
    let r = server
        .post("/cds-services/patient-quality-gaps")
        .json(&serde_json::json!({
            "hook": "patient-view",
            "hookInstance": "00000000-0000-0000-0000-000000000043",
            "context": {
                "userId": "Practitioner/1",
                "patientId": "1288992"
            },
            "prefetch": {
                "patient": {
                    "resourceType": "Patient",
                    "id": "1288992",
                    "birthDate": "1950-06-15"
                }
            }
        }))
        .await;

    r.assert_status_ok();
    let v: serde_json::Value = r.json();
    let summaries: String = v["cards"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["summary"].as_str())
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        summaries.to_ascii_lowercase().contains("colorectal") || summaries.contains("50+"),
        "expected screening gap; got: {summaries}"
    );
}

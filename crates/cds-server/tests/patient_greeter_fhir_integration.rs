//! Full stack: Axum [`cds_server::cds_hooks_router`], [`cds_core::PatientGreeterService`], and a
//! wiremock EHR returning `Patient` JSON (same path as production: `GET {fhirServer}/Patient/{id}`).

use std::sync::Arc;

use axum::Router;
use axum_test::TestServer;
use cds_core::PatientGreeterService;
use cds_server::{CdsServiceDispatch, CdsServiceRegistry, ServiceWrapper, cds_hooks_router};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app_with_patient_greeter() -> Router {
    let s: Arc<dyn CdsServiceDispatch> =
        Arc::new(ServiceWrapper::new(Arc::new(PatientGreeterService)));
    let registry = CdsServiceRegistry::try_from_services([s]).unwrap();
    cds_hooks_router(registry)
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

    let server = TestServer::new(app_with_patient_greeter()).unwrap();
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
    let summary = v["cards"][0]["summary"].as_str().expect("card summary");
    assert!(
        summary.contains("EHR: Jane Doe"),
        "expected EHR name in summary; got: {summary}"
    );
    assert!(
        summary.contains("1288992"),
        "expected patient id; got: {summary}"
    );
}

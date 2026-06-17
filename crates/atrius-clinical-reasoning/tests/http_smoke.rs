#![cfg(feature = "http-client")]

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use atrius_clinical_reasoning::{
    ClinicalReasoningClient, ClinicalReasoningConfig, ClinicalReasoningError,
    EvaluateExpressionRequest, NormalizedSidecarResult,
};

#[tokio::test]
async fn evaluate_expression_posts_to_sidecar() {
    let srv = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/evaluate/expression"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "expression": "InPopulation",
            "resultType": "java.lang.Boolean",
            "result": {"valueBoolean": true}
        })))
        .mount(&srv)
        .await;

    let cfg = ClinicalReasoningConfig::new(srv.uri().to_string());
    let client = ClinicalReasoningClient::new(cfg).expect("client");

    let req = EvaluateExpressionRequest {
        elm: Some("{}".into()),
        elm_format: Default::default(),
        library_id: "Dummy".into(),
        library_version: Some("1.0.0".into()),
        expression: "InPopulation".into(),
        hfs_base_url: "http://hfs.fhir".into(),
        hts_base_url: "http://hts.fhir".into(),
        library_base_url: None,
        resolve_library_artifacts_from_fhir: false,
        included_libraries: vec![],
        patient_id: None,
        parameters: None,
        evaluation_date_time: None,
        prefetch: None,
        fhir_authorization: None,
    };

    let out = client.evaluate_expression(req).await.expect("eval");
    assert_eq!(out.expression, "InPopulation");
    assert_eq!(out.result_type.as_deref(), Some("java.lang.Boolean"));
    assert_eq!(out.result, json!({"valueBoolean": true}));
    assert!(matches!(
        out.normalized_result(),
        NormalizedSidecarResult::Object(_)
    ));
}

#[tokio::test]
async fn evaluate_expression_accepts_missing_result_type() {
    let srv = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/evaluate/expression"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "expression": "X",
            "result": null
        })))
        .mount(&srv)
        .await;

    let cfg = ClinicalReasoningConfig::new(srv.uri().to_string());
    let client = ClinicalReasoningClient::new(cfg).expect("client");

    let req = EvaluateExpressionRequest {
        elm: Some("{}".into()),
        elm_format: Default::default(),
        library_id: "L".into(),
        library_version: None,
        expression: "X".into(),
        hfs_base_url: "http://hfs".into(),
        hts_base_url: "http://hts".into(),
        library_base_url: None,
        resolve_library_artifacts_from_fhir: false,
        included_libraries: vec![],
        patient_id: None,
        parameters: None,
        evaluation_date_time: None,
        prefetch: None,
        fhir_authorization: None,
    };

    let out = client.evaluate_expression(req).await.expect("eval");
    assert!(out.result_type.is_none());
    assert!(matches!(
        out.normalized_result(),
        NormalizedSidecarResult::Null
    ));
}

#[tokio::test]
async fn evaluate_expression_non_success_preserves_body_detail() {
    let srv = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/evaluate/expression"))
        .respond_with(ResponseTemplate::new(422).set_body_json(json!({
            "resourceType": "OperationOutcome",
            "issue": [{"severity": "error", "code": "exception", "diagnostics": "CQL failure"}]
        })))
        .mount(&srv)
        .await;

    let cfg = ClinicalReasoningConfig::new(srv.uri().to_string());
    let client = ClinicalReasoningClient::new(cfg).expect("client");

    let req = EvaluateExpressionRequest {
        elm: Some("{}".into()),
        elm_format: Default::default(),
        library_id: "L".into(),
        library_version: None,
        expression: "X".into(),
        hfs_base_url: "http://hfs".into(),
        hts_base_url: "http://hts".into(),
        library_base_url: None,
        resolve_library_artifacts_from_fhir: false,
        included_libraries: vec![],
        patient_id: None,
        parameters: None,
        evaluation_date_time: None,
        prefetch: None,
        fhir_authorization: None,
    };

    let err = client.evaluate_expression(req).await.expect_err("422");
    match err {
        ClinicalReasoningError::SidecarRejected(r) => {
            assert_eq!(r.status, 422);
            assert!(r.summarize().contains("CQL failure"));
        }
        other => panic!("expected SidecarRejected, got {other:?}"),
    }
}

//! R5 binding validation integration tests.
//!
//! Run with R5 enabled:
//! `cargo test -p fhir-validation --features R5 --test r5_bindings`
#![cfg(feature = "R5")]
mod common {
    pub mod fixtures;
}

use common::fixtures::{
    assert_has_binding_issue, eval_r5_patient_expr, load_r5_patient, load_resource,
    r5_evaluator_for,
};
use fhir_validation::LocalTerminologyService;
use fhir_validation::Validator;
use fhir_validation::issue_to_op_outcome::validation_issues_to_operation_outcome;
use fhir_validation::terminology::service::RemoteTerminologyService;
use helios_fhir::FhirVersion;
use reqwest::Client;
use std::time::Duration;

#[ignore]
#[tokio::test]
async fn r5_patient_invalid_identifier() {
    let resource = load_resource(FhirVersion::R5, "invalid/patient/patient-bindings.json");
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(8))
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(8)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client");
    let terminology = RemoteTerminologyService::with_client(
        client,
        "http://localhost:8080/fhir".to_string(),
        FhirVersion::R5,
    );
    let evaluator = r5_evaluator_for(&resource);
    let validator = Validator::default();
    let issues = validator
        .validate_resource_async(&resource, Some(&terminology), &evaluator)
        .await;
    assert_has_binding_issue(
        &issues,
        "Patient.identifier[0].type",
        "http://hl7.org/fhir/ValueSet/identifier-type",
    );
    assert_has_binding_issue(
        &issues,
        "Patient.identifier[0].use",
        "http://hl7.org/fhir/ValueSet/identifier-use|5.0.0",
    );
}

#[ignore = "This is a debug test to see what the bindings look like"]
#[test]
fn r5_debug() {
    let patient = load_r5_patient("invalid/patient/patient-empty_name_ele1.json");
    let exprs = [
        "name[0].hasValue()",
        "name[0].children().count()",
        "name[0].id.count()",
        "name[0].hasValue() or (name[0].children().count() > name[0].id.count())",
    ];

    for expr in exprs {
        let result = eval_r5_patient_expr(&patient, expr);
        println!("\nEXPR: {expr}\nRESULT: {result:#?}");
    }
}

#[ignore = "This is a debug test to see what the bindings look like"]
#[test]
fn r5_debug_2() {
    let patient = load_r5_patient("invalid/patient/patient-empty-meta-security-code.json");
    let exprs = [
        "meta.security[0].hasValue()",
        "meta.security[0].children()",
        "meta.security[0].children().count()",
        "meta.security[0].id.count()",
        "meta.security[0].hasValue() or (meta.security[0].children().count() > meta.security[0].id.count())",
    ];

    for expr in exprs {
        let result = eval_r5_patient_expr(&patient, expr);
        println!("\nEXPR: {expr}\nRESULT: {result:#?}");
    }
}

#[ignore = "This is a debug test to see what the bindings look like"]
#[tokio::test]
async fn r5_sd_async() {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(8))
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(8)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client");

    let terminology = RemoteTerminologyService::with_client(
        client,
        "http://localhost:8080/fhir".to_string(),
        FhirVersion::R5,
    );

    let resource = load_resource(FhirVersion::R5, "valid/structuredefinition-language.json");
    let evaluator = r5_evaluator_for(&resource);
    let validator = Validator::default();
    let issues = validator
        .validate_resource_async(&resource, Some(&terminology), &evaluator)
        .await;
    let _outcome = validation_issues_to_operation_outcome(&issues);
}

#[ignore]
#[test]
fn r5_sd_sync() {
    let term = LocalTerminologyService::new(FhirVersion::R5);

    let resource = load_resource(FhirVersion::R5, "valid/structuredefinition-language.json");
    let evaluator = r5_evaluator_for(&resource);
    let validator = Validator::default();
    let _issues = validator.validate_resource(&resource, Some(&term), &evaluator);
}

#[tokio::test]
async fn r5_slot_async() {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(8))
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(8)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client");

    let terminology = RemoteTerminologyService::with_client(
        client,
        "http://localhost:8080/fhir".to_string(),
        FhirVersion::R5,
    );

    let resource = load_resource(FhirVersion::R5, "valid/slot-codeable-reference.json");
    let evaluator = r5_evaluator_for(&resource);
    let validator = Validator::default();
    let _issues = validator
        .validate_resource_async(&resource, Some(&terminology), &evaluator)
        .await;
}

#[tokio::test]
async fn r5_obs_async() {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(8))
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(8)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client");

    let terminology = RemoteTerminologyService::with_client(
        client,
        "http://localhost:8080/fhir".to_string(),
        FhirVersion::R5,
    );

    let resource = load_resource(FhirVersion::R5, "invalid/obs-resource-element-test.json");
    let evaluator = r5_evaluator_for(&resource);
    let validator = Validator::default();
    let _issues = validator
        .validate_resource_async(&resource, Some(&terminology), &evaluator)
        .await;
}

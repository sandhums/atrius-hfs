//! **Optional** parity checks against a remote FHIR server’s `$validate` (default: HAPI public).
//!
//! These tests are **`#[ignore]` by default** (network + third-party behavior). To run:
//!
//! ```text
//! FHIR_ONLINE_VALIDATOR_BASE_URL=https://hapi.fhir.org/baseR5 \
//!   cargo test -p fhir-validation --features R5 \
//!   --test json_examples_online_parity_tests -- --ignored
//! ```
//!
//! We compare **severity counts** (error/warning/information) between Atrius and the remote
//! `OperationOutcome`, not issue text. Validators legitimately differ on warnings and terminology.

#![cfg(feature = "R5")]

mod common {
    pub mod fhir_json_examples;
    pub mod fixtures;
    pub mod online_fhir_validate;
}

use crate::common::fhir_json_examples::{
    count_severities, fhir_json_dir, read_fhir_example_json, resource_type_of_json,
};
use crate::common::fixtures::local_terminology_r5;
use crate::common::online_fhir_validate::{
    count_operation_outcome_severities, post_instance_validate,
};
use crate::common::fhir_json_examples::load_r5_fhir_resource;
use fhir_validation::{R5FhirPathEvaluator, Validator};
use helios_fhir::FhirResource;

fn r5_eval(resource: &FhirResource) -> R5FhirPathEvaluator {
    let FhirResource::R5(r) = resource else {
        unreachable!()
    };
    R5FhirPathEvaluator::new((**r).clone())
}

#[tokio::test]
#[ignore = "requires FHIR_ONLINE_VALIDATOR_BASE_URL and network"]
async fn hapi_validate_genomic_patient_severity_counts() {
    let path = fhir_json_dir("R5").join("Patient-genomicPatient.json");
    let json = read_fhir_example_json("R5", "Patient-genomicPatient.json");
    let rt = resource_type_of_json(&path, &json);

    let oo = post_instance_validate(&rt, &json).await.expect("remote validate");
    let (re, rw, ri) = count_operation_outcome_severities(&oo);

    let resource = load_r5_fhir_resource("Patient-genomicPatient.json");
    let issues = Validator::default().validate_resource(
        &resource,
        Some(&local_terminology_r5()),
        &r5_eval(&resource),
    );
    let (le, lw, li) = count_severities(&issues);

    assert_eq!(re, 0, "HAPI should report no errors for this example: {oo:#?}");
    assert_eq!(le, 0, "local validator should report no errors: {issues:#?}");
    // Warnings may differ; log both for human review when running --ignored.
    assert!(
        lw < 100 && rw < 100,
        "local errors={le} warnings={lw} info={li}; remote errors={re} warnings={rw} info={ri}"
    );
}

#[tokio::test]
#[ignore = "requires FHIR_ONLINE_VALIDATOR_BASE_URL and network"]
async fn hapi_validate_practitioner_severity_counts() {
    let path = fhir_json_dir("R5").join("Practitioner-practitioner01.json");
    let json = read_fhir_example_json("R5", "Practitioner-practitioner01.json");
    let rt = resource_type_of_json(&path, &json);

    let oo = post_instance_validate(&rt, &json).await.expect("remote validate");
    let (re, rw, _ri) = count_operation_outcome_severities(&oo);

    let resource = load_r5_fhir_resource("Practitioner-practitioner01.json");
    let issues = Validator::default().validate_resource(
        &resource,
        Some(&local_terminology_r5()),
        &r5_eval(&resource),
    );
    let (le, lw, _li) = count_severities(&issues);

    assert_eq!(re, 0, "HAPI: {oo:#?}");
    assert_eq!(le, 0, "local: {issues:#?}");
    assert!(lw < 100 && rw < 100, "local warnings={lw} remote warnings={rw}");
}

//! Full-resource validation using HL7 examples from `crates/fhir/tests/data/json/R5/`.
//!
//! Some `#[tokio::test]` cases use [`crate::harness::remote_terminology_for_tests`] and require a
//! live terminology server (see crate root docs).

use crate::common::fhir_json_examples::load_r5_fhir_resource;
use crate::common::fixtures::{
    assert_has_binding_issue, assert_has_binding_issue_with_diagnostics, assert_no_errors,
    load_resource, local_terminology_r5,
};
use crate::harness::{
    assert_remote_terminology_reachable, r5_evaluator_for, remote_terminology_for_tests,
};
use fhir_validation::Validator;
use helios_fhir::FhirVersion;

// --- Patient.genomicPatient: primitive `code`, CodeableConcept on identifiers, `meta.tag` --------

/// Local validation: no **error**/**fatal** issues. `meta.tag` uses a code from `v3-ActReason` while
/// the element binding targets `common-tags` (example strength); locally we only defer with a
/// terminology message, not an error.
#[test]
fn patient_genomic_local_no_errors_meta_tag_deferred_to_remote() {
    let resource = load_r5_fhir_resource("Patient-genomicPatient.json");
    let evaluator = r5_evaluator_for(&resource);
    let term = local_terminology_r5();
    let issues = Validator::default().validate_resource(&resource, Some(&term), &evaluator);
    assert_no_errors(&issues);
    assert_has_binding_issue_with_diagnostics(
        &issues,
        "Patient.meta.tag[0]",
        "http://hl7.org/fhir/ValueSet/common-tags",
        "Remote terminology validation required for http://hl7.org/fhir/ValueSet/common-tags",
    );
}

/// Same instance with remote terminology: still no severity **errors**; server reports `v3-ActReason`
/// is not in `common-tags`.
#[tokio::test]
async fn patient_genomic_remote_no_errors_meta_tag_not_in_valueset() {
    assert_remote_terminology_reachable().await;
    let resource = load_r5_fhir_resource("Patient-genomicPatient.json");
    let evaluator = r5_evaluator_for(&resource);
    let term = remote_terminology_for_tests();
    let issues = Validator::default()
        .validate_resource_async(&resource, Some(&term), &evaluator)
        .await;
    assert_no_errors(&issues);
    assert_has_binding_issue_with_diagnostics(
        &issues,
        "Patient.meta.tag[0]",
        "http://hl7.org/fhir/ValueSet/common-tags",
        "The provided code 'http://hl7.org/fhir/ValueSet/common-tags#HTEST ('test health data')' was not found in the value set 'http://hl7.org/fhir/ValueSet/common-tags|4.0.1'",
    );
}

// --- CodeableConcept / Coding (meta.tag, priority) -----------------------------------------

#[test]
fn coverage_eligibility_request_wrong_valueset_in_meta_tag_local() {
    let resource = load_r5_fhir_resource("coverageeligibilityrequest-example.json");
    let evaluator = r5_evaluator_for(&resource);
    let term = local_terminology_r5();
    let issues = Validator::default().validate_resource(&resource, Some(&term), &evaluator);
    assert_has_binding_issue(
        &issues,
        "CoverageEligibilityRequest.meta.tag[0]",
        "http://hl7.org/fhir/ValueSet/common-tags",
    );
}

#[test]
fn coverage_eligibility_request_priority_code_without_system_local() {
    let resource = load_r5_fhir_resource("coverageeligibilityrequest-example.json");
    let evaluator = r5_evaluator_for(&resource);
    let term = local_terminology_r5();
    let issues = Validator::default().validate_resource(&resource, Some(&term), &evaluator);
    assert_has_binding_issue_with_diagnostics(
        &issues,
        "CoverageEligibilityRequest.priority",
        "http://hl7.org/fhir/ValueSet/process-priority",
        "Coding.system is required",
    );
}

// --- CodeableReference (`Slot.serviceType`) — HTS + HL7 `service-type` ValueSet ------------

/// Valid `service-type` code in `Slot.serviceType` passes locally (no remote server).
#[test]
fn slot_service_type_codeableref_local_no_errors() {
    let resource = load_resource(FhirVersion::R5, "valid/slot-codeable-reference.json");
    let evaluator = r5_evaluator_for(&resource);
    let term = local_terminology_r5();
    let issues = Validator::default().validate_resource(&resource, Some(&term), &evaluator);
    assert_no_errors(&issues);
}

/// Same fixture against HTS: `service-type` membership resolves remotely with no errors.
#[tokio::test]
async fn slot_service_type_codeableref_remote_no_errors() {
    assert_remote_terminology_reachable().await;
    let resource = load_resource(FhirVersion::R5, "valid/slot-codeable-reference.json");
    let evaluator = r5_evaluator_for(&resource);
    let term = remote_terminology_for_tests();
    let issues = Validator::default()
        .validate_resource_async(&resource, Some(&term), &evaluator)
        .await;
    assert_no_errors(&issues);
}

#[tokio::test]
async fn slot_service_type_codeableref_remote_invalid_code() {
    assert_remote_terminology_reachable().await;
    let resource = load_resource(
        FhirVersion::R5,
        "valid/slot-codeable-reference-invalid-code.json",
    );
    let evaluator = r5_evaluator_for(&resource);
    let term = remote_terminology_for_tests();
    let issues = Validator::default()
        .validate_resource_async(&resource, Some(&term), &evaluator)
        .await;
    assert_has_binding_issue_with_diagnostics(
        &issues,
        "Slot.serviceType[0]",
        "http://hl7.org/fhir/ValueSet/service-type",
        "Code 'not-valid-code' is not in value set 'http://hl7.org/fhir/ValueSet/service-type'",
    );
}

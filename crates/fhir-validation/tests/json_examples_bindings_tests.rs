//! Binding-related validation against examples in `crates/fhir/tests/data/json/R5/`.
//!
//! These tests use **local** terminology ([`LocalTerminologyService`]) like production’s
//! local-first binding path. They record binding-like issues (ValueSet / terminology detail
//! codes) for curated examples so regressions show up as diffs in counts or fingerprints.

#![cfg(feature = "R5")]

mod common {
    pub mod fhir_json_examples;
    pub mod fixtures;
}

use crate::common::fhir_json_examples::{
    count_severities, fhir_json_dir, is_binding_like_issue, load_r5_fhir_resource, R5_CURATED,
};
use crate::common::fixtures::{assert_no_errors, local_terminology_r5};
use fhir_validation::{R5FhirPathEvaluator, Validator};
use helios_fhir::FhirResource;

fn evaluator_for(resource: &FhirResource) -> R5FhirPathEvaluator {
    let FhirResource::R5(r) = resource else {
        unreachable!()
    };
    R5FhirPathEvaluator::new((**r).clone())
}

#[test]
fn curated_r5_examples_resolve_each_file() {
    let root = fhir_json_dir("R5");
    for name in R5_CURATED {
        let p = root.join(name);
        assert!(
            p.is_file(),
            "missing example {} (expected at {})",
            name,
            p.display()
        );
        let _ = load_r5_fhir_resource(name);
    }
}

#[test]
fn curated_examples_binding_validation_completes_without_excessive_issues() {
    let validator = Validator::default();
    let term = local_terminology_r5();
    for name in R5_CURATED {
        let resource = load_r5_fhir_resource(name);
        let evaluator = evaluator_for(&resource);
        let issues = validator.validate_resource(&resource, Some(&term), &evaluator);
        assert!(
            issues.len() < 5000,
            "{} produced excessive issues (possible loop): {}",
            name,
            issues.len()
        );
        let (e, w, _i) = count_severities(&issues);
        let binding_n = issues.iter().filter(|i| is_binding_like_issue(i)).count();
        // Keep a loose fingerprint so an accidental explosion of binding work is visible in CI.
        assert!(
            binding_n < 200,
            "{name}: binding-like issues = {binding_n}, errors={e} warnings={w}"
        );
    }
}

/// Canonical HL7 example Patient + coded identifiers should not report **errors** once terminology resolves locally.
#[test]
fn patient_genomic_binding_example_no_errors() {
    let resource = load_r5_fhir_resource("Patient-genomicPatient.json");
    let evaluator = evaluator_for(&resource);
    let term = local_terminology_r5();
    let issues = Validator::default().validate_resource(&resource, Some(&term), &evaluator);
    assert_no_errors(&issues);
}

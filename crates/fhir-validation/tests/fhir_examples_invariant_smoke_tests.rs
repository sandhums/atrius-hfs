//! Smoke tests using official-style example JSON from the `helios-fhir` crate tree
//! (`crates/fhir/tests/data/json/{R4,R5,R6}/`, HL7 FHIR examples).
//!
//! These assert that [`fhir_validation::Validator::validate_resource`] completes and returns a
//! well-formed issue list for real-world shapes. Tight assertions on issue counts are
//! intentionally avoided here (they belong next to specific invariant or profile fixtures).
//!
//! Add targeted regression tests under `tests/` when a particular example must always pass or fail
//! a named constraint.

#![cfg(feature = "R5")]

mod common {
    pub mod fixtures;
}

use crate::common::fixtures::local_terminology_r5;
use fhir_validation::{R5FhirPathEvaluator, Validator};
use helios_fhir::FhirResource;
use std::fs;
use std::path::PathBuf;

fn r5_example_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../fhir/tests/data/json/R5")
        .join(name)
}

fn load_r5_resource_from_example(name: &str) -> FhirResource {
    let path = r5_example_path(name);
    let json = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("read {}: {e}", path.display());
    });
    let r: helios_fhir::r5::Resource = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {} as R5 Resource: {e}", path.display()));
    FhirResource::R5(Box::new(r))
}

#[test]
fn validate_resource_succeeds_for_fhir_repo_patient_genomic_example() {
    let resource = load_r5_resource_from_example("Patient-genomicPatient.json");
    let FhirResource::R5(r) = &resource else {
        unreachable!();
    };
    let evaluator = R5FhirPathEvaluator::new((**r).clone());
    let term = local_terminology_r5();
    let issues = Validator::default().validate_resource(&resource, Some(&term), &evaluator);
    for issue in &issues {
        assert!(
            !issue.diagnostics.is_empty() || issue.summary.is_some(),
            "issue should carry diagnostics or summary: {issue:?}"
        );
    }
}

#[test]
fn validate_resource_succeeds_for_fhir_repo_practitioner_example() {
    let resource = load_r5_resource_from_example("Practitioner-practitioner01.json");
    let FhirResource::R5(r) = &resource else {
        unreachable!();
    };
    let evaluator = R5FhirPathEvaluator::new((**r).clone());
    let term = local_terminology_r5();
    let issues = Validator::default().validate_resource(&resource, Some(&term), &evaluator);
    assert!(
        issues.len() < 10_000,
        "absurd issue count suggests a loop bug: {}",
        issues.len()
    );
}

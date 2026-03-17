use std::fs;
use std::path::PathBuf;

use fhir_validation::R4FhirPathEvaluator;
use fhir_validation::{
    R5FhirPathEvaluator, Severity, TerminologyService, ValidationIssue, Validator,
};
use fhir_validation_types::BindingStrength;
use helios_fhir::{FhirResource, FhirVersion};

pub fn fixture_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("r4")
        .join(rel)
}
pub fn fixture_path_r5(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("r5")
        .join(rel)
}

pub fn load_fixture(version: FhirVersion, rel: &str) -> String {
    let path = match version {
        #[cfg(feature = "R4")]
        FhirVersion::R4 => fixture_path(rel),
        #[cfg(feature = "R4B")]
        FhirVersion::R4B => fixture_path(rel),
        #[cfg(feature = "R5")]
        FhirVersion::R5 => fixture_path_r5(rel),
        #[cfg(feature = "R6")]
        FhirVersion::R6 => fixture_path_r5(rel),
    };

    fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read fixture {rel}: {e}"))
}
pub fn load_resource(version: FhirVersion, rel: &str) -> FhirResource {
    let json = load_fixture(version, rel);

    match version {
        #[cfg(feature = "R4")]
        FhirVersion::R4 => {
            let r: helios_fhir::r4::Resource = serde_json::from_str(&json).unwrap();
            FhirResource::R4(Box::new(r))
        }

        #[cfg(feature = "R4B")]
        FhirVersion::R4B => {
            let r: helios_fhir::r4b::Resource = serde_json::from_str(&json).unwrap();
            FhirResource::R4B(Box::new(r))
        }

        #[cfg(feature = "R5")]
        FhirVersion::R5 => {
            let r: helios_fhir::r5::Resource = serde_json::from_str(&json).unwrap();
            FhirResource::R5(Box::new(r))
        }

        #[cfg(feature = "R6")]
        FhirVersion::R6 => {
            let r: helios_fhir::r6::Resource = serde_json::from_str(&json).unwrap();
            FhirResource::R6(Box::new(r))
        }
    }
}

pub fn validate_resource(
    resource: &FhirResource,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue> {
    match resource {
        #[cfg(feature = "R4")]
        FhirResource::R4(r) => {
            let evaluator = R4FhirPathEvaluator::new((**r).clone());
            validator().validate_r4_resource(r.as_ref(), terminology, &evaluator)
        }

        #[cfg(feature = "R4B")]
        FhirResource::R4B(r) => {
            todo!("R4B validator")
        }

        #[cfg(feature = "R5")]
        FhirResource::R5(r) => {
            let evaluator = R5FhirPathEvaluator::new((**r).clone());
            validator().validate_r5_resource(r.as_ref(), terminology, &evaluator)
        }

        #[cfg(feature = "R6")]
        FhirResource::R6(r) => {
            todo!("R6 validator")
        }
    }
}

pub fn validator() -> Validator {
    Validator::default()
}

fn issue_matches_requested_severity(issue: &ValidationIssue, severity: Severity) -> bool {
    match severity {
        Severity::Warning => {
            matches!(issue.severity, Severity::Warning | Severity::Information)
        }
        Severity::Information => issue.severity == Severity::Information,
        Severity::Error => issue.severity == Severity::Error,
        Severity::Fatal => issue.severity == Severity::Fatal,
    }
}

fn assert_has_severity_like(issues: &[ValidationIssue], severity: Severity, label: &str) {
    assert!(
        issues
            .iter()
            .any(|issue| issue_matches_requested_severity(issue, severity)),
        "expected {label}, got issues: {issues:#?}"
    );
}

pub fn assert_has_invariant_issue(
    issues: &[ValidationIssue],
    path: &str,
    expression: &str,
    severity: Severity,
) {
    assert!(
        issues.iter().any(|issue| {
            issue.instance_path.as_deref() == Some(path)
                && issue.expression.as_deref() == Some(expression)
                && issue_matches_requested_severity(issue, severity)
        }),
        "expected issue at path {path:?} with expression {expression:?} and severity {severity:?}, got issues: {issues:#?}"
    );
}
pub fn assert_has_binding_issue(issues: &[ValidationIssue], path: &str) {
    assert!(
        issues.iter().any(|issue| {
            issue.instance_path.as_deref() == Some(path)
            // && issue.expression.as_deref() == Some(expression)
        }),
        "expected issue at path {path:?}, got issues: {issues:#?}"
    );
}
pub fn assert_has_error(issues: &[ValidationIssue]) {
    assert_has_severity_like(issues, Severity::Error, "error/fatal issue");
}
pub fn assert_has_warning(issues: &[ValidationIssue]) {
    assert_has_severity_like(issues, Severity::Warning, "warning/info issue");
}
pub fn assert_has_info(issues: &[ValidationIssue]) {
    assert_has_severity_like(issues, Severity::Information, "info issue");
}
pub fn assert_no_errors(issues: &[ValidationIssue]) {
    let errors: Vec<_> = issues
        .iter()
        .filter(|i| matches!(i.severity, Severity::Error | Severity::Fatal))
        .collect();

    assert!(
        errors.is_empty(),
        "expected no errors, got issues: {:#?}",
        issues
    );
}
#[allow(dead_code)]
pub fn assert_no_errors_or_warnings(issues: &[ValidationIssue]) {
    let bad: Vec<_> = issues
        .iter()
        .filter(|i| {
            matches!(
                i.severity,
                Severity::Error | Severity::Fatal | Severity::Warning
            )
        })
        .collect();

    assert!(
        bad.is_empty(),
        "expected no errors/warnings, got issues: {:#?}",
        issues
    );
}

#[allow(dead_code)]
pub fn assert_issue_count(issues: &[ValidationIssue], expected: usize) {
    assert_eq!(
        issues.len(),
        expected,
        "expected {expected} issues, got issues: {issues:#?}"
    );
}
#[allow(dead_code)]
pub fn assert_has_issue_code(issues: &[ValidationIssue], code: &str) {
    assert!(
        issues.iter().any(|issue| issue.code == code),
        "expected issue code {code:?}, got issues: {issues:#?}"
    );
}
#[allow(dead_code)]
pub fn assert_has_issue_at_path(issues: &[ValidationIssue], code: &str, instance_path: &str) {
    assert!(
        issues.iter().any(|issue| {
            issue.code == code && issue.instance_path.as_deref() == Some(instance_path)
        }),
        "expected issue code {code:?} at path {instance_path:?}, got issues: {issues:#?}"
    );
}

pub fn assert_has_invariant(
    issues: &[ValidationIssue],
    instance_path: &str,
    diagnostics_contains: &str,
) {
    assert!(
        issues.iter().any(|issue| {
            issue.code == "invariant"
                && issue.instance_path.as_deref() == Some(instance_path)
                && issue.diagnostics.contains(diagnostics_contains)
        }),
        "expected invariant at path {instance_path:?} containing {diagnostics_contains:?}, got issues: {issues:#?}"
    );
}
#[allow(dead_code)]
pub fn assert_has_severity(issues: &[ValidationIssue], severity: Severity) {
    assert!(
        issues.iter().any(|issue| issue.severity == severity),
        "expected severity {severity:?}, got issues: {issues:#?}"
    );
}
#[allow(dead_code)]
pub fn assert_has_invariant_expression(
    issues: &[ValidationIssue],
    instance_path: &str,
    expression: &str,
) {
    assert!(
        issues.iter().any(|issue| {
            issue.code == "invariant"
                && issue.instance_path.as_deref() == Some(instance_path)
                && issue.expression.as_deref() == Some(expression)
        }),
        "expected invariant at path {instance_path:?} with expression {expression:?}, got issues: {issues:#?}"
    );
}
#[allow(dead_code)]
pub fn eval_r4_patient_expr(
    patient: &helios_fhir::r4::Patient,
    expr: &str,
) -> Result<Vec<helios_fhirpath_support::EvaluationResult>, String> {
    let root = helios_fhir::r4::Resource::Patient(Box::new(patient.clone()));
    let evaluator = fhir_validation::R4FhirPathEvaluator::new(root);

    evaluator
        .eval_expression(expr)
        .map_err(|e| format!("{e:?}"))
}

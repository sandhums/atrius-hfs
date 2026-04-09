

//! Convert internal `ValidationIssue` values into a FHIR OperationOutcome.
//!
//! The validation engine returns rich internal issues that are convenient for
//! generator-driven validation and testing. This module provides a FHIR-facing
//! projection of those issues so callers can expose validation results through
//! standard FHIR APIs such as `$validate`.
//!
//! Design notes:
//! - The validator continues to return `Vec<ValidationIssue>` as its canonical
//!   internal representation.
//! - OperationOutcome is treated as a derived presentation layer.
//! - This module supports both JSON OperationOutcome generation and typed R5
//!   OperationOutcome construction.
//!
//! Mapping policy:
//! - `ValidationIssue.severity` -> `OperationOutcome.issue.severity`
//! - internal validator `code` strings -> conservative FHIR `issue.code`
//! - prefer `instance_path` over `fhir_path` for `expression`
//! - `diagnostics` is surfaced as `details.text`
//!
//! Unknown internal issue codes are mapped to `processing`.

use crate::ValidationIssue;
use fhir_validation_types::Severity;
#[cfg(feature = "R5")]
use helios_fhir::r5::OperationOutcome as R5OperationOutcome;
use serde_json::{json, Value};
#[cfg(feature = "R5")]
use helios_fhir::r5::Resource;

fn severity_to_fhir(value: Severity) -> &'static str {
    match value {
        Severity::Fatal => "fatal",
        Severity::Error => "error",
        Severity::Warning => "warning",
        Severity::Information => "information",
    }
}

fn code_to_fhir(code: &str) -> &'static str {
    match code {
        "invalid" => "invalid",
        "structure" => "structure",
        "required" => "required",
        "value" => "value",
        "not-found" => "not-found",
        "deleted" => "deleted",
        "multiple-matches" => "multiple-matches",
        "conflict" => "conflict",
        "lock-error" => "lock-error",
        "not-supported" => "not-supported",
        "duplicate" => "duplicate",
        "processing" => "processing",
        "transient" => "transient",
        "security" => "security",
        "login" => "login",
        "unknown" => "unknown",
        "informational" => "informational",
        "success" => "success",
        // Internal validation categories that do not have a dedicated 1:1 FHIR
        // issue-type mapping are surfaced as generic processing failures.
        "invariant" => "processing",
        _ => "processing",
    }
}

fn issue_expression(issue: &ValidationIssue) -> Option<String> {
    issue
        .instance_path
        .clone()
        .or_else(|| Some(issue.fhir_path.clone()))
}

/// Convert a single `ValidationIssue` into an `OperationOutcome.issue` JSON object.
pub fn validation_issue_to_operation_outcome_issue(issue: &ValidationIssue) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "severity".to_string(),
        Value::String(severity_to_fhir(issue.severity).to_string()),
    );
    obj.insert(
        "code".to_string(),
        Value::String(code_to_fhir(&issue.code).to_string()),
    );
    obj.insert(
        "details".to_string(),
        json!({
            "text": issue.diagnostics,
        }),
    );

    if let Some(expr) = issue_expression(issue) {
        obj.insert("expression".to_string(), json!([expr]));
    }

    if let Some(source_expr) = &issue.expression {
        obj.insert(
            "extension".to_string(),
            json!([
                {
                    "url": "https://atrius.health/fhir/StructureDefinition/validation-source-expression",
                    "valueString": source_expr
                }
            ]),
        );
    }

    Value::Object(obj)
}

/// Convert a slice of validation issues into a FHIR `OperationOutcome` JSON document.
pub fn validation_issues_to_operation_outcome(issues: &[ValidationIssue]) -> Value {
    let converted: Vec<Value> = issues
        .iter()
        .map(validation_issue_to_operation_outcome_issue)
        .collect();

    json!({
        "resourceType": "OperationOutcome",
        "issue": converted,
    })
}

/// Convert a slice of validation issues into a typed R5 `OperationOutcome`.
///
/// This reuses the canonical JSON projection and then deserializes it into the
/// generated R5 model so the mapping logic remains centralized in one place.
#[cfg(feature = "R5")]
pub fn validation_issues_to_r5_operation_outcome(
    issues: &[ValidationIssue],
) -> Result<R5OperationOutcome, serde_json::Error> {
    serde_json::from_value(validation_issues_to_operation_outcome(issues))
}

/// Convert validation issues into OperationOutcome JSON by serializing the
/// typed R5 resource.
///
/// This keeps the typed R5 conversion as the canonical implementation while
/// still exposing a convenient JSON helper for callers that want to return a
/// JSON payload directly.
#[cfg(feature = "R5")]
pub fn validation_issues_to_r5_operation_outcome_json(
    issues: &[ValidationIssue],
) -> Result<Value, serde_json::Error> {
    let outcome = validation_issues_to_r5_operation_outcome(issues)?;
    serde_json::to_value(Resource::OperationOutcome(Box::new(outcome)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_issue(
        severity: Severity,
        code: &str,
        fhir_path: &str,
        instance_path: Option<&str>,
        expression: Option<&str>,
        diagnostics: &str,
    ) -> ValidationIssue {
        ValidationIssue {
            severity,
            code: code.to_string(),
            fhir_path: fhir_path.to_string(),
            instance_path: instance_path.map(str::to_string),
            expression: expression.map(str::to_string),
            diagnostics: diagnostics.to_string(),
        }
    }

    #[test]
    fn converts_single_issue_to_operation_outcome_issue() {
        let issue = mk_issue(
            Severity::Error,
            "value",
            "Patient.gender",
            Some("Patient.gender"),
            Some("http://hl7.org/fhir/ValueSet/administrative-gender|4.0.1"),
            "Gender code is invalid",
        );

        let json = validation_issue_to_operation_outcome_issue(&issue);
        assert_eq!(json["severity"], "error");
        assert_eq!(json["code"], "value");
        assert_eq!(json["details"]["text"], "Gender code is invalid");
        assert_eq!(json["expression"][0], "Patient.gender");
        assert_eq!(
            json["extension"][0]["valueString"],
            "http://hl7.org/fhir/ValueSet/administrative-gender|4.0.1"
        );
    }

    #[test]
    fn prefers_instance_path_over_fhir_path() {
        let issue = mk_issue(
            Severity::Error,
            "invariant",
            "Reference",
            Some("Slot.schedule"),
            None,
            "Constraint failed",
        );

        let json = validation_issue_to_operation_outcome_issue(&issue);
        assert_eq!(json["expression"][0], "Slot.schedule");
    }

    #[test]
    fn falls_back_to_fhir_path_when_instance_path_absent() {
        let issue = mk_issue(
            Severity::Warning,
            "invariant",
            "Observation",
            None,
            None,
            "Constraint failed",
        );

        let json = validation_issue_to_operation_outcome_issue(&issue);
        assert_eq!(json["expression"][0], "Observation");
    }

    #[test]
    fn maps_invariant_code_to_processing() {
        let issue = mk_issue(
            Severity::Error,
            "invariant",
            "Observation",
            Some("Observation"),
            None,
            "Constraint failed",
        );

        let json = validation_issue_to_operation_outcome_issue(&issue);
        assert_eq!(json["code"], "processing");
    }

    #[test]
    fn converts_multiple_issues_to_operation_outcome() {
        let issues = vec![
            mk_issue(
                Severity::Error,
                "value",
                "Patient.gender",
                Some("Patient.gender"),
                None,
                "Gender code is invalid",
            ),
            mk_issue(
                Severity::Warning,
                "invariant",
                "Patient",
                Some("Patient"),
                None,
                "Narrative should be present",
            ),
        ];

        let json = validation_issues_to_operation_outcome(&issues);
        assert_eq!(json["resourceType"], "OperationOutcome");
        assert_eq!(json["issue"].as_array().unwrap().len(), 2);
        assert_eq!(json["issue"][0]["severity"], "error");
        assert_eq!(json["issue"][1]["severity"], "warning");
    }
    #[cfg(feature = "R5")]
    #[test]
    fn converts_multiple_issues_to_typed_r5_operation_outcome() {
        let issues = vec![
            mk_issue(
                Severity::Error,
                "value",
                "Patient.gender",
                Some("Patient.gender"),
                None,
                "Gender code is invalid",
            ),
            mk_issue(
                Severity::Warning,
                "invariant",
                "Patient",
                Some("Patient"),
                None,
                "Narrative should be present",
            ),
        ];

        let outcome = validation_issues_to_r5_operation_outcome(&issues)
            .expect("typed R5 OperationOutcome should deserialize from generated JSON");
        let json = serde_json::to_value(Resource::OperationOutcome(Box::new(outcome)))
            .expect("typed R5 OperationOutcome should serialize back to JSON");

        assert_eq!(json["resourceType"], "OperationOutcome");
        assert_eq!(json["issue"].as_array().unwrap().len(), 2);
        assert_eq!(json["issue"][0]["severity"], "error");
        assert_eq!(json["issue"][1]["severity"], "warning");
    }

    #[cfg(feature = "R5")]
    #[test]
    fn r5_operation_outcome_json_helper_matches_direct_json_projection() {
        let issues = vec![mk_issue(
            Severity::Error,
            "value",
            "Patient.gender",
            Some("Patient.gender"),
            Some("http://hl7.org/fhir/ValueSet/administrative-gender|4.0.1"),
            "Gender code is invalid",
        )];

        let direct = validation_issues_to_operation_outcome(&issues);
        let typed_json = validation_issues_to_r5_operation_outcome_json(&issues)
            .expect("typed R5 OperationOutcome JSON helper should succeed");

        assert_eq!(typed_json, direct);
    }
}
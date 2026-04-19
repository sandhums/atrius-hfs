//! Shared helpers for binding validation across FHIR versions.
//!
//! This module contains:
//! - helpers for constructing binding-related `ValidationIssue`s
//! - array-aware JSON path traversal utilities used to resolve generated binding
//!   paths to concrete instance paths
//! - utilities for mapping a generated binding path to the root resource/datatype
//!   instance path used during validation
//!
//! Version-specific binding modules (`r4/binding.rs`, `r5/binding.rs`, etc.) use these
//! helpers to validate ValueSet bindings on the
//! [FHIR bindable types](https://hl7.org/fhir/elementdefinition-definitions.html#ElementDefinition.binding):
//! primitive `code`, `string`, `uri`, `Coding`, `CodeableConcept`, `Quantity`,
//! and `CodeableReference` (where the version supports it), while preserving
//! precise instance locations.

use crate::binding::engine::LocalBindingOutcome;
use crate::service::{TerminologyService, TerminologyServiceSync};
use crate::types::TerminologyMembershipOutcome;
use crate::validation_issue_detail::{ValidationIssueDetailCode, ValidationSourceKind};
use crate::{ValidationError, ValidationIssue, Validator};
use fhir_validation_types::{
    binding_target_kind_for_element_type_code, BindingStrength, BindingTargetKind, Severity,
};
use helios_fhir::TerminologyValidationError;
use serde_json::Value;

/// Whether `kind` is among declared choice type codes (`None` / empty slice = unrestricted).
pub(crate) fn choice_declared_allows_kind(
    declared: Option<&[String]>,
    kind: BindingTargetKind,
) -> bool {
    match declared {
        None => true,
        Some(codes) if codes.is_empty() => true,
        Some(codes) => codes
            .iter()
            .filter_map(|c| binding_target_kind_for_element_type_code(c.as_str()))
            .any(|k| k == kind),
    }
}

/// Map string-ish instance JSON to a primitive [`BindingTargetKind`] using declared choice codes.
pub(crate) fn primitive_choice_target_kind(declared: Option<&[String]>) -> BindingTargetKind {
    match declared {
        None => BindingTargetKind::String,
        Some(codes) => {
            if codes.iter().any(|c| c.as_str() == "code") {
                BindingTargetKind::Code
            } else if codes.iter().any(|c| c.as_str() == "string") {
                BindingTargetKind::String
            } else if codes.iter().any(|c| c.as_str() == "uri") {
                BindingTargetKind::Uri
            } else {
                BindingTargetKind::String
            }
        }
    }
}

/// Extract primitive text for terminology binding from instance JSON.
///
/// Accepts a JSON string or the generated element shape `{"value": "..."}` used
/// for FHIR primitives such as `code`, `string`, and `uri`.
pub(crate) fn bindable_primitive_string_value(value: &Value) -> Option<&str> {
    match value {
        Value::String(s) => Some(s.as_str()),
        Value::Object(map) => map.get("value").and_then(|v| v.as_str()),
        _ => None,
    }
}

/// Convert a binding miss into a `ValidationIssue` using validator policy for
/// the supplied binding strength.
///
/// Returns `None` when the binding strength should not surface an issue.
pub fn issue_for_binding_miss(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    diagnostics: String,
) -> Option<ValidationIssue> {
    validator
        .binding_miss_severity(strength)
        .map(|severity| ValidationIssue {
            severity,
            code: "value".to_string(),
            fhir_path: fhir_path.to_string(),
            instance_path: None,
            expression: Some(valueset_url.to_string()),
            expression_kind: Some(ValidationSourceKind::CanonicalUri),
            source_invariant_key: None,
            summary: Some(binding_miss_summary(strength).to_string()),
            detail_code: Some(binding_miss_detail_code(strength)),
            diagnostics,
        })
}

/// Construct a terminology-related validation issue.
///
/// Used when local validation is insufficient and remote terminology validation
/// is required or fails.
pub fn terminology_validation_issue(
    fhir_path: &str,
    valueset_url: &str,
    diagnostics: String,
) -> ValidationIssue {
    ValidationIssue {
        severity: Severity::Error,
        code: "terminology".to_string(),
        fhir_path: fhir_path.to_string(),
        instance_path: None,
        expression: Some(valueset_url.to_string()),
        expression_kind: Some(ValidationSourceKind::CanonicalUri),
        source_invariant_key: None,
        detail_code: Some(ValidationIssueDetailCode::TerminologyValidationFailed),
        summary: Some("Terminology validation failed".to_string()),
        diagnostics,
    }
}

pub fn terminology_unavailable_issue(
    fhir_path: &str,
    valueset_url: &str,
    diagnostics: String,
) -> ValidationIssue {
    ValidationIssue {
        severity: Severity::Error,
        code: "terminology".to_string(),
        fhir_path: fhir_path.to_string(),
        instance_path: None,
        expression: Some(valueset_url.to_string()),
        expression_kind: Some(ValidationSourceKind::CanonicalUri),
        source_invariant_key: None,
        detail_code: Some(ValidationIssueDetailCode::TerminologyServiceUnavailable),
        summary: Some(
            "Terminology validation could not be completed - Service unavailable".to_string(),
        ),
        diagnostics,
    }
}
/// Construct a value-shape validation issue.
///
/// Used when a bound field cannot be validated locally because the value is
/// malformed or missing the required structure for terminology validation.
pub fn value_issue(
    fhir_path: &str,
    valueset_url: &str,
    summary: &str,
    detail_code: ValidationIssueDetailCode,
    diagnostics: String,
) -> ValidationIssue {
    ValidationIssue {
        severity: Severity::Error,
        code: "value".to_string(),
        fhir_path: fhir_path.to_string(),
        instance_path: None,
        expression: Some(valueset_url.to_string()),
        expression_kind: Some(ValidationSourceKind::CanonicalUri),
        source_invariant_key: None,
        summary: Some(summary.to_string()),
        detail_code: Some(detail_code),
        diagnostics,
    }
}

/// Convert a terminal local terminology validation error into concrete issues.
///
/// This helper assumes the caller has already separated control-flow outcomes
/// such as `Valid` and `NeedsRemote { .. }` from terminal local errors.
pub fn local_error_to_issues(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    err: TerminologyValidationError,
) -> Vec<ValidationIssue> {
    match err {
        TerminologyValidationError::NotInValueSet {
            valueset_url: _,
            system,
            code,
        } => {
            let diagnostics = match system {
                Some(system) => format!(
                    "Code '{}' from system '{}' not found in ValueSet '{}'",
                    code, system, valueset_url
                ),
                None => format!("Code '{}' not found in ValueSet '{}'", code, valueset_url),
            };

            issue_for_binding_miss(validator, fhir_path, valueset_url, strength, diagnostics)
                .into_iter()
                .collect()
        }

        TerminologyValidationError::MissingSystem(msg) => {
            vec![value_issue(
                fhir_path,
                valueset_url,
                "Code cannot be validated without a system",
                ValidationIssueDetailCode::CodeWithoutSystem,
                msg,
            )]
        }

        TerminologyValidationError::UnknownCode { system, code } => {
            vec![value_issue(
                fhir_path,
                valueset_url,
                "Code is not recognized in the declared code system",
                ValidationIssueDetailCode::InvalidBindableValue,
                format!("Unknown code '{}' in system '{}'", code, system),
            )]
        }

        TerminologyValidationError::WrongDisplay {
            system,
            code,
            expected,
            provided,
        } => {
            vec![value_issue(
                fhir_path,
                valueset_url,
                "Display does not match the code",
                ValidationIssueDetailCode::InvalidBindableValue,
                format!(
                    "Wrong display '{}' for code '{}' in system '{}'; expected '{}'",
                    provided, code, system, expected
                ),
            )]
        }

        TerminologyValidationError::InvalidInput(msg) => {
            vec![value_issue(
                fhir_path,
                valueset_url,
                "Value could not be validated against the bound value set",
                ValidationIssueDetailCode::InvalidBindableValue,
                msg,
            )]
        }

        TerminologyValidationError::RemoteValidationRequired(_) => {
            unreachable!(
                "Terminal local error conversion should not receive RemoteValidationRequired; use NeedsRemote instead"
            )
        }
    }
}

/// Resolve all JSON values matching a relative binding path and return each
/// value together with its concrete indexed instance path.
///
/// This is array-aware and is used for repeated FHIR elements such as
/// `HumanName.use`, `Identifier.use`, and `ContactPoint.system`.
pub(crate) fn get_json_values_with_instance_paths<'a>(
    value: &'a Value,
    root_instance_path: &str,
    relative_path: &str,
) -> Vec<(&'a Value, String)> {
    if relative_path.is_empty() {
        return vec![(value, root_instance_path.to_string())];
    }

    let segments: Vec<&str> = relative_path.split('.').collect();
    let mut out = Vec::new();
    collect_json_values_with_paths(value, root_instance_path, &segments, &mut out);
    out
}

/// Recursive worker used by `get_json_values_with_instance_paths`.
///
/// Traverses objects and arrays, preserving indexed instance paths such as
/// `Patient.name[1].use` or `Patient.telecom[2].system`.
pub(crate) fn collect_json_values_with_paths<'a>(
    value: &'a Value,
    current_path: &str,
    remaining_segments: &[&str],
    out: &mut Vec<(&'a Value, String)>,
) {
    if remaining_segments.is_empty() {
        match value {
            Value::Array(items) => {
                for (idx, item) in items.iter().enumerate() {
                    out.push((item, format!("{current_path}[{idx}]")));
                }
            }
            _ => out.push((value, current_path.to_string())),
        }
        return;
    }

    let segment = remaining_segments[0];
    let rest = &remaining_segments[1..];

    match value {
        Value::Object(map) => {
            if let Some(child) = map.get(segment) {
                let next_path = format!("{current_path}.{segment}");
                collect_json_values_with_paths(child, &next_path, rest, out);
            }
        }

        Value::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                let indexed_path = format!("{current_path}[{idx}]");
                collect_json_values_with_paths(item, &indexed_path, remaining_segments, out);
            }
        }

        _ => {}
    }
}
/// Extract the current generated-type root from a generated binding path.
///
/// Binding definitions are attached to direct child fields of the current
/// generated type, so runtime path resolution needs the parent path of the
/// bound field rather than only the first segment.
///
/// Examples:
/// - `Patient.gender` -> `Patient`
/// - `HumanName.use` -> `HumanName`
/// - `Patient.contact.relationship` -> `Patient.contact`
/// - `Observation.component.code` -> `Observation.component`
///
/// Version-specific binding modules then resolve the final child segment
/// relative to this root to build concrete indexed instance paths.
pub(crate) fn root_instance_path(binding_path: &str) -> &str {
    binding_path
        .rsplit_once('.')
        .map(|(head, _)| head)
        .unwrap_or(binding_path)
}

/// Return the direct child binding path relative to the serialized focus object
/// being validated.
///
/// Generated binding definitions are always attached to direct child fields of
/// the current generated type. For top-level types this means:
/// - `Patient.gender` → `gender`
/// - `HumanName.use` → `use`
///
/// For nested helper types such as `Patient.contact`, the generated binding path
/// still carries the full logical path:
/// - `Patient.contact.relationship` → `relationship`
/// - `Patient.contact.gender` → `gender`
///
/// Using the final path segment keeps runtime binding resolution aligned with
/// the local serialized helper object shape.
pub(crate) fn relative_binding_path(binding_path: &str) -> &str {
    binding_path
        .rsplit_once('.')
        .map(|(_, tail)| tail)
        .unwrap_or(binding_path)
}
pub(crate) fn prettify_remote_terminology_error(
    valueset_url: &str,
    err: &crate::ValidationError,
) -> String {
    match err {
        crate::ValidationError::TerminologyRemote(remote) => {
            if !remote.diagnostics.is_empty() {
                return format!(
                    "Remote terminology validation failed for ValueSet '{}': {}",
                    valueset_url,
                    remote.diagnostics.join("; ")
                );
            }
            if let Some(body) = &remote.raw_body {
                return format!(
                    "Remote terminology validation failed for ValueSet '{}': {}",
                    valueset_url, body
                );
            }

            if let Some(status) = remote.status {
                return format!(
                    "Remote terminology validation failed for ValueSet '{}' with status {}",
                    valueset_url, status
                );
            }

            format!(
                "Remote terminology validation failed for ValueSet '{}'",
                valueset_url
            )
        }
        _ => format!("Remote terminology validation failed: {}", err),
    }
}

#[derive(Debug, Clone)]
pub struct RemoteMembershipRequest {
    pub valueset_url: String,
    pub system: Option<String>,
    pub code: String,
    pub display: Option<String>,
}

pub enum LocalBindingDisposition {
    Done(Vec<ValidationIssue>),
    NeedsRemote(RemoteMembershipRequest),
    Valid,
}

pub fn remote_request_from_outcome(
    outcome: &LocalBindingOutcome,
) -> Option<RemoteMembershipRequest> {
    match outcome {
        LocalBindingOutcome::NeedsRemote {
            valueset_url,
            system,
            code,
            display,
        } => Some(RemoteMembershipRequest {
            valueset_url: valueset_url.clone(),
            system: system.clone(),
            code: code.clone(),
            display: display.clone(),
        }),
        _ => None,
    }
}
pub fn classify_local_outcome(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    outcome: LocalBindingOutcome,
) -> LocalBindingDisposition {
    match outcome {
        LocalBindingOutcome::Valid => LocalBindingDisposition::Valid,
        LocalBindingOutcome::NeedsRemote {
            valueset_url,
            system,
            code,
            display,
        } => LocalBindingDisposition::NeedsRemote(RemoteMembershipRequest {
            valueset_url,
            system,
            code,
            display,
        }),
        LocalBindingOutcome::Error(err) => LocalBindingDisposition::Done(local_error_to_issues(
            validator,
            fhir_path,
            valueset_url,
            strength,
            err,
        )),
    }
}

pub fn remote_result_to_issues(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    req: &RemoteMembershipRequest,
    outcome: Result<TerminologyMembershipOutcome, ValidationError>,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    match outcome {
        Ok(outcome) if outcome.is_member => issues,
        Ok(outcome) => {
            let diagnostics = outcome.message.unwrap_or_else(|| {
                if let Some(system) = &req.system {
                    format!(
                        "The provided coding {}#{} was not found in ValueSet {}",
                        system, req.code, valueset_url
                    )
                } else {
                    format!(
                        "The provided code '{}' was not found in ValueSet {}",
                        req.code, valueset_url
                    )
                }
            });

            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                diagnostics,
            ) {
                issues.push(issue);
            }
            issues
        }
        Err(e) => {
            issues.push(crate::binding::common::terminology_validation_issue(
                fhir_path,
                valueset_url,
                prettify_remote_terminology_error(valueset_url, &e),
            ));
            issues
        }
    }
}
pub fn execute_remote_sync(
    validator: &Validator,
    fhir_path: &str,
    strength: BindingStrength,
    terminology: Option<&dyn TerminologyServiceSync>,
    req: &RemoteMembershipRequest,
) -> Vec<ValidationIssue> {
    let Some(terminology) = terminology else {
        return vec![crate::binding::common::terminology_unavailable_issue(
            fhir_path,
            &req.valueset_url,
            "Remote terminology validation required but no TerminologyService was provided"
                .to_string(),
        )];
    };

    let outcome = terminology.member_of(
        &req.valueset_url,
        req.system.as_deref(),
        &req.code,
        req.display.as_deref(),
    );

    remote_result_to_issues(
        validator,
        fhir_path,
        &req.valueset_url,
        strength,
        req,
        outcome,
    )
}

pub async fn execute_remote_async(
    validator: &Validator,
    fhir_path: &str,
    strength: BindingStrength,
    terminology: Option<&dyn TerminologyService>,
    req: &RemoteMembershipRequest,
) -> Vec<ValidationIssue> {
    let Some(terminology) = terminology else {
        return vec![crate::binding::common::terminology_unavailable_issue(
            fhir_path,
            &req.valueset_url,
            "Remote terminology validation required but no TerminologyService was provided"
                .to_string(),
        )];
    };

    let outcome = terminology
        .member_of(
            &req.valueset_url,
            req.system.as_deref(),
            &req.code,
            req.display.as_deref(),
        )
        .await;

    remote_result_to_issues(
        validator,
        fhir_path,
        &req.valueset_url,
        strength,
        req,
        outcome,
    )
}

fn binding_miss_summary(strength: BindingStrength) -> &'static str {
    match strength {
        BindingStrength::Required => "Code is not in the required value set",
        BindingStrength::Extensible => "Code is outside the extensible value set",
        BindingStrength::Preferred => "Code is outside the preferred value set",
        BindingStrength::Example => "Code is outside the example value set",
    }
}

fn binding_miss_detail_code(strength: BindingStrength) -> ValidationIssueDetailCode {
    match strength {
        BindingStrength::Required => ValidationIssueDetailCode::RequiredBindingMiss,
        BindingStrength::Extensible => ValidationIssueDetailCode::ExtensibleBindingMiss,
        BindingStrength::Preferred => ValidationIssueDetailCode::PreferredBindingMiss,
        BindingStrength::Example => ValidationIssueDetailCode::ExampleBindingMiss,
    }
}

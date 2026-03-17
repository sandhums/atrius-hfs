//! Shared helpers for binding validation across FHIR versions.
//!
//! This module contains:
//! - helpers for constructing binding-related `ValidationIssue`s
//! - array-aware JSON path traversal utilities used to resolve generated binding
//!   paths to concrete instance paths
//! - utilities for mapping a generated binding path to the root resource/datatype
//!   instance path used during validation
//!
//! Version-specific binding modules (`r4/binding.rs`, `r5/binding.rs`) use these
//! helpers to validate primitive `code`, `Coding`, and `CodeableConcept` values
//! while preserving precise instance locations.

use crate::{ValidationIssue, Validator};
use fhir_validation_types::{BindingStrength, Severity};
use serde_json::Value;

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
            code: "value",
            fhir_path: fhir_path.to_string(),
            instance_path: None,
            expression: Some(valueset_url.to_string()),
            diagnostics,
        })
}

/// Construct a terminology-related validation issue.
///
/// Used when local validation is insufficient and remote terminology validation
/// is required or fails.
pub fn terminology_issue(
    fhir_path: &str,
    valueset_url: &str,
    diagnostics: String,
) -> ValidationIssue {
    ValidationIssue {
        severity: Severity::Error,
        code: "terminology",
        fhir_path: fhir_path.to_string(),
        instance_path: None,
        expression: Some(valueset_url.to_string()),
        diagnostics,
    }
}

/// Construct a value-shape validation issue.
///
/// Used when a bound field cannot be validated locally because the value is
/// malformed or missing the required structure for terminology validation.
pub fn value_issue(fhir_path: &str, valueset_url: &str, diagnostics: String) -> ValidationIssue {
    ValidationIssue {
        severity: Severity::Error,
        code: "value",
        fhir_path: fhir_path.to_string(),
        instance_path: None,
        expression: Some(valueset_url.to_string()),
        diagnostics,
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
        out.push((value, current_path.to_string()));
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

/// Extract the top-level validation root from a generated binding path.
///
/// For example, `Patient.gender` becomes `Patient`, and
/// `Observation.component.code` becomes `Observation`.
///
/// Version-specific binding modules then refine this into concrete indexed
/// instance paths during traversal.
pub(crate) fn root_instance_path(binding_path: &str) -> &str {
    binding_path.split('.').next().unwrap_or(binding_path)
}

/// Strip the leading type/resource name from a generated binding path.
///
/// For example:
/// - `Patient.gender` → `gender`
/// - `HumanName.use` → `use`
///
/// This allows the binding resolver to apply paths relative to the serialized
/// focus object being validated.
pub(crate) fn relative_binding_path(binding_path: &str) -> &str {
    binding_path
        .split_once('.')
        .map(|(_, rest)| rest)
        .unwrap_or(binding_path)
}

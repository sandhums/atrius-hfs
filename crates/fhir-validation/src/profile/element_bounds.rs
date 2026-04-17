//! `ElementDefinition.maxLength` and `minValue` / `maxValue` checks against instance JSON.

use crate::profile::cardinality::relative_profile_path;
use crate::profile::helpers::get_values_with_paths_at_relative_path;
use crate::profile::types::ExtractedElementRule;
use crate::validation_issue_detail::ValidationIssueDetailCode;
use crate::{Severity, ValidationIssue};
use serde::Serialize;
use serde_json::Value;

pub fn validate_element_bounds<T: Serialize>(
    resource: &T,
    resource_type: &str,
    rules: &[ExtractedElementRule],
) -> Vec<ValidationIssue> {
    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for element bound validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ValidationException),
                diagnostics: format!(
                    "Failed to serialize resource while validating profile element bounds: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    validate_element_bounds_from_json(&root, resource_type, rules)
}

fn validate_element_bounds_from_json(
    root: &Value,
    resource_type: &str,
    rules: &[ExtractedElementRule],
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    for rule in rules {
        let has_bounds =
            rule.max_length.is_some() || rule.min_value.is_some() || rule.max_value.is_some();
        if !has_bounds {
            continue;
        }

        let Some(rel) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        let values = get_values_with_paths_at_relative_path(root, resource_type, rel);

        for (value, instance_path) in values {
            if let Some(max_len) = rule.max_length {
                issues.extend(check_max_length(&rule.path, &instance_path, value, max_len));
            }
            if let Some(ref min_v) = rule.min_value {
                issues.extend(check_min_value(&rule.path, &instance_path, value, min_v));
            }
            if let Some(ref max_v) = rule.max_value {
                issues.extend(check_max_value(&rule.path, &instance_path, value, max_v));
            }
        }
    }

    issues
}

fn check_max_length(
    path: &str,
    instance_path: &str,
    value: &Value,
    max_len: u32,
) -> Vec<ValidationIssue> {
    let s = match primitive_string_value(value) {
        Some(s) => s,
        None => return Vec::new(),
    };
    if s.chars().count() as u32 <= max_len {
        return Vec::new();
    }
    vec![ValidationIssue {
        severity: Severity::Error,
        code: "structure".to_string(),
        summary: Some("Element value exceeds maxLength".to_string()),
        expression_kind: None,
        source_invariant_key: None,
        detail_code: Some(ValidationIssueDetailCode::ConstraintViolation),
        diagnostics: format!(
            "String '{}' at '{}' has length {} which exceeds maxLength {}",
            path,
            instance_path,
            s.chars().count(),
            max_len
        ),
        expression: None,
        fhir_path: path.to_string(),
        instance_path: Some(instance_path.to_string()),
    }]
}

fn check_min_value(
    path: &str,
    instance_path: &str,
    value: &Value,
    bound: &Value,
) -> Vec<ValidationIssue> {
    let Some((key, bound_val)) = single_bound_key_value(bound, "minValue") else {
        return Vec::new();
    };
    let actual = match primitive_compare_value(value) {
        Some(v) => v,
        None => return Vec::new(),
    };
    if compare_values(key, &actual, bound_val) != Ordering::Less {
        return Vec::new();
    }
    vec![ValidationIssue {
        severity: Severity::Error,
        code: "structure".to_string(),
        summary: Some("Element value is below minValue".to_string()),
        expression_kind: None,
        source_invariant_key: None,
        detail_code: Some(ValidationIssueDetailCode::ConstraintViolation),
        diagnostics: format!(
            "Value at '{}' ({}) is below {} bound {:?}",
            instance_path, path, key, bound
        ),
        expression: None,
        fhir_path: path.to_string(),
        instance_path: Some(instance_path.to_string()),
    }]
}

fn check_max_value(
    path: &str,
    instance_path: &str,
    value: &Value,
    bound: &Value,
) -> Vec<ValidationIssue> {
    let Some((key, bound_val)) = single_bound_key_value(bound, "maxValue") else {
        return Vec::new();
    };
    let actual = match primitive_compare_value(value) {
        Some(v) => v,
        None => return Vec::new(),
    };
    if compare_values(key, &actual, bound_val) != Ordering::Greater {
        return Vec::new();
    }
    vec![ValidationIssue {
        severity: Severity::Error,
        code: "structure".to_string(),
        summary: Some("Element value is above maxValue".to_string()),
        expression_kind: None,
        source_invariant_key: None,
        detail_code: Some(ValidationIssueDetailCode::ConstraintViolation),
        diagnostics: format!(
            "Value at '{}' ({}) is above {} bound {:?}",
            instance_path, path, key, bound
        ),
        expression: None,
        fhir_path: path.to_string(),
        instance_path: Some(instance_path.to_string()),
    }]
}

fn single_bound_key_value<'a>(bound: &'a Value, prefix: &str) -> Option<(&'a str, &'a Value)> {
    let obj = bound.as_object()?;
    let (k, v) = obj.iter().find(|(k, _)| k.starts_with(prefix))?;
    Some((k.as_str(), v))
}

fn primitive_string_value(value: &Value) -> Option<&str> {
    match value {
        Value::String(s) => Some(s.as_str()),
        Value::Object(map) => map.get("value").and_then(|v| v.as_str()),
        _ => None,
    }
}

#[derive(Clone, Debug)]
enum CompareValue {
    String(String),
    Number(f64),
    Integer(i64),
}

use std::cmp::Ordering;

fn primitive_compare_value(value: &Value) -> Option<CompareValue> {
    match value {
        Value::String(s) => Some(CompareValue::String(s.clone())),
        Value::Number(n) => n
            .as_f64()
            .map(CompareValue::Number)
            .or_else(|| n.as_i64().map(CompareValue::Integer)),
        Value::Bool(b) => Some(CompareValue::Integer(if *b { 1 } else { 0 })),
        Value::Object(map) => {
            if let Some(v) = map.get("value") {
                return primitive_compare_value(v);
            }
            None
        }
        _ => None,
    }
}

fn compare_values(bound_key: &str, actual: &CompareValue, bound: &Value) -> Ordering {
    match bound_key {
        k if k.contains("DateTime") || k.contains("Instant") || k.contains("Date") => {
            let a = match actual {
                CompareValue::String(s) => s.as_str(),
                _ => return Ordering::Equal,
            };
            let b = match bound {
                Value::String(s) => s.as_str(),
                Value::Object(m) => m.get("value").and_then(|v| v.as_str()).unwrap_or(""),
                _ => "",
            };
            a.cmp(b)
        }
        k if k.contains("Time") => {
            let a = match actual {
                CompareValue::String(s) => s.as_str(),
                _ => return Ordering::Equal,
            };
            let b = match bound {
                Value::String(s) => s.as_str(),
                Value::Object(m) => m.get("value").and_then(|v| v.as_str()).unwrap_or(""),
                _ => "",
            };
            a.cmp(b)
        }
        k if k.contains("Decimal") || k.contains("Integer") || k.contains("UnsignedInt") => {
            let a = match actual {
                CompareValue::Number(n) => *n,
                CompareValue::Integer(i) => *i as f64,
                CompareValue::String(s) => s.parse::<f64>().unwrap_or(f64::NAN),
            };
            let b = match bound {
                Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
                Value::String(s) => s.parse::<f64>().unwrap_or(f64::NAN),
                Value::Object(m) => m
                    .get("value")
                    .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
                    .unwrap_or(f64::NAN),
                _ => f64::NAN,
            };
            a.partial_cmp(&b).unwrap_or(Ordering::Equal)
        }
        _ => Ordering::Equal,
    }
}

//! Min / max cardinality and **mustSupport** checks against instance JSON.
//!
//! All three validators serialize the resource once to [`serde_json::Value`] and count
//! or test paths **relative to the resource type** (e.g. `Patient` → `identifier`, not
//! `Patient.identifier`). Rules come from [`ExtractedElementRule`]
//! rows the extractor already merged (snapshot-first).
//!
//! # Slice names in reported paths
//!
//! When [`ExtractedElementRule::slice_name`] is set (FHIR `ElementDefinition.sliceName`),
//! issue [`crate::ValidationIssue::fhir_path`] / `instance_path` and diagnostics use
//! `{path}:{sliceName}` (e.g. `Patient.extension:birthPlace`), consistent with
//! [`crate::profile::slicing`].
//!
//! For rules with [`ExtractedElementRule::slice_name`], counts are limited to repeated items
//! that match the slice (typically via nested extension `url` fixed values). Slice base-path
//! min/max on profiles with explicit slicing is also enforced in [`crate::profile::slicing`].
//!
//! For **nested** paths (`participant.actor`, `name.given`, …), min/max are evaluated **per
//! repeating parent instance** (each `participant` entry), not as a flat total across the resource.
//!
//! # Optional parents
//!
//! `skip_when_optional_parent_absent` ensures child minimums (e.g. `communication.language`) do
//! not fire when the parent repeat (`communication`) is absent—matching common IG expectations.

use crate::issue_code;
use crate::profile::helpers::get_values_at_relative_path;
use crate::profile::slice_matching::{
    count_slice_instances, get_slice_scoped_values, matches_slice_instance,
    profile_element_display_path, slice_repeating_base_path,
};
use crate::profile::types::{ExtractedElementRule, ExtractedProfile};
use crate::{Severity, ValidationConfig, ValidationIssue, ValidationIssueDetailCode};
use serde::Serialize;
use serde_json::Value;

/// Emit **required** (minimum cardinality) issues for rules where `min > 0` and the instance has
/// fewer matching values than required.
pub fn validate_min_cardinality<T: Serialize>(
    resource: &T,
    resource_type: &str,
    profile: &ExtractedProfile,
) -> Vec<ValidationIssue> {
    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for cardinality validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ValidationException),
                diagnostics: format!(
                    "Failed to serialize resource while validating profile cardinality: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    validate_min_cardinality_from_json(&root, resource_type, profile)
}

/// Emit issues when repeated elements exceed `ElementDefinition.max` (non-`*`) for a rule.
pub fn validate_max_cardinality<T: Serialize>(
    resource: &T,
    resource_type: &str,
    profile: &ExtractedProfile,
) -> Vec<ValidationIssue> {
    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for cardinality validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ValidationException),
                diagnostics: format!(
                    "Failed to serialize resource while validating profile cardinality: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    validate_max_cardinality_from_json(&root, resource_type, profile)
}

/// When [`crate::ValidationConfig::validate_must_support`] is enabled, warn or error if
/// `ElementDefinition.mustSupport` is true but the rule’s path has no values in the instance
/// (same JSON traversal as [`validate_min_cardinality`]).
///
/// Severity is [`crate::ValidationConfig::must_support_missing_severity`].
pub fn validate_must_support<T: Serialize>(
    resource: &T,
    resource_type: &str,
    profile: &ExtractedProfile,
    config: &ValidationConfig,
) -> Vec<ValidationIssue> {
    if !config.validate_must_support {
        return Vec::new();
    }

    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for mustSupport validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ValidationException),
                diagnostics: format!(
                    "Failed to serialize resource while validating mustSupport: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    validate_must_support_from_json(&root, resource_type, profile, config)
}

fn validate_must_support_from_json(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    config: &ValidationConfig,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    for rule in &profile.element_rules {
        if rule.must_support != Some(true) {
            continue;
        }

        let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        if skip_when_optional_parent_absent(root, relative_path) {
            continue;
        }

        if skip_when_optional_slice_absent(root, resource_type, profile, rule) {
            continue;
        }

        let count = slice_rule_populated_count(root, resource_type, profile, rule, relative_path);
        if count > 0 {
            continue;
        }

        let display_path = profile_element_display_path(rule);
        issues.push(ValidationIssue {
            severity: config.must_support_missing_severity,
            code: issue_code::STRUCTURE.to_string(),
            summary: Some(
                "Element is marked mustSupport in the profile but has no value in this instance"
                    .to_string(),
            ),
            expression_kind: None,
            source_invariant_key: None,
            detail_code: Some(ValidationIssueDetailCode::BusinessRuleViolation),
            diagnostics: format!(
                "Element '{}' is mustSupport but is not populated in the instance.",
                display_path
            ),
            expression: None,
            fhir_path: display_path.clone(),
            instance_path: Some(display_path),
        });
    }

    issues
}

fn validate_min_cardinality_from_json(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    for rule in &profile.element_rules {
        let Some(min) = rule.min else {
            continue;
        };

        if min == 0 {
            continue;
        }

        let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        if skip_when_optional_parent_absent(root, relative_path) {
            continue;
        }

        if skip_when_optional_slice_absent(root, resource_type, profile, rule) {
            continue;
        }

        if cardinality_meets_min(root, resource_type, profile, rule, relative_path, min as usize) {
            continue;
        }

        let display_path = profile_element_display_path(rule);
        issues.push(ValidationIssue {
            severity: Severity::Error,
            code: "required".to_string(),
            summary: Some("Required element is missing or below minimum cardinality".to_string()),
            expression_kind: None,
            source_invariant_key: None,
            detail_code: Some(ValidationIssueDetailCode::RequiredElementMissing),
            diagnostics: format!(
                "Required element '{}' is missing or does not meet minimum cardinality {}.",
                display_path, min
            ),
            expression: None,
            fhir_path: display_path.clone(),
            instance_path: Some(display_path),
        });
    }

    issues
}

fn validate_max_cardinality_from_json(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    for rule in &profile.element_rules {
        let Some(max) = rule.max.as_deref() else {
            continue;
        };

        if max == "*" {
            continue;
        }

        let Ok(max_value) = max.parse::<usize>() else {
            continue;
        };

        let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        if skip_when_optional_parent_absent(root, relative_path) {
            continue;
        }

        if skip_when_optional_slice_absent(root, resource_type, profile, rule) {
            continue;
        }

        if !cardinality_above_max(root, resource_type, profile, rule, relative_path, max_value) {
            continue;
        }

        let display_path = profile_element_display_path(rule);
        issues.push(ValidationIssue {
            severity: Severity::Error,
            code: issue_code::STRUCTURE.to_string(),
            summary: Some("Element exceeds maximum cardinality for this profile".to_string()),
            expression_kind: None,
            source_invariant_key: None,
            detail_code: Some(ValidationIssueDetailCode::MaximumCardinalityExceeded),
            diagnostics: format!(
                "Element '{}' exceeds maximum cardinality {}.",
                display_path, max
            ),
            expression: None,
            fhir_path: display_path.clone(),
            instance_path: Some(display_path),
        });
    }

    issues
}

/// Strip the `{ResourceType}.` prefix from an `ElementDefinition.path`, yielding the **relative**
/// dotted path used when walking JSON (e.g. `Patient.identifier` → `identifier`). Returns `""` for
/// the resource root path equal to `resource_type`.
pub fn relative_profile_path<'a>(resource_type: &str, absolute_path: &'a str) -> Option<&'a str> {
    if absolute_path == resource_type {
        return Some("");
    }

    let prefix = format!("{}.", resource_type);
    absolute_path.strip_prefix(&prefix)
}

/// `true` when `structure_path` is only the resource root (e.g. `Patient`).
///
/// Constraints on the root are evaluated with the resource as FHIRPath context (same as
/// [`crate::Validator::apply_invariants`]). Nested paths use declared-path focus via
/// [`FhirPathEvaluator::eval_invariant`](crate::FhirPathEvaluator::eval_invariant).
pub fn is_root_profile_element_path(resource_type: &str, structure_path: &str) -> bool {
    relative_profile_path(resource_type, structure_path)
        .map(|rel| rel.is_empty())
        .unwrap_or(false)
}

fn count_relative_path(root: &Value, relative_path: &str) -> usize {
    if relative_path.is_empty() {
        return terminal_count(root);
    }

    get_values_at_relative_path(root, relative_path).len()
}

/// Return `true` when the instance has more values than `max` allows for this rule.
fn cardinality_above_max(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    relative_path: &str,
    max: usize,
) -> bool {
    cardinality_exceeds_max(root, resource_type, profile, rule, relative_path, max)
}

fn cardinality_meets_min(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    relative_path: &str,
    min: usize,
) -> bool {
    if rule.slice_name.is_some() {
        return slice_cardinality_meets_min(root, resource_type, profile, rule, relative_path, min);
    }

    per_parent_cardinality_meets_min(root, relative_path, min)
}

fn cardinality_exceeds_max(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    relative_path: &str,
    max: usize,
) -> bool {
    if rule.slice_name.is_some() {
        return slice_cardinality_exceeds_max(root, resource_type, profile, rule, relative_path, max);
    }

    per_parent_cardinality_exceeds_max(root, relative_path, max)
}

fn slice_rule_populated_count(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    relative_path: &str,
) -> usize {
    if rule.slice_name.is_some() {
        if slice_repeating_base_path(profile, rule) == Some(rule.path.as_str()) {
            return count_slice_instances(root, resource_type, profile, rule);
        }
        return get_slice_scoped_values(root, resource_type, profile, rule).len();
    }

    count_relative_path(root, relative_path)
}

fn per_parent_cardinality_meets_min(root: &Value, relative_path: &str, min: usize) -> bool {
    let Some((parent_rel, child_rel)) = relative_path.rsplit_once('.') else {
        return count_relative_path(root, relative_path) >= min;
    };

    let parents = get_values_at_relative_path(root, parent_rel);
    if parents.is_empty() {
        return min == 0;
    }

    parents.iter().all(|parent| {
        get_values_at_relative_path(parent, child_rel).len() >= min
    })
}

fn per_parent_cardinality_exceeds_max(root: &Value, relative_path: &str, max: usize) -> bool {
    let Some((parent_rel, child_rel)) = relative_path.rsplit_once('.') else {
        return count_relative_path(root, relative_path) > max;
    };

    let parents = get_values_at_relative_path(root, parent_rel);
    if parents.is_empty() {
        return false;
    }

    parents.iter().any(|parent| {
        get_values_at_relative_path(parent, child_rel).len() > max
    })
}

fn slice_cardinality_meets_min(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    _relative_path: &str,
    min: usize,
) -> bool {
    let Some(base_path) = slice_repeating_base_path(profile, rule) else {
        return false;
    };

    if rule.path == base_path {
        return count_slice_instances(root, resource_type, profile, rule) >= min;
    }

    let Some(child_rel) = rule.path.strip_prefix(&format!("{base_path}.")) else {
        return false;
    };
    let Some(relative_base) = relative_profile_path(resource_type, base_path) else {
        return false;
    };

    if min == 0 {
        return true;
    }

    get_values_at_relative_path(root, relative_base)
        .into_iter()
        .filter(|instance| matches_slice_instance(profile, rule, instance))
        .all(|instance| get_values_at_relative_path(instance, child_rel).len() >= min)
}

fn slice_cardinality_exceeds_max(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    _relative_path: &str,
    max: usize,
) -> bool {
    let Some(base_path) = slice_repeating_base_path(profile, rule) else {
        return false;
    };

    if rule.path == base_path {
        return count_slice_instances(root, resource_type, profile, rule) > max;
    }

    let Some(child_rel) = rule.path.strip_prefix(&format!("{base_path}.")) else {
        return false;
    };
    let Some(relative_base) = relative_profile_path(resource_type, base_path) else {
        return false;
    };

    get_values_at_relative_path(root, relative_base)
        .into_iter()
        .filter(|instance| matches_slice_instance(profile, rule, instance))
        .any(|instance| get_values_at_relative_path(instance, child_rel).len() > max)
}

/// Skip child-element cardinality on optional slices that have no matching instances.
fn skip_when_optional_slice_absent(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
) -> bool {
    let Some(slice_name) = rule.slice_name.as_deref() else {
        return false;
    };
    let Some(base_path) = slice_repeating_base_path(profile, rule) else {
        return false;
    };
    if rule.path == base_path {
        return false;
    }

    let slice_root = profile.element_rules.iter().find(|candidate| {
        candidate.path == base_path && candidate.slice_name.as_deref() == Some(slice_name)
    });
    let Some(slice_root) = slice_root else {
        return false;
    };

    let required = slice_root.min.unwrap_or(0) > 0;
    if required {
        return false;
    }

    count_slice_instances(root, resource_type, profile, slice_root) == 0
}

fn terminal_count(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Array(items) => items.len(),
        _ => 1,
    }
}

/// For nested `relative_path` (`communication.language`), ignore the rule when the parent
/// (`communication`) has no instances: child minimums apply only inside an existing slice.
fn skip_when_optional_parent_absent(root: &Value, relative_path: &str) -> bool {
    let Some((parent_rel, _)) = relative_path.rsplit_once('.') else {
        return false;
    };
    if parent_rel.is_empty() {
        return false;
    }
    count_relative_path(root, parent_rel) == 0
}

#[cfg(test)]
mod tests {
    use super::{validate_max_cardinality, validate_min_cardinality, validate_must_support};
    use crate::ValidationConfig;
    use crate::ValidationIssueDetailCode;
    use crate::profile::types::{ExtractedElementRule, ExtractedProfile};
    use fhir_validation_types::BindingDef;
    use fhir_validation_types::Severity;
    use serde_json::json;

    fn test_profile(resource_type: &str, rules: Vec<ExtractedElementRule>) -> ExtractedProfile {
        ExtractedProfile {
            url: "http://example.org/fhir/test-profile".to_string(),
            resource_type: resource_type.to_string(),
            element_rules: rules,
            ..Default::default()
        }
    }

    fn rule(path: &str, min: Option<u32>, max: Option<&str>) -> ExtractedElementRule {
        ExtractedElementRule {
            id: path.to_string(),
            path: path.to_string(),
            min,
            max: max.map(str::to_owned),
            binding: None::<BindingDef>,
            constraints: Vec::new(),
            value_constraint: None,
            type_constraints: vec![],
            slicing: None,
            slice_name: None,
            ..Default::default()
        }
    }

    fn must_support_rule(path: &str, ms: bool) -> ExtractedElementRule {
        ExtractedElementRule {
            id: path.to_string(),
            path: path.to_string(),
            must_support: Some(ms),
            ..rule(path, None, None)
        }
    }

    fn must_support_rule_slice(path: &str, slice: &str, ms: bool) -> ExtractedElementRule {
        ExtractedElementRule {
            id: format!("{path}:{slice}"),
            path: path.to_string(),
            slice_name: Some(slice.to_string()),
            must_support: Some(ms),
            ..rule(path, None, None)
        }
    }

    #[test]
    fn min_cardinality_counts_polymorphic_value_address() {
        let ext = json!({
            "url": "http://hl7.org/fhir/StructureDefinition/patient-birthPlace",
            "valueAddress": { "city": "Mysuru", "country": "IN" }
        });
        let issues = validate_min_cardinality(
            &ext,
            "Extension",
            &test_profile("Extension", vec![rule("Extension.value[x]", Some(1), None)]),
        );
        assert!(
            issues.is_empty(),
            "valueAddress should satisfy Extension.value[x] min cardinality, got {issues:?}"
        );
    }

    #[test]
    fn must_support_slice_name_in_issue_paths() {
        let patient = json!({ "resourceType": "Patient" });
        let profile = test_profile(
            "Patient",
            vec![must_support_rule_slice(
                "Patient.extension",
                "birthPlace",
                true,
            )],
        );
        let issues = validate_must_support(&patient, "Patient", &profile, &ValidationConfig::default());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].fhir_path, "Patient.extension:birthPlace");
        assert_eq!(
            issues[0].instance_path.as_deref(),
            Some("Patient.extension:birthPlace")
        );
        assert!(
            issues[0]
                .diagnostics
                .contains("Patient.extension:birthPlace"),
            "diagnostics should name the slice: {:?}",
            issues[0].diagnostics
        );
    }

    #[test]
    fn must_support_missing_emits_warning_by_default() {
        let patient = json!({ "resourceType": "Patient" });
        let profile = test_profile("Patient", vec![must_support_rule("Patient.active", true)]);
        let issues =
            validate_must_support(&patient, "Patient", &profile, &ValidationConfig::default());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert_eq!(issues[0].fhir_path, "Patient.active");
    }

    #[test]
    fn must_support_present_produces_no_issue() {
        let patient = json!({
            "resourceType": "Patient",
            "active": true
        });
        let profile = test_profile("Patient", vec![must_support_rule("Patient.active", true)]);
        let issues =
            validate_must_support(&patient, "Patient", &profile, &ValidationConfig::default());
        assert!(issues.is_empty());
    }

    #[test]
    fn must_support_disabled_emits_nothing() {
        let patient = json!({ "resourceType": "Patient" });
        let profile = test_profile("Patient", vec![must_support_rule("Patient.active", true)]);
        let mut cfg = ValidationConfig::default();
        cfg.validate_must_support = false;
        let issues = validate_must_support(&patient, "Patient", &profile, &cfg);
        assert!(issues.is_empty());
    }

    #[test]
    fn must_support_respects_configured_severity() {
        let patient = json!({ "resourceType": "Patient" });
        let profile = test_profile("Patient", vec![must_support_rule("Patient.active", true)]);
        let mut cfg = ValidationConfig::default();
        cfg.must_support_missing_severity = Severity::Error;
        let issues = validate_must_support(&patient, "Patient", &profile, &cfg);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
    }

    #[test]
    fn nested_min_skipped_when_parent_slice_absent() {
        let patient = json!({
            "resourceType": "Patient"
        });
        let profile = test_profile("Patient", vec![rule("Patient.contact.name", Some(1), None)]);
        let issues = validate_min_cardinality(&patient, "Patient", &profile);
        assert!(
            issues.is_empty(),
            "min on contact.name should not apply when contact is absent: {issues:?}"
        );
    }

    #[test]
    fn missing_required_element_produces_issue() {
        let patient = json!({
            "resourceType": "Patient"
        });

        let profile = test_profile("Patient", vec![rule("Patient.identifier", Some(1), None)]);
        let issues = validate_min_cardinality(&patient, "Patient", &profile);

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].fhir_path, "Patient.identifier");
        assert_eq!(issues[0].code, "required");
        assert!(issues[0]
            .diagnostics
            .contains("Required element 'Patient.identifier' is missing or does not meet minimum cardinality 1."));
        assert_eq!(
            issues[0].summary.as_deref(),
            Some("Required element is missing or below minimum cardinality")
        );
        assert_eq!(
            issues[0].detail_code,
            Some(ValidationIssueDetailCode::RequiredElementMissing)
        );
    }

    #[test]
    fn present_required_element_produces_no_issue() {
        let patient = json!({
            "resourceType": "Patient",
            "identifier": [
                { "value": "123" }
            ]
        });

        let profile = test_profile("Patient", vec![rule("Patient.identifier", Some(1), None)]);
        let issues = validate_min_cardinality(&patient, "Patient", &profile);

        assert!(issues.is_empty());
    }

    #[test]
    fn zero_minimum_allows_missing_element() {
        let patient = json!({
            "resourceType": "Patient"
        });

        let profile = test_profile("Patient", vec![rule("Patient.maritalStatus", Some(0), None)]);
        let issues = validate_min_cardinality(&patient, "Patient", &profile);

        assert!(issues.is_empty());
    }

    #[test]
    fn nested_element_minimum_cardinality() {
        let patient = json!({
            "resourceType": "Patient",
            "contact": [
                {
                    "name": {
                        "family": "Smith"
                    }
                }
            ]
        });

        let profile = test_profile("Patient", vec![rule("Patient.contact.name", Some(1), None)]);
        let issues = validate_min_cardinality(&patient, "Patient", &profile);
        assert!(issues.is_empty());
    }

    #[test]
    fn missing_nested_element_produces_issue() {
        let patient = json!({
            "resourceType": "Patient",
            "contact": [{}]
        });

        let profile = test_profile("Patient", vec![rule("Patient.contact.name", Some(1), None)]);
        let issues = validate_min_cardinality(&patient, "Patient", &profile);

        assert_eq!(issues.len(), 1);
    }

    #[test]
    fn exceeding_maximum_cardinality_produces_issue() {
        let patient = json!({
            "resourceType": "Patient",
            "identifier": [
                { "value": "1" },
                { "value": "2" }
            ]
        });

        let profile = test_profile("Patient", vec![rule("Patient.identifier", None, Some("1"))]);
        let issues = validate_max_cardinality(&patient, "Patient", &profile);

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].fhir_path, "Patient.identifier");
        assert_eq!(issues[0].code, "structure");
        assert!(
            issues[0]
                .diagnostics
                .contains("Element 'Patient.identifier' exceeds maximum cardinality 1.")
        );
        assert_eq!(
            issues[0].summary.as_deref(),
            Some("Element exceeds maximum cardinality for this profile")
        );
        assert_eq!(
            issues[0].detail_code,
            Some(ValidationIssueDetailCode::MaximumCardinalityExceeded)
        );
    }

    #[test]
    fn satisfying_maximum_cardinality_produces_no_issue() {
        let patient = json!({
            "resourceType": "Patient",
            "identifier": [
                { "value": "1" }
            ]
        });

        let profile = test_profile("Patient", vec![rule("Patient.identifier", None, Some("1"))]);
        let issues = validate_max_cardinality(&patient, "Patient", &profile);

        assert!(issues.is_empty());
    }

    #[test]
    fn nested_max_cardinality_is_per_repeating_parent() {
        let appointment = json!({
            "resourceType": "Appointment",
            "participant": [
                {
                    "actor": { "reference": "Patient/p1" },
                    "status": "accepted"
                },
                {
                    "actor": { "reference": "Practitioner/pr1" },
                    "status": "accepted"
                }
            ]
        });

        let profile = test_profile(
            "Appointment",
            vec![
                rule("Appointment.participant.actor", None, Some("1")),
                rule("Appointment.participant.status", None, Some("1")),
            ],
        );
        let issues = validate_max_cardinality(&appointment, "Appointment", &profile);
        assert!(
            issues.is_empty(),
            "max 1 actor/status per participant should allow multiple participants: {issues:?}"
        );
    }

    #[test]
    fn unbounded_maximum_is_ignored() {
        let patient = json!({
            "resourceType": "Patient",
            "identifier": [
                { "value": "1" },
                { "value": "2" },
                { "value": "3" }
            ]
        });

        let profile = test_profile("Patient", vec![rule("Patient.identifier", None, Some("*"))]);
        let issues = validate_max_cardinality(&patient, "Patient", &profile);

        assert!(issues.is_empty());
    }
}

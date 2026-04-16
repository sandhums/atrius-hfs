use crate::profile::cardinality::{
    relative_profile_path, validate_max_cardinality, validate_min_cardinality,
};
use crate::profile::helpers::{
    get_values_at_relative_path, get_values_with_paths_at_relative_path,
};
use crate::profile::profile_registry::ProfileRegistry;
use crate::profile::slicing::validate_slicing;
use crate::profile::types::{ExtractedProfile, ExtractedValueConstraint};
use crate::validation_context::AsyncValidationContext;
pub use crate::validation_context::{ValidationContext, ValidationState};
use crate::validation_issue_detail::ValidationIssueDetailCode;
use crate::{TypeProfileMatchMode, ValidationIssue};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::pin::Pin;

/// Validate a resource instance against a single extracted profile.
///
/// This is the public entry point for profile-based validation. It initializes
/// recursion-cycle tracking and then delegates to [`validate_profile_with_depth`]
/// for the actual validation pipeline.
pub fn validate_profile<T: Serialize>(
    ctx: &ValidationContext<'_>,
    state: &mut ValidationState,
    resource: &T,
    resource_type: &str,
    profile: &ExtractedProfile,
) -> Vec<ValidationIssue> {
    validate_profile_with_depth(ctx, state, resource, resource_type, profile)
}

pub async fn validate_profile_async<T: Serialize>(
    ctx: &AsyncValidationContext<'_>,
    state: &mut ValidationState,
    resource: &T,
    resource_type: &str,
    profile: &ExtractedProfile,
) -> Vec<ValidationIssue> {
    validate_profile_with_depth_async(ctx, state, resource, resource_type, profile).await
}

/// Internal profile validation pipeline with recursion-depth and cycle tracking.
///
/// The pipeline currently performs, in order:
/// - profile-level invariants
/// - element-level invariants
/// - minimum cardinality
/// - maximum cardinality
/// - slicing validation
/// - fixed/pattern value constraints
/// - narrowed choice-type constraints
/// - target profile constraints on references
/// - `type.profile` constraints, including optional recursive profile validation
/// - terminology bindings
///
/// Recursive profile validation is guarded by both a maximum recursion depth and
/// an active-profile set to prevent infinite cycles.
pub(crate) fn validate_profile_with_depth_async<'a, T: Serialize + 'a>(
    ctx: &'a AsyncValidationContext<'a>,
    state: &'a mut ValidationState,
    resource: &'a T,
    resource_type: &'a str,
    profile: &'a ExtractedProfile,
) -> Pin<Box<dyn Future<Output = Vec<ValidationIssue>> + 'a>> {
    Box::pin(async move {
        if state.recursion_depth >= ctx.validator.config.max_profile_recursion_depth {
            if ctx.validator.config.warn_on_profile_recursion_depth_reached {
                return vec![ValidationIssue {
                    severity: crate::Severity::Warning,
                    code: "business-rule".to_string(),
                    summary: Some(
                        "Recursive profile validation stopped: maximum depth reached".to_string(),
                    ),
                    expression_kind: None,
                    source_invariant_key: None,
                    detail_code: Some(ValidationIssueDetailCode::RecursionDepthReached),
                    diagnostics: format!(
                        "Skipping recursive profile validation for '{}' because the maximum recursion depth {} was reached.",
                        profile.url, ctx.validator.config.max_profile_recursion_depth
                    ),
                    expression: Some(profile.url.clone()),
                    fhir_path: resource_type.to_string(),
                    instance_path: Some(resource_type.to_string()),
                }];
            }
            return Vec::new();
        }

        if !state.active_profiles.insert(profile.url.clone()) {
            if ctx.validator.config.warn_on_profile_cycle {
                return vec![ValidationIssue {
                    severity: crate::Severity::Warning,
                    code: "business-rule".to_string(),
                    summary: Some(
                        "Recursive profile validation skipped: profile cycle detected".to_string(),
                    ),
                    expression_kind: None,
                    source_invariant_key: None,
                    detail_code: Some(ValidationIssueDetailCode::ProfileCycleDetected),
                    diagnostics: format!(
                        "Skipping recursive profile validation for '{}' because a validation cycle was detected.",
                        profile.url
                    ),
                    expression: Some(profile.url.clone()),
                    fhir_path: resource_type.to_string(),
                    instance_path: Some(resource_type.to_string()),
                }];
            }
            return Vec::new();
        }

        let mut issues = Vec::new();

        issues.extend(ctx.validator.apply_invariants(
            resource,
            profile.invariants.as_slice(),
            ctx.evaluator,
            resource_type,
        ));

        for rule in &profile.element_rules {
            if !rule.constraints.is_empty() {
                // validator.trace(format!(
                //     "Applying {} invariant(s) on {}",
                //     rule.constraints.len(),
                //     rule.path
                // ));
                issues.extend(ctx.validator.apply_invariants(
                    resource,
                    rule.constraints.as_slice(),
                    ctx.evaluator,
                    rule.path.as_str(),
                ));
            }
        }

        issues.extend(validate_min_cardinality(
            resource,
            resource_type,
            profile.element_rules.as_slice(),
        ));

        issues.extend(validate_max_cardinality(
            resource,
            resource_type,
            profile.element_rules.as_slice(),
        ));

        issues.extend(validate_slicing(resource, resource_type, profile));

        issues.extend(validate_value_constraints(
            resource,
            resource_type,
            profile.element_rules.as_slice(),
        ));

        issues.extend(validate_type_constraints(
            resource,
            resource_type,
            profile.element_rules.as_slice(),
        ));

        issues.extend(validate_target_profile_constraints(
            resource,
            resource_type,
            profile.element_rules.as_slice(),
        ));

        issues.extend(
            validate_type_profile_constraints_async(
                ctx,
                state,
                resource,
                resource_type,
                profile.element_rules.as_slice(),
            )
            .await,
        );

        let bindings: Vec<_> = profile
            .element_rules
            .iter()
            .filter_map(|rule| rule.binding.clone())
            .collect();

        if !bindings.is_empty() {
            issues.extend(
                ctx.validator
                    .apply_bindings_for_version_async(
                        ctx.fhir_version,
                        resource,
                        bindings.as_slice(),
                        ctx.terminology,
                    )
                    .await,
            );
        }

        state.active_profiles.remove(&profile.url);
        issues
    })
}

pub(crate) fn validate_profile_with_depth<T: Serialize>(
    ctx: &ValidationContext<'_>,
    state: &mut ValidationState,
    resource: &T,
    resource_type: &str,
    profile: &ExtractedProfile,
) -> Vec<ValidationIssue> {
    if state.recursion_depth >= ctx.validator.config.max_profile_recursion_depth {
        if ctx.validator.config.warn_on_profile_recursion_depth_reached {
            return vec![ValidationIssue {
                severity: crate::Severity::Warning,
                code: "business-rule".to_string(),
                summary: Some(
                    "Recursive profile validation stopped: maximum depth reached".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::RecursionDepthReached),
                diagnostics: format!(
                    "Skipping recursive profile validation for '{}' because the maximum recursion depth {} was reached.",
                    profile.url, ctx.validator.config.max_profile_recursion_depth
                ),
                expression: Some(profile.url.clone()),
                fhir_path: resource_type.to_string(),
                instance_path: Some(resource_type.to_string()),
            }];
        }
        return Vec::new();
    }

    if !state.active_profiles.insert(profile.url.clone()) {
        if ctx.validator.config.warn_on_profile_cycle {
            return vec![ValidationIssue {
                severity: crate::Severity::Warning,
                code: "business-rule".to_string(),
                summary: Some(
                    "Recursive profile validation skipped: profile cycle detected".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ProfileCycleDetected),
                diagnostics: format!(
                    "Skipping recursive profile validation for '{}' because a validation cycle was detected.",
                    profile.url
                ),
                expression: Some(profile.url.clone()),
                fhir_path: resource_type.to_string(),
                instance_path: Some(resource_type.to_string()),
            }];
        }
        return Vec::new();
    }

    let mut issues = Vec::new();

    issues.extend(ctx.validator.apply_invariants(
        resource,
        profile.invariants.as_slice(),
        ctx.evaluator,
        resource_type,
    ));

    for rule in &profile.element_rules {
        if !rule.constraints.is_empty() {
            // validator.trace(format!(
            //     "Applying {} invariant(s) on {}",
            //     rule.constraints.len(),
            //     rule.path
            // ));
            issues.extend(ctx.validator.apply_invariants(
                resource,
                rule.constraints.as_slice(),
                ctx.evaluator,
                rule.path.as_str(),
            ));
        }
    }

    issues.extend(validate_min_cardinality(
        resource,
        resource_type,
        profile.element_rules.as_slice(),
    ));

    issues.extend(validate_max_cardinality(
        resource,
        resource_type,
        profile.element_rules.as_slice(),
    ));

    issues.extend(validate_slicing(resource, resource_type, profile));

    issues.extend(validate_value_constraints(
        resource,
        resource_type,
        profile.element_rules.as_slice(),
    ));

    issues.extend(validate_type_constraints(
        resource,
        resource_type,
        profile.element_rules.as_slice(),
    ));

    issues.extend(validate_target_profile_constraints(
        resource,
        resource_type,
        profile.element_rules.as_slice(),
    ));

    issues.extend(validate_type_profile_constraints(
        ctx,
        state,
        resource,
        resource_type,
        profile.element_rules.as_slice(),
    ));

    let bindings: Vec<_> = profile
        .element_rules
        .iter()
        .filter_map(|rule| rule.binding.clone())
        .collect();

    if !bindings.is_empty() {
        issues.extend(ctx.validator.apply_bindings_for_version_sync(
            ctx.fhir_version,
            resource,
            bindings.as_slice(),
            ctx.terminology,
        ));
    }

    state.active_profiles.remove(&profile.url);
    issues
}
/// Prefix issue paths produced during recursive nested profile validation so
/// they are reported relative to the parent element path in the outer resource.
fn prefix_nested_issue_paths(
    mut issues: Vec<ValidationIssue>,
    parent_path: &str,
    nested_resource_type: &str,
) -> Vec<ValidationIssue> {
    let nested_prefix = format!("{}.", nested_resource_type);
    let parent_prefix = format!("{}.", parent_path);

    for issue in &mut issues {
        issue.fhir_path = prefix_single_issue_path(
            &issue.fhir_path,
            parent_path,
            &parent_prefix,
            nested_resource_type,
            &nested_prefix,
        );

        if let Some(instance_path) = issue.instance_path.as_mut() {
            let updated = prefix_single_issue_path(
                instance_path,
                parent_path,
                &parent_prefix,
                nested_resource_type,
                &nested_prefix,
            );
            *instance_path = updated;
        }
    }

    issues
}

/// Prefix a single nested issue path with the parent path while stripping the
/// nested resource type prefix when present.
fn prefix_single_issue_path(
    path: &str,
    parent_path: &str,
    parent_prefix: &str,
    nested_resource_type: &str,
    nested_prefix: &str,
) -> String {
    if path.is_empty() || path == nested_resource_type {
        return parent_path.to_string();
    }

    if let Some(rest) = path.strip_prefix(nested_prefix) {
        return format!("{}{}", parent_prefix, rest);
    }

    format!("{}{}", parent_prefix, path)
}

/// Validate narrowed polymorphic choice-element type constraints.
///
/// This currently applies only to `[x]` elements and checks that the concrete
/// JSON choice representation matches one of the allowed extracted type codes.
fn validate_type_constraints<T: Serialize>(
    resource: &T,
    resource_type: &str,
    rules: &[crate::profile::types::ExtractedElementRule],
) -> Vec<ValidationIssue> {
    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: crate::Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for type constraint validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::StructureInvalid),
                diagnostics: format!(
                    "Failed to serialize resource while validating type constraints: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    let mut issues = Vec::new();

    for rule in rules {
        if rule.type_constraints.is_empty() {
            continue;
        }

        // First pass: only enforce narrowed polymorphic choice elements like value[x], deceased[x], etc.
        if !rule.path.contains("[x]") {
            continue;
        }

        let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        let Some(choice_info) = actual_choice_type_codes(&root, relative_path) else {
            continue;
        };

        if choice_info.has_multiple_in_same_parent {
            issues.push(ValidationIssue {
                severity: crate::Severity::Error,
                code: "structure".to_string(),
                summary: Some(
                    "Polymorphic [x] element has multiple type representations at once".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::StructureInvalid),
                diagnostics: format!(
                    "Element '{}' has multiple [x] representations present in the same object: {}.",
                    rule.path,
                    choice_info
                        .actual_type_codes
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                expression: None,
                fhir_path: rule.path.clone(),
                instance_path: Some(rule.path.clone()),
            });
            continue;
        }

        let allowed_codes: Vec<&str> = rule
            .type_constraints
            .iter()
            .map(|constraint| constraint.code.as_str())
            .collect();

        let disallowed_actual_types: Vec<&str> = choice_info
            .actual_type_codes
            .iter()
            .map(String::as_str)
            .filter(|actual_type_code| {
                !allowed_codes
                    .iter()
                    .any(|allowed| type_code_matches_choice_suffix(allowed, actual_type_code))
            })
            .collect();

        if disallowed_actual_types.is_empty() {
            continue;
        }

        issues.push(ValidationIssue {
            severity: crate::Severity::Error,
            code: "structure".to_string(),
            summary: Some("Choice element type is not allowed by the profile".to_string()),
            expression_kind: None,
            source_invariant_key: None,
            detail_code: Some(ValidationIssueDetailCode::StructureInvalid),
            diagnostics: format!(
                "Element '{}' uses disallowed type(s) '{}'. Allowed types: {}.",
                rule.path,
                disallowed_actual_types.join(", "),
                rule.type_constraints
                    .iter()
                    .map(|constraint| constraint.code.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            expression: None,
            fhir_path: rule.path.clone(),
            instance_path: Some(rule.path.clone()),
        });
    }

    issues
}

/// Summary of the concrete choice-type representations found for a single `[x]`
/// element path.
struct ChoiceTypeInfo {
    actual_type_codes: Vec<String>,
    has_multiple_in_same_parent: bool,
}

/// Discover the concrete JSON suffixes present for a polymorphic `[x]` element.
///
/// For example, `value[x]` may resolve to concrete keys such as `valueString`
/// or `valueCodeableConcept`.
fn actual_choice_type_codes(root: &Value, relative_path: &str) -> Option<ChoiceTypeInfo> {
    let choice_stem = relative_path.strip_suffix("[x]")?;

    let (parent, last_segment) = split_parent_path(choice_stem);
    let parent_values = get_values_at_relative_path(root, parent);
    if parent_values.is_empty() {
        return None;
    }

    let mut actual_type_codes = BTreeSet::new();
    let mut has_multiple_in_same_parent = false;

    for parent_value in parent_values {
        if let Value::Object(map) = parent_value {
            let matches: Vec<String> = map
                .keys()
                .filter_map(|key| key.strip_prefix(last_segment))
                .filter(|suffix| !suffix.is_empty())
                .map(str::to_owned)
                .collect();

            if matches.len() > 1 {
                has_multiple_in_same_parent = true;
            }

            for matched in matches {
                actual_type_codes.insert(matched);
            }
        }
    }

    if actual_type_codes.is_empty() {
        return None;
    }

    Some(ChoiceTypeInfo {
        actual_type_codes: actual_type_codes.into_iter().collect(),
        has_multiple_in_same_parent,
    })
}

/// Split a dotted relative path into `(parent_path, last_segment)`.
fn split_parent_path(path: &str) -> (&str, &str) {
    match path.rsplit_once('.') {
        Some((parent, last)) => (parent, last),
        None => ("", path),
    }
}

/// Return `true` when an extracted FHIR type code corresponds to the concrete
/// JSON suffix used for a choice element representation.
fn type_code_matches_choice_suffix(allowed_type_code: &str, actual_suffix: &str) -> bool {
    normalize_choice_type_code(allowed_type_code) == actual_suffix
}

/// Normalize a FHIR type code to the suffix convention used by generated JSON
/// keys for choice elements.
fn normalize_choice_type_code(type_code: &str) -> &str {
    match type_code {
        "boolean" => "Boolean",
        "integer" => "Integer",
        "integer64" => "Integer64",
        "decimal" => "Decimal",
        "base64Binary" => "Base64Binary",
        "instant" => "Instant",
        "string" => "String",
        "uri" => "Uri",
        "url" => "Url",
        "canonical" => "Canonical",
        "date" => "Date",
        "dateTime" => "DateTime",
        "time" => "Time",
        "code" => "Code",
        "oid" => "Oid",
        "id" => "Id",
        "markdown" => "Markdown",
        "unsignedInt" => "UnsignedInt",
        "positiveInt" => "PositiveInt",
        "xhtml" => "Xhtml",
        other => other,
    }
}

/// Validate `targetProfile`-style restrictions on reference targets.
///
/// This checks whether a reference points to an allowed resource type, including
/// inline contained references of the form `#id` when the contained resource is
/// present in the current JSON instance.
fn validate_target_profile_constraints<T: Serialize>(
    resource: &T,
    resource_type: &str,
    rules: &[crate::profile::types::ExtractedElementRule],
) -> Vec<ValidationIssue> {
    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: crate::Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for reference target validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ValidationException),
                diagnostics: format!(
                    "Failed to serialize resource while validating targetProfile constraints: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    let mut issues = Vec::new();

    for rule in rules {
        let allowed_target_types: Vec<String> = rule
            .type_constraints
            .iter()
            .flat_map(|constraint| constraint.target_profiles.iter())
            .filter_map(|url| target_profile_resource_type(url))
            .collect();

        if allowed_target_types.is_empty() {
            continue;
        }

        let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        let actual_values = get_values_at_relative_path(&root, relative_path);
        if actual_values.is_empty() {
            continue;
        }

        for actual in actual_values {
            let Some(actual_target_type) = actual_reference_target_type(actual, &root) else {
                continue;
            };

            if allowed_target_types
                .iter()
                .any(|allowed| allowed == actual_target_type)
            {
                continue;
            }

            issues.push(ValidationIssue {
                severity: crate::Severity::Error,
                code: "structure".to_string(),
                summary: Some(
                    "Reference target resource type is not allowed by targetProfile".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::StructureInvalid),
                diagnostics: format!(
                    "Element '{}' references resource type '{}', which is not allowed by the profile. Allowed target types: {}.",
                    rule.path,
                    actual_target_type,
                    allowed_target_types.join(", ")
                ),
                expression: None,
                fhir_path: rule.path.clone(),
                instance_path: Some(rule.path.clone()),
            });
        }
    }
    issues
}

/// Determine the referenced target resource type from a reference-like JSON value.
fn actual_reference_target_type<'a>(value: &'a Value, root: &'a Value) -> Option<&'a str> {
    let Value::Object(map) = value else {
        return None;
    };

    let reference = map.get("reference")?.as_str()?;
    if let Some(contained_id) = reference.strip_prefix('#') {
        return contained_resource_type_by_id(root, contained_id);
    }

    parse_reference_resource_type(reference)
}

/// Resolve the resource type of a contained resource by its local `#id` target.
fn contained_resource_type_by_id<'a>(root: &'a Value, contained_id: &str) -> Option<&'a str> {
    let Value::Object(root_map) = root else {
        return None;
    };

    let contained = root_map.get("contained")?;
    let Value::Array(items) = contained else {
        return None;
    };

    for item in items {
        let Value::Object(resource_map) = item else {
            continue;
        };

        let Some(id) = resource_map.get("id").and_then(Value::as_str) else {
            continue;
        };

        if id == contained_id {
            return resource_map.get("resourceType").and_then(Value::as_str);
        }
    }

    None
}

/// Infer the target resource type name from a non-local FHIR reference string.
fn parse_reference_resource_type(reference: &str) -> Option<&str> {
    if reference.starts_with('#') {
        return None;
    }

    let path = reference
        .split_once("://")
        .map(|(_, rest)| rest)
        .and_then(|rest| rest.split_once('/').map(|(_, path)| path))
        .unwrap_or(reference);

    let mut segments = path.split('/').filter(|segment| !segment.is_empty());
    let first = segments.next()?;

    if first == "_history" {
        return None;
    }

    Some(first)
}

/// Best-effort mapping from a target profile URL to its implied resource type.
///
/// This is used as a fallback when a concrete extracted profile is not available
/// in the runtime profile registry.
pub fn target_profile_resource_type(url: &str) -> Option<String> {
    let tail = url.rsplit('/').next()?;

    let candidate = match tail {
        "Patient" => "Patient",
        "Practitioner" => "Practitioner",
        "PractitionerRole" => "PractitionerRole",
        "RelatedPerson" => "RelatedPerson",
        "Person" => "Person",
        "Group" => "Group",
        "Organization" => "Organization",
        "Location" => "Location",
        "Device" => "Device",
        "Observation" => "Observation",
        "Encounter" => "Encounter",
        "Condition" => "Condition",
        "Procedure" => "Procedure",
        "MedicationRequest" => "MedicationRequest",
        "Medication" => "Medication",
        "Substance" => "Substance",
        "Specimen" => "Specimen",
        "ServiceRequest" => "ServiceRequest",
        "CarePlan" => "CarePlan",
        "DiagnosticReport" => "DiagnosticReport",
        "ImagingStudy" => "ImagingStudy",
        "AllergyIntolerance" => "AllergyIntolerance",
        "Immunization" => "Immunization",
        other if other.ends_with("-patient") => "Patient",
        other if other.ends_with("-practitioner") => "Practitioner",
        other if other.ends_with("-practitionerrole") => "PractitionerRole",
        other if other.ends_with("-relatedperson") => "RelatedPerson",
        other if other.ends_with("-group") => "Group",
        other if other.ends_with("-organization") => "Organization",
        other if other.ends_with("-location") => "Location",
        other if other.ends_with("-observation") => "Observation",
        other if other.ends_with("-encounter") => "Encounter",
        _ => return None,
    };

    Some(candidate.to_string())
}

/// Validate `type.profile` constraints on nested resource-valued elements.
///
/// This supports two matching strategies:
/// - explicit declared `meta.profile` matching on the nested resource
/// - optional fallback to resource type matching when enabled by configuration
///
/// When matching profiles are known in the registry, recursive validation of the
/// nested resource against the matched profile(s) may also be performed.
fn validate_type_profile_constraints<T: Serialize>(
    ctx: &ValidationContext<'_>,
    state: &mut ValidationState,
    resource: &T,
    resource_type: &str,
    rules: &[crate::profile::types::ExtractedElementRule],
) -> Vec<ValidationIssue> {
    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: crate::Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for type.profile validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ValidationException),
                diagnostics: format!(
                    "Failed to serialize resource while validating type.profile constraints: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    let mut issues = Vec::new();

    for rule in rules {
        let required_profiles: Vec<&str> = rule
            .type_constraints
            .iter()
            .flat_map(|constraint| constraint.profiles.iter().map(String::as_str))
            .collect();

        if required_profiles.is_empty() {
            continue;
        }

        let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        let actual_values =
            get_values_with_paths_at_relative_path(&root, resource_type, relative_path);
        if actual_values.is_empty() {
            continue;
        }

        for (actual, actual_path) in actual_values {
            let mut unknown_required_profiles: Vec<&str> = Vec::new();
            let mut known_required_profiles: Vec<&str> = Vec::new();

            if let Some(registry) = ctx.runtime_profile_registry {
                for profile_url in &required_profiles {
                    if registry.get(profile_url).is_some() {
                        known_required_profiles.push(*profile_url);
                    } else {
                        unknown_required_profiles.push(*profile_url);
                    }
                }
            } else {
                unknown_required_profiles.extend(required_profiles.iter().copied());
            }

            if !unknown_required_profiles.is_empty() {
                if ctx.validator.config.error_on_unknown_profile {
                    issues.push(ValidationIssue {
                        severity: crate::Severity::Error,
                        code: "not-found".to_string(),
                        summary: Some(
                            "Required StructureDefinition profile URL is not in the profile registry"
                                .to_string(),
                        ),
                        expression_kind: None,
                        source_invariant_key: None,
                        detail_code: Some(ValidationIssueDetailCode::ReferenceNotFound),
                        diagnostics: format!(
                            "Element '{}' requires unknown profile(s): {}.",
                            rule.path,
                            unknown_required_profiles.join(", ")
                        ),
                        expression: None,
                        fhir_path: actual_path.clone(),
                        instance_path: Some(actual_path.clone()),
                    });
                    continue;
                }

                if ctx.validator.config.warn_on_unknown_profile {
                    issues.push(ValidationIssue {
                        severity: crate::Severity::Warning,
                        code: "not-found".to_string(),
                        summary: Some(
                            "Referenced profile URL is not available in the profile registry"
                                .to_string(),
                        ),
                        expression_kind: None,
                        source_invariant_key: None,
                        detail_code: Some(ValidationIssueDetailCode::ReferenceNotFound),
                        diagnostics: format!(
                            "Element '{}' references unknown profile(s): {}.",
                            rule.path,
                            unknown_required_profiles.join(", ")
                        ),
                        expression: None,
                        fhir_path: actual_path.clone(),
                        instance_path: Some(actual_path.clone()),
                    });
                }
            }

            let declared_profiles = declared_profiles_on_value(actual);
            if !declared_profiles.is_empty() {
                let matching_required_profiles: Vec<&str> = known_required_profiles
                    .iter()
                    .copied()
                    .filter(|required| {
                        declared_profiles
                            .iter()
                            .any(|declared| declared == required)
                    })
                    .collect();

                let declared_match_ok = match ctx.validator.config.type_profile_match_mode {
                    TypeProfileMatchMode::Any => !matching_required_profiles.is_empty(),
                    TypeProfileMatchMode::All => {
                        !known_required_profiles.is_empty()
                            && known_required_profiles.iter().all(|required| {
                                declared_profiles
                                    .iter()
                                    .any(|declared| declared == required)
                            })
                    }
                };

                if !declared_match_ok {
                    issues.push(ValidationIssue {
                        severity: crate::Severity::Error,
                        code: "structure".to_string(),
                        summary: Some(
                            "Nested resource meta.profile does not satisfy type.profile requirement"
                                .to_string(),
                        ),
                        expression_kind: None,
                        source_invariant_key: None,
                        detail_code: Some(ValidationIssueDetailCode::BusinessRuleViolation),
                        diagnostics: format!(
                            "Element '{}' does not declare the required profile match. Required profiles: {}. Declared profiles: {}. Match mode: {:?}.",
                            rule.path,
                            known_required_profiles.join(", "),
                            declared_profiles.join(", "),
                            ctx.validator.config.type_profile_match_mode
                        ),
                        expression: None,
                        fhir_path: actual_path.clone(),
                        instance_path: Some(actual_path.clone()),
                    });
                    continue;
                }

                if let Some(registry) = ctx.runtime_profile_registry {
                    let profiles_to_recurse: Vec<&str> =
                        match ctx.validator.config.type_profile_match_mode {
                            TypeProfileMatchMode::Any => matching_required_profiles,
                            TypeProfileMatchMode::All => known_required_profiles.clone(),
                        };

                    for profile_url in profiles_to_recurse {
                        if let Some(nested_profile) = registry.get(profile_url) {
                            let nested_resource_type = nested_profile.resource_type.as_str();
                            let mut child_state = ValidationState {
                                recursion_depth: state.recursion_depth + 1,
                                active_profiles: state.active_profiles.clone(),
                            };
                            let nested_issues = validate_profile_with_depth(
                                &ctx,
                                &mut child_state,
                                actual,
                                nested_resource_type,
                                nested_profile,
                            );
                            issues.extend(prefix_nested_issue_paths(
                                nested_issues,
                                &actual_path,
                                nested_resource_type,
                            ));
                        }
                    }
                }
                continue;
            }

            if let Some(registry) = ctx.runtime_profile_registry {
                let actual_resource_type = resource_type_name_from_value(actual);
                if let Some(actual_resource_type) = actual_resource_type {
                    if !ctx
                        .validator
                        .config
                        .allow_type_profile_resource_type_fallback
                    {
                        issues.push(ValidationIssue {
                            severity: crate::Severity::Error,
                            code: "structure".to_string(),
                            summary: Some(
                                "meta.profile is missing and resourceType fallback is disabled for type.profile"
                                    .to_string(),
                            ),
                            expression_kind: None,
                            source_invariant_key: None,
                            detail_code: Some(ValidationIssueDetailCode::BusinessRuleViolation),
                            diagnostics: format!(
                                "Element '{}' does not explicitly declare any of the required profiles, and resourceType fallback is disabled. Expected profiles: {}.",
                                rule.path,
                                known_required_profiles.join(", ")
                            ),
                            expression: None,
                            fhir_path: actual_path.clone(),
                            instance_path: Some(actual_path.clone()),
                        });
                        continue;
                    }

                    let matching_profiles: Vec<&str> = known_required_profiles
                        .iter()
                        .copied()
                        .filter(|url| {
                            profile_resource_type(url, Some(registry))
                                .as_deref()
                                .map(|allowed| allowed == actual_resource_type)
                                .unwrap_or(false)
                        })
                        .collect();

                    let fallback_match_ok = match ctx.validator.config.type_profile_match_mode {
                        TypeProfileMatchMode::Any => !matching_profiles.is_empty(),
                        TypeProfileMatchMode::All => {
                            !known_required_profiles.is_empty()
                                && known_required_profiles.iter().all(|url| {
                                    profile_resource_type(url, Some(registry))
                                        .as_deref()
                                        .map(|allowed| allowed == actual_resource_type)
                                        .unwrap_or(false)
                                })
                        }
                    };

                    if !fallback_match_ok {
                        let allowed_resource_types: Vec<String> = known_required_profiles
                            .iter()
                            .filter_map(|url| profile_resource_type(url, Some(registry)))
                            .collect();

                        if !allowed_resource_types.is_empty() {
                            issues.push(ValidationIssue {
                                severity: crate::Severity::Error,
                                code: "structure".to_string(),
                                summary: Some(
                                    "Nested resource type does not match type.profile expectation"
                                        .to_string(),
                                ),
                                expression_kind: None,
                                source_invariant_key: None,
                                detail_code: Some(ValidationIssueDetailCode::StructureInvalid),
                                diagnostics: format!(
                                    "Element '{}' has resource type '{}', which does not match the required profiled type(s): {}. Match mode: {:?}.",
                                    rule.path,
                                    actual_resource_type,
                                    allowed_resource_types.join(", "),
                                    ctx.validator.config.type_profile_match_mode
                                ),
                                expression: None,
                                fhir_path: actual_path.clone(),
                                instance_path: Some(actual_path.clone()),
                            });
                        }
                        continue;
                    }

                    if ctx.validator.config.warn_on_type_profile_fallback {
                        issues.push(ValidationIssue {
                            severity: crate::Severity::Warning,
                            code: "business-rule".to_string(),
                            summary: Some(
                                "type.profile validation used resourceType fallback (meta.profile missing)"
                                    .to_string(),
                            ),
                            expression_kind: None,
                            source_invariant_key: None,
                            detail_code: Some(ValidationIssueDetailCode::BusinessRuleViolation),
                            diagnostics: format!(
                                "Element '{}' does not explicitly declare any of the required profiles. Falling back to resourceType match '{}'. Expected profiles: {}. Match mode: {:?}.",
                                rule.path,
                                actual_resource_type,
                                known_required_profiles.join(", "),
                                ctx.validator.config.type_profile_match_mode
                            ),
                            expression: None,
                            fhir_path: actual_path.clone(),
                            instance_path: Some(actual_path.clone()),
                        });
                    }

                    if ctx.validator.config.recurse_on_type_profile_fallback {
                        let profiles_to_recurse: Vec<&str> =
                            match ctx.validator.config.type_profile_match_mode {
                                TypeProfileMatchMode::Any => matching_profiles,
                                TypeProfileMatchMode::All => known_required_profiles.clone(),
                            };

                        for profile_url in profiles_to_recurse {
                            if let Some(nested_profile) = registry.get(profile_url) {
                                let nested_resource_type = nested_profile.resource_type.as_str();
                                let mut child_state = ValidationState {
                                    recursion_depth: state.recursion_depth + 1,
                                    active_profiles: state.active_profiles.clone(),
                                };
                                let nested_issues = validate_profile_with_depth(
                                    &ctx,
                                    &mut child_state,
                                    actual,
                                    nested_resource_type,
                                    nested_profile,
                                );
                                issues.extend(prefix_nested_issue_paths(
                                    nested_issues,
                                    &actual_path,
                                    nested_resource_type,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    issues
}
fn validate_type_profile_constraints_async<'a, T: Serialize + 'a>(
    ctx: &'a AsyncValidationContext<'a>,
    state: &'a mut ValidationState,
    resource: &'a T,
    resource_type: &'a str,
    rules: &'a [crate::profile::types::ExtractedElementRule],
) -> Pin<Box<dyn Future<Output = Vec<ValidationIssue>> + 'a>> {
    Box::pin(async move {
        let root = match serde_json::to_value(resource) {
            Ok(value) => value,
            Err(err) => {
                return vec![ValidationIssue {
                    severity: crate::Severity::Error,
                    code: "processing".to_string(),
                    summary: Some(
                        "Resource could not be serialized for type.profile validation".to_string(),
                    ),
                    expression_kind: None,
                    source_invariant_key: None,
                    detail_code: Some(ValidationIssueDetailCode::ValidationException),
                    diagnostics: format!(
                        "Failed to serialize resource while validating type.profile constraints: {}",
                        err
                    ),
                    expression: None,
                    fhir_path: "".to_string(),
                    instance_path: None,
                }];
            }
        };

        let mut issues = Vec::new();

        for rule in rules {
            let required_profiles: Vec<&str> = rule
                .type_constraints
                .iter()
                .flat_map(|constraint| constraint.profiles.iter().map(String::as_str))
                .collect();

            if required_profiles.is_empty() {
                continue;
            }

            let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
                continue;
            };

            let actual_values =
                get_values_with_paths_at_relative_path(&root, resource_type, relative_path);
            if actual_values.is_empty() {
                continue;
            }

            for (actual, actual_path) in actual_values {
                let mut unknown_required_profiles: Vec<&str> = Vec::new();
                let mut known_required_profiles: Vec<&str> = Vec::new();

                if let Some(registry) = ctx.runtime_profile_registry {
                    for profile_url in &required_profiles {
                        if registry.get(profile_url).is_some() {
                            known_required_profiles.push(*profile_url);
                        } else {
                            unknown_required_profiles.push(*profile_url);
                        }
                    }
                } else {
                    unknown_required_profiles.extend(required_profiles.iter().copied());
                }

                if !unknown_required_profiles.is_empty() {
                    if ctx.validator.config.error_on_unknown_profile {
                        issues.push(ValidationIssue {
                            severity: crate::Severity::Error,
                            code: "not-found".to_string(),
                            summary: Some(
                                "Required StructureDefinition profile URL is not in the profile registry"
                                    .to_string(),
                            ),
                            expression_kind: None,
                            source_invariant_key: None,
                            detail_code: Some(ValidationIssueDetailCode::ReferenceNotFound),
                            diagnostics: format!(
                                "Element '{}' requires unknown profile(s): {}.",
                                rule.path,
                                unknown_required_profiles.join(", ")
                            ),
                            expression: None,
                            fhir_path: actual_path.clone(),
                            instance_path: Some(actual_path.clone()),
                        });
                        continue;
                    }

                    if ctx.validator.config.warn_on_unknown_profile {
                        issues.push(ValidationIssue {
                            severity: crate::Severity::Warning,
                            code: "not-found".to_string(),
                            summary: Some(
                                "Referenced profile URL is not available in the profile registry"
                                    .to_string(),
                            ),
                            expression_kind: None,
                            source_invariant_key: None,
                            detail_code: Some(ValidationIssueDetailCode::ReferenceNotFound),
                            diagnostics: format!(
                                "Element '{}' references unknown profile(s): {}.",
                                rule.path,
                                unknown_required_profiles.join(", ")
                            ),
                            expression: None,
                            fhir_path: actual_path.clone(),
                            instance_path: Some(actual_path.clone()),
                        });
                    }
                }

                let declared_profiles = declared_profiles_on_value(actual);
                if !declared_profiles.is_empty() {
                    let matching_required_profiles: Vec<&str> = known_required_profiles
                        .iter()
                        .copied()
                        .filter(|required| {
                            declared_profiles
                                .iter()
                                .any(|declared| declared == required)
                        })
                        .collect();

                    let declared_match_ok = match ctx.validator.config.type_profile_match_mode {
                        TypeProfileMatchMode::Any => !matching_required_profiles.is_empty(),
                        TypeProfileMatchMode::All => {
                            !known_required_profiles.is_empty()
                                && known_required_profiles.iter().all(|required| {
                                    declared_profiles
                                        .iter()
                                        .any(|declared| declared == required)
                                })
                        }
                    };

                    if !declared_match_ok {
                        issues.push(ValidationIssue {
                        severity: crate::Severity::Error,
                        code: "structure".to_string(),
                        summary: Some(
                            "Nested resource meta.profile does not satisfy type.profile requirement"
                                .to_string(),
                        ),
                        expression_kind: None,
                        source_invariant_key: None,
                            detail_code: Some(ValidationIssueDetailCode::BusinessRuleViolation),
                        diagnostics: format!(
                            "Element '{}' does not declare the required profile match. Required profiles: {}. Declared profiles: {}. Match mode: {:?}.",
                            rule.path,
                            known_required_profiles.join(", "),
                            declared_profiles.join(", "),
                            ctx.validator.config.type_profile_match_mode
                        ),
                        expression: None,
                        fhir_path: actual_path.clone(),
                        instance_path: Some(actual_path.clone()),
                    });
                        continue;
                    }

                    if let Some(registry) = ctx.runtime_profile_registry {
                        let profiles_to_recurse: Vec<&str> =
                            match ctx.validator.config.type_profile_match_mode {
                                TypeProfileMatchMode::Any => matching_required_profiles,
                                TypeProfileMatchMode::All => known_required_profiles.clone(),
                            };

                        for profile_url in profiles_to_recurse {
                            if let Some(nested_profile) = registry.get(profile_url) {
                                let nested_resource_type = nested_profile.resource_type.as_str();
                                let mut child_state = ValidationState {
                                    recursion_depth: state.recursion_depth + 1,
                                    active_profiles: state.active_profiles.clone(),
                                };
                                let nested_issues = validate_profile_with_depth_async(
                                    &ctx,
                                    &mut child_state,
                                    actual,
                                    nested_resource_type,
                                    nested_profile,
                                )
                                .await;
                                issues.extend(prefix_nested_issue_paths(
                                    nested_issues,
                                    &actual_path,
                                    nested_resource_type,
                                ));
                            }
                        }
                    }
                    continue;
                }

                if let Some(registry) = ctx.runtime_profile_registry {
                    let actual_resource_type = resource_type_name_from_value(actual);
                    if let Some(actual_resource_type) = actual_resource_type {
                        if !ctx
                            .validator
                            .config
                            .allow_type_profile_resource_type_fallback
                        {
                            issues.push(ValidationIssue {
                            severity: crate::Severity::Error,
                            code: "structure".to_string(),
                            summary: Some(
                                "meta.profile is missing and resourceType fallback is disabled for type.profile"
                                    .to_string(),
                            ),
                            expression_kind: None,
                            source_invariant_key: None,
                                detail_code: Some(ValidationIssueDetailCode::BusinessRuleViolation),
                            diagnostics: format!(
                                "Element '{}' does not explicitly declare any of the required profiles, and resourceType fallback is disabled. Expected profiles: {}.",
                                rule.path,
                                known_required_profiles.join(", ")
                            ),
                            expression: None,
                            fhir_path: actual_path.clone(),
                            instance_path: Some(actual_path.clone()),
                        });
                            continue;
                        }

                        let matching_profiles: Vec<&str> = known_required_profiles
                            .iter()
                            .copied()
                            .filter(|url| {
                                profile_resource_type(url, Some(registry))
                                    .as_deref()
                                    .map(|allowed| allowed == actual_resource_type)
                                    .unwrap_or(false)
                            })
                            .collect();

                        let fallback_match_ok = match ctx.validator.config.type_profile_match_mode {
                            TypeProfileMatchMode::Any => !matching_profiles.is_empty(),
                            TypeProfileMatchMode::All => {
                                !known_required_profiles.is_empty()
                                    && known_required_profiles.iter().all(|url| {
                                        profile_resource_type(url, Some(registry))
                                            .as_deref()
                                            .map(|allowed| allowed == actual_resource_type)
                                            .unwrap_or(false)
                                    })
                            }
                        };

                        if !fallback_match_ok {
                            let allowed_resource_types: Vec<String> = known_required_profiles
                                .iter()
                                .filter_map(|url| profile_resource_type(url, Some(registry)))
                                .collect();

                            if !allowed_resource_types.is_empty() {
                                issues.push(ValidationIssue {
                                severity: crate::Severity::Error,
                                code: "structure".to_string(),
                                summary: Some(
                                    "Nested resource type does not match type.profile expectation"
                                        .to_string(),
                                ),
                                expression_kind: None,
                                source_invariant_key: None,
                                    detail_code: Some(ValidationIssueDetailCode::StructureInvalid),
                                diagnostics: format!(
                                    "Element '{}' has resource type '{}', which does not match the required profiled type(s): {}. Match mode: {:?}.",
                                    rule.path,
                                    actual_resource_type,
                                    allowed_resource_types.join(", "),
                                    ctx.validator.config.type_profile_match_mode
                                ),
                                expression: None,
                                fhir_path: actual_path.clone(),
                                instance_path: Some(actual_path.clone()),
                            });
                            }
                            continue;
                        }

                        if ctx.validator.config.warn_on_type_profile_fallback {
                            issues.push(ValidationIssue {
                            severity: crate::Severity::Warning,
                            code: "business-rule".to_string(),
                            summary: Some(
                                "type.profile validation used resourceType fallback (meta.profile missing)"
                                    .to_string(),
                            ),
                            expression_kind: None,
                            source_invariant_key: None,
                                detail_code: Some(ValidationIssueDetailCode::BusinessRuleViolation),
                            diagnostics: format!(
                                "Element '{}' does not explicitly declare any of the required profiles. Falling back to resourceType match '{}'. Expected profiles: {}. Match mode: {:?}.",
                                rule.path,
                                actual_resource_type,
                                known_required_profiles.join(", "),
                                ctx.validator.config.type_profile_match_mode
                            ),
                            expression: None,
                            fhir_path: actual_path.clone(),
                            instance_path: Some(actual_path.clone()),
                        });
                        }

                        if ctx.validator.config.recurse_on_type_profile_fallback {
                            let profiles_to_recurse: Vec<&str> =
                                match ctx.validator.config.type_profile_match_mode {
                                    TypeProfileMatchMode::Any => matching_profiles,
                                    TypeProfileMatchMode::All => known_required_profiles.clone(),
                                };

                            for profile_url in profiles_to_recurse {
                                if let Some(nested_profile) = registry.get(profile_url) {
                                    let nested_resource_type =
                                        nested_profile.resource_type.as_str();
                                    let mut child_state = ValidationState {
                                        recursion_depth: state.recursion_depth + 1,
                                        active_profiles: state.active_profiles.clone(),
                                    };
                                    let nested_issues = validate_profile_with_depth_async(
                                        &ctx,
                                        &mut child_state,
                                        actual,
                                        nested_resource_type,
                                        nested_profile,
                                    )
                                    .await;
                                    issues.extend(prefix_nested_issue_paths(
                                        nested_issues,
                                        &actual_path,
                                        nested_resource_type,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        issues
    })
}

/// Extract declared `meta.profile` URLs from a JSON object value.
fn declared_profiles_on_value(value: &Value) -> Vec<String> {
    let Value::Object(map) = value else {
        return Vec::new();
    };

    let Some(meta) = map.get("meta") else {
        return Vec::new();
    };

    let Value::Object(meta_map) = meta else {
        return Vec::new();
    };

    let Some(profile) = meta_map.get("profile") else {
        return Vec::new();
    };

    let Value::Array(items) = profile else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| item.as_str().map(str::to_owned))
        .collect()
}

/// Extract `resourceType` from a JSON object value, if present.
fn resource_type_name_from_value(value: &Value) -> Option<&str> {
    let Value::Object(map) = value else {
        return None;
    };

    map.get("resourceType")?.as_str()
}

/// Resolve the resource type associated with a profile URL, using the profile
/// registry first and then falling back to heuristic URL-based mapping.
fn profile_resource_type(url: &str, profile_registry: Option<&ProfileRegistry>) -> Option<String> {
    if let Some(registry) = profile_registry {
        if let Some(profile) = registry.get(url) {
            return Some(profile.resource_type.clone());
        }
    }

    target_profile_resource_type(url)
}

/// Validate a resource instance against every profile declared in `meta.profile`.
///
/// Missing declared profiles are reported as `not-found` issues.
pub async fn validate_declared_profiles_async<T: Serialize>(
    ctx: &AsyncValidationContext<'_>,
    state: &mut ValidationState,
    resource: &T,
    resource_type: &str,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    let declared_profiles = declared_profile_urls(resource);
    for profile_url in declared_profiles {
        match ctx
            .runtime_profile_registry
            .and_then(|registry| registry.get(&profile_url))
        {
            Some(profile) => {
                issues.extend(
                    validate_profile_async(&ctx, state, resource, resource_type, profile).await,
                );
            }
            None => {
                issues.push(ValidationIssue {
                    severity: crate::Severity::Error,
                    code: "not-found".to_string(),
                    summary: Some(
                        "Declared meta.profile URL is not available in the profile registry"
                            .to_string(),
                    ),
                    expression_kind: None,
                    source_invariant_key: None,
                    detail_code: Some(ValidationIssueDetailCode::ReferenceNotFound),
                    diagnostics: format!(
                        "Declared profile '{}' was not found in the profile registry.",
                        profile_url
                    ),
                    expression: Some(profile_url.clone()),
                    fhir_path: format!("{}.meta.profile", resource_type),
                    instance_path: Some(format!("{}.meta.profile", resource_type)),
                });
            }
        }
    }

    issues
}
pub fn validate_declared_profiles<T: Serialize>(
    ctx: &ValidationContext<'_>,
    state: &mut ValidationState,
    resource: &T,
    resource_type: &str,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    let declared_profiles = declared_profile_urls(resource);
    for profile_url in declared_profiles {
        match ctx
            .runtime_profile_registry
            .and_then(|registry| registry.get(&profile_url))
        {
            Some(profile) => {
                issues.extend(validate_profile(
                    &ctx,
                    state,
                    resource,
                    resource_type,
                    profile,
                ));
            }
            None => {
                issues.push(ValidationIssue {
                    severity: crate::Severity::Error,
                    code: "not-found".to_string(),
                    summary: Some(
                        "Declared meta.profile URL is not available in the profile registry"
                            .to_string(),
                    ),
                    expression_kind: None,
                    source_invariant_key: None,
                    detail_code: Some(ValidationIssueDetailCode::ReferenceNotFound),
                    diagnostics: format!(
                        "Declared profile '{}' was not found in the profile registry.",
                        profile_url
                    ),
                    expression: Some(profile_url.clone()),
                    fhir_path: format!("{}.meta.profile", resource_type),
                    instance_path: Some(format!("{}.meta.profile", resource_type)),
                });
            }
        }
    }

    issues
}

/// Serialize a resource and extract all declared `meta.profile` URLs.
fn declared_profile_urls<T: Serialize>(resource: &T) -> Vec<String> {
    let value = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };

    extract_declared_profile_urls(&value)
}

/// Extract all declared `meta.profile` URLs from a serialized JSON value.
fn extract_declared_profile_urls(value: &Value) -> Vec<String> {
    let Some(meta) = value.get("meta") else {
        return Vec::new();
    };

    let Some(profile) = meta.get("profile") else {
        return Vec::new();
    };

    let Some(items) = profile.as_array() else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| item.as_str().map(str::to_owned))
        .collect()
}

/// Validate fixed/pattern constraints extracted from the profile differential.
fn validate_value_constraints<T: Serialize>(
    resource: &T,
    resource_type: &str,
    rules: &[crate::profile::types::ExtractedElementRule],
) -> Vec<ValidationIssue> {
    let root = match serde_json::to_value(resource) {
        Ok(value) => value,
        Err(err) => {
            return vec![ValidationIssue {
                severity: crate::Severity::Error,
                code: "processing".to_string(),
                summary: Some(
                    "Resource could not be serialized for fixed/pattern validation".to_string(),
                ),
                expression_kind: None,
                source_invariant_key: None,
                detail_code: Some(ValidationIssueDetailCode::ValidationException),
                diagnostics: format!(
                    "Failed to serialize resource while validating fixed/pattern constraints: {}",
                    err
                ),
                expression: None,
                fhir_path: "".to_string(),
                instance_path: None,
            }];
        }
    };

    let mut issues = Vec::new();

    for rule in rules {
        let Some(value_constraint) = &rule.value_constraint else {
            continue;
        };

        let Some(relative_path) = relative_profile_path(resource_type, &rule.path) else {
            continue;
        };

        let actual = get_relative_path(&root, relative_path);
        let matched = match value_constraint {
            ExtractedValueConstraint::Fixed(expected) => actual
                .map(|value| values_equal(value, expected))
                .unwrap_or(false),
            ExtractedValueConstraint::Pattern(expected) => actual
                .map(|value| value_matches_pattern(value, expected))
                .unwrap_or(false),
        };

        if matched {
            continue;
        }
        let (kind, expected, detail_code, summary) = match value_constraint {
            ExtractedValueConstraint::Fixed(expected) => (
                "fixed",
                expected,
                Some(ValidationIssueDetailCode::FixedConstraintMismatch),
                Some("Element value does not match fixed constraint".to_string()),
            ),
            ExtractedValueConstraint::Pattern(expected) => (
                "pattern",
                expected,
                Some(ValidationIssueDetailCode::PatternConstraintMismatch),
                Some("Element value does not match pattern constraint".to_string()),
            ),
        };

        issues.push(ValidationIssue {
            severity: crate::Severity::Error,
            code: "value".to_string(),
            summary,
            expression_kind: None,
            source_invariant_key: None,
            detail_code,
            diagnostics: format!(
                "Element '{}' does not satisfy {} constraint. Expected pattern/value: {}",
                rule.path, kind, expected
            ),
            expression: None,
            fhir_path: rule.path.clone(),
            instance_path: Some(rule.path.clone()),
        });
    }

    issues
}

/// Resolve a single non-repeating relative dotted path from the given root.
fn get_relative_path<'a>(root: &'a Value, relative_path: &str) -> Option<&'a Value> {
    if relative_path.is_empty() {
        return Some(root);
    }

    let mut current = root;
    for segment in relative_path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(segment)?;
            }
            _ => return None,
        }
    }

    Some(current)
}

/// Compare an actual JSON value with an extracted fixed JSON value.
fn values_equal(actual: &Value, expected: &Value) -> bool {
    actual == expected
}

/// Compare an actual JSON value against an extracted pattern JSON value.
///
/// Objects are matched by subset semantics, while arrays are matched by checking
/// that each pattern element matches at least one actual array element.
fn value_matches_pattern(actual: &Value, pattern: &Value) -> bool {
    match (actual, pattern) {
        (Value::Object(actual_map), Value::Object(pattern_map)) => {
            pattern_map.iter().all(|(key, pattern_value)| {
                actual_map
                    .get(key)
                    .map(|actual_value| value_matches_pattern(actual_value, pattern_value))
                    .unwrap_or(false)
            })
        }
        (Value::Array(actual_items), Value::Array(pattern_items)) => {
            if pattern_items.is_empty() {
                return true;
            }

            pattern_items.iter().all(|pattern_item| {
                actual_items
                    .iter()
                    .any(|actual_item| value_matches_pattern(actual_item, pattern_item))
            })
        }
        _ => actual == pattern,
    }
}

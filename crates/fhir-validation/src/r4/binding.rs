//! R4 binding validation.
//!
//! This module applies generated R4 `BindingDef` metadata to concrete resource
//! and datatype instances.
//!
//! Responsibilities:
//! - validate primitive `code` bindings
//! - validate `Coding` bindings
//! - validate `CodeableConcept` bindings
//! - use generated local terminology validators first
//! - fall back to remote terminology via `TerminologyService` when local
//!   validation is insufficient
//! - preserve indexed instance paths for repeated elements such as
//!   `Patient.name[1].use` or `Patient.telecom[2].system`
//!
//! The high-level flow is:
//! `apply_r4_bindings`
//!   → serialize current focus to JSON
//!   → resolve all bound values for each generated binding path
//!   → dispatch by binding target kind
//!   → call the appropriate typed binding validator
//!   → stamp concrete instance paths on emitted issues
//! Shared traversal and issue-construction helpers live in `binding/common.rs`.
//! This module owns the typed R4 validation layer on top of those helpers.
use crate::binding::common::{
    get_json_values_with_instance_paths, relative_binding_path, root_instance_path,
};
use crate::{TerminologyService, ValidationIssue, Validator};
use fhir_validation_types::{BindingDef, BindingStrength, BindingTargetKind};
use helios_fhir::r4::terminology::TerminologyValidationError;
use helios_fhir::r4::terminology::index as terminology_index;
use helios_fhir::r4::{CodeableConcept, Coding};
use serde::Serialize;

#[cfg(feature = "R4")]
pub fn coding_system(coding: &Coding) -> Option<&str> {
    coding.system.as_ref().and_then(|v| v.value.as_deref())
}

#[cfg(feature = "R4")]
fn coding_code(coding: &Coding) -> Option<&str> {
    coding.code.as_ref().and_then(|v| v.value.as_deref())
}

#[cfg(feature = "R4")]
pub fn coding_display(coding: &Coding) -> Option<&str> {
    coding.display.as_ref().and_then(|v| v.value.as_deref())
}
/// Validate a primitive FHIR `code` binding.
///
/// This is used for fields such as:
/// - `Patient.gender`
/// - `Observation.status`
/// - `Encounter.status`
///
/// The caller provides the already-extracted primitive string value (`code_value`) and
/// a generated local ValueSet wrapper function via `local_check`.
///
/// Validation flow:
/// 1. If the value is absent, do nothing. Presence/cardinality is handled elsewhere.
/// 2. Try local generated ValueSet validation.
/// 3. If local validation says remote terminology validation is required, use `TerminologyService`.
/// 4. Convert any miss into a `ValidationIssue` based on binding strength.
pub fn validate_primitive_code_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    code_value: Option<&str>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(code) = code_value else {
        // Missing primitive value is handled by structural/cardinality validation and/or invariants.
        return issues;
    };

    match local_check(code) {
        Ok(()) => issues,

        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            let Some(terminology) = terminology else {
                issues.push(crate::binding::common::terminology_issue(
                    fhir_path,
                    valueset_url,
                    "Remote terminology validation required but no TerminologyService was provided"
                        .to_string(),
                ));
                return issues;
            };

            match terminology.member_of(valueset_url, None, code, None) {
                Ok(true) => issues,
                Ok(false) => {
                    if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                        validator,
                        fhir_path,
                        valueset_url,
                        strength,
                        format!("Code '{}' is not in ValueSet {}", code, valueset_url),
                    ) {
                        issues.push(issue);
                    }
                    issues
                }
                Err(e) => {
                    issues.push(crate::binding::common::terminology_issue(
                        fhir_path,
                        valueset_url,
                        format!("Remote terminology validation failed: {}", e),
                    ));
                    issues
                }
            }
        }

        Err(TerminologyValidationError::NotInValueSet(_)) => {
            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!("Code '{}' is not in ValueSet {}", code, valueset_url),
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::value_issue(
                fhir_path,
                valueset_url,
                format!("Local ValueSet validation failed: {}", msg),
            ));
            issues
        }
    }
}
/// Validate a `CodeableConcept` binding.
///
/// Semantics:
/// - If there is no `coding`, do nothing here. Cardinality / profile rules handle presence.
/// - Try local generated ValueSet validation first for the whole concept.
/// - If local validation succeeds, stop.
/// - If local validation says remote validation is required, try each usable coding remotely.
///   If any coding validates true, the concept is accepted.
/// - If local validation definitively says the concept is not in the ValueSet, surface an issue
///   based on binding strength.
///
/// The caller supplies the generated local wrapper function, typically something like:
/// `|cc| MaritalStatusCodes::validate_codeable_concept(cc)`
pub fn validate_codeable_concept_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    codeable_concept: Option<&CodeableConcept>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(cc) = codeable_concept else {
        return issues;
    };

    let codings = match cc.coding.as_ref() {
        Some(codings) if !codings.is_empty() => codings,
        _ => return issues,
    };

    match local_check(cc) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet(_)) => {
            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!("CodeableConcept is not in ValueSet {}", valueset_url),
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            let Some(terminology) = terminology else {
                issues.push(crate::binding::common::terminology_issue(
                    fhir_path,
                    valueset_url,
                    "Remote terminology validation required but no TerminologyService was provided"
                        .to_string(),
                ));
                return issues;
            };

            let mut any_usable_coding = false;
            let mut any_match = false;

            for coding in codings {
                let system = coding_system(coding);
                let code = coding_code(coding);
                let display = coding_display(coding);

                let Some(code) = code else {
                    continue;
                };

                any_usable_coding = true;

                match terminology.member_of(valueset_url, system, code, display) {
                    Ok(true) => {
                        any_match = true;
                        break;
                    }
                    Ok(false) => {
                        // Keep checking other codings.
                    }
                    Err(e) => {
                        issues.push(crate::binding::common::terminology_issue(
                            fhir_path,
                            valueset_url,
                            format!("Remote terminology validation failed: {}", e),
                        ));
                        return issues;
                    }
                }
            }

            if any_match {
                return issues;
            }

            if !any_usable_coding {
                issues.push(crate::binding::common::value_issue(
                    fhir_path,
                    valueset_url,
                    "CodeableConcept has no usable coding with a code value for terminology validation"
                        .to_string(),
                ));
                return issues;
            }

            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!("CodeableConcept is not in ValueSet {}", valueset_url),
            ) {
                issues.push(issue);
            }

            return issues;
        }

        Err(TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::value_issue(
                fhir_path,
                valueset_url,
                format!("Local ValueSet validation failed: {}", msg),
            ));
            issues
        }
    }
}

/// Validate a `Coding` binding.
///
/// Semantics:
/// - if the value is absent, do nothing here
/// - try local generated ValueSet validation first
/// - if local validation requires remote terminology, validate using the
///   supplied `TerminologyService`
/// - if the coding is definitively not in the bound ValueSet, emit a
///   `ValidationIssue` according to binding strength
pub fn validate_coding_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    coding: Option<&Coding>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&Coding) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(coding) = coding else {
        return issues;
    };

    match local_check(coding) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet(_)) => {
            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!("Coding is not in ValueSet {}", valueset_url),
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            let Some(terminology) = terminology else {
                issues.push(crate::binding::common::terminology_issue(
                    fhir_path,
                    valueset_url,
                    "Remote terminology validation required but no TerminologyService was provided"
                        .to_string(),
                ));
                return issues;
            };

            let system = coding_system(coding);
            let code = coding_code(coding);
            let display = coding_display(coding);

            let Some(code) = code else {
                issues.push(crate::binding::common::value_issue(
                    fhir_path,
                    valueset_url,
                    "Coding has no code value for terminology validation".to_string(),
                ));
                return issues;
            };

            match terminology.member_of(valueset_url, system, code, display) {
                Ok(true) => issues,
                Ok(false) => {
                    if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                        validator,
                        fhir_path,
                        valueset_url,
                        strength,
                        format!("Coding is not in ValueSet {}", valueset_url),
                    ) {
                        issues.push(issue);
                    }
                    issues
                }
                Err(e) => {
                    issues.push(crate::binding::common::terminology_issue(
                        fhir_path,
                        valueset_url,
                        format!("Remote terminology validation failed: {}", e),
                    ));
                    issues
                }
            }
        }

        Err(TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::value_issue(
                fhir_path,
                valueset_url,
                format!("Local ValueSet validation failed: {}", msg),
            ));
            issues
        }
    }
}

/// Apply generated R4 binding definitions to the current focus value.
///
/// This function is the main entry point used by generated R4 validation code.
/// It serializes the focused value, resolves every generated binding path to one
/// or more concrete JSON values, and validates each value according to the
/// binding target kind.
///
/// Important behavior:
/// - repeated arrays are expanded and validated element-by-element
/// - concrete indexed instance paths are preserved, such as
///   `Patient.name[1].use` or `Patient.telecom[2].system`
/// - primitive `code`, `Coding`, and `CodeableConcept` are validated through
///   their dedicated helper functions
/// - local generated terminology validation is attempted first
/// - remote terminology validation is used when local validation reports that
///   remote validation is required
pub fn apply_r4_bindings<T>(
    validator: &Validator,
    focus: &T,
    bindings: &[BindingDef],
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    T: Serialize,
{
    let mut issues = Vec::new();

    let focus_json = match serde_json::to_value(focus) {
        Ok(value) => value,
        Err(err) => {
            issues.push(ValidationIssue::error(
                "structure",
                "binding",
                format!("Failed to serialize focus for binding validation: {}", err),
            ));
            return issues;
        }
    };

    for binding in bindings {
        let relative_path = relative_binding_path(&binding.path);
        let field_values = get_json_values_with_instance_paths(
            &focus_json,
            root_instance_path(&binding.path),
            relative_path,
        );

        match binding.target_kind {
            BindingTargetKind::Code => {
                for (field_value, instance_path) in &field_values {
                    let code_value = field_value.as_str();

                    let mut child_issues = validate_primitive_code_binding(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        code_value,
                        |code| terminology_index::validate_code(binding.value_set, code),
                        terminology,
                    );

                    for issue in &mut child_issues {
                        issue.instance_path = Some(instance_path.clone());
                    }

                    issues.extend(child_issues);
                }
            }

            BindingTargetKind::Coding => {
                for (field_value, instance_path) in &field_values {
                    let coding = serde_json::from_value::<Coding>((*field_value).clone()).ok();

                    let mut child_issues = validate_coding_binding(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        coding.as_ref(),
                        |coding| terminology_index::validate_coding(binding.value_set, coding),
                        terminology,
                    );

                    for issue in &mut child_issues {
                        issue.instance_path = Some(instance_path.clone());
                    }

                    issues.extend(child_issues);
                }
            }

            BindingTargetKind::CodeableConcept => {
                for (field_value, instance_path) in &field_values {
                    let codeable_concept =
                        serde_json::from_value::<CodeableConcept>((*field_value).clone()).ok();

                    let mut child_issues = validate_codeable_concept_binding(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        codeable_concept.as_ref(),
                        |cc| terminology_index::validate_codeable_concept(binding.value_set, cc),
                        terminology,
                    );

                    for issue in &mut child_issues {
                        issue.instance_path = Some(instance_path.clone());
                    }

                    issues.extend(child_issues);
                }
            }

            _ => {
                // no-op for unsupported / unhandled target kinds
            }
        }
    }

    issues
}

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
use crate::{ValidationIssue, Validator};
use fhir_validation_types::{BindingDef, BindingStrength, BindingTargetKind};
use helios_fhir::r4::terminology::TerminologyValidationError;
use helios_fhir::r4::terminology::index as terminology_index;
use helios_fhir::r4::{CodeableConcept, Coding};
use serde::Serialize;
use crate::terminology::service::{TerminologyService, TerminologyServiceSync};

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
fn prettify_remote_terminology_error(valueset_url: &str, err: &crate::ValidationError) -> String {
    match err {
        crate::ValidationError::TerminologyRemote(remote) => {
            // if remote
            //     .diagnostics
            //     .iter()
            //     .any(|d| d.contains("does not support this ValueSet property filter"))
            // {
            //     return format!(
            //         "Remote terminology server could not validate ValueSet '{}' because the server does not support the required ValueSet property filters",
            //         valueset_url
            //     );
            // }

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

fn summarize_codeable_concept_codings(cc: &helios_fhir::r4::CodeableConcept) -> String {
    let Some(codings) = cc.coding.as_ref() else {
        return "CodeableConcept has no codings".to_string();
    };

    let mut rendered = Vec::new();
    for coding in codings {
        let system = coding
            .system
            .as_ref()
            .and_then(|e| e.value.as_deref())
            .filter(|v| !v.is_empty());
        let code = coding
            .code
            .as_ref()
            .and_then(|e| e.value.as_deref())
            .filter(|v| !v.is_empty());

        match (system, code) {
            (Some(system), Some(code)) => rendered.push(format!("{}#{}", system, code)),
            (None, Some(code)) => rendered.push(code.to_string()),
            (Some(system), None) => rendered.push(format!("{}#<missing-code>", system)),
            (None, None) => rendered.push("<empty-coding>".to_string()),
        }
    }

    if rendered.is_empty() {
        "CodeableConcept has no codings".to_string()
    } else {
        rendered.join(", ")
    }
}
/// Validate a primitive FHIR `code` binding.
///
/// This is used for fields such as -
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
    terminology: Option<&dyn TerminologyServiceSync>,
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
                Ok(outcome) if outcome.is_member => issues,
                Ok(_outcome) => {
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
                        prettify_remote_terminology_error(valueset_url, &e),
                    ));
                    issues
                }
            }
        }

        Err(TerminologyValidationError::NotInValueSet {
            valueset_url: _err_valueset_url,
            system,
            code: err_code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided code '{}', from CodeSystem '{}' was not found in ValueSet {}",
                    err_code, system, valueset_url
                )
            } else {
                format!(
                    "The provided code '{}' was not found in ValueSet {}",
                    err_code, valueset_url
                )
            };

            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                &valueset_url,
                strength,
                diagnostics,
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(TerminologyValidationError::MissingSystem(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                &valueset_url,
                msg.clone(),
            ));

            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                &valueset_url,
                strength,
                format!(
                    "The value provided ('{}') was not found in the value set '{}'. ({})",
                    code, valueset_url, msg
                ),
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(TerminologyValidationError::UnknownCode { system, code }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                &valueset_url,
                format!("Unknown code '{}' in CodeSystem '{}'", code, system),
            ));
            issues
        }

        Err(TerminologyValidationError::WrongDisplay {
            system,
            code,
            expected,
            provided,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!(
                    "Wrong display '{}' for {}#{}. Expected '{}'",
                    provided, system, code, expected
                ),
            ));
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

pub async fn validate_primitive_code_binding_async<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    code: Option<&str>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), helios_fhir::r4::terminology::TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(code) = code.filter(|c| !c.is_empty()) else {
        return issues;
    };

    match local_check(code) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet {
            valueset_url,
            system,
            code: err_code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided code '{}', from CodeSystem '{}' was not found in ValueSet {}",
                    err_code, system, valueset_url
                )
            } else {
                format!(
                    "The provided code '{}' was not found in ValueSet {}",
                    err_code, valueset_url
                )
            };

            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                &*valueset_url,
                strength,
                diagnostics,
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::MissingSystem(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                msg,
            ));
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::UnknownCode {
            system,
            code,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Unknown code '{}' in CodeSystem '{}'", code, system),
            ));
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::WrongDisplay {
            system,
            code,
            expected,
            provided,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!(
                    "Wrong display '{}' for {}#{}. Expected '{}'",
                    provided, system, code, expected
                ),
            ));
            issues
        }

        Err(
            helios_fhir::r4::terminology::TerminologyValidationError::RemoteValidationRequired(_),
        ) => {
            let Some(terminology) = terminology else {
                issues.push(crate::binding::common::terminology_issue(
                    fhir_path,
                    valueset_url,
                    "Remote terminology validation required but no TerminologyService was provided"
                        .to_string(),
                ));
                return issues;
            };

            match terminology.member_of(valueset_url, None, code, None).await {
                Ok(outcome) if outcome.is_member => issues,
                Ok(outcome) => {
                    let diagnostics = outcome.message.unwrap_or_else(|| {
                        format!(
                            "The provided code '{}' was not found in ValueSet {}",
                            code, valueset_url
                        )
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
                    issues.push(crate::binding::common::terminology_issue(
                        fhir_path,
                        valueset_url,
                        prettify_remote_terminology_error(valueset_url, &e),
                    ));
                    issues
                }
            }
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
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
    terminology: Option<&dyn TerminologyServiceSync>,
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

        Err(TerminologyValidationError::NotInValueSet { .. }) => {
            let coding_summary = summarize_codeable_concept_codings(cc);
            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!(
                    "The provided coding(s) {} were not found in ValueSet {}",
                    coding_summary, valueset_url
                ),
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(TerminologyValidationError::MissingSystem(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                msg,
            ));
            issues
        }

        Err(TerminologyValidationError::UnknownCode { system, code }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Unknown code '{}' in CodeSystem '{}'", code, system),
            ));
            issues
        }

        Err(TerminologyValidationError::WrongDisplay {
            system,
            code,
            expected,
            provided,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!(
                    "Wrong display '{}' for {}#{}. Expected '{}'",
                    provided, system, code, expected
                ),
            ));
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
                    Ok(outcome) if outcome.is_member => {
                        any_match = true;
                        break;
                    }
                    Ok(_outcome) => {
                        // Keep checking other codings.
                    }
                    Err(e) => {
                        issues.push(crate::binding::common::terminology_issue(
                            fhir_path,
                            valueset_url,
                            prettify_remote_terminology_error(valueset_url, &e),
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
            let coding_summary = summarize_codeable_concept_codings(cc);
            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!(
                    "The provided coding(s) {} were not found in ValueSet {}",
                    coding_summary, valueset_url
                ),
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
pub async fn validate_codeable_concept_binding_async<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    codeable_concept: Option<&helios_fhir::r4::CodeableConcept>,
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

        Err(helios_fhir::r4::terminology::TerminologyValidationError::NotInValueSet { .. }) => {
            let coding_summary = summarize_codeable_concept_codings(cc);
            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!(
                    "The provided coding(s) {} were not found in ValueSet {}",
                    coding_summary, valueset_url
                ),
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::MissingSystem(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                msg,
            ));
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::UnknownCode {
            system,
            code,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Unknown code '{}' in CodeSystem '{}'", code, system),
            ));
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::WrongDisplay {
            system,
            code,
            expected,
            provided,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!(
                    "Wrong display '{}' for {}#{}. Expected '{}'",
                    provided, system, code, expected
                ),
            ));
            issues
        }

        Err(
            helios_fhir::r4::terminology::TerminologyValidationError::RemoteValidationRequired(_),
        ) => {
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
                let system = crate::r4::binding::coding_system(coding);
                let code = crate::r4::binding::coding_code(coding);
                let display = crate::r4::binding::coding_display(coding);

                let Some(code) = code else {
                    continue;
                };

                any_usable_coding = true;

                match terminology
                    .member_of(valueset_url, system, code, display)
                    .await
                {
                    Ok(outcome) if outcome.is_member => {
                        any_match = true;
                        break;
                    }
                    Ok(_outcome) => {
                        // Keep checking other codings.
                    }
                    Err(e) => {
                        issues.push(crate::binding::common::terminology_issue(
                            fhir_path,
                            valueset_url,
                            prettify_remote_terminology_error(valueset_url, &e),
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
            let coding_summary = summarize_codeable_concept_codings(cc);
            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!(
                    "The provided coding(s) {} were not found in ValueSet {}",
                    coding_summary, valueset_url
                ),
            ) {
                issues.push(issue);
            }

            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::value_issue(
                fhir_path,
                valueset_url,
                format!("Local ValueSet validation failed: {}", msg),
            ));
            issues
        }
    }
}
pub fn validate_coding_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    coding: Option<&Coding>,
    local_check: F,
    terminology: Option<&dyn TerminologyServiceSync>,
) -> Vec<ValidationIssue>
where
    F: Fn(&Coding) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(coding) = coding else {
        return issues;
    };
    let system = coding_system(coding).filter(|s| !s.is_empty());
    let code = coding_code(coding).filter(|c| !c.is_empty());

    if code.is_some() && system.is_none() {
        issues.push(ValidationIssue {
            severity: fhir_validation_types::Severity::Warning,
            code: "terminology",
            fhir_path: fhir_path.to_string(),
            instance_path: None,
            expression: Some(valueset_url.to_string()),
            diagnostics:
                "A code with no system has no defined meaning, and it cannot be validated. A system should be provided"
                    .to_string(),
        });
        return issues;
    }

    match local_check(coding) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet { system, code, .. }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided coding {}#{} was not found in ValueSet {}",
                    system, code, valueset_url
                )
            } else {
                format!(
                    "The provided code '{}' was not found in ValueSet {}",
                    code, valueset_url
                )
            };
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

        Err(TerminologyValidationError::MissingSystem(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                msg,
            ));
            issues
        }

        Err(TerminologyValidationError::UnknownCode { system, code }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Unknown code '{}' in CodeSystem '{}'", code, system),
            ));
            issues
        }

        Err(TerminologyValidationError::WrongDisplay {
            system,
            code,
            expected,
            provided,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!(
                    "Wrong display '{}' for {}#{}. Expected '{}'",
                    provided, system, code, expected
                ),
            ));
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
                Ok(outcome) if outcome.is_member => issues,
                Ok(_outcome) => {
                    let diagnostics = if let Some(system) = system {
                        format!(
                            "The provided coding {}#{} was not found in ValueSet {}",
                            system, code, valueset_url
                        )
                    } else {
                        format!(
                            "The provided code '{}' was not found in ValueSet {}",
                            code, valueset_url
                        )
                    };
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
                    issues.push(crate::binding::common::terminology_issue(
                        fhir_path,
                        valueset_url,
                        prettify_remote_terminology_error(valueset_url, &e),
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
pub async fn validate_coding_binding_async<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    coding: Option<&helios_fhir::r4::Coding>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(
        &helios_fhir::r4::Coding,
    ) -> Result<(), helios_fhir::r4::terminology::TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(coding) = coding else {
        return issues;
    };
    let system = crate::r4::binding::coding_system(coding).filter(|s| !s.is_empty());
    let code = crate::r4::binding::coding_code(coding).filter(|c| !c.is_empty());

    if code.is_some() && system.is_none() {
        issues.push(ValidationIssue {
            severity: fhir_validation_types::Severity::Warning,
            code: "terminology",
            fhir_path: fhir_path.to_string(),
            instance_path: None,
            expression: Some(valueset_url.to_string()),
            diagnostics:
            "A code with no system has no defined meaning, and it cannot be validated. A system should be provided"
                .to_string(),
        });
        return issues;
    }

    match local_check(coding) {
        Ok(()) => issues,

        Err(helios_fhir::r4::terminology::TerminologyValidationError::NotInValueSet {
            system,
            code,
            ..
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided coding {}#{} was not found in ValueSet {}",
                    system, code, valueset_url
                )
            } else {
                format!(
                    "The provided code '{}' was not found in ValueSet {}",
                    code, valueset_url
                )
            };
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

        Err(helios_fhir::r4::terminology::TerminologyValidationError::MissingSystem(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                msg,
            ));
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::UnknownCode {
            system,
            code,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Unknown code '{}' in CodeSystem '{}'", code, system),
            ));
            issues
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::WrongDisplay {
            system,
            code,
            expected,
            provided,
        }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!(
                    "Wrong display '{}' for {}#{}. Expected '{}'",
                    provided, system, code, expected
                ),
            ));
            issues
        }

        Err(
            helios_fhir::r4::terminology::TerminologyValidationError::RemoteValidationRequired(_),
        ) => {
            let Some(terminology) = terminology else {
                issues.push(crate::binding::common::terminology_issue(
                    fhir_path,
                    valueset_url,
                    "Remote terminology validation required but no TerminologyService was provided"
                        .to_string(),
                ));
                return issues;
            };

            let system = crate::r4::binding::coding_system(coding);
            let code = crate::r4::binding::coding_code(coding);
            let display = crate::r4::binding::coding_display(coding);

            let Some(code) = code else {
                issues.push(crate::binding::common::value_issue(
                    fhir_path,
                    valueset_url,
                    "Coding has no code value for terminology validation".to_string(),
                ));
                return issues;
            };

            match terminology
                .member_of(valueset_url, system, code, display)
                .await
            {
                Ok(outcome) if outcome.is_member => issues,
                Ok(_outcome) => {
                    let diagnostics = if let Some(system) = system {
                        format!(
                            "The provided coding {}#{} was not found in ValueSet {}",
                            system, code, valueset_url
                        )
                    } else {
                        format!(
                            "The provided code '{}' was not found in ValueSet {}",
                            code, valueset_url
                        )
                    };
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
                    issues.push(crate::binding::common::terminology_issue(
                        fhir_path,
                        valueset_url,
                        prettify_remote_terminology_error(valueset_url, &e),
                    ));
                    issues
                }
            }
        }

        Err(helios_fhir::r4::terminology::TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::value_issue(
                fhir_path,
                valueset_url,
                format!("Local ValueSet validation failed: {}", msg),
            ));
            issues
        }
    }
}
fn local_binding_instance_path(binding_path: &str, matched_instance_path: &str) -> String {
    let root = root_instance_path(binding_path);

    if let Some((_, local_root)) = root.rsplit_once('.') {
        if let Some(suffix) = matched_instance_path.strip_prefix(root) {
            return format!("{}{}", local_root, suffix);
        }
    }

    matched_instance_path.to_string()
}

pub fn apply_r4_bindings<T>(
    validator: &Validator,
    focus: &T,
    bindings: &[BindingDef],
    terminology: Option<&dyn TerminologyServiceSync>,
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
        // println!(
        //     "binding path={}, target={:?}, matches={:?}",
        //     binding.path,
        //     binding.target_kind,
        //     field_values
        //         .iter()
        //         .map(|(_, p)| p.clone())
        //         .collect::<Vec<_>>()
        // );
        // println!(
        //     "binding path={}, relative={}, matches={:?}",
        //     binding.path,
        //     relative_binding_path(binding.path),
        //     field_values.iter().map(|(_, p)| p.clone()).collect::<Vec<_>>()
        // );
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
                    let stamped_instance_path =
                        local_binding_instance_path(&binding.path, instance_path);
                    for issue in &mut child_issues {
                        issue.instance_path = Some(stamped_instance_path.clone());
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
                    let stamped_instance_path =
                        local_binding_instance_path(&binding.path, instance_path);
                    for issue in &mut child_issues {
                        issue.instance_path = Some(stamped_instance_path.clone());
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
                    let stamped_instance_path =
                        local_binding_instance_path(&binding.path, instance_path);
                    for issue in &mut child_issues {
                        issue.instance_path = Some(stamped_instance_path.clone());
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
pub async fn apply_r4_bindings_async<T>(
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
        Ok(v) => v,
        Err(e) => {
            issues.push(ValidationIssue::error(
                "structure",
                "",
                format!("Failed to serialize focus for binding validation: {}", e),
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

                    let mut child_issues =
                        crate::r4::binding::validate_primitive_code_binding_async(
                            validator,
                            &binding.path,
                            binding.value_set,
                            binding.strength,
                            code_value,
                            |code| terminology_index::validate_code(binding.value_set, code),
                            terminology,
                        )
                        .await;

                    let stamped_instance_path =
                        local_binding_instance_path(&binding.path, instance_path);
                    for issue in &mut child_issues {
                        issue.instance_path = Some(stamped_instance_path.clone());
                    }

                    issues.extend(child_issues);
                }
            }

            BindingTargetKind::Coding => {
                for (field_value, instance_path) in &field_values {
                    let coding =
                        serde_json::from_value::<helios_fhir::r4::Coding>((*field_value).clone())
                            .ok();

                    let mut child_issues = crate::r4::binding::validate_coding_binding_async(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        coding.as_ref(),
                        |coding| terminology_index::validate_coding(binding.value_set, coding),
                        terminology,
                    )
                    .await;

                    let stamped_instance_path =
                        local_binding_instance_path(&binding.path, instance_path);
                    for issue in &mut child_issues {
                        issue.instance_path = Some(stamped_instance_path.clone());
                    }

                    issues.extend(child_issues);
                }
            }

            BindingTargetKind::CodeableConcept => {
                for (field_value, instance_path) in &field_values {
                    let codeable_concept =
                        serde_json::from_value::<helios_fhir::r4::CodeableConcept>(
                            (*field_value).clone(),
                        )
                        .ok();

                    let mut child_issues =
                        crate::r4::binding::validate_codeable_concept_binding_async(
                            validator,
                            &binding.path,
                            binding.value_set,
                            binding.strength,
                            codeable_concept.as_ref(),
                            |cc| {
                                terminology_index::validate_codeable_concept(binding.value_set, cc)
                            },
                            terminology,
                        )
                        .await;

                    let stamped_instance_path =
                        local_binding_instance_path(&binding.path, instance_path);
                    for issue in &mut child_issues {
                        issue.instance_path = Some(stamped_instance_path.clone());
                    }

                    issues.extend(child_issues);
                }
            }

            _ => {}
        }
    }

    issues
}

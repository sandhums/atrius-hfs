//! Binding validation for FHIR R5.
//!
//! This module implements ValueSet binding validation for generated R5 resources.
//!
//! Supported binding target kinds:
//! - primitive `code`
//! - `Coding`
//! - `CodeableConcept`
//!
//! Validation flow:
//!
//! 1. Local generated ValueSet helpers are used first.
//! 2. If local validation returns `RemoteValidationRequired`, the validator
//!    calls the provided `TerminologyService` / `TerminologyServiceSync`.
//! 3. The result is converted into `ValidationIssue` according to binding strength.
//!
//! Sync vs async:
//!
//! - `*_binding` functions use `TerminologyServiceSync`
//! - `*_binding_async` functions use `TerminologyService`
//!
//! The async path is preferred for production validation where remote
//! terminology servers may be required.

use crate::binding::common::{
    get_json_values_with_instance_paths, relative_binding_path, root_instance_path,
};
use crate::{ValidationIssue, Validator};
use fhir_validation_types::{BindingDef, BindingStrength, BindingTargetKind};
use helios_fhir::r5::terminology::TerminologyValidationError;
use helios_fhir::r5::terminology::index as terminology_index;
use helios_fhir::r5::{CodeableConcept, CodeableReference, Coding, Quantity};
use serde::Serialize;
use crate::terminology::service::{TerminologyService, TerminologyServiceSync};

#[cfg(feature = "R5")]
pub fn coding_system(coding: &Coding) -> Option<&str> {
    coding.system.as_ref().and_then(|v| v.value.as_deref())
}
#[cfg(feature = "R5")]
pub fn coding_code(coding: &Coding) -> Option<&str> {
    coding.code.as_ref().and_then(|v| v.value.as_deref())
}
#[cfg(feature = "R5")]
pub fn coding_display(coding: &Coding) -> Option<&str> {
    coding.display.as_ref().and_then(|v| v.value.as_deref())
}
#[cfg(feature = "R5")]
pub fn quantity_system(quantity: &Quantity) -> Option<&str> {
    quantity.system.as_ref().and_then(|v| v.value.as_deref())
}
#[cfg(feature = "R5")]
pub fn quantity_code(quantity: &Quantity) -> Option<&str> {
    quantity.code.as_ref().and_then(|v| v.value.as_deref())
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

/// Render a CodeableConcept into a readable summary for diagnostics.
///
/// Produces strings like:
/// - system#code
/// - code
/// - <empty-coding>
fn summarize_codeable_concept_codings(cc: &CodeableConcept) -> String {
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
///
/// This is the synchronous variant.
/// See also: `validate_primitive_code_binding_async`.
pub fn validate_primitive_code_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    code_value: Option<&str>,
    implicit_system: Option<&str>,
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

            match terminology.member_of(valueset_url, implicit_system, code, None) {
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
                        diagnostics
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
            valueset_url: _local_valueset_url,
            system,
            code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided code '{}', from CodeSystem '{}' was not found in ValueSet {}",
                    code, system, valueset_url
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
                msg.clone(),
            ));

            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
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
pub fn validate_primitive_value_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    value: Option<&str>,
    local_check: F,
    terminology: Option<&dyn TerminologyServiceSync>,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(value) = value else {
        // Missing primitive value is handled by structural/cardinality validation and/or invariants.
        return issues;
    };

    match local_check(value) {
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

            match terminology.member_of(valueset_url, None, value, None) {
                Ok(outcome) if outcome.is_member => issues,
                Ok(outcome) => {
                    let diagnostics = outcome.message.unwrap_or_else(|| {
                        format!(
                            "The provided code '{}' was not found in ValueSet {}",
                            value, valueset_url
                        )
                    });
                    if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                        validator,
                        fhir_path,
                        valueset_url,
                        strength,
                        diagnostics
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
                valueset_url: _local_valueset_url,
                system,
                code,
            }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided value '{}', from CodeSystem '{}' was not found in ValueSet {}",
                    code, system, valueset_url
                )
            } else {
                format!(
                    "The provided value '{}' was not found in ValueSet {}",
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
                msg.clone(),
            ));

            if let Some(issue) = crate::binding::common::issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!(
                    "The value provided ('{}') was not found in the value set '{}'. ({})",
                    value, valueset_url, msg
                ),
            ) {
                issues.push(issue);
            }
            issues
        }

        Err(TerminologyValidationError::UnknownCode { system, code }) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Unknown value '{}' in CodeSystem '{}'", code, system),
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

/// Async variant of `validate_primitive_code_binding`.
///
/// Uses `TerminologyService` for remote terminology calls.
pub async fn validate_primitive_code_binding_async<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    code: Option<&str>,
    implicit_system: Option<&str>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(code) = code.filter(|c| !c.is_empty()) else {
        return issues;
    };

    match local_check(code) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet {
            valueset_url: _local_valueset_url,
            system,
            code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided code '{}', from CodeSystem '{}' was not found in ValueSet {}",
                    code, system, valueset_url
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

            match terminology.member_of(valueset_url, implicit_system, code, None).await {
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

        Err(TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Local ValueSet validation failed: {}", msg),
            ));
            issues
        }
    }
}
pub async fn validate_primitive_value_binding_async<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    value: Option<&str>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(value) = value.filter(|c| !c.is_empty()) else {
        return issues;
    };

    match local_check(value) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet {
                valueset_url: _local_valueset_url,
                system,
                code,
            }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided value '{}', from CodeSystem '{}' was not found in ValueSet {}",
                    code, system, valueset_url
                )
            } else {
                format!(
                    "The provided value '{}' was not found in ValueSet {}",
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
                format!("Unknown value '{}' in CodeSystem '{}'", code, system),
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

            match terminology.member_of(valueset_url, None, value, None).await {
                Ok(outcome) if outcome.is_member => issues,
                Ok(outcome) => {
                    let diagnostics = outcome.message.unwrap_or_else(|| {
                        format!(
                            "The provided code '{}' was not found in ValueSet {}",
                            value, valueset_url
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

        Err(TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(crate::binding::common::terminology_issue(
                fhir_path,
                valueset_url,
                format!("Local ValueSet validation failed: {}", msg),
            ));
            issues
        }
    }
}
/// Validate a `Quantity` binding.
///
/// For terminology purposes, `Quantity` bindings validate the `system` + `code`
/// pair carried by the quantity.
pub fn validate_quantity_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    quantity: Option<&Quantity>,
    local_check: F,
    terminology: Option<&dyn TerminologyServiceSync>,
) -> Vec<ValidationIssue>
where
    F: Fn(&Quantity) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(quantity) = quantity else {
        return issues;
    };

    let system = quantity_system(quantity).filter(|s| !s.is_empty());
    let code = quantity_code(quantity).filter(|c| !c.is_empty());

    if code.is_some() && system.is_none() {
        issues.push(ValidationIssue {
            severity: fhir_validation_types::Severity::Warning,
            code: "terminology",
            fhir_path: fhir_path.to_string(),
            instance_path: None,
            expression: Some(valueset_url.to_string()),
            diagnostics:
                "A quantity code with no system has no defined meaning, and it cannot be validated. A system should be provided"
                    .to_string(),
        });
        return issues;
    }

    match local_check(quantity) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet {
            valueset_url: _local_valueset_url,
            system,
            code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided quantity coding '{}#{}' was not found in ValueSet {}",
                    system, code, valueset_url
                )
            } else {
                format!(
                    "The provided quantity code '{}' was not found in ValueSet {}",
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

            let Some(code) = code else {
                issues.push(crate::binding::common::value_issue(
                    fhir_path,
                    valueset_url,
                    "Quantity has no code value for terminology validation".to_string(),
                ));
                return issues;
            };

            match terminology.member_of(valueset_url, system, code, None) {
                Ok(outcome) if outcome.is_member => issues,
                Ok(outcome) => {
                    let diagnostics = outcome.message.unwrap_or_else(|| {
                        if let Some(system) = system {
                            format!(
                                "The provided quantity coding '{}#{}' was not found in ValueSet {}",
                                system, code, valueset_url
                            )
                        } else {
                            format!(
                                "The provided quantity code '{}' was not found in ValueSet {}",
                                code, valueset_url
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
/// Async variant of `validate_quantity_binding`.
pub async fn validate_quantity_binding_async<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    quantity: Option<&Quantity>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&Quantity) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(quantity) = quantity else {
        return issues;
    };

    let system = quantity_system(quantity).filter(|s| !s.is_empty());
    let code = quantity_code(quantity).filter(|c| !c.is_empty());

    if code.is_some() && system.is_none() {
        issues.push(ValidationIssue {
            severity: fhir_validation_types::Severity::Warning,
            code: "terminology",
            fhir_path: fhir_path.to_string(),
            instance_path: None,
            expression: Some(valueset_url.to_string()),
            diagnostics:
                "A quantity code with no system has no defined meaning, and it cannot be validated. A system should be provided"
                    .to_string(),
        });
        return issues;
    }

    match local_check(quantity) {
        Ok(()) => issues,

        Err(TerminologyValidationError::NotInValueSet {
            valueset_url: _local_valueset_url,
            system,
            code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided quantity coding '{}#{}' was not found in ValueSet {}",
                    system, code, valueset_url
                )
            } else {
                format!(
                    "The provided quantity code '{}' was not found in ValueSet {}",
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

            let Some(code) = code else {
                issues.push(crate::binding::common::value_issue(
                    fhir_path,
                    valueset_url,
                    "Quantity has no code value for terminology validation".to_string(),
                ));
                return issues;
            };

            match terminology.member_of(valueset_url, system, code, None).await {
                Ok(outcome) if outcome.is_member => issues,
                Ok(outcome) => {
                    let diagnostics = outcome.message.unwrap_or_else(|| {
                        if let Some(system) = system {
                            format!(
                                "The provided quantity coding '{}#{}' was not found in ValueSet {}",
                                system, code, valueset_url
                            )
                        } else {
                            format!(
                                "The provided quantity code '{}' was not found in ValueSet {}",
                                code, valueset_url
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
/// Validate a `CodeableReference` binding.
///
/// Binding semantics apply to the `concept` side when present. A reference-only
/// CodeableReference does not produce a terminology binding issue here.
pub fn validate_codeable_reference_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    codeable_reference: Option<&CodeableReference>,
    local_check: F,
    terminology: Option<&dyn TerminologyServiceSync>,
) -> Vec<ValidationIssue>
where
    F: Fn(&CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let Some(cr) = codeable_reference else {
        return Vec::new();
    };

    validate_codeable_concept_binding(
        validator,
        fhir_path,
        valueset_url,
        strength,
        cr.concept.as_ref(),
        local_check,
        terminology,
    )
}
/// Async variant of `validate_codeable_reference_binding`.
pub async fn validate_codeable_reference_binding_async<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    codeable_reference: Option<&CodeableReference>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: Fn(&CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let Some(cr) = codeable_reference else {
        return Vec::new();
    };

    validate_codeable_concept_binding_async(
        validator,
        fhir_path,
        valueset_url,
        strength,
        cr.concept.as_ref(),
        local_check,
        terminology,
    )
    .await
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
///
/// This is the synchronous variant.
/// See also: `validate_codeable_concept_binding_async`.
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
            let mut last_remote_miss_message: Option<String> = None;

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
                    Ok(outcome) => {
                        last_remote_miss_message = Some(outcome.message.unwrap_or_else(|| {
                            if let Some(system) = system {
                                format!(
                                    "The provided coding {}#{} was not found in ValueSet {}",
                                    system, code, valueset_url
                                )
                            } else {
                                format!(
                                    "The provided code '{}' was not found in ValueSet {}",
                                    code, valueset_url
                                )
                            }
                        }));
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
            let diagnostics = last_remote_miss_message.unwrap_or_else(|| {
                let coding_summary = summarize_codeable_concept_codings(cc);
                format!(
                    "The provided coding(s) {} were not found in ValueSet {}",
                    coding_summary, valueset_url
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

/// Async variant of `validate_codeable_concept_binding`.
///
/// Remote terminology is awaited using `TerminologyService`.
pub async fn validate_codeable_concept_binding_async<F>(
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
            let mut last_remote_miss_message: Option<String> = None;

            for coding in codings {
                let system = coding_system(coding);
                let code = coding_code(coding);
                let display = coding_display(coding);

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
                    Ok(outcome) => {
                        last_remote_miss_message = Some(outcome.message.unwrap_or_else(|| {
                            if let Some(system) = system {
                                format!(
                                    "The provided coding {}#{} was not found in ValueSet {}",
                                    system, code, valueset_url
                                )
                            } else {
                                format!(
                                    "The provided code '{}' was not found in ValueSet {}",
                                    code, valueset_url
                                )
                            }
                        }));
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
            let diagnostics = last_remote_miss_message.unwrap_or_else(|| {
                let coding_summary = summarize_codeable_concept_codings(cc);
                format!(
                    "The provided coding(s) {} were not found in ValueSet {}",
                    coding_summary, valueset_url
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
/// This handles bindings declared on elements of type `Coding`.
///
/// Flow:
/// - local ValueSet validation first
/// - optional remote terminology
/// - diagnostics based on binding strength
///
/// This is the synchronous variant.
/// See also: `validate_coding_binding_async`.
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

        Err(TerminologyValidationError::NotInValueSet {
            valueset_url: _local_valueset_url,
            system,
            code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided coding '{}#{}' was not found in ValueSet {}",
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
                Ok(outcome) => {
                    let diagnostics = if let Some(system) = system {
                        outcome.message.unwrap_or_else(|| {
                            format!(
                                "The provided coding '{}#{}' was not found in ValueSet {}",
                                system, code, valueset_url
                            )
                        })
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

/// Async variant of `validate_coding_binding`.
pub async fn validate_coding_binding_async<F>(
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

        Err(TerminologyValidationError::NotInValueSet {
            valueset_url: _local_valueset_url,
            system,
            code,
        }) => {
            let diagnostics = if let Some(system) = system {
                format!(
                    "The provided coding '{}#{}' was not found in ValueSet {}",
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

            match terminology
                .member_of(valueset_url, system, code, display)
                .await
            {
                Ok(outcome) if outcome.is_member => issues,
                Ok(outcome) => {
                    let diagnostics = outcome.message.unwrap_or_else(|| {
                        if let Some(system) = system {
                            format!(
                                "The provided coding {}#{} was not found in ValueSet {}",
                                system, code, valueset_url
                            )
                        } else {
                            format!(
                                "The provided code '{}' was not found in ValueSet {}",
                                code, valueset_url
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

/// Convert a matched JSON instance path into a local binding instance path.
///
/// Binding paths are declared relative to the resource root,
/// but matches are found using absolute JSON paths.
/// This helper rebases the path so that the final `ValidationIssue`
/// points to the correct local element.
fn local_binding_instance_path(binding_path: &str, matched_instance_path: &str) -> String {
    let root = root_instance_path(binding_path);

    if let Some((_, local_root)) = root.rsplit_once('.') {
        if let Some(suffix) = matched_instance_path.strip_prefix(root) {
            return format!("{}{}", local_root, suffix);
        }
    }

    matched_instance_path.to_string()
}

/// Apply binding validation to a serialized R5 resource.
///
/// This function:
/// - walks JSON using generated binding paths
/// - extracts values with instance paths
/// - dispatches to the correct binding validator
/// - stamps `instance_path` on all produced issues
///
/// Local-first terminology validation is used.
/// Remote validation is performed only if required.
///
/// This is the synchronous binding dispatcher.
/// See also: `apply_r5_bindings_async`.
pub fn apply_r5_bindings<T>(
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

                    let implicit_system = terminology_index::implicit_system(binding.value_set);
                    let mut child_issues = validate_primitive_code_binding(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        code_value,
                        implicit_system,
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

            BindingTargetKind::Quantity => {
                for (field_value, instance_path) in &field_values {
                    let quantity = serde_json::from_value::<Quantity>((*field_value).clone()).ok();

                    let mut child_issues = validate_quantity_binding(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        quantity.as_ref(),
                        |quantity| terminology_index::validate_quantity(binding.value_set, quantity),
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

            BindingTargetKind::CodeableReference => {
                for (field_value, instance_path) in &field_values {
                    let codeable_reference =
                        serde_json::from_value::<CodeableReference>((*field_value).clone()).ok();

                    let mut child_issues = validate_codeable_reference_binding(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        codeable_reference.as_ref(),
                        |cc| terminology_index::validate_codeable_concept(binding.value_set, cc),
                        terminology,
                    );
                    let stamped_instance_path =
                        local_binding_instance_path(&binding.path, instance_path);
                    for issue in &mut child_issues {
                        issue.instance_path = Some(stamped_instance_path.clone());
                    }
                    eprintln!("binding path = {}", binding.path);
                    eprintln!("field_values = {:#?}", field_values);
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

/// Async binding dispatcher for R5 resources.
///
/// Same semantics as `apply_r5_bindings`, but uses
/// `TerminologyService` for remote terminology calls.
pub async fn apply_r5_bindings_async<T>(
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
        // println!(
        //     "binding path={}, relative={}, matches={:?},field_values len = {}",
        //     binding.path,
        //     relative_binding_path(binding.path),
        //     field_values.iter().map(|(_, p)| p.clone()).collect::<Vec<_>>(),
        //     field_values.len()
        // );
        match binding.target_kind {
            BindingTargetKind::Code => {
                for (field_value, instance_path) in &field_values {
                    let code_value = field_value.as_str();

                    let implicit_system = terminology_index::implicit_system(binding.value_set);
                    let mut child_issues = validate_primitive_code_binding_async(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        code_value,
                        implicit_system,
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
                    let coding = serde_json::from_value::<Coding>((*field_value).clone()).ok();

                    let mut child_issues = validate_coding_binding_async(
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
                        serde_json::from_value::<CodeableConcept>((*field_value).clone()).ok();

                    let mut child_issues = validate_codeable_concept_binding_async(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        codeable_concept.as_ref(),
                        |cc| terminology_index::validate_codeable_concept(binding.value_set, cc),
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

            BindingTargetKind::Quantity => {
                for (field_value, instance_path) in &field_values {
                    let quantity = serde_json::from_value::<Quantity>((*field_value).clone()).ok();

                    let mut child_issues = validate_quantity_binding_async(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        quantity.as_ref(),
                        |quantity| terminology_index::validate_quantity(binding.value_set, quantity),
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

            BindingTargetKind::CodeableReference => {
                for (field_value, instance_path) in &field_values {
                    let codeable_reference =
                        serde_json::from_value::<CodeableReference>((*field_value).clone()).ok();

                    let mut child_issues = validate_codeable_reference_binding_async(
                        validator,
                        &binding.path,
                        binding.value_set,
                        binding.strength,
                        codeable_reference.as_ref(),
                        |cc| terminology_index::validate_codeable_concept(binding.value_set, cc),
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

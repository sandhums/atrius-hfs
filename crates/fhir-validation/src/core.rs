//! Core validation engine for the `fhir-validation` crate.
//!
//! This module defines the shared validator types and orchestration layer used by
//! all version-specific validation code.
//!
//! Responsibilities:
//! - represent validation issues in a form that can later be mapped to
//!   `OperationOutcome.issue`
//! - provide configurable severity handling for binding misses
//! - dispatch validation by FHIR version (`R4`, `R5`, etc.)
//! - combine resource-level bindings and invariants into a single issue list
//! - provide shared helpers for rebasing instance paths and converting JSON into
//!   evaluation results for FHIRPath-based invariant evaluation
//!
//! Binding path resolution and typed terminology dispatch now live in the
//! version-specific binding modules (`r4/binding.rs`, `r5/binding.rs`)
//!
//! Version-specific logic such as binding extraction and terminology dispatch is
//! implemented in `r4/*`, `r5/*`, and generated validation modules.

pub use fhir_validation_types::{
    BindingDef, BindingStrength, BindingTargetKind, InvariantDef, Severity,
};

use crate::{FhirPathEvaluator, TerminologyService};
use serde_json::Value;
use std::fmt;

/// A single validation issue that can later be mapped to
/// `OperationOutcome.issue`.
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: Severity,

    /// Category such as "value", "invariant", "structure", "terminology"
    pub code: &'static str,

    /// Declared logical FHIR path from the StructureDefinition metadata,
    /// for example "Reference" or "Patient.contact".
    pub fhir_path: String,

    /// Concrete instance path in the validated resource, for example
    /// "Patient.managingOrganization" or "Patient.identifier[0].assigner".
    ///
    /// This is optional for now because not all call sites/threaded recursion
    /// paths have been updated yet.
    pub instance_path: Option<String>,

    /// Optional expression (FHIRPath or ValueSet URL)
    pub expression: Option<String>,

    /// Human readable diagnostics
    pub diagnostics: String,
}

impl ValidationIssue {
    /// Construct an error-level validation issue with a declared FHIR path.
    pub fn error(code: &'static str, path: impl Into<String>, diag: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code,
            fhir_path: path.into(),
            instance_path: None,
            expression: None,
            diagnostics: diag.into(),
        }
    }
    /// Construct a warning-level validation issue with a declared FHIR path.
    pub fn warning(code: &'static str, path: impl Into<String>, diag: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            code,
            fhir_path: path.into(),
            instance_path: None,
            expression: None,
            diagnostics: diag.into(),
        }
    }
    /// Convert a generated invariant definition into a validation issue for a failed check.
    pub fn from_invariant_def(invariant: &InvariantDef) -> Self {
        Self {
            severity: invariant.severity,
            code: "invariant",
            fhir_path: invariant.path.to_string(),
            instance_path: None,
            expression: Some(invariant.expression.to_string()),
            diagnostics: if invariant.human.is_empty() {
                format!("Constraint failed: {}", invariant.key)
            } else {
                format!("Constraint failed: {}: '{}'", invariant.key, invariant.human)
            },
        }
    }
    /// Convert an invariant evaluation failure into an exception-style validation issue
    pub fn from_invariant_error(invariant: &InvariantDef, err: ValidationError) -> Self {
        Self {
            severity: Severity::Error,
            code: "exception",
            fhir_path: invariant.path.to_string(),
            instance_path: None,
            expression: Some(invariant.expression.to_string()),
            diagnostics: if invariant.human.is_empty() {
                format!("Constraint evaluation error: {}: {err}", invariant.key)
            } else {
                format!(
                    "Constraint evaluation error: {}: '{}': {err}",
                    invariant.key,
                    invariant.human
                )
            },
        }
    }
    /// Attach a concrete instance path to this issue.
    pub fn with_instance_path(mut self, instance_path: impl Into<String>) -> Self {
        self.instance_path = Some(instance_path.into());
        self
    }
}
/// Configuration for validation behavior.
#[derive(Debug, Clone, Copy)]
pub struct ValidationConfig {
    /// Treat extensible bindings as errors
    pub strict_extensible_bindings: bool,

    /// Emit warnings for preferred bindings
    pub warn_on_preferred_bindings: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strict_extensible_bindings: false,
            warn_on_preferred_bindings: false,
        }
    }
}
/// Errors raised while evaluating invariants or terminology-backed validation.
#[derive(Debug)]
pub enum ValidationError {
    FhirPath(helios_fhirpath_support::EvaluationError),
    Terminology(String),
    Other(String),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FhirPath(e) => write!(f, "{}", e),
            Self::Terminology(e) => write!(f, "{}", e),
            Self::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<helios_fhirpath_support::EvaluationError> for ValidationError {
    fn from(e: helios_fhirpath_support::EvaluationError) -> Self {
        Self::FhirPath(e)
    }
}

/// Shared validator entry point used by generated and handwritten validation code.
#[derive(Debug, Clone, Copy)]
pub struct Validator {
    pub config: ValidationConfig,
}

impl Default for Validator {
    fn default() -> Self {
        Self {
            config: ValidationConfig::default(),
        }
    }
}

impl Validator {
    /// Create a validator with the provided configuration
    pub fn new(config: ValidationConfig) -> Self {
        Self { config }
    }
    /// Map a binding strength to the severity that should be emitted when a value
    /// is not in the bound ValueSet.
    pub fn binding_miss_severity(&self, strength: BindingStrength) -> Option<Severity> {
        match strength {
            BindingStrength::Required => Some(Severity::Error),
            BindingStrength::Extensible => Some(if self.config.strict_extensible_bindings {
                Severity::Error
            } else {
                Severity::Warning
            }),
            BindingStrength::Preferred => Some(if self.config.warn_on_preferred_bindings {
                Severity::Warning
            } else {
                Severity::Information
            }),
            BindingStrength::Example => None,
        }
    }
    /// Validate a versioned `FhirResource` by dispatching to the appropriate
    /// version-specific resource validator.
    pub fn validate_resource(
        &self,
        resource: &helios_fhir::FhirResource,
        terminology: Option<&dyn TerminologyService>,
        evaluator: &dyn FhirPathEvaluator,
    ) -> Vec<ValidationIssue> {
        match resource {
            #[cfg(feature = "R4")]
            helios_fhir::FhirResource::R4(res) => {
                self.validate_r4_resource(res.as_ref(), terminology, evaluator)
            }

            #[cfg(feature = "R4B")]
            helios_fhir::FhirResource::R4B(res) => {
                self.validate_r4b_resource(res, terminology, evaluator)
            }

            #[cfg(feature = "R5")]
            helios_fhir::FhirResource::R5(res) => {
                self.validate_r5_resource(res, terminology, evaluator)
            }

            #[cfg(feature = "R6")]
            helios_fhir::FhirResource::R6(res) => {
                self.validate_r6_resource(res, terminology, evaluator)
            }
        }
    }
    /// Validate an R4 resource by applying generated bindings first, then invariants.
    #[cfg(feature = "R4")]
    pub fn validate_r4_resource(
        &self,
        resource: &helios_fhir::r4::Resource,
        terminology: Option<&dyn TerminologyService>,
        evaluator: &dyn FhirPathEvaluator,
    ) -> Vec<ValidationIssue> {
        let mut issues = self.validate_r4_resource_bindings(resource, terminology);
        issues.extend(self.validate_r4_resource_invariants(resource, evaluator));
        issues
    }
    /// Validate an R5 resource by applying generated bindings first, then invariants.
    #[cfg(feature = "R5")]
    pub fn validate_r5_resource(
        &self,
        resource: &helios_fhir::r5::Resource,
        terminology: Option<&dyn TerminologyService>,
        evaluator: &dyn FhirPathEvaluator,
    ) -> Vec<ValidationIssue> {
        let mut issues = self.validate_r5_resource_bindings(resource, terminology);
        issues.extend(self.validate_r5_resource_invariants(resource, evaluator));
        issues
    }
    /// Apply R4 binding definitions to the current focus value.
    ///
    /// This delegates to the R4 binding module, which resolves binding paths,
    /// validates primitive codes / `Coding` / `CodeableConcept`, and falls back
    /// to remote terminology when local validation is insufficient.
    #[cfg(feature = "R4")]
    pub fn apply_r4_bindings<T>(
        &self,
        focus: &T,
        bindings: &[BindingDef],
        terminology: Option<&dyn TerminologyService>,
    ) -> Vec<ValidationIssue>
    where
        T: serde::Serialize,
    {
        crate::r4::binding::apply_r4_bindings(self, focus, bindings, terminology)
    }
    /// Apply R5 binding definitions to the current focus value.
    ///
    /// This delegates to the R5 binding module, which resolves binding paths,
    /// validates primitive codes / `Coding` / `CodeableConcept`, and falls back
    /// to remote terminology when local validation is insufficient.
    #[cfg(feature = "R5")]
    pub fn apply_r5_bindings<T>(
        &self,
        focus: &T,
        bindings: &[BindingDef],
        terminology: Option<&dyn TerminologyService>,
    ) -> Vec<ValidationIssue>
    where
        T: serde::Serialize,
    {
        crate::r5::binding::apply_r5_bindings(self, focus, bindings, terminology)
    }
    /// Apply generated invariants to a focused value using the supplied
    /// `FhirPathEvaluator`.
    ///
    /// The `instance_root_path` is used to stamp concrete instance paths on all
    /// emitted issues so callers can map a failure back to the exact location in
    /// the validated resource.
    pub fn apply_invariants<T>(
        &self,
        focus: &T,
        invariants: &[fhir_validation_types::InvariantDef],
        evaluator: &dyn FhirPathEvaluator,
        instance_root_path: &str,
    ) -> Vec<ValidationIssue>
    where
        T: serde::Serialize,
    {
        let mut issues = Vec::new();

        let focus_value = match serde_json::to_value(focus) {
            Ok(value) => json_value_to_evaluation_result(value),
            Err(err) => {
                for invariant in invariants {
                    issues.push(ValidationIssue::from_invariant_error(
                        invariant,
                        ValidationError::Other(format!(
                            "Failed to serialize focus for invariant evaluation: {}",
                            err
                        )),
                    ));
                }
                return issues;
            }
        };

        for invariant in invariants {
            match evaluator.eval_invariant_on(
                focus_value.clone(),
                &invariant.path,
                &invariant.expression,
            ) {
                Ok(true) => {}
                Ok(false) => {
                    issues.push(
                        ValidationIssue::from_invariant_def(invariant)
                            .with_instance_path(instance_root_path),
                    );
                }
                Err(err) => {
                    issues.push(
                        ValidationIssue::from_invariant_error(invariant, err)
                            .with_instance_path(instance_root_path),
                    );
                }
            }
        }

        issues
    }
    /// Rebase a list of child issues from their local root to the caller's actual
    /// instance path.
    ///
    /// This is used heavily by recursive validation of nested datatypes and
    /// backbone elements.
    pub fn rebase_instance_paths(
        &self,
        issues: Vec<ValidationIssue>,
        actual_root_path: &str,
    ) -> Vec<ValidationIssue> {
        issues
            .into_iter()
            .map(|issue| {
                let rebased = match issue.instance_path.as_deref() {
                    Some(path) => rebase_instance_path(path, actual_root_path),
                    None => actual_root_path.to_string(),
                };
                issue.with_instance_path(rebased)
            })
            .collect()
    }
}
/// Rebase a single instance path from a local validation root to the caller's
/// actual root path.
fn rebase_instance_path(current: &str, actual_root_path: &str) -> String {
    if current.is_empty() {
        return actual_root_path.to_string();
    }

    let split_at = current
        .find(|c| c == '.' || c == '[')
        .unwrap_or(current.len());

    let suffix = &current[split_at..];
    if suffix.is_empty() {
        actual_root_path.to_string()
    } else {
        format!("{}{}", actual_root_path, suffix)
    }
}
/// Convert serialized JSON into a `helios_fhirpath_support::EvaluationResult`
/// so the invariant engine can evaluate generated FHIRPath constraints against
/// the current focus value.
fn json_value_to_evaluation_result(value: Value) -> helios_fhirpath_support::EvaluationResult {
    use helios_fhirpath_support::EvaluationResult;

    match value {
        Value::Null => EvaluationResult::Empty,
        Value::Bool(b) => EvaluationResult::Boolean(b, None, None),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                EvaluationResult::Integer(i, None, None)
            } else if let Some(f) = n.as_i64() {
                EvaluationResult::Decimal(f.into(), None, None)
            } else {
                EvaluationResult::Empty
            }
        }
        Value::String(s) => EvaluationResult::String(s, None, None),
        Value::Array(items) => EvaluationResult::Collection {
            items: items
                .into_iter()
                .map(json_value_to_evaluation_result)
                .collect(),
            has_undefined_order: false,
            type_info: None,
        },
        Value::Object(map) => {
            let converted = map
                .into_iter()
                .map(|(k, v)| (k, json_value_to_evaluation_result(v)))
                .collect();
            EvaluationResult::object(converted)
        }
    }
}

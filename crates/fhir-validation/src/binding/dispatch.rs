//! Shared binding dispatch loop for all FHIR versions.
//!
//! Version modules supply [`BindingChoiceProbe`] (choice `[x]` shape detection) and
//! per-kind validators via callbacks; this module owns JSON serialization, path walking,
//! and `instance_path` stamping.

use crate::binding::common::{
    bindable_primitive_string_value, binding_issue_summary, choice_declared_allows_kind,
    field_values_for_binding, primitive_choice_target_kind, stamp_binding_instance_path,
};
use crate::issue_code;
use crate::terminology::service::TerminologyServiceSync;
use crate::{ValidationIssue, Validator};
use fhir_validation_types::{BindingDef, BindingTargetKind, Severity};
use serde::Serialize;
use serde_json::Value;

/// Detect instance JSON shapes for choice `[x]` bindings.
///
/// Each FHIR version implements parsing probes against its generated types.
pub trait BindingChoiceProbe {
    fn parse_quantity(value: &Value) -> bool;
    fn parse_codeable_reference(value: &Value) -> bool;
    fn parse_codeable_concept(value: &Value) -> bool;
    fn parse_coding(value: &Value) -> bool;
}

/// Infer a concrete [`BindingTargetKind`] from instance JSON for choice `[x]` elements.
pub fn infer_choice_kind<P: BindingChoiceProbe>(
    value: &Value,
    declared: Option<&[String]>,
) -> Option<BindingTargetKind> {
    if bindable_primitive_string_value(value).is_some() {
        let k = primitive_choice_target_kind(declared);
        return choice_declared_allows_kind(declared, k).then_some(k);
    }
    if !matches!(value, Value::Object(_)) {
        return None;
    }
    let v = value.clone();
    if choice_declared_allows_kind(declared, BindingTargetKind::Quantity) && P::parse_quantity(&v) {
        return Some(BindingTargetKind::Quantity);
    }
    if choice_declared_allows_kind(declared, BindingTargetKind::CodeableReference)
        && P::parse_codeable_reference(&v)
    {
        return Some(BindingTargetKind::CodeableReference);
    }
    if choice_declared_allows_kind(declared, BindingTargetKind::CodeableConcept)
        && P::parse_codeable_concept(&v)
    {
        return Some(BindingTargetKind::CodeableConcept);
    }
    if choice_declared_allows_kind(declared, BindingTargetKind::Coding) && P::parse_coding(&v) {
        return Some(BindingTargetKind::Coding);
    }
    None
}

pub(crate) fn serialization_failed_issue(err: impl std::fmt::Display) -> ValidationIssue {
    ValidationIssue {
        severity: Severity::Error,
        code: issue_code::STRUCTURE.to_string(),
        fhir_path: "binding".to_string(),
        instance_path: None,
        expression: None,
        expression_kind: None,
        source_invariant_key: None,
        summary: Some(binding_issue_summary::RESOURCE_SERIALIZATION_FAILED.to_string()),
        detail_code: Some(crate::ValidationIssueDetailCode::ValidationException),
        diagnostics: format!("Failed to serialize focus for binding validation: {err}"),
    }
}

#[allow(clippy::result_large_err)]
fn focus_json_for_bindings<T: Serialize>(focus: &T) -> Result<Value, ValidationIssue> {
    serde_json::to_value(focus).map_err(serialization_failed_issue)
}

fn stamp_issues(
    binding_path: &str,
    instance_path: &str,
    focus_json: &Value,
    child_issues: &mut [ValidationIssue],
) {
    let stamped_instance_path =
        stamp_binding_instance_path(binding_path, instance_path, focus_json);
    for issue in child_issues {
        issue.instance_path = Some(stamped_instance_path.clone());
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_binding_at_value_sync<I, A>(
    validator: &Validator,
    binding: &BindingDef,
    field_value: &Value,
    instance_path: &str,
    focus_json: &Value,
    terminology: Option<&dyn TerminologyServiceSync>,
    infer_choice: I,
    apply_single: A,
    issues: &mut Vec<ValidationIssue>,
) where
    I: Fn(&Value, Option<&[String]>) -> Option<BindingTargetKind>,
    A: Fn(
        &Validator,
        &BindingDef,
        &Value,
        BindingTargetKind,
        Option<&dyn TerminologyServiceSync>,
    ) -> Vec<ValidationIssue>,
{
    match binding.target_kind {
        BindingTargetKind::Unsupported => {}
        BindingTargetKind::Choice => {
            if let Some(kind) = infer_choice(field_value, binding.choice_type_codes.as_deref()) {
                let mut child_issues =
                    apply_single(validator, binding, field_value, kind, terminology);
                stamp_issues(&binding.path, instance_path, focus_json, &mut child_issues);
                issues.extend(child_issues);
            }
        }
        kind => {
            let mut child_issues = apply_single(validator, binding, field_value, kind, terminology);
            stamp_issues(&binding.path, instance_path, focus_json, &mut child_issues);
            issues.extend(child_issues);
        }
    }
}

/// Walk `bindings` on a serialized resource and apply synchronous binding validators.
#[allow(clippy::too_many_arguments)]
pub fn apply_bindings_sync<T, I, A>(
    validator: &Validator,
    focus: &T,
    bindings: &[BindingDef],
    terminology: Option<&dyn TerminologyServiceSync>,
    infer_choice: I,
    apply_single: A,
) -> Vec<ValidationIssue>
where
    T: Serialize,
    I: Fn(&Value, Option<&[String]>) -> Option<BindingTargetKind>,
    A: Fn(
        &Validator,
        &BindingDef,
        &Value,
        BindingTargetKind,
        Option<&dyn TerminologyServiceSync>,
    ) -> Vec<ValidationIssue>,
{
    let focus_json = match focus_json_for_bindings(focus) {
        Ok(v) => v,
        Err(issue) => return vec![issue],
    };

    let mut issues = Vec::new();
    for binding in bindings {
        let field_values = field_values_for_binding(&focus_json, &binding.path);
        for (field_value, instance_path) in &field_values {
            dispatch_binding_at_value_sync(
                validator,
                binding,
                field_value,
                instance_path,
                &focus_json,
                terminology,
                &infer_choice,
                &apply_single,
                &mut issues,
            );
        }
    }
    issues
}

/// Defines a version-specific `pub async fn $fn_name` binding dispatcher.
///
/// Invoked from `r4/binding.rs`, `r5/binding.rs`, etc. with the version's
/// [`BindingChoiceProbe`] type and `apply_*_binding_async_single` function.
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! define_apply_bindings_async {
    (
        $(#[$fn_meta:meta])*
        $fn_name:ident,
        $probe:ty,
        $apply_single:path $(,)?
    ) => {
        $(#[$fn_meta])*
        pub async fn $fn_name<T>(
            validator: &crate::Validator,
            focus: &T,
            bindings: &[fhir_validation_types::BindingDef],
            terminology: Option<&dyn crate::terminology::service::TerminologyService>,
        ) -> Vec<crate::ValidationIssue>
        where
            T: serde::Serialize,
        {
            use crate::binding::common::{field_values_for_binding, stamp_binding_instance_path};
            use crate::binding::dispatch::{infer_choice_kind, serialization_failed_issue};
            use fhir_validation_types::BindingTargetKind;

            let focus_json = match serde_json::to_value(focus) {
                Ok(v) => v,
                Err(err) => return vec![serialization_failed_issue(err)],
            };

            let mut issues = Vec::new();
            for binding in bindings {
                let field_values = field_values_for_binding(&focus_json, &binding.path);
                for (field_value, instance_path) in &field_values {
                    match binding.target_kind {
                        BindingTargetKind::Unsupported => {}
                        BindingTargetKind::Choice => {
                            if let Some(kind) = infer_choice_kind::<$probe>(
                                field_value,
                                binding.choice_type_codes.as_deref(),
                            ) {
                                let mut child_issues = $apply_single(
                                    validator,
                                    binding,
                                    field_value,
                                    kind,
                                    terminology,
                                )
                                .await;
                                let stamped_instance_path = stamp_binding_instance_path(
                                    &binding.path,
                                    instance_path,
                                    &focus_json,
                                );
                                for issue in &mut child_issues {
                                    issue.instance_path = Some(stamped_instance_path.clone());
                                }
                                issues.extend(child_issues);
                            }
                        }
                        kind => {
                            let mut child_issues = $apply_single(
                                validator,
                                binding,
                                field_value,
                                kind,
                                terminology,
                            )
                            .await;
                            let stamped_instance_path = stamp_binding_instance_path(
                                &binding.path,
                                instance_path,
                                &focus_json,
                            );
                            for issue in &mut child_issues {
                                issue.instance_path = Some(stamped_instance_path.clone());
                            }
                            issues.extend(child_issues);
                        }
                    }
                }
            }
            issues
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TestProbe;

    impl BindingChoiceProbe for TestProbe {
        fn parse_quantity(value: &Value) -> bool {
            value.get("value").is_some_and(|v| v.is_number())
        }
        fn parse_codeable_reference(_: &Value) -> bool {
            false
        }
        fn parse_codeable_concept(value: &Value) -> bool {
            value.get("coding").is_some()
        }
        fn parse_coding(value: &Value) -> bool {
            value.get("system").is_some() && value.get("code").is_some()
        }
    }

    #[test]
    fn infer_choice_primitive_code() {
        let kind = infer_choice_kind::<TestProbe>(&json!("male"), Some(&["code".to_string()]));
        assert_eq!(kind, Some(BindingTargetKind::Code));
    }

    #[test]
    fn infer_choice_coding_object() {
        let kind = infer_choice_kind::<TestProbe>(
            &json!({"system": "http://x", "code": "a"}),
            Some(&["Coding".to_string()]),
        );
        assert_eq!(kind, Some(BindingTargetKind::Coding));
    }
}

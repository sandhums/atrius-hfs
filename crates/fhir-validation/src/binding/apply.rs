//! Generic per-field binding dispatch (`apply_*_binding_{sync,async}_single`).
//!
//! Version modules implement [`BindingTerminologyHooks`] on their adapter struct and
//! delegate here.

use crate::ValidationIssue;
use crate::Validator;
use crate::binding::common::{
    BindingCheckContextAsync, BindingCheckContextSync, bindable_primitive_string_value,
};
use crate::binding::engine::BindingVersionAdapter;
use crate::binding::validators;
use crate::terminology::service::{TerminologyService, TerminologyServiceSync};
use fhir_terminology::TerminologyValidationError;
use fhir_validation_types::{BindingDef, BindingTargetKind};
use serde_json::Value;

/// Version-specific generated terminology helpers for ValueSet binding checks.
pub trait BindingTerminologyHooks: BindingVersionAdapter {
    /// Whether this FHIR version supports [`BindingTargetKind::CodeableReference`].
    const SUPPORTS_CODEABLE_REFERENCE: bool;

    fn implicit_system(value_set: &str) -> Option<&str>;

    fn validate_code(value_set: &str, code: &str) -> Result<(), TerminologyValidationError>;

    fn validate_coding(
        value_set: &str,
        coding: &Self::Coding,
    ) -> Result<(), TerminologyValidationError>;

    fn validate_codeable_concept(
        value_set: &str,
        cc: &Self::CodeableConcept,
    ) -> Result<(), TerminologyValidationError>;

    fn validate_quantity(
        value_set: &str,
        quantity: &Self::Quantity,
    ) -> Result<(), TerminologyValidationError>;

    fn parse_codeable_reference(field_value: &Value) -> Option<Self::CodeableReference>;
}

pub fn apply_binding_sync_single<H>(
    validator: &Validator,
    binding: &BindingDef,
    field_value: &Value,
    kind: BindingTargetKind,
    terminology: Option<&dyn TerminologyServiceSync>,
) -> Vec<ValidationIssue>
where
    H: BindingTerminologyHooks,
    H::Coding: serde::de::DeserializeOwned,
    H::CodeableConcept: serde::de::DeserializeOwned,
    H::Quantity: serde::de::DeserializeOwned,
{
    let ctx = BindingCheckContextSync::from_binding(validator, binding, terminology);
    let value_set = binding.value_set.as_str();
    match kind {
        BindingTargetKind::Code => {
            let code_value =
                bindable_primitive_string_value(field_value).or_else(|| field_value.as_str());
            validators::validate_primitive_code_binding::<H, _>(
                &ctx,
                code_value,
                H::implicit_system(value_set),
                |code| H::validate_code(value_set, code),
            )
        }
        BindingTargetKind::String | BindingTargetKind::Uri => {
            let text =
                bindable_primitive_string_value(field_value).or_else(|| field_value.as_str());
            validators::validate_primitive_value_binding::<H, _>(&ctx, text, |code| {
                H::validate_code(value_set, code)
            })
        }
        BindingTargetKind::Coding => {
            let coding = serde_json::from_value::<H::Coding>(field_value.clone()).ok();
            validators::validate_coding_binding::<H, _>(&ctx, coding.as_ref(), |coding| {
                H::validate_coding(value_set, coding)
            })
        }
        BindingTargetKind::CodeableConcept => {
            let codeable_concept =
                serde_json::from_value::<H::CodeableConcept>(field_value.clone()).ok();
            validators::validate_codeable_concept_binding::<H, _>(
                &ctx,
                codeable_concept.as_ref(),
                |cc| H::validate_codeable_concept(value_set, cc),
            )
        }
        BindingTargetKind::Quantity => {
            let quantity = serde_json::from_value::<H::Quantity>(field_value.clone()).ok();
            validators::validate_quantity_binding::<H, _>(&ctx, quantity.as_ref(), |quantity| {
                H::validate_quantity(value_set, quantity)
            })
        }
        BindingTargetKind::CodeableReference if H::SUPPORTS_CODEABLE_REFERENCE => {
            let codeable_reference = H::parse_codeable_reference(field_value);
            validators::validate_codeable_reference_binding::<H, _>(
                &ctx,
                codeable_reference.as_ref(),
                |cc| H::validate_codeable_concept(value_set, cc),
            )
        }
        BindingTargetKind::CodeableReference => vec![],
        BindingTargetKind::Choice | BindingTargetKind::Unsupported => vec![],
    }
}

pub async fn apply_binding_async_single<H>(
    validator: &Validator,
    binding: &BindingDef,
    field_value: &Value,
    kind: BindingTargetKind,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    H: BindingTerminologyHooks,
    H::Coding: serde::de::DeserializeOwned,
    H::CodeableConcept: serde::de::DeserializeOwned,
    H::Quantity: serde::de::DeserializeOwned,
{
    let ctx = BindingCheckContextAsync::from_binding(validator, binding, terminology);
    let value_set = binding.value_set.as_str();
    match kind {
        BindingTargetKind::Code => {
            let code_value =
                bindable_primitive_string_value(field_value).or_else(|| field_value.as_str());
            validators::validate_primitive_code_binding_async::<H, _>(
                &ctx,
                code_value,
                H::implicit_system(value_set),
                |code| H::validate_code(value_set, code),
            )
            .await
        }
        BindingTargetKind::String | BindingTargetKind::Uri => {
            let text =
                bindable_primitive_string_value(field_value).or_else(|| field_value.as_str());
            validators::validate_primitive_value_binding_async::<H, _>(&ctx, text, |code| {
                H::validate_code(value_set, code)
            })
            .await
        }
        BindingTargetKind::Coding => {
            let coding = serde_json::from_value::<H::Coding>(field_value.clone()).ok();
            validators::validate_coding_binding_async::<H, _>(&ctx, coding.as_ref(), |coding| {
                H::validate_coding(value_set, coding)
            })
            .await
        }
        BindingTargetKind::CodeableConcept => {
            let codeable_concept =
                serde_json::from_value::<H::CodeableConcept>(field_value.clone()).ok();
            validators::validate_codeable_concept_binding_async::<H, _>(
                &ctx,
                codeable_concept.as_ref(),
                |cc| H::validate_codeable_concept(value_set, cc),
            )
            .await
        }
        BindingTargetKind::Quantity => {
            let quantity = serde_json::from_value::<H::Quantity>(field_value.clone()).ok();
            validators::validate_quantity_binding_async::<H, _>(
                &ctx,
                quantity.as_ref(),
                |quantity| H::validate_quantity(value_set, quantity),
            )
            .await
        }
        BindingTargetKind::CodeableReference if H::SUPPORTS_CODEABLE_REFERENCE => {
            let codeable_reference = H::parse_codeable_reference(field_value);
            validators::validate_codeable_reference_binding_async::<H, _>(
                &ctx,
                codeable_reference.as_ref(),
                |cc| H::validate_codeable_concept(value_set, cc),
            )
            .await
        }
        BindingTargetKind::CodeableReference => vec![],
        BindingTargetKind::Choice | BindingTargetKind::Unsupported => vec![],
    }
}

//! Binding validation for FHIR r4b.
//!
//! This module implements ValueSet binding validation for generated r4b resources.
//!
//! Supported binding target kinds (per FHIR `ElementDefinition.binding`):
//! - primitives `code`, `string`, `uri`
//! - `Coding`, `CodeableConcept`, `Quantity`, `CodeableReference`
//! - choice `[x]` elements ([`BindingTargetKind::Choice`]): handler is chosen from the instance JSON shape
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

use crate::binding::apply::{
    BindingTerminologyHooks, apply_binding_async_single, apply_binding_sync_single,
};
use crate::binding::common::{BindingCheckContextAsync, BindingCheckContextSync};
use crate::binding::dispatch::{BindingChoiceProbe, apply_bindings_sync, infer_choice_kind};
use crate::binding::engine::BindingVersionAdapter;
use crate::terminology::service::{TerminologyService, TerminologyServiceSync};
use crate::{ValidationIssue, Validator};
use fhir_validation_types::{BindingDef, BindingTargetKind};
use helios_fhir::r4b::terminology::index as terminology_index;
use helios_fhir::r4b::{CodeableConcept, CodeableReference, Coding, Quantity};
use serde::Serialize;
use serde_json::Value;

struct R4BBindingChoiceProbe;

impl BindingChoiceProbe for R4BBindingChoiceProbe {
    fn parse_quantity(value: &Value) -> bool {
        serde_json::from_value::<Quantity>(value.clone())
            .ok()
            .is_some()
    }

    fn parse_codeable_reference(value: &Value) -> bool {
        serde_json::from_value::<CodeableReference>(value.clone())
            .ok()
            .is_some()
    }

    fn parse_codeable_concept(value: &Value) -> bool {
        serde_json::from_value::<CodeableConcept>(value.clone())
            .ok()
            .is_some()
    }

    fn parse_coding(value: &Value) -> bool {
        serde_json::from_value::<Coding>(value.clone())
            .ok()
            .is_some()
    }
}

struct R4BBindingAdapter;

impl BindingVersionAdapter for R4BBindingAdapter {
    type Coding = Coding;
    type CodeableConcept = CodeableConcept;
    type Quantity = Quantity;
    type CodeableReference = CodeableReference;
    type PrimitiveCode = String;

    fn coding_system(coding: &Self::Coding) -> Option<&str> {
        coding.system.as_ref().and_then(|v| v.value.as_deref())
    }

    fn coding_code(coding: &Self::Coding) -> Option<&str> {
        coding.code.as_ref().and_then(|v| v.value.as_deref())
    }

    fn coding_display(coding: &Self::Coding) -> Option<&str> {
        coding.display.as_ref().and_then(|v| v.value.as_deref())
    }

    fn codeable_concept_codings(
        cc: &Self::CodeableConcept,
    ) -> Box<dyn Iterator<Item = &Self::Coding> + '_> {
        match cc.coding.as_ref() {
            Some(codings) => Box::new(codings.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    fn codeable_concept_has_codings(cc: &Self::CodeableConcept) -> bool {
        cc.coding.as_ref().is_some_and(|c| !c.is_empty())
    }

    fn quantity_system(quantity: &Self::Quantity) -> Option<&str> {
        quantity.system.as_ref().and_then(|v| v.value.as_deref())
    }

    fn quantity_code(quantity: &Self::Quantity) -> Option<&str> {
        quantity.code.as_ref().and_then(|v| v.value.as_deref())
    }

    fn codeable_reference_concept(
        value: &Self::CodeableReference,
    ) -> Option<&Self::CodeableConcept> {
        value.concept.as_ref()
    }

    crate::impl_binding_version_adapter_rebuild! {
        helios_fhir::r4b::Code,
        helios_fhir::r4b::String,
        Coding,
        CodeableConcept
    }
}

impl BindingTerminologyHooks for R4BBindingAdapter {
    const SUPPORTS_CODEABLE_REFERENCE: bool = true;

    fn implicit_system(value_set: &str) -> Option<&str> {
        terminology_index::implicit_system(value_set)
    }

    fn validate_code(
        value_set: &str,
        code: &str,
    ) -> Result<(), helios_fhir::TerminologyValidationError> {
        terminology_index::validate_code(value_set, code)
    }

    fn validate_coding(
        value_set: &str,
        coding: &Coding,
    ) -> Result<(), helios_fhir::TerminologyValidationError> {
        terminology_index::validate_coding(value_set, coding)
    }

    fn validate_codeable_concept(
        value_set: &str,
        cc: &CodeableConcept,
    ) -> Result<(), helios_fhir::TerminologyValidationError> {
        terminology_index::validate_codeable_concept(value_set, cc)
    }

    fn validate_quantity(
        value_set: &str,
        quantity: &Quantity,
    ) -> Result<(), helios_fhir::TerminologyValidationError> {
        terminology_index::validate_quantity(value_set, quantity)
    }

    fn parse_codeable_reference(field_value: &Value) -> Option<CodeableReference> {
        serde_json::from_value(field_value.clone()).ok()
    }
}

pub fn validate_primitive_code_binding<F>(
    ctx: &BindingCheckContextSync<'_>,
    code_value: Option<&str>,
    implicit_system: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_primitive_code_binding::<R4BBindingAdapter, F>(
        ctx,
        code_value,
        implicit_system,
        local_check,
    )
}

pub fn validate_primitive_value_binding<F>(
    ctx: &BindingCheckContextSync<'_>,
    value: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_primitive_value_binding::<R4BBindingAdapter, F>(
        ctx,
        value,
        local_check,
    )
}

pub async fn validate_primitive_code_binding_async<F>(
    ctx: &BindingCheckContextAsync<'_>,
    code: Option<&str>,
    implicit_system: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_primitive_code_binding_async::<R4BBindingAdapter, F>(
        ctx,
        code,
        implicit_system,
        local_check,
    )
    .await
}

pub async fn validate_primitive_value_binding_async<F>(
    ctx: &BindingCheckContextAsync<'_>,
    value: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&str) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_primitive_value_binding_async::<R4BBindingAdapter, F>(
        ctx,
        value,
        local_check,
    )
    .await
}

pub fn validate_quantity_binding<F>(
    ctx: &BindingCheckContextSync<'_>,
    quantity: Option<&Quantity>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&Quantity) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_quantity_binding::<R4BBindingAdapter, F>(
        ctx,
        quantity,
        local_check,
    )
}

pub async fn validate_quantity_binding_async<F>(
    ctx: &BindingCheckContextAsync<'_>,
    quantity: Option<&Quantity>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&Quantity) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_quantity_binding_async::<R4BBindingAdapter, F>(
        ctx,
        quantity,
        local_check,
    )
    .await
}

pub fn validate_coding_binding<F>(
    ctx: &BindingCheckContextSync<'_>,
    coding: Option<&Coding>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&Coding) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_coding_binding::<R4BBindingAdapter, F>(
        ctx,
        coding,
        local_check,
    )
}

pub async fn validate_coding_binding_async<F>(
    ctx: &BindingCheckContextAsync<'_>,
    coding: Option<&Coding>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&Coding) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_coding_binding_async::<R4BBindingAdapter, F>(
        ctx,
        coding,
        local_check,
    )
    .await
}

pub fn validate_codeable_concept_binding<F>(
    ctx: &BindingCheckContextSync<'_>,
    codeable_concept: Option<&CodeableConcept>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&CodeableConcept) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_codeable_concept_binding::<R4BBindingAdapter, F>(
        ctx,
        codeable_concept,
        local_check,
    )
}

pub async fn validate_codeable_concept_binding_async<F>(
    ctx: &BindingCheckContextAsync<'_>,
    codeable_concept: Option<&CodeableConcept>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&CodeableConcept) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_codeable_concept_binding_async::<R4BBindingAdapter, F>(
        ctx,
        codeable_concept,
        local_check,
    )
    .await
}

pub fn validate_codeable_reference_binding<F>(
    ctx: &BindingCheckContextSync<'_>,
    codeable_reference: Option<&CodeableReference>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&CodeableConcept) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_codeable_reference_binding::<R4BBindingAdapter, F>(
        ctx,
        codeable_reference,
        local_check,
    )
}

pub async fn validate_codeable_reference_binding_async<F>(
    ctx: &BindingCheckContextAsync<'_>,
    codeable_reference: Option<&CodeableReference>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    F: Fn(&CodeableConcept) -> Result<(), helios_fhir::TerminologyValidationError>,
{
    crate::binding::validators::validate_codeable_reference_binding_async::<R4BBindingAdapter, F>(
        ctx,
        codeable_reference,
        local_check,
    )
    .await
}

fn apply_r4b_binding_sync_single(
    validator: &Validator,
    binding: &BindingDef,
    field_value: &Value,
    kind: BindingTargetKind,
    terminology: Option<&dyn TerminologyServiceSync>,
) -> Vec<ValidationIssue> {
    apply_binding_sync_single::<R4BBindingAdapter>(
        validator,
        binding,
        field_value,
        kind,
        terminology,
    )
}

async fn apply_r4b_binding_async_single(
    validator: &Validator,
    binding: &BindingDef,
    field_value: &Value,
    kind: BindingTargetKind,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue> {
    apply_binding_async_single::<R4BBindingAdapter>(
        validator,
        binding,
        field_value,
        kind,
        terminology,
    )
    .await
}

/// Apply binding validation to a serialized r4b resource.
pub fn apply_r4b_bindings<T>(
    validator: &Validator,
    focus: &T,
    bindings: &[BindingDef],
    terminology: Option<&dyn TerminologyServiceSync>,
) -> Vec<ValidationIssue>
where
    T: Serialize,
{
    apply_bindings_sync(
        validator,
        focus,
        bindings,
        terminology,
        infer_choice_kind::<R4BBindingChoiceProbe>,
        apply_r4b_binding_sync_single,
    )
}

crate::define_apply_bindings_async! {
    /// Async binding dispatcher for r4b resources.
    apply_r4b_bindings_async,
    R4BBindingChoiceProbe,
    apply_r4b_binding_async_single,
}

use fhir_validation_types::BindingStrength;
#[cfg(feature = "R4")]
use helios_fhir::r4::Coding;
use crate::{ValidationIssue, Validator};
#[cfg(feature = "R4")]
pub fn coding_system(coding: &Coding) -> Option<&str> {
    coding.system.as_ref().and_then(|v| v.value.as_deref())
}
#[cfg(feature = "R4")]
pub fn coding_code(coding: &Coding) -> Option<&str> {
    coding.code.as_ref().and_then(|v| v.value.as_deref())
}
#[cfg(feature = "R4")]
pub fn coding_display(coding: &Coding) -> Option<&str> {
    coding.display.as_ref().and_then(|v| v.value.as_deref())
}

pub fn issue_for_binding_miss(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    diagnostics: String,
) -> Option<ValidationIssue> {
    validator.binding_miss_severity(strength).map(|severity| ValidationIssue {
        severity,
        code: "value",
        fhir_path: fhir_path.to_string(),
        expression: Some(valueset_url.to_string()),
        diagnostics,
    })
}
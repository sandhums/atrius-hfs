/// Severity of a validation issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Fatal,
    Error,
    Warning,
    Information,
}
/// Binding strength defined by FHIR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingStrength {
    Required,
    Extensible,
    Preferred,
    Example,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingTargetKind {
    Code,
    Coding,
    CodeableConcept,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingDef {
    pub path: &'static str,
    pub strength: crate::BindingStrength,
    pub value_set: &'static str,
    pub binding_name: Option<&'static str>,
    pub target_kind: BindingTargetKind,
}
/// One generated FHIR invariant attached to a resource or element.
///
/// Examples:
/// - `ele-1`
/// - `ext-1`
/// - `pat-1`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvariantDef {
    /// Invariant key, e.g. `pat-1`
    pub key: &'static str,

    /// Severity declared by the specification/profile.
    pub severity: Severity,

    /// Declared logical path, e.g. `Patient.contact`
    pub path: &'static str,

    /// FHIRPath expression to evaluate.
    pub expression: &'static str,

    /// Human-readable message.
    pub human: &'static str,
}


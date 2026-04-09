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
    String,
    Uri,
    Choice,
    CodeableReference,
    Quantity,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingDef {
    pub path: String,
    pub strength: crate::BindingStrength,
    pub value_set: String,
    pub binding_name: Option<String,>,
    pub target_kind: BindingTargetKind,
}
/// One generated FHIR invariant attached to a resource or element.
///
/// Examples:
/// - `ele-1`
/// - `ext-1`
/// - `pat-1`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantDef {
    /// Invariant key, e.g. `pat-1`
    pub key: String,

    /// Severity declared by the specification/profile.
    pub severity: Severity,

    /// Declared logical path, e.g. `Patient.contact`
    pub path: String,

    /// FHIRPath expression to evaluate.
    pub expression: String,

    /// Human-readable message.
    pub human: String,
}

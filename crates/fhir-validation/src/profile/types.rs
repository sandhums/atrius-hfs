use fhir_validation_types::{BindingDef, InvariantDef};

/// Extracted validation metadata for a single constrained profile.
///
/// This is the normalized representation produced from a StructureDefinition
/// differential and consumed by the runtime validator.
#[derive(Debug, Clone)]
pub struct ExtractedProfile {
    pub url: String,
    pub version: Option<String>,
    pub name: Option<String>,
    pub title: Option<String>,
    pub resource_type: String, // e.g. "Patient"
    pub base_definition: Option<String>,

    pub invariants: Vec<InvariantDef>,
    pub element_rules: Vec<ExtractedElementRule>,
}

/// Extracted fixed or pattern constraint captured as normalized JSON.
///
/// These constraints are used both for direct value validation and for slicing
/// discriminators of kind `value`.
#[derive(Debug, Clone)]
pub enum ExtractedValueConstraint {
    Fixed(serde_json::Value),
    Pattern(serde_json::Value),
}

/// Extracted rule metadata for a single constrained element path.
///
/// Depending on the source ElementDefinition, a rule may carry cardinality,
/// binding, invariant, fixed/pattern, type, or slicing metadata.
#[derive(Debug, Clone)]
pub struct ExtractedElementRule {
    pub id: String,
    pub path: String,
    pub min: Option<u32>,
    pub max: Option<String>,
    pub binding: Option<BindingDef>,
    pub constraints: Vec<InvariantDef>,
    pub value_constraint: Option<ExtractedValueConstraint>,
    pub type_constraints: Vec<ExtractedTypeConstraint>,
    pub slicing: Option<ExtractedSlicing>,
    pub slice_name: Option<String>,
}
/// Extracted type constraint for an element.
///
/// `code` is the primary allowed type code, while `profiles` and
/// `target_profiles` capture profile-qualified type restrictions when present.
#[derive(Debug, Clone)]
pub struct ExtractedTypeConstraint {
    pub code: String,
    pub profiles: Vec<String>,
    pub target_profiles: Vec<String>,
}

/// Extracted slicing metadata for a repeating element.
///
/// This corresponds to `ElementDefinition.slicing` and records the declared
/// discriminators, whether slice order matters, and the slicing openness rules.
#[derive(Debug, Clone)]
pub struct ExtractedSlicing {
    pub discriminators: Vec<ExtractedSliceDiscriminator>,
    pub ordered: bool,
    pub rules: ExtractedSlicingRules,
}

/// One extracted slicing discriminator.
///
/// `discriminator_type` identifies how slice membership is differentiated, and
/// `path` identifies the nominated element relative to the sliced element.
#[derive(Debug, Clone)]
pub struct ExtractedSliceDiscriminator {
    pub discriminator_type: ExtractedDiscriminatorType,
    pub path: String,
}

/// Supported discriminator kinds extracted from
/// `ElementDefinition.slicing.discriminator.type`.
///
/// Note that runtime support may intentionally lag behind the full set of
/// extracted enum values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractedDiscriminatorType {
    Value,
    Exists,
    Pattern,
    Type,
    Profile,
    Position,
}

/// Extracted slicing openness rules.
///
/// These correspond to the FHIR slicing rule codes:
/// - `Closed`
/// - `Open`
/// - `OpenAtEnd`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractedSlicingRules {
    Closed,
    Open,
    OpenAtEnd,
}
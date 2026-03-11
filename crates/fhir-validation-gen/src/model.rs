

use std::collections::BTreeMap;

use crate::versions::FhirVersion;

// /// Internal, version-aware model used by the validation generator.
// ///
// /// This is the normalized representation produced by `extract.rs` and
// /// consumed by `emit.rs`.
// #[derive(Debug, Clone, Default)]
// pub struct ValidationModel {
//     pub version: FhirVersion,
//     pub types: Vec<TypeValidationModel>,
// }
//
// impl ValidationModel {
//     pub fn new(version: FhirVersion) -> Self {
//         Self {
//             version,
//             types: Vec::new(),
//         }
//     }
//
//     pub fn push_type(&mut self, ty: TypeValidationModel) {
//         self.types.push(ty);
//     }
//
//     pub fn find_type(&self, rust_type: &str) -> Option<&TypeValidationModel> {
//         self.types.iter().find(|t| t.rust_type == rust_type)
//     }
//
//     pub fn find_type_mut(&mut self, rust_type: &str) -> Option<&mut TypeValidationModel> {
//         self.types.iter_mut().find(|t| t.rust_type == rust_type)
//     }
// }

/// Normalized validation metadata for one generated Rust type.
#[derive(Debug, Clone, Default)]
pub struct TypeValidationModel {
    /// Rust type name in the generated FHIR crate, e.g. `Patient`.
    pub rust_type: String,

    /// Canonical FHIR path / root path, e.g. `Patient` or `Patient.contact`.
    pub fhir_path: String,

    /// The source StructureDefinition canonical URL, when available.
    pub structure_definition_url: Option<String>,

    /// The base definition canonical URL, when available.
    pub base_definition: Option<String>,

    /// Coarse type classification used by the generator.
    pub kind: TypeKind,

    /// Invariants declared directly on this type path.
    pub invariants: Vec<InvariantModel>,

    /// Direct child bindings for this type path.
    pub bindings: Vec<BindingModel>,

    /// Direct child fields of this type from snapshot elements.
    pub fields: Vec<FieldModel>,

    /// Optional ancestry / hierarchy hints captured during extraction.
    ///
    /// Key = child type, value = direct parent type.
    pub direct_supertypes: BTreeMap<String, String>,
}

impl TypeValidationModel {
    pub fn new(rust_type: impl Into<String>, fhir_path: impl Into<String>, kind: TypeKind) -> Self {
        Self {
            rust_type: rust_type.into(),
            fhir_path: fhir_path.into(),
            structure_definition_url: None,
            base_definition: None,
            kind,
            invariants: Vec::new(),
            bindings: Vec::new(),
            fields: Vec::new(),
            direct_supertypes: BTreeMap::new(),
        }
    }

    // pub fn has_validation_metadata(&self) -> bool {
    //     !(self.invariants.is_empty() && self.bindings.is_empty())
    // }
    //
    // pub fn direct_binding_for_path(&self, path: &str) -> Option<&BindingModel> {
    //     self.bindings.iter().find(|b| b.path == path)
    // }
    //
    // pub fn direct_field_for_path(&self, path: &str) -> Option<&FieldModel> {
    //     self.fields.iter().find(|f| f.fhir_path == path)
    // }
}

/// High-level classification of a generated FHIR Rust type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TypeKind {
    #[default]
    Unknown,
    Primitive,
    Element,
    BackboneElement,
    ComplexType,
    Resource,
    DomainResource,
}

/// One direct field/child element under a type.
#[derive(Debug, Clone, Default)]
pub struct FieldModel {
    /// Snapshot element id, e.g. `Patient.gender`.
    pub element_id: String,

    /// Snapshot element path, e.g. `Patient.gender`.
    pub fhir_path: String,

    /// Rust field name expected in the generated model, e.g. `gender, multiple_birth`.
    pub rust_field_name: String,

    /// FHIR field name, e.g. `gender, multipleBirth`.
    pub fhir_field_name: String,

    /// True when the element path is a choice like `value[x]`.
    pub is_choice: bool,

    /// When the element is a choice, the base name of the choice, e.g. `value, multipleBirth`
    pub choice_base_name: Option<String>,

    /// When the element is a choice, the enum name of the choice, e.g. `PatientMultipleBirth`
    pub choice_enum_name: Option<String>, 

    /// Declared FHIR type codes for the element.
    pub type_codes: Vec<String>,

    /// Declared target profiles, if any.
    pub target_profiles: Vec<String>,

    /// Declared profiles, if any.
    pub profiles: Vec<String>,

    /// Minimum cardinality.
    pub min: u32,

    /// Raw max cardinality, e.g. `1`, `*`.
    pub max: String,

    /// True when the element is repeating.
    pub is_array: bool,

    /// True when min > 0.
    pub is_required: bool,

    /// True when the element is a direct child of the containing type path.
    pub is_direct_child: bool,
}

// impl FieldModel {
//     pub fn is_singular(&self) -> bool {
//         !self.is_array
//     }
// }

/// Normalized invariant definition extracted from StructureDefinition.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InvariantModel {
    pub key: String,
    pub severity: SeverityModel,
    pub path: String,
    pub expression: String,
    pub human: String,
    pub source: Option<String>,
    pub element_id: String,
}

/// Normalized binding definition extracted from StructureDefinition.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BindingModel {
    pub path: String,
    pub strength: BindingStrengthModel,
    pub value_set: String,
    pub binding_name: Option<String>,
    pub description: Option<String>,
    pub target_kind: BindingTargetKindModel,
    pub element_id: String,
    pub element_path: String,
    pub type_codes: Vec<String>,
    pub bindable_type_codes: Vec<String>,
    pub is_choice_binding: bool,
}

/// Internal severity enum used during extraction before emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SeverityModel {
    Fatal,
    #[default]
    Error,
    Warning,
    Information,
}

impl SeverityModel {
    pub fn as_rust_tokens(self) -> &'static str {
        match self {
            Self::Fatal => "fhir_validation_types::Severity::Fatal",
            Self::Error => "fhir_validation_types::Severity::Error",
            Self::Warning => "fhir_validation_types::Severity::Warning",
            Self::Information => "fhir_validation_types::Severity::Information",
        }
    }
}

/// Internal binding strength enum used during extraction before emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BindingStrengthModel {
    Required,
    Extensible,
    Preferred,
    #[default]
    Example,
}

impl BindingStrengthModel {
    pub fn as_rust_tokens(self) -> &'static str {
        match self {
            Self::Required => "fhir_validation_types::BindingStrength::Required",
            Self::Extensible => "fhir_validation_types::BindingStrength::Extensible",
            Self::Preferred => "fhir_validation_types::BindingStrength::Preferred",
            Self::Example => "fhir_validation_types::BindingStrength::Example",
        }
    }
}

/// What kind of binding target an element represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BindingTargetKindModel {
    Code,
    Coding,
    CodeableConcept,
    #[default]
    Unsupported,
}

impl BindingTargetKindModel {
    pub fn as_rust_tokens(self) -> &'static str {
        match self {
            Self::Code => "fhir_validation_types::BindingTargetKind::Code",
            Self::Coding => "fhir_validation_types::BindingTargetKind::Coding",
            Self::CodeableConcept => {
                "fhir_validation_types::BindingTargetKind::CodeableConcept"
            }
            Self::Unsupported => "fhir_validation_types::BindingTargetKind::Unsupported",
        }
    }
}
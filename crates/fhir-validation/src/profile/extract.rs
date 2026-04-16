use crate::profile::types::{
    ExtractedDiscriminatorType, ExtractedElementRule, ExtractedProfile,
    ExtractedSliceDiscriminator, ExtractedSlicing, ExtractedSlicingRules, ExtractedTypeConstraint,
    ExtractedValueConstraint,
};
use crate::{BindingDef, BindingStrength, InvariantDef, ValidationError};
use fhir_validation_types::{BindingTargetKind, Severity};
use helios_fhir::Element;
use serde_json::Value;

#[cfg(feature = "R5")]
use helios_fhir::r5::{ElementDefinition, Extension, StructureDefinition};

/// Extract normalized validation metadata from an R5 `StructureDefinition`.
///
/// This currently supports resource constraint profiles expressed through a
/// differential. The extracted result is an [`ExtractedProfile`] that is later
/// consumed by the runtime validator.
#[cfg(feature = "R5")]
pub fn extract_r5_structure_definition_profile(
    sd: &StructureDefinition,
) -> Result<ExtractedProfile, ValidationError> {
    validate_profile_header(sd)?;

    let url = required_string(sd.url.value.as_deref(), "StructureDefinition.url")?;
    let resource_type = required_code(sd.r#type.value.as_deref(), "StructureDefinition.type")?;

    let mut profile_invariants = Vec::new();
    let mut element_rules = Vec::new();

    let differential = sd.differential.as_ref().ok_or_else(|| {
        ValidationError::InvalidStructureDefinition(
            "StructureDefinition.differential is required for profile extraction".to_string(),
        )
    })?;

    let elements = differential.element.as_ref().ok_or_else(|| {
        ValidationError::InvalidStructureDefinition(
            "StructureDefinition.differential.element is required for profile extraction"
                .to_string(),
        )
    })?;

    for element in elements {
        let path = required_string(element.path.value.as_deref(), "ElementDefinition.path")?;
        let id = primitive_opt(element.id.as_ref())
            .map(str::to_owned)
            .unwrap_or_else(|| path.clone());

        let constraints = extract_constraints(&path, element)?;

        if path == resource_type {
            profile_invariants.extend(constraints);
            continue;
        }

        let min = element.min.as_ref().and_then(|m| m.value).map(|v| v as u32);
        let max = element.max.as_ref().and_then(|m| m.value.clone());
        let binding = extract_binding(&path, element)?;
        let value_constraint = extract_value_constraint(element)?;
        let type_constraints = extract_type_constraints(element)?;
        let slicing = extract_slicing(element)?;
        let slice_name = primitive_opt(element.slice_name.as_ref()).map(str::to_owned);

        if min.is_some()
            || max.is_some()
            || binding.is_some()
            || !constraints.is_empty()
            || value_constraint.is_some()
            || !type_constraints.is_empty()
            || slicing.is_some()
            || slice_name.is_some()
        {
            element_rules.push(ExtractedElementRule {
                id,
                path,
                min,
                max,
                binding,
                constraints,
                value_constraint,
                type_constraints,
                slicing,
                slice_name,
            });
        }
    }

    Ok(ExtractedProfile {
        url,
        version: primitive_opt(sd.version.as_ref()).map(str::to_owned),
        name: Some(required_string(
            sd.name.value.as_deref(),
            "StructureDefinition.name",
        )?),
        title: primitive_opt(sd.title.as_ref()).map(str::to_owned),
        resource_type,
        base_definition: primitive_opt(sd.base_definition.as_ref()).map(str::to_owned),
        invariants: profile_invariants,
        element_rules,
    })
}

/// Validate the subset of StructureDefinition header semantics required by the
/// current extractor.
///
/// At present, only resource constraint profiles are supported.
#[cfg(feature = "R5")]
fn validate_profile_header(sd: &StructureDefinition) -> Result<(), ValidationError> {
    let kind = sd.kind.value.as_deref().unwrap_or_default().to_string();

    if kind != "resource" {
        return Err(ValidationError::InvalidStructureDefinition(format!(
            "Only StructureDefinition.kind='resource' is currently supported, got '{kind}'"
        )));
    }

    let derivation = primitive_opt(sd.derivation.as_ref())
        .unwrap_or_default()
        .to_string();

    if derivation != "constraint" {
        return Err(ValidationError::InvalidStructureDefinition(format!(
            "Only StructureDefinition.derivation='constraint' is currently supported, got '{derivation}'"
        )));
    }

    Ok(())
}

/// Extract invariant constraints from an `ElementDefinition`.
///
/// These are converted into normalized [`InvariantDef`] records used during
/// runtime validation.
#[cfg(feature = "R5")]
fn extract_constraints(
    path: &str,
    element: &ElementDefinition,
) -> Result<Vec<InvariantDef>, ValidationError> {
    let mut out = Vec::new();

    if let Some(constraints) = &element.constraint {
        for c in constraints {
            let key = required_string(c.key.value.as_deref(), "ElementDefinition.constraint.key")?;
            let human = c
                .human
                .value
                .clone()
                .unwrap_or_else(|| "Constraint failed".to_string());

            let expression = required_string(
                c.expression.as_ref().and_then(|v| v.value.as_deref()),
                "ElementDefinition.constraint.expression",
            )?;

            let severity = map_constraint_severity(c.severity.value.as_deref().unwrap_or("error"))?;

            out.push(InvariantDef {
                key,
                severity,
                human: human.to_string(),
                expression,
                path: path.to_string(),
            });
        }
    }

    Ok(out)
}

/// Extract terminology binding metadata from an `ElementDefinition`, if present.
///
/// The binding target kind is currently normalized to [`BindingTargetKind::Code`]
/// during extraction and may be refined later by higher-level validation logic.
#[cfg(feature = "R5")]
fn extract_binding(
    path: &str,
    element: &ElementDefinition,
) -> Result<Option<BindingDef>, ValidationError> {
    let Some(binding) = &element.binding else {
        return Ok(None);
    };

    let strength_code = required_code(
        binding.strength.value.as_deref(),
        "ElementDefinition.binding.strength",
    )?;

    let value_set = required_string(
        binding.value_set.as_ref().and_then(|v| v.value.as_deref()),
        "ElementDefinition.binding.valueSet",
    )?;

    let strength = map_binding_strength(&strength_code)?;

    Ok(Some(BindingDef {
        path: path.to_string(),
        strength,
        value_set,
        binding_name: None,
        target_kind: BindingTargetKind::Code,
    }))
}

/// Extract a fixed or pattern value constraint from an `ElementDefinition`.
///
/// FHIR encodes `fixed[x]` and `pattern[x]` as choice-wrapper objects. This
/// extractor unwraps the single inner JSON value and stores only that normalized
/// value in [`ExtractedValueConstraint`].
#[cfg(feature = "R5")]
fn extract_value_constraint(
    element: &ElementDefinition,
) -> Result<Option<ExtractedValueConstraint>, ValidationError> {
    let fixed_json = element
        .fixed
        .as_ref()
        .map(|fixed| extract_choice_inner_json(fixed, "ElementDefinition.fixed"))
        .transpose()?;

    let pattern_json = element
        .pattern
        .as_ref()
        .map(|pattern| extract_choice_inner_json(pattern, "ElementDefinition.pattern"))
        .transpose()?;

    match (fixed_json, pattern_json) {
        (Some(_), Some(_)) => Err(ValidationError::InvalidStructureDefinition(
            "ElementDefinition cannot contain both fixed[x] and pattern[x] for profile extraction"
                .to_string(),
        )),
        (Some(value), None) => Ok(Some(ExtractedValueConstraint::Fixed(prune_json_nulls(
            value,
        )))),
        (None, Some(value)) => Ok(Some(ExtractedValueConstraint::Pattern(prune_json_nulls(
            value,
        )))),
        (None, None) => Ok(None),
    }
}

/// Extract type constraints from `ElementDefinition.type`.
///
/// This captures the primary type code plus any declared `profile` and
/// `targetProfile` qualifiers.
#[cfg(feature = "R5")]
fn extract_type_constraints(
    element: &ElementDefinition,
) -> Result<Vec<ExtractedTypeConstraint>, ValidationError> {
    let mut out = Vec::new();

    let Some(types) = &element.r#type else {
        return Ok(out);
    };

    for ty in types {
        let code = required_code(ty.code.value.as_deref(), "ElementDefinition.type.code")?;

        let profiles = ty
            .profile
            .as_ref()
            .map(|profiles| {
                profiles
                    .iter()
                    .filter_map(|p| p.value.clone())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        let target_profiles = ty
            .target_profile
            .as_ref()
            .map(|profiles| {
                profiles
                    .iter()
                    .filter_map(|p| p.value.clone())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        out.push(ExtractedTypeConstraint {
            code,
            profiles,
            target_profiles,
        });
    }

    Ok(out)
}

/// Recursively remove `null` entries from serialized JSON values.
///
/// This keeps extracted fixed/pattern values compact and easier to compare at
/// runtime.
fn prune_json_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                let pruned = prune_json_nulls(value);
                if !pruned.is_null() {
                    out.insert(key, pruned);
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(prune_json_nulls).collect()),
        other => other,
    }
}
/// Map the raw FHIR binding-strength code into the normalized enum used by the
/// validator.
fn map_binding_strength(code: &str) -> Result<BindingStrength, ValidationError> {
    match code {
        "required" => Ok(BindingStrength::Required),
        "extensible" => Ok(BindingStrength::Extensible),
        "preferred" => Ok(BindingStrength::Preferred),
        "example" => Ok(BindingStrength::Example),
        other => Err(ValidationError::InvalidStructureDefinition(format!(
            "Unknown binding strength '{other}'"
        ))),
    }
}

/// Map the raw FHIR constraint severity code into the normalized severity enum
/// used by the validator.
fn map_constraint_severity(code: &str) -> Result<Severity, ValidationError> {
    match code {
        "error" => Ok(Severity::Error),
        "warning" => Ok(Severity::Warning),
        other => Err(ValidationError::InvalidStructureDefinition(format!(
            "Unknown constraint severity '{other}'"
        ))),
    }
}

/// Convenience alias for primitive string/code-like FHIR elements used by the
/// extractor helpers.
#[cfg(feature = "R5")]
type PrimitiveStringElement = Element<std::string::String, Extension>;

/// Extract the primitive string value from an optional FHIR primitive element.
#[cfg(feature = "R5")]
fn primitive_opt(value: Option<&PrimitiveStringElement>) -> Option<&str> {
    value.and_then(|v| v.value.as_deref())
}

/// Require a string-like field during extraction, returning a normalized owned
/// `String` or an extraction error.
#[cfg(feature = "R5")]
fn required_string(value: Option<&str>, field: &str) -> Result<String, ValidationError> {
    value.map(str::to_owned).ok_or_else(|| {
        ValidationError::InvalidStructureDefinition(format!("Missing required field: {field}"))
    })
}

/// Require a code-like field during extraction, returning a normalized owned
/// `String` or an extraction error.
#[cfg(feature = "R5")]
fn required_code(value: Option<&str>, field: &str) -> Result<String, ValidationError> {
    value.map(str::to_owned).ok_or_else(|| {
        ValidationError::InvalidStructureDefinition(format!("Missing required field: {field}"))
    })
}

/// Extract slicing metadata from `ElementDefinition.slicing`, if present.
///
/// This captures the discriminator list, `ordered` flag, and slicing openness
/// rules into [`ExtractedSlicing`].
#[cfg(feature = "R5")]
fn extract_slicing(
    element: &ElementDefinition,
) -> Result<Option<ExtractedSlicing>, ValidationError> {
    let Some(slicing) = &element.slicing else {
        return Ok(None);
    };

    let discriminators = slicing
        .discriminator
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(extract_slice_discriminator)
                .collect::<Result<Vec<_>, ValidationError>>()
        })
        .transpose()?
        .unwrap_or_default();

    let ordered = slicing
        .ordered
        .as_ref()
        .and_then(|v| v.value)
        .unwrap_or(false);

    let rules_code = required_code(
        slicing.rules.value.as_deref(),
        "ElementDefinition.slicing.rules",
    )?;

    let rules = map_slicing_rules(&rules_code)?;

    Ok(Some(ExtractedSlicing {
        discriminators,
        ordered,
        rules,
    }))
}

/// Extract one slicing discriminator from the raw FHIR representation.
#[cfg(feature = "R5")]
fn extract_slice_discriminator(
    discriminator: &helios_fhir::r5::ElementDefinitionSlicingDiscriminator,
) -> Result<ExtractedSliceDiscriminator, ValidationError> {
    let discriminator_type_code = required_code(
        discriminator.r#type.value.as_deref(),
        "ElementDefinition.slicing.discriminator.type",
    )?;

    let path = required_string(
        discriminator.path.value.as_deref(),
        "ElementDefinition.slicing.discriminator.path",
    )?;

    Ok(ExtractedSliceDiscriminator {
        discriminator_type: map_discriminator_type(&discriminator_type_code)?,
        path,
    })
}

/// Map a FHIR slicing discriminator type code into the normalized extracted enum.
#[cfg(feature = "R5")]
fn map_discriminator_type(code: &str) -> Result<ExtractedDiscriminatorType, ValidationError> {
    match code {
        "value" => Ok(ExtractedDiscriminatorType::Value),
        "exists" => Ok(ExtractedDiscriminatorType::Exists),
        "pattern" => Ok(ExtractedDiscriminatorType::Pattern),
        "type" => Ok(ExtractedDiscriminatorType::Type),
        "profile" => Ok(ExtractedDiscriminatorType::Profile),
        "position" => Ok(ExtractedDiscriminatorType::Position),
        other => Err(ValidationError::InvalidStructureDefinition(format!(
            "Unknown slicing discriminator type '{other}'"
        ))),
    }
}

/// Map a FHIR slicing rules code into the normalized extracted enum.
#[cfg(feature = "R5")]
fn map_slicing_rules(code: &str) -> Result<ExtractedSlicingRules, ValidationError> {
    match code {
        "closed" => Ok(ExtractedSlicingRules::Closed),
        "open" => Ok(ExtractedSlicingRules::Open),
        "openAtEnd" => Ok(ExtractedSlicingRules::OpenAtEnd),
        other => Err(ValidationError::InvalidStructureDefinition(format!(
            "Unknown slicing rules '{other}'"
        ))),
    }
}
/// Serialize a FHIR choice-wrapper value and return only the single inner JSON
/// payload.
///
/// For example, a serialized wrapper like `{ "patternCodeableConcept": {...} }`
/// is reduced to just `{...}` for downstream comparison logic.
#[cfg(feature = "R5")]
fn extract_choice_inner_json<T: serde::Serialize>(
    value: &T,
    field_name: &str,
) -> Result<Value, ValidationError> {
    let serialized = serde_json::to_value(value).map_err(|err| {
        ValidationError::InvalidStructureDefinition(format!(
            "Failed to serialize {field_name} into JSON: {err}"
        ))
    })?;
    match serialized {
        Value::Object(map) => {
            if map.len() != 1 {
                return Err(ValidationError::InvalidStructureDefinition(format!(
                    "Expected {field_name} to serialize as a single-choice wrapper object, got keys: {}",
                    map.keys().cloned().collect::<Vec<_>>().join(", ")
                )));
            }
            let (_, inner) = map.into_iter().next().expect("single-entry map");
            Ok(inner)
        }
        other => Err(ValidationError::InvalidStructureDefinition(format!(
            "Expected {field_name} to serialize as an object wrapper, got: {other:?}"
        ))),
    }
}

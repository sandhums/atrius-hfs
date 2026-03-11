

use serde_json::Value;

use crate::model::{
    BindingModel, BindingStrengthModel, BindingTargetKindModel, FieldModel, InvariantModel,
    SeverityModel, TypeKind, TypeValidationModel,
};
use crate::versions::{FhirVersion, StructureKind};
use helios_fhir_gen::initial_fhir_model::{ElementDefinition, StructureDefinition};


/// Extract normalized validation models for all Rust types generated from a
/// single StructureDefinition snapshot.
///
/// This mirrors the FHIR library generator more closely:
/// - one root model for the StructureDefinition itself
/// - additional models for nested backbone/element paths that become generated
///   Rust types from the same snapshot
pub fn extract_type_validation_models(
    version: FhirVersion,
    def: &StructureDefinition,
) -> Option<Vec<TypeValidationModel>> {
    let def_json = serde_json::to_value(def).ok()?;
    let root_path = structure_root_path(def)?;
    let elements = snapshot_elements_typed(def)?;

    let type_paths = generated_type_paths(elements, root_path);
    let mut models = Vec::new();

    for path in type_paths {
        if let Some(model) = extract_type_validation_model_for_path(
            version,
            def,
            &def_json,
            elements,
            root_path,
            &path,
        ) {
            models.push(model);
        }
    }

    Some(models)
}

fn extract_type_validation_model_for_path(
    version: FhirVersion,
    def: &StructureDefinition,
    def_json: &Value,
    elements: &[ElementDefinition],
    root_path: &str,
    path: &str,
) -> Option<TypeValidationModel> {
    let rust_type = if path == root_path {
        root_rust_type_name(def_json, path)
    } else {
        rust_type_name(path)
    };

    if !should_emit_type_model(path, root_path, &rust_type) {
        return None;
    }

    let structure_kind = parse_structure_kind(def_json);
    let type_kind = classify_type_kind_for_path(path, structure_kind, def_json, elements);

    let mut model = TypeValidationModel::new(rust_type, path.to_string(), type_kind);
    model.structure_definition_url = json_str_field(def_json, "url").map(str::to_string);
    model.base_definition = json_str_field(def_json, "baseDefinition").map(str::to_string);

    let _ = (version, def);

    model.invariants = extract_invariants_from_elements(elements, path);
    model.bindings = extract_bindings_from_elements(elements, path);
    model.fields = extract_direct_fields_from_elements(elements, path);

    if let Some(base_name) = structure_base_name(def_json) {
        model
            .direct_supertypes
            .insert(model.rust_type.clone(), base_name.to_string());
    }

    Some(model)
}

/// Extract invariants declared exactly on `root_path`.
pub fn extract_invariants_from_elements(
    elements: &[ElementDefinition],
    root_path: &str,
) -> Vec<InvariantModel> {
    let mut out = Vec::new();

    for element in elements {
        let element_path = element.path.as_str();
        if element_path != root_path {
            continue;
        }

        let element_id = element.id.clone().unwrap_or_else(|| element_path.to_string());

        let Some(constraints) = element.constraint.as_ref() else {
            continue;
        };

        for constraint in constraints {
            let Some(expression) = constraint.expression.as_deref() else {
                continue;
            };
            if expression.is_empty() {
                continue;
            }

            out.push(InvariantModel {
                key: constraint.key.clone(),
                severity: parse_severity(Some(constraint.severity.as_str())),
                path: element_path.to_string(),
                expression: expression.to_string(),
                human: constraint.human.clone(),
                source: constraint.source.clone(),
                element_id: element_id.clone(),
            });
        }
    }

    out
}

/// Extract direct child bindings under `root_path` from typed snapshot elements.
pub fn extract_bindings_from_elements(
    elements: &[ElementDefinition],
    root_path: &str,
) -> Vec<BindingModel> {
    let mut out = Vec::new();

    for element in elements {
        let element_path = element.path.as_str();
        if !is_direct_child(root_path, element_path) {
            continue;
        }

        let Ok(element_json) = serde_json::to_value(element) else {
            continue;
        };

        let Some(binding_obj) = element_json.get("binding") else {
            continue;
        };

        let Some(value_set) = json_str_field(binding_obj, "valueSet") else {
            continue;
        };
        if value_set.is_empty() {
            continue;
        }

        let type_codes = element_type_codes(&element_json);
        let element_id = element
            .id
            .clone()
            .unwrap_or_else(|| element_path.to_string());
        let bindable_type_codes = type_codes
            .iter()
            .filter(|code| matches!(code.as_str(), "code" | "Coding" | "CodeableConcept"))
            .cloned()
            .collect::<Vec<_>>();

        let is_choice_binding = element_path.ends_with("[x]");

        out.push(BindingModel {
            path: element_path.to_string(),
            strength: parse_binding_strength(json_str_field(binding_obj, "strength")),
            value_set: value_set.to_string(),
            binding_name: extract_binding_name_from_extensions(binding_obj),
            description: json_str_field(binding_obj, "description").map(str::to_string),
            target_kind: binding_target_kind(&type_codes),
            element_id,
            element_path: element_path.to_string(),
            type_codes,
            bindable_type_codes,
            is_choice_binding
        });
    }

    out
}

/// Extract direct child field metadata under `root_path` from typed snapshot elements.
pub fn extract_direct_fields_from_elements(
    elements: &[ElementDefinition],
    root_path: &str,
) -> Vec<FieldModel> {
    let mut out = Vec::new();

    for element in elements {
        let element_path = element.path.as_str();
        let is_direct = is_direct_child(root_path, element_path);
        if !is_direct {
            continue;
        }

        let Ok(element_json) = serde_json::to_value(element) else {
            continue;
        };

        let min = element_json
            .get("min")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0);

        let max = json_str_field(&element_json, "max").unwrap_or("0").to_string();
        let rust_field_name = direct_child_field_name(root_path, element_path);
        let type_codes = element_type_codes(&element_json);
        let element_id = element
            .id
            .clone()
            .unwrap_or_else(|| element_path.to_string());

        let fhir_field_name = direct_child_fhir_field_name(root_path, element_path);
        let is_choice = element_path.ends_with("[x]");
        let choice_base_name = if is_choice {
            Some(fhir_field_name.clone())
        } else {
            None
        };
        let choice_enum_name = choice_base_name
            .as_ref()
            .map(|base| format!("{}{}", rust_type_name(root_path), capitalize_first_letter(base)));

        out.push(FieldModel {
            element_id,
            fhir_path: element_path.to_string(),
            fhir_field_name: fhir_field_name.clone(),
            rust_field_name,
            type_codes,
            target_profiles: extract_target_profiles(&element_json),
            profiles: extract_profiles(&element_json),
            min,
            max: max.clone(),
            is_array: is_repeating_max(&max),
            is_choice,
            choice_base_name,
            is_required: min > 0,
            is_direct_child: is_direct,
            choice_enum_name,
        });
    }

    out
}

pub fn parse_severity(severity: Option<&str>) -> SeverityModel {
    match severity.unwrap_or("error") {
        "fatal" => SeverityModel::Fatal,
        "error" => SeverityModel::Error,
        "warning" => SeverityModel::Warning,
        "information" | "info" => SeverityModel::Information,
        _ => SeverityModel::Error,
    }
}

pub fn parse_binding_strength(strength: Option<&str>) -> BindingStrengthModel {
    match strength.unwrap_or("example") {
        "required" => BindingStrengthModel::Required,
        "extensible" => BindingStrengthModel::Extensible,
        "preferred" => BindingStrengthModel::Preferred,
        "example" => BindingStrengthModel::Example,
        _ => BindingStrengthModel::Example,
    }
}

pub fn binding_target_kind(type_codes: &[String]) -> BindingTargetKindModel {
    if type_codes.len() != 1 {
        return BindingTargetKindModel::Unsupported;
    }

    match type_codes[0].as_str() {
        "code" => BindingTargetKindModel::Code,
        "Coding" => BindingTargetKindModel::Coding,
        "CodeableConcept" => BindingTargetKindModel::CodeableConcept,
        _ => BindingTargetKindModel::Unsupported,
    }
}

pub fn classify_type_kind(type_name: &str, structure_kind: StructureKind, def_json: &Value) -> TypeKind {
    match type_name {
        "Resource" => return TypeKind::Resource,
        "DomainResource" => return TypeKind::DomainResource,
        "Element" => return TypeKind::Element,
        "BackboneElement" => return TypeKind::BackboneElement,
        _ => {}
    }

    if let Some(base_name) = structure_base_name(def_json) {
        match base_name {
            "Resource" => return TypeKind::Resource,
            "DomainResource" => return TypeKind::DomainResource,
            "Element" => return TypeKind::Element,
            "BackboneElement" => return TypeKind::BackboneElement,
            _ => {}
        }
    }

    match structure_kind {
        StructureKind::PrimitiveType => TypeKind::Primitive,
        StructureKind::ComplexType => TypeKind::ComplexType,
        StructureKind::Resource => TypeKind::Resource,
        StructureKind::Logical | StructureKind::Unknown => TypeKind::Unknown,
    }
}

pub fn classify_type_kind_for_path(
    path: &str,
    structure_kind: StructureKind,
    def_json: &Value,
    elements: &[ElementDefinition],
) -> TypeKind {
    if let Some(element) = find_element_by_path(elements, path) {
        let type_codes = element_type_codes_typed(element);

        if type_codes.iter().any(|code| code == "BackboneElement") {
            return TypeKind::BackboneElement;
        }
        if type_codes.iter().any(|code| code == "Element") {
            return TypeKind::Element;
        }
        if type_codes.iter().any(|code| code == "Resource") {
            return TypeKind::Resource;
        }
        if type_codes.iter().any(|code| code == "DomainResource") {
            return TypeKind::DomainResource;
        }
    }

    let type_name = rust_type_name(path);
    classify_type_kind(&type_name, structure_kind, def_json)
}


fn snapshot_elements_typed(def: &StructureDefinition) -> Option<&[ElementDefinition]> {
    def.snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.element.as_ref())
        .map(|elements| elements.as_slice())
}

fn generated_type_paths(elements: &[ElementDefinition], root_path: &str) -> Vec<String> {
    let mut paths = vec![root_path.to_string()];

    for element in elements {
        let path = element.path.as_str();
        if path == root_path {
            continue;
        }

        let type_codes = element_type_codes_typed(element);
        if type_codes.iter().any(|code| code == "BackboneElement" || code == "Element") {
            paths.push(path.to_string());
        }
    }

    paths
}

fn find_element_by_path<'a>(
    elements: &'a [ElementDefinition],
    path: &str,
) -> Option<&'a ElementDefinition> {
    elements.iter().find(|element| element.path == path)
}

fn structure_root_path(def: &StructureDefinition) -> Option<&str> {
    snapshot_elements_typed(def)?
        .first()
        .map(|e| e.path.as_str())
}

fn parse_structure_kind(def_json: &Value) -> StructureKind {
    match json_str_field(def_json, "kind") {
        Some("primitive-type") => StructureKind::PrimitiveType,
        Some("complex-type") => StructureKind::ComplexType,
        Some("resource") => StructureKind::Resource,
        Some("logical") => StructureKind::Logical,
        _ => StructureKind::Unknown,
    }
}

fn structure_base_name(def_json: &Value) -> Option<&str> {
    let base = json_str_field(def_json, "baseDefinition")?;
    base.rsplit('/').next()
}

fn json_str_field<'a>(obj: &'a Value, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(Value::as_str)
}

fn extract_binding_name_from_extensions(binding_obj: &Value) -> Option<String> {
    let extensions = binding_obj.get("extension")?.as_array()?;

    for ext in extensions {
        let url = json_str_field(ext, "url")?;
        if url == "http://hl7.org/fhir/StructureDefinition/elementdefinition-bindingName" {
            if let Some(name) = json_str_field(ext, "valueString") {
                return Some(name.to_string());
            }
        }
    }

    None
}

fn element_type_codes(element: &Value) -> Vec<String> {
    element
        .get("type")
        .and_then(Value::as_array)
        .map(|types| {
            types
                .iter()
                .filter_map(|t| json_str_field(t, "code"))
                .map(normalize_type_code)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn element_type_codes_typed(element: &ElementDefinition) -> Vec<String> {
    let Ok(element_json) = serde_json::to_value(element) else {
        return Vec::new();
    };

    element_type_codes(&element_json)
}
fn normalize_type_code(code: &str) -> String {
    match code {
        "http://hl7.org/fhirpath/System.Boolean" => "boolean".to_string(),
        "http://hl7.org/fhirpath/System.String" => "string".to_string(),
        "http://hl7.org/fhirpath/System.Integer" => "integer".to_string(),
        "http://hl7.org/fhirpath/System.Long" => "integer64".to_string(),
        "http://hl7.org/fhirpath/System.Decimal" => "decimal".to_string(),
        "http://hl7.org/fhirpath/System.Date" => "date".to_string(),
        "http://hl7.org/fhirpath/System.DateTime" => "dateTime".to_string(),
        "http://hl7.org/fhirpath/System.Time" => "time".to_string(),
        other => other.to_string(),
    }
}

fn extract_target_profiles(element: &Value) -> Vec<String> {
    element
        .get("type")
        .and_then(Value::as_array)
        .map(|types| {
            types
                .iter()
                .flat_map(|t| {
                    t.get("targetProfile")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flat_map(|profiles| profiles.iter())
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn extract_profiles(element: &Value) -> Vec<String> {
    element
        .get("type")
        .and_then(Value::as_array)
        .map(|types| {
            types
                .iter()
                .flat_map(|t| {
                    t.get("profile")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flat_map(|profiles| profiles.iter())
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn is_direct_child(parent: &str, child: &str) -> bool {
    if !child.starts_with(parent) {
        return false;
    }

    let rest = &child[parent.len()..];
    rest.starts_with('.') && !rest[1..].contains('.')
}

fn direct_child_field_name(parent: &str, child: &str) -> String {
    if !is_direct_child(parent, child) {
        return child.to_string();
    }

    make_rust_safe(child[parent.len() + 1..].trim_end_matches("[x]"))
}

fn direct_child_fhir_field_name(parent: &str, child: &str) -> String {
    if !is_direct_child(parent, child) {
        return child.to_string();
    }

    child[parent.len() + 1..]
        .trim_end_matches("[x]")
        .to_string()
}

fn rust_type_name(path: &str) -> String {
    let mut out = String::new();

    for segment in path.split('.') {
        let segment = segment.trim_end_matches("[x]");
        if segment.is_empty() {
            continue;
        }
        out.push_str(&capitalize_first_letter(segment));
    }

    if out.is_empty() {
        path.to_string()
    } else {
        out
    }
}

fn root_rust_type_name(def_json: &Value, root_path: &str) -> String {
    if let Some(name) = json_str_field(def_json, "name") {
        if !name.is_empty() {
            return capitalize_first_letter(name);
        }
    }

    if let Some(id) = json_str_field(def_json, "id") {
        if !id.is_empty() {
            return capitalize_first_letter(id);
        }
    }

    rust_type_name(root_path)
}

fn should_emit_type_model(path: &str, root_path: &str, rust_type: &str) -> bool {
    if path != root_path {
        return true;
    }

    !matches!(
        rust_type,
        "Element"
            | "BackboneElement"
            | "Resource"
            | "DomainResource"
            | "MetadataResource"
            | "MoneyQuantity"
            | "SimpleQuantity"
    )
}

fn capitalize_first_letter(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn make_rust_safe(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_is_lower_or_digit = false;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() {
                if prev_is_lower_or_digit {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
                prev_is_lower_or_digit = false;
            } else {
                out.push(ch);
                prev_is_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
            }
        } else {
            if !out.ends_with('_') {
                out.push('_');
            }
            prev_is_lower_or_digit = false;
        }
    }

    let out = out.trim_matches('_').to_string();
    match out.as_str() {
        "type" | "match" | "ref" | "loop" | "self" | "super" | "crate" | "mod" | "move"
        | "async" | "await" | "dyn" => format!("r#{out}"),
        _ => out,
    }
}

fn is_repeating_max(max: &str) -> bool {
    if max == "*" {
        return true;
    }

    max.parse::<u32>().map(|n| n > 1).unwrap_or(false)
}
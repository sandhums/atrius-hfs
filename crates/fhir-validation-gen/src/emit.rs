//! Emit generated validation code from normalized `TypeValidationModel`s.
//!
//! This module is the second half of the validation generator pipeline:
//! - `extract.rs` parses raw FHIR definitions into normalized intermediate models
//! - `emit.rs` turns those models into generated Rust code
//!
//! Responsibilities:
//! - emit generated invariant metadata (`InvariantDef` constants)
//! - emit generated binding metadata (`BindingDef` constants)
//! - emit version-aware `Validatable` impls for each generated Rust type
//! - emit recursive validation traversal for nested datatypes, backbone elements,
//!   choice fields, and contained resources
//! - emit version-aware resource dispatcher methods used by the runtime validator
//!
//! Key design points:
//! - `StructureKind` is used to identify concrete dispatchable resources
//! - `ParentKind` is used to decide recursive structural traversal behavior
//! - binding application is version-aware (`apply_r4_bindings`, `apply_r5_bindings`, ...)
//! - invariant evaluation stays on the shared runtime validator path
use crate::model::{
    BindingModel, FieldModel, InvariantModel, ParentKind, StructureKind, TypeValidationModel,
};
use crate::versions::FhirVersion;
use std::collections::HashMap;

/// Emit generated metadata constants for a single normalized type.
///
/// This emits only declarative metadata:
/// - invariant definitions
/// - binding definitions
///
/// Executable recursive traversal is emitted separately by
/// `emit_validatable_impl_for_type`.
pub fn emit_validation_metadata_for_type(ty: &TypeValidationModel, output: &mut String) {
    if ty.invariants.is_empty() && ty.bindings.is_empty() {
        return;
    }

    if !ty.invariants.is_empty() {
        emit_invariants_const(ty, &ty.invariants, output);
        output.push_str("\n");
    }

    if !ty.bindings.is_empty() {
        emit_bindings_const(ty, &ty.bindings, output);
        output.push_str("\n");
    }
}

/// Emit the version-specific `Validatable` impl for one generated Rust type.
///
/// The generated impl has two responsibilities:
/// - apply metadata declared directly on `Self`
/// - recurse into child fields that require nested validation
///
/// The emitted binding path is version-aware because typed binding validation is
/// implemented in version-specific runtime modules (`apply_r4_bindings`,
/// `apply_r5_bindings`, etc.).
///
/// Invariant application remains shared at runtime and uses the supplied
/// `FhirPathEvaluator`.
pub fn emit_validatable_impl_for_type(
    version: FhirVersion,
    ty: &TypeValidationModel,
    type_index_by_path: &HashMap<&str, &TypeValidationModel>,
    type_index_by_rust_type: &HashMap<&str, &TypeValidationModel>,
    output: &mut String,
) {
    let trait_name = version.validatable_trait_name();
    let feature_name = version.validation_feature();
    let validation_module_path = validation_trait_module_path(version);

    output.push_str(&format!("#[cfg(feature = {:?})]\n", feature_name));
    output.push_str(&format!(
        "impl {validation_module_path}::{} for {} {{\n",
        trait_name, ty.rust_type
    ));

    output.push_str("    fn validate_bindings(\n");
    output.push_str("        &self,\n");
    output.push_str("        validator: &fhir_validation::Validator,\n");
    output.push_str("        terminology: Option<&dyn fhir_validation::TerminologyService>,\n");
    output.push_str("    ) -> Vec<fhir_validation::ValidationIssue> {\n");
    output.push_str("        let mut issues = Vec::new();\n");

    if ty.bindings.is_empty() {
        output.push_str("        let _ = (validator, terminology);\n");
    } else {
        let apply_bindings_method = apply_bindings_method_name(version);
        output.push_str(&format!(
            "        issues.extend(validator.{apply_bindings_method}(self, {}, terminology));\n",
            bindings_const_name(ty),
            apply_bindings_method = apply_bindings_method,
        ));
    }

    emit_recursive_validation(
        version,
        ty,
        ValidationPass::Bindings,
        type_index_by_path,
        type_index_by_rust_type,
        output,
    );

    output.push_str("        issues\n");
    output.push_str("    }\n\n");

    output.push_str("    fn validate_invariants(\n");
    output.push_str("        &self,\n");
    output.push_str("        validator: &fhir_validation::Validator,\n");
    output.push_str("        evaluator: &dyn fhir_validation::FhirPathEvaluator,\n");
    output.push_str("    ) -> Vec<fhir_validation::ValidationIssue> {\n");
    output.push_str("        let mut issues = Vec::new();\n");

    if ty.invariants.is_empty() {
        output.push_str("        let _ = (validator, evaluator);\n");
    } else {
        output.push_str(&format!(
            "        issues.extend(validator.apply_invariants(self, {}, evaluator, {:?}));\n",
            invariants_const_name(ty),
            ty.fhir_path
        ));
    }

    emit_recursive_validation(
        version,
        ty,
        ValidationPass::Invariants,
        type_index_by_path,
        type_index_by_rust_type,
        output,
    );

    output.push_str("        issues\n");
    output.push_str("    }\n");
    output.push_str("}\n\n");
}

/// Emit all generated validation code for a set of normalized type models.
///
/// This function builds lookup indices used during recursive emission, emits
/// each individual type, and then emits the top-level version-specific resource
/// dispatchers for bindings and invariants.
pub fn emit_types(version: FhirVersion, types: &[TypeValidationModel], output: &mut String) {
    let type_index_by_path: HashMap<&str, &TypeValidationModel> =
        types.iter().map(|ty| (ty.fhir_path.as_str(), ty)).collect();

    let type_index_by_rust_type: HashMap<&str, &TypeValidationModel> =
        types.iter().map(|ty| (ty.rust_type.as_str(), ty)).collect();

    for ty in types {
        emit_type(
            version,
            ty,
            &type_index_by_path,
            &type_index_by_rust_type,
            output,
        );
    }
    // for ty in types {
    //     eprintln!("{} -> {:?} - {:?}", ty.rust_type, ty.structure_kind, ty.parent_kind);
    // }
    emit_resource_bindings_dispatcher(version, types, output);
    emit_resource_invariants_dispatcher(version, types, output);
}

/// Emit metadata and executable validation code for one normalized type.
pub fn emit_type(
    version: FhirVersion,
    ty: &TypeValidationModel,
    type_index_by_path: &HashMap<&str, &TypeValidationModel>,
    type_index_by_rust_type: &HashMap<&str, &TypeValidationModel>,
    output: &mut String,
) {
    emit_validation_metadata_for_type(ty, output);
    emit_validatable_impl_for_type(
        version,
        ty,
        type_index_by_path,
        type_index_by_rust_type,
        output,
    );
}

/// Return true when a type should participate in top-level resource dispatch.
///
/// This is based on `StructureKind`, not `ParentKind`, because resource
/// dispatcher generation follows FHIR specification category rather than
/// inheritance/runtime structure.
fn is_dispatchable_resource(ty: &TypeValidationModel) -> bool {
    matches!(ty.structure_kind, StructureKind::Resource)
}

/// Emit the version-specific resource dispatcher for binding validation.
///
/// The generated method matches over the versioned `Resource` enum and forwards
/// to each concrete resource type's generated `validate_bindings` impl.
fn emit_resource_bindings_dispatcher(
    version: FhirVersion,
    types: &[TypeValidationModel],
    output: &mut String,
) {
    let resources: Vec<&TypeValidationModel> = types
        .iter()
        .filter(|ty| is_dispatchable_resource(ty))
        .collect();

    if resources.is_empty() {
        return;
    }

    let feature_name = version.validation_feature();
    let trait_name = version.validatable_trait_name();
    let validation_module_path = validation_trait_module_path(version);
    let resource_enum_path = resource_enum_path(version);
    let method_name = contained_dispatch_method_name(version, ValidationPass::Bindings);

    output.push_str(&format!("#[cfg(feature = {:?})]\n", feature_name));
    output.push_str("impl fhir_validation::Validator {\n");
    output.push_str(&format!(
        "    pub fn {method_name}(\n",
        method_name = method_name,
    ));
    output.push_str(&format!(
        "        &self,\n        resource: &{resource_enum_path},\n",
        resource_enum_path = resource_enum_path,
    ));
    output.push_str("        terminology: Option<&dyn fhir_validation::TerminologyService>,\n");
    output.push_str("    ) -> Vec<fhir_validation::ValidationIssue> {\n");
    output.push_str("        match resource {\n");

    for resource in resources {
        let rust_type = &resource.rust_type;
        output.push_str(&format!(
            "            {resource_enum_path}::{rust_type}(value) => <{rust_type} as {validation_module_path}::{trait_name}>::validate_bindings(value.as_ref(), self, terminology),\n",
            resource_enum_path = resource_enum_path,
            rust_type = rust_type,
            validation_module_path = validation_module_path,
            trait_name = trait_name,
        ));
    }

    output.push_str("        }\n");
    output.push_str("    }\n");
    output.push_str("}\n\n");
}

/// Emit the version-specific resource dispatcher for invariant validation.
///
/// The generated method matches over the versioned `Resource` enum and forwards
/// to each concrete resource type's generated `validate_invariants` impl.
fn emit_resource_invariants_dispatcher(
    version: FhirVersion,
    types: &[TypeValidationModel],
    output: &mut String,
) {
    let resources: Vec<&TypeValidationModel> = types
        .iter()
        .filter(|ty| is_dispatchable_resource(ty))
        .collect();

    if resources.is_empty() {
        return;
    }

    let feature_name = version.validation_feature();
    let trait_name = version.validatable_trait_name();
    let validation_module_path = validation_trait_module_path(version);
    let resource_enum_path = resource_enum_path(version);
    let method_name = contained_dispatch_method_name(version, ValidationPass::Invariants);

    output.push_str(&format!("#[cfg(feature = {:?})]\n", feature_name));
    output.push_str("impl fhir_validation::Validator {\n");
    output.push_str(&format!(
        "    pub fn {method_name}(\n",
        method_name = method_name,
    ));
    output.push_str(&format!(
        "        &self,\n        resource: &{resource_enum_path},\n",
        resource_enum_path = resource_enum_path,
    ));
    output.push_str("        evaluator: &dyn fhir_validation::FhirPathEvaluator,\n");
    output.push_str("    ) -> Vec<fhir_validation::ValidationIssue> {\n");
    output.push_str("        match resource {\n");

    for resource in resources {
        let rust_type = &resource.rust_type;
        output.push_str(&format!(
            "            {resource_enum_path}::{rust_type}(value) => <{rust_type} as {validation_module_path}::{trait_name}>::validate_invariants(value.as_ref(), self, evaluator),\n",
            resource_enum_path = resource_enum_path,
            rust_type = rust_type,
            validation_module_path = validation_module_path,
            trait_name = trait_name,
        ));
    }

    output.push_str("        }\n");
    output.push_str("    }\n");
    output.push_str("}\n\n");
}

/// Return the version-specific runtime binding application method name.
///
/// Bindings are applied in version-aware runtime modules because typed binding
/// validation depends on versioned `Coding` / `CodeableConcept` model types.
fn apply_bindings_method_name(version: FhirVersion) -> &'static str {
    match version {
        FhirVersion::R4 => "apply_r4_bindings",
        FhirVersion::R4B => "apply_r4b_bindings",
        FhirVersion::R5 => "apply_r5_bindings",
        FhirVersion::R6 => "apply_r6_bindings",
    }
}

/// Return the version-specific validation trait module path used in generated impls.
fn validation_trait_module_path(version: FhirVersion) -> &'static str {
    match version {
        FhirVersion::R4 => "fhir_validation::r4",
        FhirVersion::R4B => "fhir_validation::r4b",
        FhirVersion::R5 => "fhir_validation::r5",
        FhirVersion::R6 => "fhir_validation::r6",
    }
}

/// Return the version-specific `Resource` enum path used in generated dispatchers.
fn resource_enum_path(version: FhirVersion) -> &'static str {
    match version {
        FhirVersion::R4 => "helios_fhir::r4::Resource",
        FhirVersion::R4B => "helios_fhir::r4b::Resource",
        FhirVersion::R5 => "helios_fhir::r5::Resource",
        FhirVersion::R6 => "helios_fhir::r6::Resource",
    }
}

/// Which validation phase the emitter is currently generating code for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidationPass {
    Bindings,
    Invariants,
}

/// Emit the generated invariant metadata constant for one type.
fn emit_invariants_const(
    ty: &TypeValidationModel,
    invariants: &[InvariantModel],
    output: &mut String,
) {
    let const_name = invariants_const_name(ty);
    output.push_str(&format!(
        "const {const_name}: &'static [fhir_validation_types::InvariantDef] = &[\n",
        const_name = const_name,
    ));

    for invariant in invariants {
        output.push_str("    fhir_validation_types::InvariantDef {\n");
        output.push_str(&format!("        key: {:?},\n", invariant.key));
        output.push_str(&format!(
            "        severity: {},\n",
            invariant.severity.as_rust_tokens()
        ));
        output.push_str(&format!("        path: {:?},\n", invariant.path));
        output.push_str(&format!(
            "        expression: {:?},\n",
            invariant.expression
        ));
        output.push_str(&format!("        human: {:?},\n", invariant.human));
        output.push_str("    },\n");
    }

    output.push_str("];\n");
}

/// Emit the generated binding metadata constant for one type.
fn emit_bindings_const(ty: &TypeValidationModel, bindings: &[BindingModel], output: &mut String) {
    let const_name = bindings_const_name(ty);
    output.push_str(&format!(
        "const {const_name}: &'static [fhir_validation_types::BindingDef] = &[\n",
        const_name = const_name,
    ));

    for binding in bindings {
        output.push_str("    fhir_validation_types::BindingDef {\n");
        output.push_str(&format!("        path: {:?},\n", binding.path));
        output.push_str(&format!(
            "        strength: {},\n",
            binding.strength.as_rust_tokens()
        ));
        output.push_str(&format!("        value_set: {:?},\n", binding.value_set));

        match &binding.binding_name {
            Some(name) => {
                output.push_str(&format!("        binding_name: Some({:?}),\n", name));
            }
            None => output.push_str("        binding_name: None,\n"),
        }

        output.push_str(&format!(
            "        target_kind: {},\n",
            binding.target_kind.as_rust_tokens()
        ));
        output.push_str("    },\n");
    }

    output.push_str("];\n");
}

/// Emit recursive validation code for child fields under one generated type.
///
/// This function separates:
/// - executable recursive candidates that can be emitted directly
/// - deferred candidates that need specialized handling, such as choice fields
///   or contained resources
///
/// The same traversal strategy is reused for both bindings and invariants.
fn emit_recursive_validation(
    version: FhirVersion,
    ty: &TypeValidationModel,
    pass: ValidationPass,
    type_index_by_path: &HashMap<&str, &TypeValidationModel>,
    type_index_by_rust_type: &HashMap<&str, &TypeValidationModel>,
    output: &mut String,
) {
    let executable_candidates: Vec<&FieldModel> = ty
        .fields
        .iter()
        .filter(|field| {
            should_emit_executable_recursion(
                pass,
                ty.parent_kind,
                field,
                type_index_by_path,
                type_index_by_rust_type,
            )
        })
        .collect();

    let deferred_candidates: Vec<&FieldModel> = ty
        .fields
        .iter()
        .filter(|field| {
            should_emit_deferred_todo(
                pass,
                ty.parent_kind,
                field,
                type_index_by_path,
                type_index_by_rust_type,
            )
        })
        .collect();

    if executable_candidates.is_empty() && deferred_candidates.is_empty() {
        return;
    }

    output.push_str("\n");

    for field in executable_candidates {
        emit_field_recursive_validation(field, pass, output);
    }

    if !deferred_candidates.is_empty() {
        output.push_str("\n");
        output.push_str("        // Deferred recursive validation candidates.\n");
        output.push_str("        // These need specialized handling (for example choice enums or contained resources).\n");

        for field in deferred_candidates {
            if field.rust_field_name == "contained" {
                emit_contained_field_recursive_validation(version, field, pass, output);
                continue;
            }
            if field.is_choice {
                emit_choice_field_recursive_validation(
                    field,
                    pass,
                    &ty.bindings,
                    type_index_by_path,
                    type_index_by_rust_type,
                    output,
                );
                continue;
            }

            let field_name = emitted_field_name(field);
            let pass_name = match pass {
                ValidationPass::Bindings => "bindings",
                ValidationPass::Invariants => "invariants",
            };

            let cardinality = if field.is_array {
                "repeating"
            } else {
                "single"
            };
            let choice = if field.is_choice { ", choice[x]" } else { "" };
            let enum_name = field
                .choice_enum_name
                .as_deref()
                .map(|name| format!(", enum={name}"))
                .unwrap_or_default();

            output.push_str(&format!(
            "        // TODO({pass_name}): recurse into self.{field_name} // path={path}, type_codes={type_codes:?}, {cardinality}{choice}{enum_name}\n",
            field_name = field_name,
            path = field.fhir_path,
            type_codes = field.type_codes,
            cardinality = cardinality,
            choice = choice,
            enum_name = enum_name,
        ));
        }
    }
}

/// Emit recursive validation for a FHIR choice field.
///
/// Choice fields require special handling because the generated Rust model uses
/// an enum rather than a single concrete child type path. This function emits a
/// match over the generated choice enum and forwards validation to the selected
/// variant when validator metadata exists for that child type.
fn emit_choice_field_recursive_validation(
    field: &FieldModel,
    pass: ValidationPass,
    current_bindings: &[BindingModel],
    type_index_by_path: &HashMap<&str, &TypeValidationModel>,
    type_index_by_rust_type: &HashMap<&str, &TypeValidationModel>,
    output: &mut String,
) {
    let Some(enum_name) = field.choice_enum_name.as_deref() else {
        emit_choice_todo(field, pass, "missing choice enum name", output);
        return;
    };

    let field_name = emitted_field_name(field);

    output.push_str(&format!(
        "        if let Some(choice) = &self.{field_name} {{\n",
        field_name = field_name,
    ));
    output.push_str("            match choice {\n");

    let mut emitted_any_arm = false;

    let direct_choice_binding = current_bindings
        .iter()
        .find(|binding| binding.path == field.fhir_path && binding.is_choice_binding);

    for type_code in &field.type_codes {
        let variant_name = choice_variant_name_from_type_code(type_code);
        let child_path = child_type_path_for_choice_variant(type_code);
        let child_rust_type = child_rust_type_for_choice_variant(type_code);
        let child_model = child_path
            .as_deref()
            .and_then(|path| type_index_by_path.get(path).copied())
            .or_else(|| {
                child_rust_type
                    .as_deref()
                    .and_then(|name| type_index_by_rust_type.get(name).copied())
            });

        match (pass, child_model) {
            (ValidationPass::Bindings, Some(model)) if !model.bindings.is_empty() => {
                output.push_str(&format!(
                    "                {enum_name}::{variant_name}(value) => {{\n"
                ));
                output.push_str(
                    "                    issues.extend(value.validate_bindings(validator, terminology));\n",
                );
                output.push_str("                }\n");
                emitted_any_arm = true;
            }
            (ValidationPass::Bindings, _) => {
                let handled_by_field_binding = direct_choice_binding
                    .map(|binding| {
                        binding
                            .bindable_type_codes
                            .iter()
                            .any(|bindable| bindable == type_code)
                    })
                    .unwrap_or(false);

                output.push_str(&format!(
                    "                {enum_name}::{variant_name}(_value) => {{\n"
                ));
                if handled_by_field_binding {
                    output.push_str(
                        "                    // Binding for this choice variant is handled by Self::BINDINGS on the parent field.\n",
                    );
                }
                output.push_str("                }\n");
            }
            (ValidationPass::Invariants, Some(model)) if !model.invariants.is_empty() => {
                output.push_str(&format!(
                    "                {enum_name}::{variant_name}(value) => {{\n"
                ));
                output.push_str(
                    "                    let child_issues = value.validate_invariants(validator, evaluator);\n",
                );
                output.push_str(&format!(
                    "                    issues.extend(validator.rebase_instance_paths(child_issues, {:?}));\n",
                    field.fhir_path,
                ));
                output.push_str("                }\n");
                emitted_any_arm = true;
            }
            (ValidationPass::Invariants, _) => {
                let reason = match (child_path.as_deref(), child_rust_type.as_deref()) {
                    (Some(path), _) => format!("no generated validator metadata for {path}"),
                    (None, Some(name)) => format!("no generated validator metadata for {name}"),
                    (None, None) => format!("no resolvable child type for {type_code}"),
                };
                output.push_str(&format!(
                    "                {enum_name}::{variant_name}(_value) => {{\n"
                ));
                output.push_str(&format!(
                    "                    // TODO({pass_name}): {reason}\n",
                    pass_name = validation_pass_name(pass),
                    reason = reason,
                ));
                output.push_str("                }\n");
            }
        }
    }

    if !emitted_any_arm && field.type_codes.is_empty() {
        output.push_str(&format!(
            "                // TODO({}): no choice variants discovered for {}\n",
            validation_pass_name(pass),
            field.fhir_path,
        ));
    }

    output.push_str("            }\n");
    output.push_str("        }\n");
}

/// Emit a TODO comment for a choice field that could not yet be emitted
/// executable recursion for.
fn emit_choice_todo(field: &FieldModel, pass: ValidationPass, reason: &str, output: &mut String) {
    let field_name = emitted_field_name(field);

    output.push_str(&format!(
        "        // TODO({pass_name}): recurse into self.{field_name} // path={path}, type_codes={type_codes:?}, single, choice[x], reason={reason}\n",
        pass_name = validation_pass_name(pass),
        field_name = field_name,
        path = field.fhir_path,
        type_codes = field.type_codes,
        reason = reason,
    ));
}

/// Return a human-readable name for the current validation pass.
fn validation_pass_name(pass: ValidationPass) -> &'static str {
    match pass {
        ValidationPass::Bindings => "bindings",
        ValidationPass::Invariants => "invariants",
    }
}

/// Convert a FHIR type code into the corresponding generated choice enum variant name.
fn choice_variant_name_from_type_code(type_code: &str) -> String {
    capitalize_first_letter(&normalize_choice_variant_base(type_code))
}

/// Normalize a FHIR type code into the base Rust naming form used for choice variants.
fn normalize_choice_variant_base(type_code: &str) -> String {
    match type_code {
        "base64Binary" => "base64Binary".to_string(),
        "boolean" => "boolean".to_string(),
        "canonical" => "canonical".to_string(),
        "code" => "code".to_string(),
        "date" => "date".to_string(),
        "dateTime" => "dateTime".to_string(),
        "decimal" => "decimal".to_string(),
        "id" => "id".to_string(),
        "instant" => "instant".to_string(),
        "integer" => "integer".to_string(),
        "integer64" => "integer64".to_string(),
        "markdown" => "markdown".to_string(),
        "oid" => "oid".to_string(),
        "positiveInt" => "positiveInt".to_string(),
        "string" => "string".to_string(),
        "time" => "time".to_string(),
        "unsignedInt" => "unsignedInt".to_string(),
        "uri" => "uri".to_string(),
        "url" => "url".to_string(),
        "uuid" => "uuid".to_string(),
        other => other.to_string(),
    }
}

/// Return the generated child type path corresponding to a choice variant type code.
fn child_type_path_for_choice_variant(type_code: &str) -> Option<String> {
    Some(choice_variant_name_from_type_code(type_code))
}

/// Capitalize the first character of a string for generated Rust type/variant names.
fn capitalize_first_letter(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Emit direct recursive validation for a normal child field.
///
/// This handles:
/// - optional vs required fields
/// - repeating vs scalar fields
/// - pass-specific rebasing for invariant issues
fn emit_field_recursive_validation(field: &FieldModel, pass: ValidationPass, output: &mut String) {
    let field_name = emitted_field_name(field);
    match pass {
        ValidationPass::Bindings => {
            if field.is_array {
                output.push_str(&format!(
                    "        if let Some(values) = &self.{field_name} {{\n",
                    field_name = field_name,
                ));
                output.push_str("            for value in values {\n");
                output.push_str(
                    "                issues.extend(value.validate_bindings(validator, terminology));\n",
                );
                output.push_str("            }\n");
                output.push_str("        }\n");
            } else if field.is_required {
                output.push_str(&format!(
                    "        issues.extend(self.{field_name}.validate_bindings(validator, terminology));\n",
                    field_name = field_name,
                ));
            } else {
                output.push_str(&format!(
                    "        if let Some(value) = &self.{field_name} {{\n",
                    field_name = field_name,
                ));
                output.push_str(
                    "            issues.extend(value.validate_bindings(validator, terminology));\n",
                );
                output.push_str("        }\n");
            }
        }
        ValidationPass::Invariants => {
            if field.is_array {
                output.push_str(&format!(
                    "        if let Some(values) = &self.{field_name} {{\n",
                    field_name = field_name,
                ));
                output.push_str("            for value in values {\n");
                output.push_str("                let child_issues = value.validate_invariants(validator, evaluator);\n");
                output.push_str(&format!(
                    "                issues.extend(validator.rebase_instance_paths(child_issues, {:?}));\n",
                    field.fhir_path,
                ));
                output.push_str("            }\n");
                output.push_str("        }\n");
            } else if field.is_required {
                output.push_str(&format!(
                    "        let child_issues = self.{field_name}.validate_invariants(validator, evaluator);\n",
                    field_name = field_name,
                ));
                output.push_str(&format!(
                    "        issues.extend(validator.rebase_instance_paths(child_issues, {:?}));\n",
                    field.fhir_path,
                ));
            } else {
                output.push_str(&format!(
                    "        if let Some(value) = &self.{field_name} {{\n",
                    field_name = field_name,
                ));
                output.push_str("            let child_issues = value.validate_invariants(validator, evaluator);\n");
                output.push_str(&format!(
                    "            issues.extend(validator.rebase_instance_paths(child_issues, {:?}));\n",
                    field.fhir_path,
                ));
                output.push_str("        }\n");
            }
        }
    }
}

/// Decide whether executable recursive validation should be emitted directly
/// for this field in the current validation pass.
fn should_emit_executable_recursion(
    pass: ValidationPass,
    parent_kind: ParentKind,
    field: &FieldModel,
    type_index_by_path: &HashMap<&str, &TypeValidationModel>,
    type_index_by_rust_type: &HashMap<&str, &TypeValidationModel>,
) -> bool {
    if !should_recurse_into_field(parent_kind, field) {
        return false;
    }

    if field.is_choice {
        return false;
    }

    if field.rust_field_name == "contained" {
        return false;
    }

    let child_model = resolve_child_model(field, type_index_by_path, type_index_by_rust_type);
    let Some(child_model) = child_model else {
        return false;
    };

    match pass {
        ValidationPass::Bindings => !child_model.bindings.is_empty(),
        ValidationPass::Invariants => !child_model.invariants.is_empty(),
    }
}

/// Decide whether this field should be emitted as a deferred TODO rather than
/// executable recursive validation.
fn should_emit_deferred_todo(
    pass: ValidationPass,
    parent_kind: ParentKind,
    field: &FieldModel,
    type_index_by_path: &HashMap<&str, &TypeValidationModel>,
    type_index_by_rust_type: &HashMap<&str, &TypeValidationModel>,
) -> bool {
    if !should_recurse_into_field(parent_kind, field) {
        return false;
    }

    if should_emit_executable_recursion(
        pass,
        parent_kind,
        field,
        type_index_by_path,
        type_index_by_rust_type,
    ) {
        return false;
    }

    if field.rust_field_name == "contained" {
        return true;
    }

    if field.is_choice {
        return true;
    }

    let resolved_child = resolve_child_model(field, type_index_by_path, type_index_by_rust_type);
    let Some(_child_model) = resolved_child else {
        return true;
    };

    false
}

/// Resolve the normalized child validation model for a field, if one exists.
///
/// Resolution first tries the full FHIR path and then falls back to a single
/// declared type code's generated Rust type name.
fn resolve_child_model<'a>(
    field: &FieldModel,
    type_index_by_path: &'a HashMap<&str, &'a TypeValidationModel>,
    type_index_by_rust_type: &'a HashMap<&str, &'a TypeValidationModel>,
) -> Option<&'a TypeValidationModel> {
    if let Some(model) = type_index_by_path.get(field.fhir_path.as_str()).copied() {
        return Some(model);
    }

    if field.type_codes.len() == 1 {
        let rust_type_name = rust_type_name_from_type_code(&field.type_codes[0]);
        if let Some(model) = type_index_by_rust_type
            .get(rust_type_name.as_str())
            .copied()
        {
            return Some(model);
        }
    }

    None
}

/// Convert a FHIR type code into the generated Rust type name.
fn rust_type_name_from_type_code(type_code: &str) -> String {
    capitalize_first_letter(type_code)
}

/// Return the generated Rust type name corresponding to a choice variant type code.
fn child_rust_type_for_choice_variant(type_code: &str) -> Option<String> {
    Some(rust_type_name_from_type_code(type_code))
}

/// Decide whether a field is structurally eligible for recursive validation.
///
/// This uses `ParentKind` because recursion is about inheritance/runtime
/// behavior (`DomainResource`, `BackboneElement`, `Element`), not specification
/// category.
fn should_recurse_into_field(parent_kind: ParentKind, field: &FieldModel) -> bool {
    if field.type_codes.is_empty() {
        return false;
    }

    if field.is_choice {
        return true;
    }

    for code in &field.type_codes {
        if is_recursive_type_code(code) {
            return true;
        }
    }

    matches!(
        parent_kind,
        ParentKind::Resource | ParentKind::DomainResource | ParentKind::BackboneElement
    ) && !field
        .type_codes
        .iter()
        .all(|code| is_primitive_type_code(code))
}

/// Return true when the supplied FHIR type code represents a recursively
/// validated complex/runtime type.
fn is_recursive_type_code(code: &str) -> bool {
    matches!(
        code,
        "Address"
            | "Attachment"
            | "CodeableConcept"
            | "Coding"
            | "ContactPoint"
            | "HumanName"
            | "Identifier"
            | "Meta"
            | "Money"
            | "Period"
            | "Quantity"
            | "Range"
            | "Ratio"
            | "Reference"
            | "SampledData"
            | "Signature"
            | "Timing"
            | "Dosage"
            | "Annotation"
            | "Narrative"
            | "Extension"
            | "Element"
            | "BackboneElement"
            | "Resource"
            | "DomainResource"
    )
}

/// Return true when the supplied FHIR type code is a primitive FHIR type.
fn is_primitive_type_code(code: &str) -> bool {
    matches!(
        code,
        "base64Binary"
            | "boolean"
            | "canonical"
            | "code"
            | "date"
            | "dateTime"
            | "decimal"
            | "id"
            | "instant"
            | "integer"
            | "integer64"
            | "markdown"
            | "oid"
            | "positiveInt"
            | "string"
            | "time"
            | "unsignedInt"
            | "uri"
            | "url"
            | "uuid"
            | "xhtml"
    )
}

/// Emit recursive validation for contained resources.
///
/// Contained resources are validated through the version-specific resource
/// dispatchers because they are held as versioned `Resource` enum values rather
/// than normal nested datatype structs.
fn emit_contained_field_recursive_validation(
    version: FhirVersion,
    field: &FieldModel,
    pass: ValidationPass,
    output: &mut String,
) {
    let dispatch_method = contained_dispatch_method_name(version, pass);
    let field_name = emitted_field_name(field);

    output.push_str(&format!(
        "        if let Some(values) = &self.{field_name} {{\n",
        field_name = field_name,
    ));
    output.push_str("            for value in values {\n");
    output.push_str(&format!(
        "                let child_issues = validator.{dispatch_method}(value, {arg_name});\n",
        dispatch_method = dispatch_method,
        arg_name = contained_dispatch_arg_name(pass),
    ));
    output.push_str(&format!(
        "                issues.extend(validator.rebase_instance_paths(child_issues, {:?}));\n",
        field.fhir_path,
    ));
    output.push_str("            }\n");
    output.push_str("        }\n");
}

/// Return the emitted Rust field identifier for a field, applying raw-identifier
/// escaping when required.
fn emitted_field_name(field: &FieldModel) -> String {
    raw_ident(&field.rust_field_name)
}

/// Escape Rust keywords so generated field access remains valid Rust syntax.
fn raw_ident(name: &str) -> String {
    match name {
        "type" | "match" | "ref" | "loop" | "self" | "super" | "crate" | "mod" | "move"
        | "async" | "await" | "dyn" | "use" | "for" | "where" => format!("r#{name}"),
        _ => name.to_string(),
    }
}

/// Return the version- and pass-specific contained-resource dispatcher method name.
fn contained_dispatch_method_name(version: FhirVersion, pass: ValidationPass) -> &'static str {
    match (version, pass) {
        (FhirVersion::R4, ValidationPass::Bindings) => "validate_r4_resource_bindings",
        (FhirVersion::R4, ValidationPass::Invariants) => "validate_r4_resource_invariants",
        (FhirVersion::R4B, ValidationPass::Bindings) => "validate_r4b_resource_bindings",
        (FhirVersion::R4B, ValidationPass::Invariants) => "validate_r4b_resource_invariants",
        (FhirVersion::R5, ValidationPass::Bindings) => "validate_r5_resource_bindings",
        (FhirVersion::R5, ValidationPass::Invariants) => "validate_r5_resource_invariants",
        (FhirVersion::R6, ValidationPass::Bindings) => "validate_r6_resource_bindings",
        (FhirVersion::R6, ValidationPass::Invariants) => "validate_r6_resource_invariants",
    }
}

/// Return the runtime argument name passed to contained-resource dispatch for
/// the current validation pass.
fn contained_dispatch_arg_name(pass: ValidationPass) -> &'static str {
    match pass {
        ValidationPass::Bindings => "terminology",
        ValidationPass::Invariants => "evaluator",
    }
}

/// Return the generated binding constant name for a type.
fn bindings_const_name(ty: &TypeValidationModel) -> String {
    format!("{}_BINDINGS", upper_snake_case(&ty.rust_type))
}

/// Return the generated invariant constant name for a type.
fn invariants_const_name(ty: &TypeValidationModel) -> String {
    format!("{}_INVARIANTS", upper_snake_case(&ty.rust_type))
}

/// Convert a Rust type name into the generated constant-name form.
fn upper_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 8);
    let mut prev_is_lower_or_digit = false;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() {
                if prev_is_lower_or_digit && !out.is_empty() {
                    out.push('_');
                }
                out.push(ch);
                prev_is_lower_or_digit = false;
            } else {
                if ch.is_ascii_lowercase() {
                    out.push(ch.to_ascii_uppercase());
                    prev_is_lower_or_digit = true;
                } else {
                    out.push(ch);
                    prev_is_lower_or_digit = true;
                }
            }
        } else {
            if !out.ends_with('_') && !out.is_empty() {
                out.push('_');
            }
            prev_is_lower_or_digit = false;
        }
    }

    out.trim_matches('_').to_string()
}

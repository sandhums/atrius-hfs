#![cfg(feature = "R5")]
//! Element-scoped profile constraints evaluated with [`FhirPathEvaluator::eval_invariant`]
//! (cpb-12-style relative expressions on `CapabilityStatement.rest.resource`).

mod common {
    pub mod fixtures;
}

#[cfg(test)]
mod tests {
    use crate::common::fixtures::{
        load_fixture, load_profile, load_resource, local_terminology_r5,
    };
    use fhir_validation::profile::types::{ExtractedElementRule, ExtractedProfile};
    use fhir_validation::profile::validate::validate_profile;
    use fhir_validation::validation_context::{ValidationContext, ValidationState};
    use fhir_validation::{
        FhirPathEvaluator, InvariantDef, R5FhirPathEvaluator, Severity, StructureDefinitionKind,
        TypeDerivationRule, Validator,
    };
    use helios_fhir::r5::{CapabilityStatement, Resource};
    use helios_fhir::{FhirResource, FhirVersion};
    use serde_json::json;

    fn r5_evaluator(resource: &FhirResource) -> R5FhirPathEvaluator {
        let FhirResource::R5(r) = resource else {
            panic!("expected R5 FhirResource");
        };
        R5FhirPathEvaluator::new((**r).clone())
    }

    fn run_validate_profile<T: serde::Serialize>(
        validator: &Validator,
        resource: &T,
        resource_type: &str,
        profile: &ExtractedProfile,
        evaluator: &dyn FhirPathEvaluator,
    ) -> Vec<fhir_validation::ValidationIssue> {
        let extracted_profile_map = std::collections::HashMap::new();
        let term = local_terminology_r5();
        let ctx = ValidationContext {
            fhir_version: FhirVersion::R5,
            validator,
            terminology: Some(&term),
            evaluator,
            runtime_profile_registry: None,
            extracted_profile_map: &extracted_profile_map,
        };
        let mut state = ValidationState::default();
        validate_profile(&ctx, &mut state, resource, resource_type, profile)
    }

    fn cpb12_profile() -> ExtractedProfile {
        ExtractedProfile {
            url: "http://example.org/StructureDefinition/cpb12-test".to_string(),
            version: None,
            name: None,
            title: None,
            resource_type: "CapabilityStatement".to_string(),
            base_definition: None,
            kind: StructureDefinitionKind::Resource,
            derivation: TypeDerivationRule::Constraint,
            invariants: vec![],
            element_rules: vec![ExtractedElementRule {
                id: "CapabilityStatement.rest.resource".to_string(),
                path: "CapabilityStatement.rest.resource".to_string(),
                min: None,
                max: None,
                binding: None,
                constraints: vec![InvariantDef {
                    key: "cpb-12".to_string(),
                    severity: Severity::Error,
                    path: "CapabilityStatement.rest.resource".to_string(),
                    expression: "searchParam.select(name).isDistinct()".to_string(),
                    human: "Search parameter names must be unique in the context of a resource."
                        .to_string(),
                }],
                value_constraint: None,
                type_constraints: vec![],
                slicing: None,
                slice_name: None,
                ..Default::default()
            }],
        }
    }

    /// Invalid: duplicate `searchParam.name` within one `rest.resource` slice (cpb-12).
    fn capability_statement_duplicate_search_param_json() -> serde_json::Value {
        json!({
            "resourceType": "CapabilityStatement",
            "status": "active",
            "date": "2020-01-01",
            "kind": "instance",
            "fhirVersion": "5.0.0",
            "format": ["json"],
            "rest": [{
                "mode": "server",
                "resource": [{
                    "type": "Patient",
                    "interaction": [{ "code": "read" }],
                    "searchParam": [
                        {
                            "name": "_id",
                            "type": "token",
                            "definition": "http://hl7.org/fhir/SearchParameter/Resource-id"
                        },
                        {
                            "name": "_id",
                            "type": "token",
                            "definition": "http://hl7.org/fhir/SearchParameter/Resource-id"
                        }
                    ]
                }]
            }]
        })
    }

    fn capability_statement_distinct_search_param_json() -> serde_json::Value {
        json!({
            "resourceType": "CapabilityStatement",
            "status": "active",
            "date": "2020-01-01",
            "kind": "instance",
            "fhirVersion": "5.0.0",
            "format": ["json"],
            "rest": [{
                "mode": "server",
                "resource": [{
                    "type": "Patient",
                    "interaction": [{ "code": "read" }],
                    "searchParam": [
                        {
                            "name": "_id",
                            "type": "token",
                            "definition": "http://hl7.org/fhir/SearchParameter/Resource-id"
                        },
                        {
                            "name": "_lastUpdated",
                            "type": "date",
                            "definition": "http://hl7.org/fhir/SearchParameter/Resource-lastUpdated"
                        }
                    ]
                }]
            }]
        })
    }

    #[test]
    fn cpb12_style_constraint_fails_when_search_param_names_not_distinct() {
        let json = capability_statement_duplicate_search_param_json();
        let cs: CapabilityStatement =
            serde_json::from_value(json.clone()).expect("CapabilityStatement");
        let fhir = FhirResource::R5(Box::new(Resource::CapabilityStatement(Box::new(cs))));
        let evaluator = r5_evaluator(&fhir);
        let profile = cpb12_profile();
        let issues = run_validate_profile(
            &Validator::default(),
            &json,
            "CapabilityStatement",
            &profile,
            &evaluator,
        );
        assert!(
            issues
                .iter()
                .any(|i| i.source_invariant_key.as_deref() == Some("cpb-12")),
            "expected cpb-12 invariant failure: {issues:#?}"
        );
    }

    #[test]
    fn cpb12_style_constraint_passes_when_search_param_names_distinct() {
        let json = capability_statement_distinct_search_param_json();
        let cs: CapabilityStatement =
            serde_json::from_value(json.clone()).expect("CapabilityStatement");
        let fhir = FhirResource::R5(Box::new(Resource::CapabilityStatement(Box::new(cs))));
        let evaluator = r5_evaluator(&fhir);
        let profile = cpb12_profile();
        let issues = run_validate_profile(
            &Validator::default(),
            &json,
            "CapabilityStatement",
            &profile,
            &evaluator,
        );
        assert!(
            !issues
                .iter()
                .any(|i| i.source_invariant_key.as_deref() == Some("cpb-12")),
            "unexpected cpb-12 issue: {issues:#?}"
        );
    }

    /// Root-only profile invariants still use bulk [`fhir_validation::Validator::apply_invariants`].
    #[test]
    fn root_profile_invariant_on_patient_still_evaluates() {
        let json_str = load_fixture(FhirVersion::R5, "profile/only-invariants.json");
        let json: serde_json::Value = serde_json::from_str(&json_str).expect("json");
        let fhir = load_resource(FhirVersion::R5, "profile/only-invariants.json");
        let mut profile = load_profile(FhirVersion::R5, "profile/atrius-profile.json");
        profile.element_rules.clear();

        let FhirResource::R5(r) = &fhir else {
            panic!("expected R5");
        };
        let evaluator = R5FhirPathEvaluator::new((**r).clone());
        let issues = run_validate_profile(
            &Validator::default(),
            &json,
            "Patient",
            &profile,
            &evaluator,
        );
        assert!(
            issues.iter().any(|i| {
                i.expression.as_deref() == Some("active = true implies name.exists()")
            })
        );
    }
}

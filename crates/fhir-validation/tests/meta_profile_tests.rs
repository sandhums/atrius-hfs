mod common {
    pub mod fixtures;
}
#[cfg(all(test, feature = "R5"))]
mod tests {

    use crate::common::fixtures::{load_profile, load_resource};
    use fhir_validation::{R5FhirPathEvaluator, Validator};
    use fhir_validation::profile::profile_registry::ProfileRegistry;
    use helios_fhir::{FhirResource, FhirVersion};

    pub fn r5_evaluator_for(resource: &FhirResource) -> R5FhirPathEvaluator {
        let FhirResource::R5(r) = resource else {
            panic!("expected R5 FhirResource");
        };
        R5FhirPathEvaluator::new((**r).clone())
    }
    fn validator() -> Validator {
        Validator::default()
    }

    #[test]
    fn profile_validation_reports_missing_required_fields_and_invariant() {
        let resource = load_resource(FhirVersion::R5, "profile/declared-meta.json");
        let profile = load_profile(FhirVersion::R5, "profile/atrius-profile.json");
        let evaluator = r5_evaluator_for(&resource);
        let mut registry = ProfileRegistry::new();
        registry.insert(profile);
        let issues =
            validator().validate_resource_with_profiles(&resource, None, &evaluator, &registry);
        // println!("issues: {:#?}", issues);
        assert!(issues.len() >= 4);
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.identifier"));
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.gender"));
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.birthDate"));
        assert!(issues.iter().any(|i| i.fhir_path == "Patient"));
        assert!(!issues.iter().any(|i| i.code == "not-found"));
        assert!(
            issues
                .iter()
                .any(|i| i.expression.as_deref() == Some("active = true implies name.exists()"))
        );
    }

    #[test]
    fn profile_validation_reports_only_invariant_when_required_fields_are_present() {
        // let patient_json = r#"
        // {
        //   "resourceType": "Patient",
        //   "meta": {
        //     "profile": [
        //             "http://atrius.health/fhir/StructureDefinition/atrius-patient"
        //             ]
        //     },
        //   "active": true,
        //   "identifier": [
        //     {
        //       "system": "http://atrius.health/mrn"
        //     }
        //   ],
        //   "gender": "male",
        //   "birthDate": "1980-01-01"
        // }
        // "#;
        //
        // let patient: Patient = serde_json::from_str(patient_json)
        //     .expect("Patient JSON should deserialize into R5 Patient");
        let resource = load_resource(FhirVersion::R5, "profile/only-invariants.json");
        let profile = load_profile(FhirVersion::R5, "profile/atrius-profile.json");
        let evaluator = r5_evaluator_for(&resource);
        let mut registry = ProfileRegistry::new();
        registry.insert(profile);
        let issues =
            validator().validate_resource_with_profiles(&resource, None, &evaluator, &registry);
        // println!("issues: {:?}", issues);
        assert_eq!(issues.len(), 4);
        assert!(issues.iter().any(|i| i.fhir_path == "Patient"));
        assert!(
            issues.iter().any(|i| {
                i.expression.as_deref() == Some("active = true implies name.exists()")
            })
        );
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.identifier"));
        assert!(issues.iter().any(|i| {
            i.expression.as_deref() == Some("identifier.exists() implies identifier.value.exists()")
        }));
    }

    #[test]
    fn declared_meta_profile_is_resolved_from_registry() {
        let resource = load_resource(FhirVersion::R5, "profile/declared-meta.json");
        let profile = load_profile(FhirVersion::R5, "profile/atrius-profile.json");
        let mut registry = ProfileRegistry::new();
        registry.insert(profile);
        let profile = load_profile(FhirVersion::R5, "profile/atrius-profile.json");
        let mut registry = ProfileRegistry::new();
        registry.insert(profile);

        let evaluator = r5_evaluator_for(&resource);

        let issues =
            validator().validate_resource_with_profiles(&resource, None, &evaluator, &registry);
        // println!("{:?}", issues);
        assert!(issues.len() >= 4);
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.identifier"));
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.gender"));
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.birthDate"));
        assert!(issues.iter().any(|i| i.fhir_path == "Patient"));
        assert!(!issues.iter().any(|i| i.code == "not-found"));
    }

    #[test]
    fn missing_declared_meta_profile_produces_not_found_issue() {
        //     let patient_json = r#"
        // {
        //   "resourceType": "Patient",
        //   "meta": {
        //     "profile": [
        //       "http://atrius.health/fhir/StructureDefinition/missing-profile"
        //     ]
        //   }
        // }
        // "#;
        //
        //     let patient: Patient = serde_json::from_str(patient_json)
        //         .expect("Patient JSON should deserialize into R5 Patient");
        let resource = load_resource(FhirVersion::R5, "profile/missing-profile.json");

        let profile = load_profile(FhirVersion::R5, "profile/atrius-profile.json");
        let mut registry = ProfileRegistry::new();
        registry.insert(profile);

        let evaluator = r5_evaluator_for(&resource);

        let validator = Validator::default();

        let issues =
            validator.validate_resource_with_profiles(&resource, None, &evaluator, &registry);

        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.code == "not-found"));
        assert!(issues.iter().any(|i| i.fhir_path == "Patient.meta.profile"));
        assert!(issues.iter().any(|i| {
            i.expression.as_deref()
                == Some("http://atrius.health/fhir/StructureDefinition/missing-profile")
        }));
    }
}

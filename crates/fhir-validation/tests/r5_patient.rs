#![cfg(feature = "R5")]
mod common {
    pub mod fixtures;
}
mod tests {
    use crate::common::fixtures::{
        assert_has_invariant, assert_issue_count, load_resource, local_terminology_r5,
    };
    use fhir_validation::R5FhirPathEvaluator;
    use helios_fhir::{FhirResource, FhirVersion};
    pub fn r5_evaluator_for(resource: &FhirResource) -> R5FhirPathEvaluator {
        let FhirResource::R5(r) = resource else {
            panic!("expected R5 FhirResource");
        };
        R5FhirPathEvaluator::new((**r).clone())
    }
    #[test]
    fn r5_patient_example_validates() {
        let r = load_resource(FhirVersion::R5, "valid/patient/patient-example.json");

        let validator = fhir_validation::Validator::default();
        let evaluator = r5_evaluator_for(&r);
        let term = local_terminology_r5();
        let _issues = validator.validate_resource(&r, Some(&term), &evaluator);

        // assert!(issues.is_empty());
        // assert_no_errors(&issues);
    }
    #[test]
    fn r5_patient_local_reference_but_no_contained() {
        let r = load_resource(
            FhirVersion::R5,
            "invalid/patient/patient-local_reference_but_no_contained.json",
        );

        let validator = fhir_validation::Validator::default();
        let evaluator = r5_evaluator_for(&r);
        let term = local_terminology_r5();
        let issues = validator.validate_resource(&r, Some(&term), &evaluator);

        assert_has_invariant(
            &issues,
            "Patient.managingOrganization",
            "contained resource",
        );
    }
    #[test]
    fn r5_dom3_no_id_in_contained() {
        let r = load_resource(
            FhirVersion::R5,
            "invalid/patient/patient_no_id_in_contained.json",
        );

        let validator = fhir_validation::Validator::default();
        let evaluator = r5_evaluator_for(&r);
        let term = local_terminology_r5();
        let issues = validator.validate_resource(&r, Some(&term), &evaluator);

        assert_has_invariant(
            &issues,
            "Patient.managingOrganization",
            "contained resource",
        );
        assert_has_invariant(
            &issues,
            "Patient.contained[0]",
            "The organization SHALL at least have a name or an identifier, and possibly more than one",
        );
    }
    #[test]
    fn r5_patient_malformed_reference() {
        let r = load_resource(
            FhirVersion::R5,
            "invalid/patient/patient_malformed_local_reference.json",
        );
        let validator = fhir_validation::Validator::default();
        let evaluator = r5_evaluator_for(&r);
        let term = local_terminology_r5();
        let issues = validator.validate_resource(&r, Some(&term), &evaluator);
        assert_issue_count(&issues, 3);
        assert_has_invariant(
            &issues,
            "Patient.contained[0]",
            "The organization SHALL at least have a name or an identifier, and possibly more than one",
        );
        assert_has_invariant(
            &issues,
            "Patient",
            "If the resource is contained in another resource, it SHALL be referred to from elsewhere in the resource or SHALL refer to the containing resource",
        );
    }
}

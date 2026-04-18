#![cfg(feature = "R4")]
mod common {
    pub mod fixtures;
}
 mod tests {
     use fhir_validation::R4FhirPathEvaluator;
     use crate::common::fixtures::{assert_has_invariant_expression, load_resource};
     use crate::common::fixtures::{assert_has_invariant, assert_no_errors};
     use helios_fhir::{FhirResource, FhirVersion};
     pub fn r4_evaluator_for(resource: &FhirResource) -> R4FhirPathEvaluator {
         let FhirResource::R4(r) = resource else { panic!("expected R4 FhirResource") };
         R4FhirPathEvaluator::new((**r).clone())
     }
     #[test]
     fn dom3_local_reference_passes_when_contained_matches() {
         let r = load_resource(
             FhirVersion::R4,
             "valid/patient/patient-local-contained-reference-valid.json",
         );
         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let issues = validator.validate_resource(&r, None, &evaluator);

         assert_no_errors(&issues);
         // assert_no_errors_or_warnings(&issues);
     }

     #[ignore]
     #[test]
     fn dom3_local_reference_fails_when_missing_contained() {
         let r = load_resource(
             FhirVersion::R4,
             "invalid/patient/patient-bad-contained-reference.json",
         );
         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let issues = validator.validate_resource(&r, None, &evaluator);

         assert_has_invariant(
             &issues,
             "Patient.managingOrganization",
             "contained resource",
         );
     }
     #[test]
     fn dom3_no_id_in_contained() {
         let r = load_resource(
             FhirVersion::R4,
             "invalid/patient/patient_no_id_in_contained.json",
         );
         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let _issues = validator.validate_resource(&r, None, &evaluator);
     }
     #[test]
     fn dom2_contained_cannot_have_contained() {
         let r = load_resource(
             FhirVersion::R4,
             "invalid/patient/patient-nested-contained.json",
         );

         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let issues = validator.validate_resource(&r, None, &evaluator);

         assert_has_invariant_expression(&issues, "Patient", "contained.contained.empty()");
     }
     #[ignore]
     #[test]
     fn dom4_contained_cannot_have_meta_version_id() {
         let r = load_resource(
             FhirVersion::R4,
             "invalid/patient/patient_contained_no_meta_versionId.json",
         );

         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let issues = validator.validate_resource(&r, None, &evaluator);

         assert_has_invariant_expression(
             &issues,
             "Patient",
             "contained.meta.versionId.empty() and contained.meta.lastUpdated.empty()",
         );
     }
     #[test]
     fn dom4_contained_cannot_have_meta_last_updated() {
         let r = load_resource(
             FhirVersion::R4,
             "invalid/patient/patient_contained_no_meta_lastUpdated.json",
         );

         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let issues = validator.validate_resource(&r, None, &evaluator);

         assert_has_invariant_expression(
             &issues,
             "Patient",
             "contained.meta.versionId.empty() and contained.meta.lastUpdated.empty()",
         );
     }
     #[test]
     fn dom5_contained_cannot_have_meta_security() {
         let r = load_resource(
             FhirVersion::R4,
             "invalid/patient/patient_contained_no_meta_security.json",
         );

         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let issues = validator.validate_resource(&r, None, &evaluator);

         assert_has_invariant_expression(&issues, "Patient", "contained.meta.security.empty()");
     }
     #[test]
     fn dom6_patient_no_narrative() {
         let r = load_resource(FhirVersion::R4, "invalid/patient/patient_no_narrative.json");

         let validator = fhir_validation::Validator::default();
         let evaluator = r4_evaluator_for(&r);
         let issues = validator.validate_resource(&r, None, &evaluator);

         assert_has_invariant_expression(&issues, "Patient", "text.`div`.exists()");
     }
 }
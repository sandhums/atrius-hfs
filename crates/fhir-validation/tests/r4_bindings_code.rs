#![cfg(feature = "R4")]

mod common {
    pub mod fixtures;
}
#[cfg(test)]
mod tests {
    use crate::common::fixtures::{
        assert_has_binding_issue, assert_has_error, load_resource, r4_evaluator_for,
    };
    use fhir_validation::LocalTerminologyService;
    use fhir_validation::r4::binding::validate_primitive_code_binding;
    use fhir_validation::{ValidationConfig, Validator};
    use fhir_validation_types::{BindingStrength, Severity};
    use helios_fhir::{FhirVersion, TerminologyValidationError};

    fn validator() -> Validator {
        Validator::new(ValidationConfig::default())
    }

    #[test]
    fn absent_code_produces_no_issue() {
        let issues = validate_primitive_code_binding(
            &validator(),
            "Patient.gender",
            "http://hl7.org/fhir/ValueSet/administrative-gender",
            BindingStrength::Required,
            None,
            None,
            |_| Ok(()),
            None,
        );

        assert!(issues.is_empty());
    }

    #[test]
    fn local_success_produces_no_issue() {
        let issues = validate_primitive_code_binding(
            &validator(),
            "Patient.gender",
            "http://hl7.org/fhir/ValueSet/administrative-gender",
            BindingStrength::Required,
            Some("male"),
            None,
            |_| Ok(()),
            None,
        );

        assert!(issues.is_empty());
    }

    #[test]
    fn remote_false_produces_warning_for_extensible_binding() {
        let term = LocalTerminologyService::new(FhirVersion::R4);

        let issues = validate_primitive_code_binding(
            &validator(),
            "Patient.language",
            "http://hl7.org/fhir/ValueSet/languages",
            BindingStrength::Extensible,
            Some("xx"),
            Some("remote required"),
            |_| {
                Err(TerminologyValidationError::RemoteValidationRequired(
                    "remote required".to_string(),
                ))
            },
            Some(&term),
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert_eq!(issues[0].code, "value");
    }

    #[test]
    fn remote_required_without_service_produces_terminology_error() {
        let issues = validate_primitive_code_binding(
            &validator(),
            "Patient.language",
            "http://hl7.org/fhir/ValueSet/languages",
            BindingStrength::Required,
            Some("en"),
            Some("remote required"),
            |_| {
                Err(TerminologyValidationError::RemoteValidationRequired(
                    "remote required".to_string(),
                ))
            },
            None,
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert_eq!(issues[0].code, "terminology");
    }
    #[test]
    fn r4_patient_invalid_gender() {
        let resource = load_resource(
            FhirVersion::R4,
            "invalid/patient/patient-invalid-gender.json",
        );
        let validator = Validator::default();
        let evaluator = r4_evaluator_for(&resource);
        let issues = validator.validate_resource(&resource, None, &evaluator);
        // println!("{:#?}", issues);
        assert_has_binding_issue(
            &issues,
            "Patient.gender",
            "http://hl7.org/fhir/ValueSet/administrative-gender|4.0.1",
        );
        assert_has_error(&issues);
    }
}

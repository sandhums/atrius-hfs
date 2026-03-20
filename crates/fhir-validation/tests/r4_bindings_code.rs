use crate::common::fixtures::{
    assert_has_binding_issue, assert_has_error, load_resource, validate_resource,
};
use helios_fhir::FhirVersion;

mod common {
    pub mod fixtures;
}
#[cfg(test)]
mod tests {
    use fhir_validation::r4::binding::validate_primitive_code_binding;
    use fhir_validation::{TerminologyService, ValidationConfig, ValidationError, Validator};
    use fhir_validation_types::{BindingStrength, Severity};
    use helios_fhir::r4::terminology::TerminologyValidationError;

    struct MockTerminologyService {
        result: Result<bool, ValidationError>,
    }

    impl TerminologyService for MockTerminologyService {
        fn member_of(
            &self,
            _valueset_url: &str,
            _system: Option<&str>,
            _code: &str,
            _display: Option<&str>,
        ) -> Result<bool, ValidationError> {
            match &self.result {
                Ok(v) => Ok(*v),
                Err(ValidationError::Terminology(msg)) => {
                    Err(ValidationError::Terminology(msg.clone()))
                }
                Err(ValidationError::Other(msg)) => Err(ValidationError::Other(msg.clone())),
                Err(ValidationError::FhirPath(_)) => Err(ValidationError::Other(
                    "unexpected fhirpath error in mock".to_string(),
                )),
            }
        }
    }

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
            |_| Ok(()),
            None,
        );

        assert!(issues.is_empty());
    }

    #[test]
    fn local_not_in_valueset_produces_error_for_required_binding() {
        let issues = validate_primitive_code_binding(
            &validator(),
            "Patient.gender",
            "http://hl7.org/fhir/ValueSet/administrative-gender",
            BindingStrength::Required,
            Some("invalid"),
            |_| {
                Err(TerminologyValidationError::NotInValueSet(
                    "Code not in ValueSet".to_string(),
                ))
            },
            None,
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert_eq!(issues[0].code, "value");
    }

    #[test]
    fn remote_false_produces_warning_for_extensible_binding() {
        let term = MockTerminologyService { result: Ok(false) };

        let issues = validate_primitive_code_binding(
            &validator(),
            "Patient.language",
            "http://hl7.org/fhir/ValueSet/languages",
            BindingStrength::Extensible,
            Some("xx"),
            |_| {
                Err(TerminologyValidationError::RemoteValidationRequired(
                    "remote required".to_string(),
                ))
            },
            Some(&term),
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
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
}
#[test]
fn r4_patient_invalid_gender() {
    let resource = load_resource(
        FhirVersion::R4,
        "invalid/patient/patient-invalid-gender.json",
    );
    let issues = validate_resource(&resource, None);
    println!("{:#?}", issues);
    assert_has_binding_issue(&issues, "Patient.gender",  "http://hl7.org/fhir/ValueSet/administrative-gender|4.0.1");
    assert_has_error(&issues);
}

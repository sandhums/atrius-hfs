use fhir_validation_types::{BindingStrength, Severity};
#[cfg(feature = "R4")]
use helios_fhir::r4::terminology::TerminologyValidationError;
use crate::{TerminologyService, ValidationIssue, Validator};

/// Validate a primitive FHIR `code` binding.
///
/// This is used for fields such as:
/// - `Patient.gender`
/// - `Observation.status`
/// - `Encounter.status`
///
/// The caller provides the already-extracted primitive string value (`code_value`) and
/// a generated local ValueSet wrapper function via `local_check`.
///
/// Validation flow:
/// 1. If the value is absent, do nothing. Presence/cardinality is handled elsewhere.
/// 2. Try local generated ValueSet validation.
/// 3. If local validation says remote terminology validation is required, use `TerminologyService`.
/// 4. Convert any miss into a `ValidationIssue` based on binding strength.
pub fn validate_primitive_code_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    code_value: Option<&str>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: FnOnce(&str) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(code) = code_value else {
        // Missing primitive value is handled by structural/cardinality validation and/or invariants.
        return issues;
    };

    match local_check(code) {
        Ok(()) => issues,

        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            let Some(terminology) = terminology else {
                issues.push(ValidationIssue {
                    severity: Severity::Error,
                    code: "terminology",
                    fhir_path: fhir_path.to_string(),
                    expression: Some(valueset_url.to_string()),
                    diagnostics: "Remote terminology validation required but no TerminologyService was provided".to_string(),
                });
                return issues;
            };

            match terminology.member_of(valueset_url, None, code, None) {
                Ok(true) => issues,
                Ok(false) => {
                    if let Some(severity) = validator.binding_miss_severity(strength) {
                        issues.push(ValidationIssue {
                            severity,
                            code: "value",
                            fhir_path: fhir_path.to_string(),
                            expression: Some(valueset_url.to_string()),
                            diagnostics: format!("Code '{}' is not in ValueSet {}", code, valueset_url),
                        });
                    }
                    issues
                }
                Err(e) => {
                    issues.push(ValidationIssue {
                        severity: Severity::Error,
                        code: "terminology",
                        fhir_path: fhir_path.to_string(),
                        expression: Some(valueset_url.to_string()),
                        diagnostics: format!("Remote terminology validation failed: {}", e),
                    });
                    issues
                }
            }
        }

        Err(TerminologyValidationError::NotInValueSet(_)) => {
            if let Some(severity) = validator.binding_miss_severity(strength) {
                issues.push(ValidationIssue {
                    severity,
                    code: "value",
                    fhir_path: fhir_path.to_string(),
                    expression: Some(valueset_url.to_string()),
                    diagnostics: format!("Code '{}' is not in ValueSet {}", code, valueset_url),
                });
            }
            issues
        }

        Err(TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(ValidationIssue {
                severity: Severity::Error,
                code: "value",
                fhir_path: fhir_path.to_string(),
                expression: Some(valueset_url.to_string()),
                diagnostics: format!("Local ValueSet validation failed: {}", msg),
            });
            issues
        }
    }
}

#[cfg(test)]
mod tests {
    use fhir_validation_types::Severity;
    use super::*;
    use crate::{ValidationConfig, ValidationError};

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
                Err(ValidationError::Terminology(msg)) => Err(ValidationError::Terminology(msg.clone())),
                Err(ValidationError::Other(msg)) => Err(ValidationError::Other(msg.clone())),
                Err(ValidationError::FhirPath(_)) => Err(ValidationError::Other("unexpected fhirpath error in mock".to_string())),
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
            |_| Err(TerminologyValidationError::NotInValueSet("Code not in ValueSet".to_string())),
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
            |_| Err(TerminologyValidationError::RemoteValidationRequired("remote required".to_string())),
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
            |_| Err(TerminologyValidationError::RemoteValidationRequired("remote required".to_string())),
            None,
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert_eq!(issues[0].code, "terminology");
    }
}

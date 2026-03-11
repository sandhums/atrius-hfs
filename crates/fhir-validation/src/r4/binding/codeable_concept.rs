use fhir_validation_types::{BindingStrength, Severity};
use crate::{TerminologyService, ValidationIssue, Validator};
#[cfg(feature = "R4")]
use helios_fhir::r4::{CodeableConcept};
#[cfg(feature = "R4")]
use helios_fhir::r4::terminology::TerminologyValidationError;
use crate::r4::binding::common::{coding_code, coding_display, coding_system, issue_for_binding_miss};

/// Validate a `CodeableConcept` binding.
///
/// Semantics:
/// - If there is no `coding`, do nothing here. Cardinality / profile rules handle presence.
/// - Try local generated ValueSet validation first for the whole concept.
/// - If local validation succeeds, stop.
/// - If local validation says remote validation is required, try each usable coding remotely.
///   If any coding validates true, the concept is accepted.
/// - If local validation definitively says the concept is not in the ValueSet, surface an issue
///   based on binding strength.
///
/// The caller supplies the generated local wrapper function, typically something like:
/// `|cc| MaritalStatusCodes::validate_codeable_concept(cc)`
pub fn validate_codeable_concept_binding<F>(
    validator: &Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    codeable_concept: Option<&CodeableConcept>,
    local_check: F,
    terminology: Option<&dyn TerminologyService>,
) -> Vec<ValidationIssue>
where
    F: FnOnce(&CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let mut issues = Vec::new();

    let Some(cc) = codeable_concept else {
        return issues;
    };

    let codings = match cc.coding.as_ref() {
        Some(codings) if !codings.is_empty() => codings,
        _ => return issues,
    };

    match local_check(cc) {
        Ok(()) => return issues,

        Err(TerminologyValidationError::NotInValueSet(_)) => {
            if let Some(issue) = issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!("CodeableConcept is not in ValueSet {}", valueset_url),
            ) {
                issues.push(issue);
            }
            return issues;
        }

        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            let Some(terminology) = terminology else {
                issues.push(ValidationIssue {
                    severity: Severity::Error,
                    code: "terminology",
                    fhir_path: fhir_path.to_string(),
                    expression: Some(valueset_url.to_string()),
                    diagnostics:
                        "Remote terminology validation required but no TerminologyService was provided"
                            .to_string(),
                });
                return issues;
            };

            let mut any_usable_coding = false;
            let mut any_match = false;

            for coding in codings {
                let system = coding_system(coding);
                let code = coding_code(coding);
                let display = coding_display(coding);

                let Some(code) = code else {
                    continue;
                };

                any_usable_coding = true;

                match terminology.member_of(valueset_url, system, code, display) {
                    Ok(true) => {
                        any_match = true;
                        break;
                    }
                    Ok(false) => {
                        // Keep checking other codings.
                    }
                    Err(e) => {
                        issues.push(ValidationIssue {
                            severity: Severity::Error,
                            code: "terminology",
                            fhir_path: fhir_path.to_string(),
                            expression: Some(valueset_url.to_string()),
                            diagnostics: format!("Remote terminology validation failed: {}", e),
                        });
                        return issues;
                    }
                }
            }

            if any_match {
                return issues;
            }

            if !any_usable_coding {
                issues.push(ValidationIssue {
                    severity: Severity::Error,
                    code: "value",
                    fhir_path: fhir_path.to_string(),
                    expression: Some(valueset_url.to_string()),
                    diagnostics:
                        "CodeableConcept has no usable coding with a code value for terminology validation"
                            .to_string(),
                });
                return issues;
            }

            if let Some(issue) = issue_for_binding_miss(
                validator,
                fhir_path,
                valueset_url,
                strength,
                format!("CodeableConcept is not in ValueSet {}", valueset_url),
            ) {
                issues.push(issue);
            }

            return issues;
        }

        Err(TerminologyValidationError::InvalidInput(msg)) => {
            issues.push(ValidationIssue {
                severity: Severity::Error,
                code: "value",
                fhir_path: fhir_path.to_string(),
                expression: Some(valueset_url.to_string()),
                diagnostics: format!("Local ValueSet validation failed: {}", msg),
            });
            return issues;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TerminologyService, ValidationConfig, ValidationError};
    use helios_fhir::r4::{Code, Coding, Uri};
    use helios_fhir::Element;

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
                Err(ValidationError::FhirPath(_)) => {
                    Err(ValidationError::Other("unexpected fhirpath error in mock".to_string()))
                }
            }
        }
    }

    fn validator() -> Validator {
        Validator::new(ValidationConfig::default())
    }

    fn code(value: &str) -> Code {
        Element {
            id: None,
            extension: None,
            value: Some(value.to_string()),
        }
    }

    fn uri(value: &str) -> Uri {
        Element {
            id: None,
            extension: None,
            value: Some(value.to_string()),
        }
    }

    fn coding(system: &str, code_value: &str, display: Option<&str>) -> Coding {
        Coding {
            id: None,
            extension: None,
            system: Some(uri(system)),
            version: None,
            code: Some(code(code_value)),
            display: display.map(|d| Element {
                id: None,
                extension: None,
                value: Some(d.to_string()),
            }),
            user_selected: None,
        }
    }

    fn cc_with_one_coding(system: &str, code_value: &str) -> CodeableConcept {
        CodeableConcept {
            id: None,
            extension: None,
            coding: Some(vec![coding(system, code_value, None)]),
            text: None,
        }
    }

    #[test]
    fn absent_codeable_concept_produces_no_issue() {
        let issues = validate_codeable_concept_binding(
            &validator(),
            "Patient.maritalStatus",
            "http://hl7.org/fhir/ValueSet/marital-status",
            BindingStrength::Extensible,
            None,
            |_| Ok(()),
            None,
        );

        assert!(issues.is_empty());
    }

    #[test]
    fn local_success_produces_no_issue() {
        let cc = cc_with_one_coding("http://terminology.hl7.org/CodeSystem/v3-NullFlavor", "UNK");

        let issues = validate_codeable_concept_binding(
            &validator(),
            "Patient.maritalStatus",
            "http://hl7.org/fhir/ValueSet/marital-status",
            BindingStrength::Extensible,
            Some(&cc),
            |_| Ok(()),
            None,
        );

        assert!(issues.is_empty());
    }

    #[test]
    fn local_not_in_valueset_produces_warning_for_extensible_binding() {
        let cc = cc_with_one_coding("http://example.org/system", "X");

        let issues = validate_codeable_concept_binding(
            &validator(),
            "Patient.maritalStatus",
            "http://hl7.org/fhir/ValueSet/marital-status",
            BindingStrength::Extensible,
            Some(&cc),
            |_| Err(TerminologyValidationError::NotInValueSet("not in valueset".to_string())),
            None,
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert_eq!(issues[0].code, "value");
    }

    #[test]
    fn remote_true_accepts_if_any_coding_matches() {
        let cc = CodeableConcept {
            id: None,
            extension: None,
            coding: Some(vec![
                coding("http://example.org/system", "A", None),
                coding("http://example.org/system", "B", None),
            ]),
            text: None,
        };

        let term = MockTerminologyService { result: Ok(true) };

        let issues = validate_codeable_concept_binding(
            &validator(),
            "Patient.maritalStatus",
            "http://hl7.org/fhir/ValueSet/marital-status",
            BindingStrength::Extensible,
            Some(&cc),
            |_| Err(TerminologyValidationError::RemoteValidationRequired("remote required".to_string())),
            Some(&term),
        );

        assert!(issues.is_empty());
    }

    #[test]
    fn remote_false_produces_warning_for_extensible_binding() {
        let cc = cc_with_one_coding("http://example.org/system", "X");
        let term = MockTerminologyService { result: Ok(false) };

        let issues = validate_codeable_concept_binding(
            &validator(),
            "Patient.maritalStatus",
            "http://hl7.org/fhir/ValueSet/marital-status",
            BindingStrength::Extensible,
            Some(&cc),
            |_| Err(TerminologyValidationError::RemoteValidationRequired("remote required".to_string())),
            Some(&term),
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert_eq!(issues[0].code, "value");
    }

    #[test]
    fn remote_required_without_service_produces_terminology_error() {
        let cc = cc_with_one_coding("http://example.org/system", "X");

        let issues = validate_codeable_concept_binding(
            &validator(),
            "Patient.maritalStatus",
            "http://hl7.org/fhir/ValueSet/marital-status",
            BindingStrength::Required,
            Some(&cc),
            |_| Err(TerminologyValidationError::RemoteValidationRequired("remote required".to_string())),
            None,
        );

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert_eq!(issues[0].code, "terminology");
    }
}

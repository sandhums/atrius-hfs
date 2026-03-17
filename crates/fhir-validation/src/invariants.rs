use crate::{ValidationError, ValidationIssue};
pub use fhir_validation_types::{InvariantDef, Severity};

pub trait FhirPathEvaluator {
    fn eval_invariant(
        &self,
        declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError>;

    fn eval_invariant_on(
        &self,
        focus: helios_fhirpath_support::EvaluationResult,
        declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError>;

    fn eval_path(
        &self,
        path: &str,
    ) -> Result<Vec<helios_fhirpath_support::EvaluationResult>, ValidationError>;
}

pub fn validate_invariants<E>(
    evaluator: &E,
    focus: helios_fhirpath_support::EvaluationResult,
    invariants: &[InvariantDef],
) -> Vec<ValidationIssue>
where
    E: FhirPathEvaluator + ?Sized,
{
    let mut issues = Vec::new();

    for inv in invariants {
        match evaluator.eval_invariant_on(focus.clone(), inv.path, inv.expression) {
            Ok(true) => {}
            Ok(false) => {
                issues.push(ValidationIssue {
                    severity: inv.severity,
                    code: "invariant",
                    fhir_path: inv.path.to_string(),
                    instance_path: None,
                    expression: Some(inv.expression.to_string()),
                    diagnostics: format!("{} ({})", inv.human, inv.key),
                });
            }
            Err(err) => {
                issues.push(ValidationIssue {
                    severity: Severity::Error,
                    code: "invariant-eval",
                    fhir_path: inv.path.to_string(),
                    instance_path: None,
                    expression: Some(inv.expression.to_string()),
                    diagnostics: format!(
                        "FHIRPath evaluation failed for invariant {}: {}",
                        inv.key, err
                    ),
                });
            }
        }
    }

    issues
}

use crate::{ValidationError, ValidationIssue};
pub use fhir_validation_types::{InvariantDef, Severity};

pub trait InvariantEvaluator {
    fn eval_invariant(
        &self,
        declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError>;
}

pub fn validate_invariants<E>(
    evaluator: &E,
    invariants: &[InvariantDef],
) -> Vec<ValidationIssue>
where
    E: InvariantEvaluator + ?Sized,
{
    let mut issues = Vec::new();

    for inv in invariants {
        match evaluator.eval_invariant(inv.path, inv.expression) {
            Ok(true) => {}
            Ok(false) => {
                issues.push(ValidationIssue {
                    severity: inv.severity,
                    code: "invariant",
                    fhir_path: inv.path.to_string(),
                    expression: Some(inv.expression.to_string()),
                    diagnostics: format!("{} ({})", inv.human, inv.key),
                });
            }
            Err(err) => {
                issues.push(ValidationIssue {
                    severity: Severity::Error,
                    code: "invariant-eval",
                    fhir_path: inv.path.to_string(),
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
use crate::{InvariantEvaluator, ValidationError};

pub struct R4FhirPathInvariantEvaluator {
    context: helios_fhirpath::EvaluationContext,
}

impl R4FhirPathInvariantEvaluator {
    pub fn new(resource: helios_fhir::r4::Resource) -> Self {
        let fhir_resource = helios_fhir::FhirResource::R4(Box::new(resource));
        let context = helios_fhirpath::EvaluationContext::new(vec![fhir_resource]);
        Self { context }
    }
}
impl InvariantEvaluator for R4FhirPathInvariantEvaluator {
    fn eval_invariant(
        &self,
        declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError> {
        let _ = declared_path;

        let result = helios_fhirpath::evaluate_expression(expression, &self.context)
            .map_err(|e| ValidationError::FhirPath(helios_fhirpath_support::EvaluationError::SemanticError(format!("{e}"))))?;

        coerce_result_to_bool(result)
    }
}    fn coerce_result_to_bool(
    result: helios_fhirpath_support::EvaluationResult,
) -> Result<bool, ValidationError> {
    use helios_fhirpath_support::EvaluationResult;

    match result {
        EvaluationResult::Boolean(b, _, _) => Ok(b),

        EvaluationResult::Collection { items, .. } => {
            match items.len() {
                0 => Ok(false),

                1 => {
                    match &items[0] {
                        EvaluationResult::Boolean(b, _, _) => Ok(*b),

                        EvaluationResult::Empty => Ok(false),

                        other => Err(ValidationError::FhirPath(
                            helios_fhirpath_support::EvaluationError::SemanticError(
                                format!(
                                    "Invariant did not evaluate to boolean, got {}",
                                    other.type_name()
                                ),
                            ),
                        )),
                    }
                }

                _ => Err(ValidationError::FhirPath(
                    helios_fhirpath_support::EvaluationError::SemanticError(
                        "Invariant returned multiple values".to_string(),
                    ),
                )),
            }
        }

        EvaluationResult::Empty => Ok(false),

        other => Err(ValidationError::FhirPath(
            helios_fhirpath_support::EvaluationError::SemanticError(
                format!(
                    "Invariant did not evaluate to boolean, got {}",
                    other.type_name()
                ),
            ),
        )),
    }
}

struct AlwaysPassEvaluator;
impl InvariantEvaluator for AlwaysPassEvaluator {
    fn eval_invariant(
        &self,
        _declared_path: &str,
        _expression: &str,
    ) -> Result<bool, ValidationError> {
        Ok(true)
    }
}
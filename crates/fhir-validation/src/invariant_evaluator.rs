use crate::{FhirPathEvaluator, ValidationError};
use helios_fhir::FhirResource;
use helios_fhir::r4::Resource;
use helios_fhir::r5::Resource as R5Resource;
use helios_fhirpath::{EvaluationContext, evaluate_expression};
use helios_fhirpath_support::{EvaluationResult, IntoEvaluationResult};

struct GenericFhirPathEvaluator {
    context: EvaluationContext,
}

impl GenericFhirPathEvaluator {
    fn from_fhir_resource(resource: FhirResource) -> Self {
        let mut context = EvaluationContext::new(vec![resource]);

        if let Some(root) = context.this.clone() {
            context.set_variable_result("%rootResource", root.clone());
            context.set_variable_result("%resource", root);
        }

        Self { context }
    }

    fn eval_expression(&self, expr: &str) -> Result<Vec<EvaluationResult>, ValidationError> {
        let result = evaluate_expression(expr, &self.context).map_err(|e| {
            ValidationError::FhirPath(helios_fhirpath_support::EvaluationError::SemanticError(
                format!("failed to evaluate expression '{expr}': {e}"),
            ))
        })?;

        Ok(collect_focus_items(result))
    }

    fn eval_invariant(
        &self,
        declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError> {
        let normalized_focus_path = normalize_declared_path(declared_path);

        if normalized_focus_path.is_empty() {
            let result = evaluate_expression(expression, &self.context).map_err(|e| {
                ValidationError::FhirPath(helios_fhirpath_support::EvaluationError::SemanticError(
                    format!("{e}"),
                ))
            })?;

            return coerce_result_to_bool(result);
        }

        let focus_result =
            evaluate_expression(&normalized_focus_path, &self.context).map_err(|e| {
                ValidationError::FhirPath(helios_fhirpath_support::EvaluationError::SemanticError(
                    format!("failed to resolve declared path '{declared_path}': {e}"),
                ))
            })?;

        let focus_items = collect_focus_items(focus_result);
        if focus_items.is_empty() {
            return Ok(true);
        }

        for focus_item in focus_items {
            let mut focus_context = self.context.clone();

            if let Some(root) = self.context.this.clone() {
                focus_context.set_variable_result("%rootResource", root.clone());
                focus_context.set_variable_result("%resource", root);
            }

            focus_context.set_this(focus_item);

            let result = evaluate_expression(expression, &focus_context).map_err(|e| {
                ValidationError::FhirPath(helios_fhirpath_support::EvaluationError::SemanticError(
                    format!(
                        "failed to evaluate invariant '{expression}' at '{declared_path}': {e}"
                    ),
                ))
            })?;

            if !coerce_result_to_bool(result)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn eval_invariant_on(
        &self,
        focus: EvaluationResult,
        expression: &str,
    ) -> Result<bool, ValidationError> {
        let mut focus_context = self.context.clone();

        if let Some(root) = self.context.this.clone() {
            focus_context.set_variable_result("%rootResource", root.clone());
            focus_context.set_variable_result("%resource", root);
        }

        focus_context.set_this(focus);

        let result = evaluate_expression(expression, &focus_context).map_err(|e| {
            ValidationError::FhirPath(helios_fhirpath_support::EvaluationError::SemanticError(
                format!("failed to evaluate invariant '{expression}' on focused value: {e}"),
            ))
        })?;

        coerce_result_to_bool(result)
    }

    fn eval_path(&self, path: &str) -> Result<Vec<EvaluationResult>, ValidationError> {
        let normalized_path = normalize_declared_path(path);

        if normalized_path.is_empty() {
            let root = self
                .context
                .this
                .clone()
                .or_else(|| {
                    self.context
                        .resources
                        .first()
                        .map(convert_resource_to_result)
                })
                .ok_or_else(|| {
                    ValidationError::FhirPath(
                        helios_fhirpath_support::EvaluationError::SemanticError(
                            "evaluation context has no root resource".to_string(),
                        ),
                    )
                })?;

            return Ok(vec![root]);
        }

        let result = evaluate_expression(&normalized_path, &self.context).map_err(|e| {
            ValidationError::FhirPath(helios_fhirpath_support::EvaluationError::SemanticError(
                format!("failed to evaluate path '{path}': {e}"),
            ))
        })?;

        Ok(collect_focus_items(result))
    }
        fn from_fhir_resource_with_focus(
            resource: FhirResource,
            focus: EvaluationResult,
        ) -> Self {
            let mut context = EvaluationContext::new(vec![resource]);

            if let Some(root) = context.this.clone() {
                context.set_variable_result("%rootResource", root.clone());
                context.set_variable_result("%resource", root);
            }

            context.set_this(focus);
            Self { context }
        }

}

pub struct R4FhirPathEvaluator {
    inner: GenericFhirPathEvaluator,
}

impl R4FhirPathEvaluator {
    pub fn new(resource: Resource) -> Self {
        Self {
            inner: GenericFhirPathEvaluator::from_fhir_resource(FhirResource::R4(Box::new(
                resource,
            ))),
        }
    }

    pub fn eval_expression(&self, expr: &str) -> Result<Vec<EvaluationResult>, ValidationError> {
        self.inner.eval_expression(expr)
    }
    pub fn new_with_focus(
        resource: Resource,
        focus: EvaluationResult,
    ) -> Self {
        Self {
            inner: GenericFhirPathEvaluator::from_fhir_resource_with_focus(
                FhirResource::R4(Box::new(resource)),
                focus,
            ),
        }
    }
}

impl FhirPathEvaluator for R4FhirPathEvaluator {
    fn eval_invariant(
        &self,
        declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError> {
        self.inner.eval_invariant(declared_path, expression)
    }

    fn eval_invariant_on(
        &self,
        focus: EvaluationResult,
        _declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError> {
        self.inner.eval_invariant_on(focus, expression)
    }

    fn eval_path(&self, path: &str) -> Result<Vec<EvaluationResult>, ValidationError> {
        self.inner.eval_path(path)
    }
}
pub struct R5FhirPathEvaluator {
    inner: GenericFhirPathEvaluator,
}

impl R5FhirPathEvaluator {
    pub fn new(resource: R5Resource) -> Self {
        Self {
            inner: GenericFhirPathEvaluator::from_fhir_resource(FhirResource::R5(Box::new(
                resource,
            ))),
        }
    }

    pub fn eval_expression(&self, expr: &str) -> Result<Vec<EvaluationResult>, ValidationError> {
        self.inner.eval_expression(expr)
    }
    pub fn new_with_focus(
        resource: R5Resource,
        focus: EvaluationResult,
    ) -> Self {
        Self {
            inner: GenericFhirPathEvaluator::from_fhir_resource_with_focus(
                FhirResource::R5(Box::new(resource)),
                focus,
            ),
        }
    }
}

impl FhirPathEvaluator for R5FhirPathEvaluator {
    fn eval_invariant(
        &self,
        declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError> {
        self.inner.eval_invariant(declared_path, expression)
    }

    fn eval_invariant_on(
        &self,
        focus: EvaluationResult,
        _declared_path: &str,
        expression: &str,
    ) -> Result<bool, ValidationError> {
        self.inner.eval_invariant_on(focus, expression)
    }

    fn eval_path(&self, path: &str) -> Result<Vec<EvaluationResult>, ValidationError> {
        self.inner.eval_path(path)
    }
    
}

fn normalize_declared_path(declared_path: &str) -> String {
    let path = declared_path.trim();
    if path.is_empty() {
        return String::new();
    }

    match path.split_once('.') {
        Some((_, rest)) => rest.to_string(),
        None => String::new(),
    }
}

fn collect_focus_items(result: EvaluationResult) -> Vec<EvaluationResult> {
    match result {
        EvaluationResult::Collection { items, .. } => items,
        EvaluationResult::Empty => Vec::new(),
        other => vec![other],
    }
}

fn coerce_result_to_bool(result: EvaluationResult) -> Result<bool, ValidationError> {
    match result {
        EvaluationResult::Boolean(b, _, _) => Ok(b),

        EvaluationResult::Collection { items, .. } => match items.len() {
            0 => Ok(true),
            1 => match &items[0] {
                EvaluationResult::Boolean(b, _, _) => Ok(*b),
                EvaluationResult::Empty => Ok(true),
                other => Err(ValidationError::FhirPath(
                    helios_fhirpath_support::EvaluationError::SemanticError(format!(
                        "Invariant did not evaluate to boolean, got {}",
                        other.type_name()
                    )),
                )),
            },
            _ => Err(ValidationError::FhirPath(
                helios_fhirpath_support::EvaluationError::SemanticError(
                    "Invariant returned multiple values".to_string(),
                ),
            )),
        },

        EvaluationResult::Empty => Ok(true),

        other => Err(ValidationError::FhirPath(
            helios_fhirpath_support::EvaluationError::SemanticError(format!(
                "Invariant did not evaluate to boolean, got {}",
                other.type_name()
            )),
        )),
    }
}
// TODO - Duplicate from fhirpath
fn convert_resource_to_result(resource: &FhirResource) -> EvaluationResult {
    // Now that FhirResource implements IntoEvaluationResult, just call the method.
    resource.to_evaluation_result()
}

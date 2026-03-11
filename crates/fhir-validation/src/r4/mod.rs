pub mod binding;
pub mod generated;

#[cfg(feature = "R4")]
pub trait R4Validatable {
    fn validate_bindings(
        &self,
        validator: &crate::Validator,
        terminology: Option<&dyn crate::TerminologyService>,
    ) -> Vec<crate::ValidationIssue>;

    fn validate_invariants(
        &self,
        validator: &crate::Validator,
        evaluator: &dyn crate::InvariantEvaluator,
    ) -> Vec<crate::ValidationIssue>;
}
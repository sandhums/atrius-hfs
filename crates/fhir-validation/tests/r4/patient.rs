use crate::common::fixtures::*;
use crate::invariants::AlwaysPassEvaluator;
use crate::Validator;


#[test]
fn patient_example_validates_without_issues_with_stub_invariant_evaluator() {
    let patient = load_r4_patient("patient-example.json");
    let validator = Validator::default();
    let evaluator = AlwaysPassEvaluator;

    let issues = validator.validate_r4(&patient, None, &evaluator);

    assert!(
        issues.is_empty(),
        "expected patient-example.json to validate cleanly, got issues: {issues:#?}"
    );
}

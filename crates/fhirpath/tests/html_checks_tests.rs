//! `htmlChecks()` — Narrative XHTML subset used by txt-1 / txt-2.

use helios_fhir::FhirResource;
use helios_fhirpath::{EvaluationContext, evaluate_expression};
use helios_fhirpath_support::EvaluationResult;

fn eval(expr: &str, ctx: &EvaluationContext) -> EvaluationResult {
    evaluate_expression(expr, ctx).unwrap_or_else(|e| panic!("{expr}: {e}"))
}

#[test]
fn html_checks_on_string_literal() {
    let ctx = EvaluationContext::new_empty_with_default_version();
    assert_eq!(
        eval(
            "'<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>Hi</p></div>'.htmlChecks()",
            &ctx
        ),
        EvaluationResult::boolean(true)
    );
    assert_eq!(
        eval(
            "'<div xmlns=\"http://www.w3.org/1999/xhtml\"></div>'.htmlChecks()",
            &ctx
        ),
        EvaluationResult::boolean(false)
    );
    assert_eq!(eval("1.htmlChecks()", &ctx), EvaluationResult::Empty);
}

#[test]
fn html_checks_on_patient_narrative() {
    let patient_json = serde_json::json!({
        "resourceType": "Patient",
        "id": "p1",
        "text": {
            "status": "generated",
            "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>Jane Doe</p></div>"
        }
    });
    let patient: helios_fhir::r4::Patient = serde_json::from_value(patient_json).unwrap();
    let ctx = EvaluationContext::new(vec![FhirResource::R4(Box::new(
        helios_fhir::r4::Resource::Patient(Box::new(patient)),
    ))]);

    assert_eq!(
        eval("Patient.text.div.htmlChecks()", &ctx),
        EvaluationResult::boolean(true)
    );
}

#[test]
fn html_checks_rejects_script_in_patient_narrative() {
    let patient_json = serde_json::json!({
        "resourceType": "Patient",
        "text": {
            "status": "generated",
            "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\"><script>x</script>Hi</div>"
        }
    });
    let patient: helios_fhir::r4::Patient = serde_json::from_value(patient_json).unwrap();
    let ctx = EvaluationContext::new(vec![FhirResource::R4(Box::new(
        helios_fhir::r4::Resource::Patient(Box::new(patient)),
    ))]);

    assert_eq!(
        eval("Patient.text.div.htmlChecks()", &ctx),
        EvaluationResult::boolean(false)
    );
}

//! Evaluate `SubscriptionTopic.resourceTrigger.fhirPathCriteria`.
//!
//! Uses `helios-fhirpath` with the focus resource as context (`%resource` / `$this`)
//! and optional `%previous` / `%current` variables (Subscription Backport / R5 style).
//!
//! # Gaps vs Helios FHIRPath (flagged for topic authors)
//!
//! | Area | Status |
//! |------|--------|
//! | `%previous` / `%current` | **Caller-supplied** — not built-in; this module sets them |
//! | `.empty()`, `=`, `!=` on primitives/codes | Supported (admit/discharge criteria) |
//! | `.where(...).exists()`, `or` in where | Supported (lab critical criteria) |
//! | Collection `!=` / `<` / `>` | Helios errors on multi-item collection compare; singleton collections often unwrap — prefer `.first()` or `exclude().exists()` for multi-location |
//! | `queryCriteria` | Not implemented (separate SubscriptionTopic field) |
//! | `eventTrigger` | Not implemented |
//! | CLI `-v` JSON objects | CLI stores objects as **strings** — do not use CLI vars to validate `%previous`; use typed context (this module / unit tests) |
//! | Deserialize failures | Fail-closed (no match) when JSON cannot parse as the event's FHIR version |

use helios_fhir::{FhirResource, FhirVersion};
use helios_fhirpath::evaluator::convert_resource_to_result;
use helios_fhirpath::{EvaluationContext, EvaluationResult, evaluate_expression};
use serde_json::Value;
use tracing::warn;

/// Returns whether `criteria` is true for `current` with optional `previous`.
///
/// Empty FHIRPath results and non-boolean results are treated as **false**
/// (fail-closed for subscription triggers). Evaluation / parse errors also
/// yield `false` and are logged.
pub fn criteria_matches(
    criteria: &str,
    current: &Value,
    previous: Option<&Value>,
    fhir_version: FhirVersion,
) -> bool {
    match evaluate_criteria(criteria, current, previous, fhir_version) {
        Ok(true) => true,
        Ok(false) => false,
        Err(e) => {
            warn!(
                error = %e,
                criteria,
                "subscription fhirPathCriteria evaluation failed; treating as non-match"
            );
            false
        }
    }
}

fn evaluate_criteria(
    criteria: &str,
    current: &Value,
    previous: Option<&Value>,
    fhir_version: FhirVersion,
) -> Result<bool, String> {
    let current_fr = json_to_fhir_resource(current, fhir_version)?;
    let current_result = convert_resource_to_result(&current_fr);
    let mut ctx = EvaluationContext::new_with_version(vec![current_fr], fhir_version);

    match previous {
        Some(prev) => {
            let prev_fr = json_to_fhir_resource(prev, fhir_version)?;
            ctx.set_variable_result("%previous", convert_resource_to_result(&prev_fr));
        }
        None => {
            ctx.set_variable_result("%previous", EvaluationResult::Empty);
        }
    }
    ctx.set_variable_result("%current", current_result);

    let result = evaluate_expression(criteria, &ctx)?;
    Ok(is_trigger_true(&result))
}

fn is_trigger_true(result: &EvaluationResult) -> bool {
    match result {
        EvaluationResult::Boolean(b, _, _) => *b,
        EvaluationResult::Empty => false,
        EvaluationResult::Collection { items, .. } if items.len() == 1 => {
            is_trigger_true(&items[0])
        }
        _ => false,
    }
}

fn json_to_fhir_resource(
    json: &Value,
    version: FhirVersion,
) -> Result<FhirResource, String> {
    match version {
        #[cfg(feature = "R4")]
        FhirVersion::R4 => {
            let resource: helios_fhir::r4::Resource =
                serde_json::from_value(json.clone()).map_err(|e| format!("R4 parse: {e}"))?;
            Ok(FhirResource::R4(Box::new(resource)))
        }
        #[cfg(feature = "R4B")]
        FhirVersion::R4B => {
            let resource: helios_fhir::r4b::Resource =
                serde_json::from_value(json.clone()).map_err(|e| format!("R4B parse: {e}"))?;
            Ok(FhirResource::R4B(Box::new(resource)))
        }
        #[cfg(feature = "R5")]
        FhirVersion::R5 => {
            let resource: helios_fhir::r5::Resource =
                serde_json::from_value(json.clone()).map_err(|e| format!("R5 parse: {e}"))?;
            Ok(FhirResource::R5(Box::new(resource)))
        }
        #[cfg(feature = "R6")]
        FhirVersion::R6 => {
            let resource: helios_fhir::r6::Resource =
                serde_json::from_value(json.clone()).map_err(|e| format!("R6 parse: {e}"))?;
            Ok(FhirResource::R6(Box::new(resource)))
        }
        #[allow(unreachable_patterns)]
        other => Err(format!(
            "FHIR version {other:?} not enabled in helios-subscriptions build"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(feature = "R4")]
    fn r4() -> FhirVersion {
        FhirVersion::R4
    }

    #[cfg(feature = "R4")]
    #[test]
    fn admit_on_create() {
        let cur = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" }
        });
        let expr = "(%previous.empty() or %previous.status != 'in-progress') and status = 'in-progress'";
        assert!(criteria_matches(expr, &cur, None, r4()));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn admit_transition_from_planned() {
        let prev = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "planned",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" }
        });
        let cur = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" }
        });
        let expr = "(%previous.empty() or %previous.status != 'in-progress') and status = 'in-progress'";
        assert!(criteria_matches(expr, &cur, Some(&prev), r4()));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn admit_noop_when_already_in_progress() {
        let prev = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" }
        });
        let cur = prev.clone();
        let expr = "(%previous.empty() or %previous.status != 'in-progress') and status = 'in-progress'";
        assert!(!criteria_matches(expr, &cur, Some(&prev), r4()));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn discharge_transition() {
        let prev = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" }
        });
        let cur = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "finished",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" }
        });
        let expr = "%previous.status != 'finished' and status = 'finished'";
        assert!(criteria_matches(expr, &cur, Some(&prev), r4()));
        assert!(!criteria_matches(expr, &cur, None, r4())); // no previous → empty != fails closed
    }

    #[cfg(feature = "R4")]
    #[test]
    fn transfer_location_change() {
        let prev = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "location": [{ "location": { "reference": "Location/ward-a" } }]
        });
        let cur = json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "location": [{ "location": { "reference": "Location/ot-1" } }]
        });
        let expr = "%previous.location.exists() and location.exists() and %previous.location != location";
        assert!(criteria_matches(expr, &cur, Some(&prev), r4()));
        assert!(!criteria_matches(expr, &cur, Some(&cur), r4()));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn lab_critical() {
        let cur = json!({
            "resourceType": "Observation",
            "id": "o1",
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "laboratory"
                }]
            }],
            "code": { "coding": [{ "system": "http://loinc.org", "code": "2823-3" }] },
            "interpretation": [{ "coding": [{ "code": "HH" }] }]
        });
        let expr = "category.coding.where(system = 'http://terminology.hl7.org/CodeSystem/observation-category' and code = 'laboratory').exists() and status = 'final' and interpretation.coding.where(code = 'H' or code = 'HH' or code = 'L' or code = 'LL' or code = 'AA').exists()";
        assert!(criteria_matches(expr, &cur, None, r4()));
        let non_crit = json!({
            "resourceType": "Observation",
            "id": "o2",
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "laboratory"
                }]
            }],
            "code": { "coding": [{ "system": "http://loinc.org", "code": "2823-3" }] }
        });
        assert!(!criteria_matches(expr, &non_crit, None, r4()));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn bad_expression_fail_closed() {
        let cur = json!({
            "resourceType": "Patient",
            "id": "p1"
        });
        assert!(!criteria_matches("this is not valid $$$", &cur, None, r4()));
    }

    #[cfg(feature = "R4")]
    const ADMIT: &str =
        "(%previous.empty() or %previous.status != 'in-progress') and status = 'in-progress'";
    #[cfg(feature = "R4")]
    const DISCHARGE: &str = "%previous.status != 'finished' and status = 'finished'";
    #[cfg(feature = "R4")]
    const TRANSFER: &str =
        "%previous.location.exists() and location.exists() and %previous.location != location";
    #[cfg(feature = "R4")]
    const LAB_CRITICAL: &str = "category.coding.where(system = 'http://terminology.hl7.org/CodeSystem/observation-category' and code = 'laboratory').exists() and status = 'final' and interpretation.coding.where(code = 'H' or code = 'HH' or code = 'L' or code = 'LL' or code = 'AA').exists()";
    #[cfg(feature = "R4")]
    const LAB_FINAL: &str = "category.coding.where(system = 'http://terminology.hl7.org/CodeSystem/observation-category' and code = 'laboratory').exists() and status = 'final'";

    #[cfg(feature = "R4")]
    fn enc(status: &str) -> Value {
        json!({
            "resourceType": "Encounter",
            "id": "e1",
            "status": status,
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "subject": { "reference": "Patient/p1" }
        })
    }

    #[cfg(feature = "R4")]
    fn enc_loc(status: &str, loc: &str) -> Value {
        let mut e = enc(status);
        e["location"] = json!([{ "location": { "reference": loc } }]);
        e
    }

    #[cfg(feature = "R4")]
    fn lab(status: &str, interp: Option<&str>) -> Value {
        let mut o = json!({
            "resourceType": "Observation",
            "id": "o1",
            "status": status,
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "laboratory"
                }]
            }],
            "code": { "coding": [{ "system": "http://loinc.org", "code": "2823-3" }] },
            "subject": { "reference": "Patient/p1" }
        });
        if let Some(c) = interp {
            o["interpretation"] = json!([{ "coding": [{ "code": c }] }]);
        }
        o
    }

    #[cfg(feature = "R4")]
    #[test]
    fn admit_matrix_status_transitions() {
        // arrived → in-progress matches; cancelled → in-progress matches; finished → in-progress matches
        for prev_status in ["arrived", "triaged", "onleave", "cancelled", "finished"] {
            assert!(
                criteria_matches(ADMIT, &enc("in-progress"), Some(&enc(prev_status)), r4()),
                "admit should fire from {prev_status}"
            );
        }
        assert!(!criteria_matches(
            ADMIT,
            &enc("planned"),
            Some(&enc("arrived")),
            r4()
        ));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn discharge_noop_when_already_finished() {
        assert!(!criteria_matches(
            DISCHARGE,
            &enc("finished"),
            Some(&enc("finished")),
            r4()
        ));
        assert!(criteria_matches(
            DISCHARGE,
            &enc("finished"),
            Some(&enc("in-progress")),
            r4()
        ));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn transfer_requires_both_sides_to_have_location() {
        assert!(!criteria_matches(
            TRANSFER,
            &enc_loc("in-progress", "Location/b"),
            Some(&enc("in-progress")),
            r4()
        ));
        assert!(!criteria_matches(
            TRANSFER,
            &enc("in-progress"),
            Some(&enc_loc("in-progress", "Location/a")),
            r4()
        ));
        assert!(criteria_matches(
            TRANSFER,
            &enc_loc("in-progress", "Location/b"),
            Some(&enc_loc("in-progress", "Location/a")),
            r4()
        ));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn lab_critical_all_interpretation_codes_and_negatives() {
        for code in ["H", "HH", "L", "LL", "AA"] {
            assert!(
                criteria_matches(LAB_CRITICAL, &lab("final", Some(code)), None, r4()),
                "critical code {code}"
            );
        }
        assert!(!criteria_matches(
            LAB_CRITICAL,
            &lab("final", Some("N")),
            None,
            r4()
        ));
        assert!(!criteria_matches(
            LAB_CRITICAL,
            &lab("preliminary", Some("HH")),
            None,
            r4()
        ));
        assert!(criteria_matches(LAB_FINAL, &lab("final", None), None, r4()));
        assert!(!criteria_matches(
            LAB_FINAL,
            &lab("preliminary", None),
            None,
            r4()
        ));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn current_variable_is_usable_in_criteria() {
        // Atrius IG uses bare `status`; also accept %current.status style.
        let expr = "%previous.status != 'finished' and %current.status = 'finished'";
        assert!(criteria_matches(
            expr,
            &enc("finished"),
            Some(&enc("in-progress")),
            r4()
        ));
        assert!(!criteria_matches(expr, &enc("finished"), None, r4()));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn invalid_resource_json_fail_closed() {
        // Missing resourceType / not a FHIR resource → deserialize error → false
        let junk = json!({ "id": "x", "status": "in-progress" });
        assert!(!criteria_matches(ADMIT, &junk, None, r4()));
    }

    #[cfg(feature = "R4")]
    #[test]
    fn undefined_external_variable_fail_closed() {
        assert!(!criteria_matches(
            "%doesNotExist = true",
            &enc("in-progress"),
            None,
            r4()
        ));
    }
}

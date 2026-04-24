//! **Care gaps & quality**-style rules over FHIR data available in the CDS request.
//!
//! These are **heuristic** starters (not HEDIS or proprietary measure engines). They read
//! [`CdsRequest::prefetch`](helios_cds_hooks::CdsRequest) JSON only unless you extend callers to pass
//! merged `Patient` from an EHR read.

use std::collections::HashMap;

use chrono::{NaiveDate, Utc};
use helios_cds_hooks::CdsRequest;
use helios_cds_hooks::Indicator;
use helios_cds_hooks::hooks::PatientViewContext;
use serde_json::Value;

/// A single quality / gap item before mapping to a CDS [`Card`](helios_cds_hooks::Card).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityFinding {
    /// Stable id for logging, feedback, and tests (e.g. `gap.age_screening.50`).
    pub id: &'static str,
    pub indicator: Indicator,
    pub title: String,
    pub detail: Option<String>,
}

const SOURCE: &str = "Quality & gaps";

/// Run all **patient-view** gap rules. Sync; uses `prefetch` and typed `context` only.
pub fn evaluate_patient_view_gaps(
    request: &CdsRequest,
    _context: &PatientViewContext,
) -> Vec<QualityFinding> {
    let mut out = Vec::new();
    let patient = patient_resource_from_prefetch(request.prefetch.as_ref());

    let Some(patient) = patient else {
        out.push(QualityFinding {
            id: "gap.prefetch.no_patient",
            indicator: Indicator::Info,
            title: "Add Patient to prefetch to enable age- and lab-based quality checks"
                .to_string(),
            detail: Some(
                "Publish a prefetch template (e.g. `Patient/{{context.patientId}}`) so services can see birthDate, conditions, and observations."
                    .to_string(),
            ),
        });
        return out;
    };
    let age = age_years_from_patient(&patient);

    if let Some(age) = age {
        if age >= 50 {
            out.push(QualityFinding {
                id: "gap.screening.colorectal.age50",
                indicator: Indicator::Info,
                title: "Verify colorectal cancer screening is up to date (50+ years)"
                    .to_string(),
                detail: Some(
                    "Heuristic only — align with your organization’s quality program (e.g. FIT, colonoscopy intervals)."
                        .to_string(),
                ),
            });
        }
    } else {
        out.push(QualityFinding {
            id: "gap.patient.no_birthdate",
            indicator: Indicator::Info,
            title: "Patient resource has no computable birthDate for age-based checks".to_string(),
            detail: None,
        });
    }

    if let Some(obs) = observation_bundle_gaps(&request) {
        out.push(obs);
    }

    out
}

/// Walk prefetch values and return the first `Patient` resource (any key).
fn patient_resource_from_prefetch(
    prefetch: Option<&HashMap<String, Option<Value>>>,
) -> Option<Value> {
    let prefetch = prefetch?;
    for v in prefetch.values() {
        let Some(v) = v else { continue };
        if resource_type(v) == Some("Patient") {
            return Some(v.clone());
        }
        if let Some(bundle) = v.get("entry").and_then(|e| e.as_array()) {
            for entry in bundle {
                if let Some(r) = entry.get("resource") {
                    if resource_type(r) == Some("Patient") {
                        return Some(r.clone());
                    }
                }
            }
        }
    }
    None
}

fn resource_type(v: &Value) -> Option<&str> {
    v.get("resourceType").and_then(|r| r.as_str())
}

/// Age in full years if `Patient.birthDate` is a full `YYYY-MM-DD` date.
fn age_years_from_patient(patient: &Value) -> Option<u32> {
    let s = patient.get("birthDate")?.as_str()?;
    let born = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let today = Utc::now().date_naive();
    let years = (today - born).num_days() / 365;
    Some((years as u32).min(200))
}

/// If a prefetch entry looks like a lab `Bundle` (or `entry` list) with no HbA1c LOINC, note it.
fn observation_bundle_gaps(request: &CdsRequest) -> Option<QualityFinding> {
    let prefetch = request.prefetch.as_ref()?;
    for (key, val) in prefetch {
        if !key.to_ascii_lowercase().contains("obs") && !key.to_ascii_lowercase().contains("lab") {
            continue;
        }
        let Some(v) = val else { continue };
        let empty_or_missing_a1c = match v.get("entry").and_then(|e| e.as_array()) {
            None => v.get("resourceType").and_then(|r| r.as_str()) == Some("Bundle"),
            Some(entries) if entries.is_empty() => true,
            Some(entries) => !entries
                .iter()
                .filter_map(|e| e.get("resource"))
                .any(has_hba1c_loinc),
        };
        if empty_or_missing_a1c {
            return Some(QualityFinding {
                id: "gap.lab.hba1c.missing_in_prefetch",
                indicator: Indicator::Info,
                title: "No HbA1c (LOINC 4548-6) in supplied lab/observation prefetch".to_string(),
                detail: Some(
                    "For diabetes quality gaps, include Observations for HbA1c when prefetching."
                        .to_string(),
                ),
            });
        }
    }
    None
}

/// R4 `Observation` with A1c LOINC 4548-6 in `code.coding` (or `code.coding[0].code`).
fn has_hba1c_loinc(res: &Value) -> bool {
    if res.get("resourceType").and_then(|r| r.as_str()) != Some("Observation") {
        return false;
    }
    let code = res.get("code");
    if let Some(codings) = code
        .and_then(|c| c.get("coding"))
        .and_then(|c| c.as_array())
    {
        for c in codings {
            if c.get("system").and_then(|s| s.as_str()) == Some("http://loinc.org")
                && c.get("code").and_then(|x| x.as_str()) == Some("4548-6")
            {
                return true;
            }
        }
    }
    false
}

impl QualityFinding {
    /// Map to a CDS card with a fixed source label.
    pub fn to_card(self) -> helios_cds_hooks::Card {
        use helios_cds_hooks::Card;
        let QualityFinding {
            indicator,
            title,
            detail,
            ..
        } = self;
        let mut c = match indicator {
            Indicator::Info => Card::info(title, SOURCE),
            Indicator::Warning => Card::warning(title, SOURCE),
            Indicator::Critical => Card::critical(title, SOURCE),
        };
        c.detail = detail;
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request_with_patient_patient(age_70: bool) -> CdsRequest {
        let y = if age_70 { 1950 } else { 2000 };
        CdsRequest {
            hook: "patient-view".to_string(),
            hook_instance: "u".to_string(),
            fhir_server: None,
            fhir_authorization: None,
            context: serde_json::json!({}),
            prefetch: Some(HashMap::from([(
                "patient".to_string(),
                Some(serde_json::json!({
                    "resourceType": "Patient",
                    "id": "1",
                    "birthDate": format!("{y}-06-15")
                })),
            )])),
            extension: None,
        }
    }

    fn sample_ctx() -> PatientViewContext {
        PatientViewContext {
            user_id: "P/1".to_string(),
            patient_id: "1".to_string(),
            encounter_id: None,
        }
    }

    #[test]
    fn no_prefetch_patient_finds_limited_context() {
        let r = CdsRequest {
            hook: "patient-view".to_string(),
            hook_instance: "u".to_string(),
            fhir_server: None,
            fhir_authorization: None,
            context: serde_json::json!({}),
            prefetch: None,
            extension: None,
        };
        let g = evaluate_patient_view_gaps(&r, &sample_ctx());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].id, "gap.prefetch.no_patient");
    }

    #[test]
    fn age_50_adds_colorectal_info() {
        let r = sample_request_with_patient_patient(true);
        let g = evaluate_patient_view_gaps(&r, &sample_ctx());
        let ids: Vec<_> = g.iter().map(|x| x.id).collect();
        assert!(
            ids.contains(&"gap.screening.colorectal.age50"),
            "gaps: {ids:?}"
        );
    }

    #[test]
    fn young_patient_no_colorectal() {
        let r = sample_request_with_patient_patient(false);
        let g = evaluate_patient_view_gaps(&r, &sample_ctx());
        assert!(!g.iter().any(|x| x.id == "gap.screening.colorectal.age50"));
    }

    #[test]
    fn lab_prefetch_empty_triggers_hba1c_hint() {
        let r = CdsRequest {
            hook: "patient-view".to_string(),
            hook_instance: "u".to_string(),
            fhir_server: None,
            fhir_authorization: None,
            context: serde_json::json!({}),
            prefetch: Some(HashMap::from([
                (
                    "patient".to_string(),
                    Some(serde_json::json!({
                        "resourceType": "Patient",
                        "id": "1",
                        "birthDate": "1980-01-01"
                    })),
                ),
                (
                    "laboratory".to_string(),
                    Some(serde_json::json!({
                        "resourceType": "Bundle",
                        "type": "searchset",
                        "entry": []
                    })),
                ),
            ])),
            extension: None,
        };
        let g = evaluate_patient_view_gaps(&r, &sample_ctx());
        assert!(
            g.iter()
                .any(|x| x.id == "gap.lab.hba1c.missing_in_prefetch")
        );
    }
}

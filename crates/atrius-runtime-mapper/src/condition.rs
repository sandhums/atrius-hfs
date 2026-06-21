use serde_json::Value;

use crate::error::MapperResult;
use crate::manifest::{ATRIUS_PROFILE_BASE, MapperManifest};
use crate::profile::{has_profile_suffix, profiles_in_meta, set_evaluation_profile};

const HL7_CONDITION_CATEGORY: &str = "http://terminology.hl7.org/CodeSystem/condition-category";

/// Which QI-Core Condition evaluation profile applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionBranch {
    EncounterDiagnosis,
    ProblemsHealthConcerns,
}

/// Read HL7 `condition-category` codes from a Condition resource.
pub fn condition_category_codes(resource: &Value) -> Vec<String> {
    let Some(categories) = resource.get("category").and_then(|c| c.as_array()) else {
        return Vec::new();
    };

    let mut codes = Vec::new();
    for category in categories {
        if let Some(coding) = category.get("coding").and_then(|c| c.as_array()) {
            for c in coding {
                if c.get("system")
                    .and_then(|s| s.as_str())
                    .is_some_and(|s| s == HL7_CONDITION_CATEGORY)
                    && let Some(code) = c.get("code").and_then(|v| v.as_str())
                {
                    codes.push(code.to_string());
                }
            }
        }
    }
    codes
}

pub fn category_contains(resource: &Value, code: &str) -> bool {
    condition_category_codes(resource).iter().any(|c| c == code)
}

pub fn encounter_reference_present(resource: &Value) -> bool {
    resource
        .get("encounter")
        .and_then(|e| e.get("reference"))
        .and_then(|r| r.as_str())
        .is_some_and(|r| !r.trim().is_empty())
}

/// Deterministic category selection from Atrius IG `runtime-mapper.md`.
pub fn select_condition_branch(resource: &Value) -> ConditionBranch {
    if has_profile_suffix(resource, "atrius-in-condition-encounter-diagnosis") {
        return ConditionBranch::EncounterDiagnosis;
    }
    if has_profile_suffix(resource, "atrius-in-condition-problems-health-concerns") {
        return ConditionBranch::ProblemsHealthConcerns;
    }

    if category_contains(resource, "encounter-diagnosis") {
        return ConditionBranch::EncounterDiagnosis;
    }
    if category_contains(resource, "problem-list-item") {
        return ConditionBranch::ProblemsHealthConcerns;
    }
    if encounter_reference_present(resource) {
        return ConditionBranch::EncounterDiagnosis;
    }
    ConditionBranch::ProblemsHealthConcerns
}

pub fn evaluation_profile_url(manifest: &MapperManifest, branch: ConditionBranch) -> &str {
    match branch {
        ConditionBranch::EncounterDiagnosis => manifest.condition_encounter_diagnosis_profile(),
        ConditionBranch::ProblemsHealthConcerns => {
            manifest.condition_problems_health_concerns_profile()
        }
    }
}

/// Project an Atrius Condition into a QI-Core evaluation shape.
///
/// v0.1: profile swap + preserve clinical elements (status, code, timing, references).
/// Terminology crosswalk (ICD-10 → SNOMED for VSAC) is a later enhancement.
pub fn project_condition(manifest: &MapperManifest, resource: &mut Value) -> MapperResult<()> {
    let branch = select_condition_branch(resource);
    let evaluation_profile = evaluation_profile_url(manifest, branch).to_string();
    set_evaluation_profile(resource, &evaluation_profile);
    Ok(())
}

pub fn is_atrius_condition(resource: &Value) -> bool {
    if resource.get("resourceType").and_then(|v| v.as_str()) != Some("Condition") {
        return false;
    }
    profiles_in_meta(resource)
        .iter()
        .any(|p| p.starts_with(ATRIUS_PROFILE_BASE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::MapperManifest;
    use serde_json::json;

    fn manifest() -> MapperManifest {
        MapperManifest::default_v0_1()
    }

    #[test]
    fn branch_from_atrius_ed_profile() {
        let resource = json!({
            "resourceType": "Condition",
            "meta": { "profile": [format!("{ATRIUS_PROFILE_BASE}atrius-in-condition-encounter-diagnosis")] },
            "category": [{ "coding": [{ "system": HL7_CONDITION_CATEGORY, "code": "encounter-diagnosis" }] }]
        });
        assert_eq!(
            select_condition_branch(&resource),
            ConditionBranch::EncounterDiagnosis
        );
    }

    #[test]
    fn branch_from_encounter_reference() {
        let resource = json!({
            "resourceType": "Condition",
            "encounter": { "reference": "Encounter/e1" }
        });
        assert_eq!(
            select_condition_branch(&resource),
            ConditionBranch::EncounterDiagnosis
        );
    }

    #[test]
    fn branch_defaults_to_phc() {
        let resource = json!({
            "resourceType": "Condition",
            "code": { "text": "hypertension" }
        });
        assert_eq!(
            select_condition_branch(&resource),
            ConditionBranch::ProblemsHealthConcerns
        );
    }

    #[test]
    fn project_sets_qicore_profile() {
        let mut resource = json!({
            "resourceType": "Condition",
            "meta": { "profile": [format!("{ATRIUS_PROFILE_BASE}atrius-in-condition-problems-health-concerns")] },
            "category": [{ "coding": [{ "system": HL7_CONDITION_CATEGORY, "code": "problem-list-item" }] }],
            "code": { "coding": [{ "system": "http://snomed.info/sct", "code": "38341003" }] }
        });
        project_condition(&manifest(), &mut resource).unwrap();
        assert_eq!(
            resource["meta"]["profile"][0],
            crate::manifest::QICORE_CONDITION_PROBLEMS_HEALTH_CONCERNS
        );
    }
}

use serde_json::Value;

use crate::condition;
use crate::error::{MapperError, MapperResult};
use crate::manifest::MapperManifest;
use crate::profile::project_profile_swap;

/// Summary counts from projecting a Bundle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BundleProjectionStats {
    pub entries_total: u32,
    pub projected: u32,
    pub skipped: u32,
}

/// Resource types projected before dependents (actor-first ordering).
const PROJECTION_ORDER: &[&str] = &[
    "Patient",
    "Organization",
    "Practitioner",
    "PractitionerRole",
    "Location",
    "RelatedPerson",
    "Coverage",
    "Encounter",
    "Condition",
    "Observation",
    "Procedure",
    "MedicationRequest",
    "Claim",
    "ClaimResponse",
];

pub fn project_bundle(
    manifest: &MapperManifest,
    mut bundle: Value,
) -> MapperResult<(Value, BundleProjectionStats)> {
    let resource_type = bundle
        .get("resourceType")
        .and_then(|v| v.as_str())
        .ok_or_else(|| MapperError::NotBundle("missing resourceType".into()))?;
    if resource_type != "Bundle" {
        return Err(MapperError::NotBundle(format!(
            "expected Bundle, got {resource_type}"
        )));
    }

    let Some(entries) = bundle.get_mut("entry").and_then(|e| e.as_array_mut()) else {
        return Ok((bundle, BundleProjectionStats::default()));
    };

    sort_entries_for_projection(entries);

    let mut stats = BundleProjectionStats {
        entries_total: entries.len() as u32,
        ..Default::default()
    };

    for entry in entries.iter_mut() {
        let Some(resource) = entry.get_mut("resource") else {
            stats.skipped += 1;
            continue;
        };

        if project_resource(manifest, resource)? {
            stats.projected += 1;
        } else {
            stats.skipped += 1;
        }
    }

    Ok((bundle, stats))
}

fn sort_entries_for_projection(entries: &mut [Value]) {
    entries.sort_by_key(|entry| {
        entry
            .get("resource")
            .and_then(|r| r.get("resourceType"))
            .and_then(|t| t.as_str())
            .and_then(projection_rank)
            .unwrap_or(u32::MAX)
    });
}

fn projection_rank(resource_type: &str) -> Option<u32> {
    PROJECTION_ORDER
        .iter()
        .position(|t| *t == resource_type)
        .map(|i| i as u32)
}

pub fn project_resource(manifest: &MapperManifest, resource: &mut Value) -> MapperResult<bool> {
    let Some(resource_type) = resource
        .get("resourceType")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
    else {
        return Ok(false);
    };

    match resource_type.as_str() {
        "Condition"
            if condition::is_atrius_condition(resource) || should_project_condition(resource) =>
        {
            condition::project_condition(manifest, resource)?;
            Ok(true)
        }
        _ => project_profile_swap(manifest, resource),
    }
}

/// Project Conditions without Atrius meta.profile when they look like clinical ABDM data.
fn should_project_condition(resource: &Value) -> bool {
    resource.get("resourceType").and_then(|v| v.as_str()) == Some("Condition")
        && (condition::encounter_reference_present(resource)
            || !condition::condition_category_codes(resource).is_empty()
            || resource.get("code").is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{MapperManifest, QICORE_CONDITION_ENCOUNTER_DIAGNOSIS};
    use serde_json::json;

    #[test]
    fn projects_condition_entries_in_bundle() {
        let manifest = MapperManifest::default_v0_1();
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [{
                "resource": {
                    "resourceType": "Condition",
                    "meta": { "profile": ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-condition-encounter-diagnosis"] },
                    "category": [{ "coding": [{ "system": "http://terminology.hl7.org/CodeSystem/condition-category", "code": "encounter-diagnosis" }] }],
                    "code": { "coding": [{ "system": "http://hl7.org/fhir/sid/icd-10", "code": "I10" }] },
                    "subject": { "reference": "Patient/p1" }
                }
            }]
        });

        let (out, stats) = project_bundle(&manifest, bundle).unwrap();
        assert_eq!(stats.projected, 1);
        assert_eq!(
            out["entry"][0]["resource"]["meta"]["profile"][0],
            QICORE_CONDITION_ENCOUNTER_DIAGNOSIS
        );
    }

    #[test]
    fn profile_swap_patient_from_full_inventory() {
        use crate::profile::project_profile_swap;
        let manifest = MapperManifest::full_inventory();
        let mut resource = json!({
            "resourceType": "Patient",
            "meta": { "profile": ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"] }
        });
        assert!(project_profile_swap(&manifest, &mut resource).unwrap());
        assert_eq!(
            resource["meta"]["profile"][0],
            "http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-patient"
        );
    }

    #[test]
    fn profile_swap_patient_when_meta_null() {
        use crate::profile::project_profile_swap;
        let manifest = MapperManifest::full_inventory();
        let mut resource = json!({
            "resourceType": "Patient",
            "id": "p1",
            "meta": null
        });
        assert!(project_profile_swap(&manifest, &mut resource).unwrap());
        assert_eq!(
            resource["meta"]["profile"][0],
            "http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-patient"
        );
    }
}

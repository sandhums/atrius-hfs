//! Examples from Atrius IG `runtime-mapper.md`.

use atrius_runtime_mapper::{
    MapperManifest, RuntimeMapper,
    manifest::{QICORE_CONDITION_ENCOUNTER_DIAGNOSIS, QICORE_CONDITION_PROBLEMS_HEALTH_CONCERNS},
};
use serde_json::{Value, json};

fn manifest() -> MapperManifest {
    MapperManifest::default_v0_1()
}

fn assert_profile_only_change(input: Value, expected_profile: &str) {
    let mut resource = input.clone();
    let mapper = RuntimeMapper::new(manifest());
    mapper.project_resource(&mut resource).unwrap();

    assert_eq!(resource["meta"]["profile"][0], expected_profile);

    let mut without_meta = resource.clone();
    without_meta.as_object_mut().unwrap().remove("meta");
    let mut input_without_meta = input;
    input_without_meta.as_object_mut().unwrap().remove("meta");
    assert_eq!(without_meta, input_without_meta);
}

#[test]
fn encounter_diagnosis_example_from_runtime_mapper_md() {
    let input = json!({
        "resourceType": "Condition",
        "meta": {
            "profile": [
                "https://atrius.in/fhir/r4/atrius-core/StructureDefinition/atrius-condition-encounter-diagnosis"
            ]
        },
        "clinicalStatus": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "active"
            }]
        },
        "verificationStatus": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                "code": "confirmed"
            }]
        },
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                "code": "encounter-diagnosis"
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://hl7.org/fhir/sid/icd-10",
                "code": "I10"
            }]
        },
        "subject": { "reference": "Patient/p1" },
        "encounter": { "reference": "Encounter/e1" },
        "extension": [{
            "url": "http://hl7.org/fhir/StructureDefinition/condition-assertedDate",
            "valueDateTime": "2026-05-24"
        }],
        "onsetDateTime": "2026-05-20",
        "recordedDate": "2026-05-24"
    });

    assert_profile_only_change(input, QICORE_CONDITION_ENCOUNTER_DIAGNOSIS);
}

#[test]
fn problems_health_concerns_example_from_runtime_mapper_md() {
    let input = json!({
        "resourceType": "Condition",
        "meta": {
            "profile": [
                "https://atrius.in/fhir/r4/atrius-core/StructureDefinition/atrius-condition-problems-health-concerns"
            ]
        },
        "clinicalStatus": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "active"
            }]
        },
        "verificationStatus": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                "code": "confirmed"
            }]
        },
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                "code": "problem-list-item"
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "38341003"
            }]
        },
        "subject": { "reference": "Patient/p2" },
        "extension": [{
            "url": "http://hl7.org/fhir/StructureDefinition/condition-assertedDate",
            "valueDateTime": "2026-05-18"
        }],
        "onsetDateTime": "2026-05-15",
        "recordedDate": "2026-05-18"
    });

    assert_profile_only_change(input, QICORE_CONDITION_PROBLEMS_HEALTH_CONCERNS);
}

#[test]
fn loads_embedded_manifest_json() {
    let text = include_str!("../data/atrius-mapper-manifest-v0.1.json");
    let manifest = MapperManifest::from_json_str(text).unwrap();
    assert_eq!(manifest, MapperManifest::default_v0_1());
}

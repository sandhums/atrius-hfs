//! Atrius → QI-Core profile inventory from `runtime-mapper.md` v0.1 table.

use crate::manifest::{ATRIUS_PROFILE_BASE, MapperManifest, ProfileMapping};

const QICORE_SD: &str = "http://hl7.org/fhir/us/qicore/StructureDefinition/";
const US_CORE_SD: &str = "http://hl7.org/fhir/us/core/StructureDefinition/";

/// Build the full v0.1 profile inventory manifest.
#[must_use]
pub fn full_inventory_manifest() -> MapperManifest {
    let mappings = INVENTORY
        .iter()
        .map(|(atrius, eval, resource_type)| {
            let evaluation_profile = if *eval == "us-core-specimen" {
                us(eval)
            } else {
                q(eval)
            };
            ProfileMapping {
                atrius_profile: format!("{ATRIUS_PROFILE_BASE}{atrius}"),
                evaluation_profile,
                resource_type: Some((*resource_type).to_string()),
            }
        })
        .collect();

    MapperManifest {
        profile_mappings: mappings,
        condition_encounter_diagnosis_evaluation_profile: Some(q(
            "qicore-condition-encounter-diagnosis",
        )),
        condition_problems_health_concerns_evaluation_profile: Some(q(
            "qicore-condition-problems-health-concerns",
        )),
    }
}

fn q(name: &str) -> String {
    format!("{QICORE_SD}{name}")
}

fn us(name: &str) -> String {
    format!("{US_CORE_SD}{name}")
}

/// `(atrius_profile_id, qicore_profile_id_or_us_core_id, fhir_resource_type)`
const INVENTORY: &[(&str, &str, &str)] = &[
    ("atrius-patient", "qicore-patient", "Patient"),
    ("atrius-encounter", "qicore-encounter", "Encounter"),
    (
        "atrius-familymemberhistory",
        "qicore-familymemberhistory",
        "FamilyMemberHistory",
    ),
    ("atrius-flag", "qicore-flag", "Flag"),
    ("atrius-goal", "qicore-goal", "Goal"),
    ("atrius-practitioner", "qicore-practitioner", "Practitioner"),
    (
        "atrius-practitionerrole",
        "qicore-practitionerrole",
        "PractitionerRole",
    ),
    ("atrius-organization", "qicore-organization", "Organization"),
    (
        "atrius-relatedperson",
        "qicore-relatedperson",
        "RelatedPerson",
    ),
    ("atrius-coverage", "qicore-coverage", "Coverage"),
    (
        "atrius-condition-encounter-diagnosis",
        "qicore-condition-encounter-diagnosis",
        "Condition",
    ),
    (
        "atrius-condition-problems-health-concerns",
        "qicore-condition-problems-health-concerns",
        "Condition",
    ),
    (
        "atrius-allergyintolerance",
        "qicore-allergyintolerance",
        "AllergyIntolerance",
    ),
    ("atrius-adverseevent", "qicore-adverseevent", "AdverseEvent"),
    (
        "atrius-bodystructure",
        "qicore-bodystructure",
        "BodyStructure",
    ),
    ("atrius-device", "qicore-device", "Device"),
    (
        "atrius-devicerequest",
        "qicore-devicerequest",
        "DeviceRequest",
    ),
    (
        "atrius-devicerequest-requested",
        "qicore-devicerequest-requested",
        "DeviceRequest",
    ),
    (
        "atrius-devicerequest-prohibited",
        "qicore-devicerequest-prohibited",
        "DeviceRequest",
    ),
    (
        "atrius-deviceusestatement",
        "qicore-deviceusestatement",
        "DeviceUseStatement",
    ),
    (
        "atrius-diagnosticreport-lab",
        "qicore-diagnosticreport-lab",
        "DiagnosticReport",
    ),
    (
        "atrius-diagnosticreport-note",
        "qicore-diagnosticreport-note",
        "DiagnosticReport",
    ),
    ("atrius-imagingstudy", "qicore-imagingstudy", "ImagingStudy"),
    ("atrius-immunization", "qicore-immunization", "Immunization"),
    (
        "atrius-immunization-done",
        "qicore-immunization-done",
        "Immunization",
    ),
    (
        "atrius-immunization-not-done",
        "qicore-immunization-not-done",
        "Immunization",
    ),
    (
        "atrius-immunizationrecommendation",
        "qicore-immunizationrecommendation",
        "ImmunizationRecommendation",
    ),
    (
        "atrius-immunizationevaluation",
        "qicore-immunizationevaluation",
        "ImmunizationEvaluation",
    ),
    ("atrius-location", "qicore-location", "Location"),
    (
        "atrius-observation",
        "qicore-observation-lab",
        "Observation",
    ),
    (
        "atrius-observation-body-measurement",
        "qicore-simple-observation",
        "Observation",
    ),
    (
        "atrius-observation-general-assessment",
        "qicore-observation-screening-assessment",
        "Observation",
    ),
    (
        "atrius-observation-lifestyle",
        "qicore-observation-screening-assessment",
        "Observation",
    ),
    (
        "atrius-observation-physical-activity",
        "qicore-simple-observation",
        "Observation",
    ),
    (
        "atrius-observation-vital-signs",
        "qicore-simple-observation",
        "Observation",
    ),
    (
        "atrius-observation-women-health",
        "qicore-observation-screening-assessment",
        "Observation",
    ),
    ("atrius-careplan", "qicore-careplan", "CarePlan"),
    ("atrius-careplan-assess-plan", "qicore-careplan", "CarePlan"),
    ("atrius-careteam", "qicore-careteam", "CareTeam"),
    (
        "atrius-communication",
        "qicore-communication",
        "Communication",
    ),
    (
        "atrius-communication-not-done",
        "qicore-communication-not-done",
        "Communication",
    ),
    (
        "atrius-communicationrequest",
        "qicore-communicationrequest",
        "CommunicationRequest",
    ),
    ("atrius-claim", "qicore-claim", "Claim"),
    (
        "atrius-claimresponse",
        "qicore-claimresponse",
        "ClaimResponse",
    ),
    ("atrius-medication", "qicore-medication", "Medication"),
    (
        "atrius-medicationrequest",
        "qicore-medicationrequest",
        "MedicationRequest",
    ),
    (
        "atrius-medicationrequest-requested",
        "qicore-medicationrequest-requested",
        "MedicationRequest",
    ),
    (
        "atrius-medicationrequest-prohibited",
        "qicore-medicationrequest-prohibited",
        "MedicationRequest",
    ),
    (
        "atrius-medicationstatement",
        "qicore-medicationstatement",
        "MedicationStatement",
    ),
    (
        "atrius-medicationadministration",
        "qicore-medicationadministration",
        "MedicationAdministration",
    ),
    (
        "atrius-medicationadministration-not-done",
        "qicore-medicationadministration-not-done",
        "MedicationAdministration",
    ),
    (
        "atrius-medicationdispense",
        "qicore-medicationdispense",
        "MedicationDispense",
    ),
    (
        "atrius-medicationdispense-declined",
        "qicore-medicationdispense-declined",
        "MedicationDispense",
    ),
    (
        "atrius-nutritionorder",
        "qicore-nutritionorder",
        "NutritionOrder",
    ),
    ("atrius-procedure", "qicore-procedure", "Procedure"),
    (
        "atrius-procedure-not-done",
        "qicore-procedure-not-done",
        "Procedure",
    ),
    (
        "atrius-servicerequest",
        "qicore-servicerequest",
        "ServiceRequest",
    ),
    (
        "atrius-servicerequest-not-requested",
        "qicore-servicerequest-not-requested",
        "ServiceRequest",
    ),
    (
        "atrius-questionnaireresponse",
        "qicore-questionnaireresponse",
        "QuestionnaireResponse",
    ),
    ("atrius-specimen", "us-core-specimen", "Specimen"),
    ("atrius-substance", "qicore-substance", "Substance"),
    ("atrius-task", "qicore-task", "Task"),
    ("atrius-task-rejected", "qicore-task-rejected", "Task"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_inventory_has_expected_count() {
        let m = full_inventory_manifest();
        m.validate().unwrap();
        assert_eq!(m.profile_mappings.len(), INVENTORY.len());
    }

    #[test]
    fn specimen_maps_to_us_core() {
        let m = full_inventory_manifest();
        let specimen = m
            .profile_mappings
            .iter()
            .find(|p| p.atrius_profile.ends_with("atrius-specimen"))
            .unwrap();
        assert_eq!(specimen.evaluation_profile, us("us-core-specimen"));
    }
}

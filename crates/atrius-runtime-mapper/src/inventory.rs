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
    ("atrius-in-patient", "qicore-patient", "Patient"),
    ("atrius-in-encounter", "qicore-encounter", "Encounter"),
    (
        "atrius-in-familymemberhistory",
        "qicore-familymemberhistory",
        "FamilyMemberHistory",
    ),
    ("atrius-in-flag", "qicore-flag", "Flag"),
    ("atrius-in-goal", "qicore-goal", "Goal"),
    ("atrius-in-practitioner", "qicore-practitioner", "Practitioner"),
    (
        "atrius-in-practitionerrole",
        "qicore-practitionerrole",
        "PractitionerRole",
    ),
    ("atrius-in-organization", "qicore-organization", "Organization"),
    (
        "atrius-in-relatedperson",
        "qicore-relatedperson",
        "RelatedPerson",
    ),
    ("atrius-in-coverage", "qicore-coverage", "Coverage"),
    (
        "atrius-in-condition-encounter-diagnosis",
        "qicore-condition-encounter-diagnosis",
        "Condition",
    ),
    (
        "atrius-in-condition-problems-health-concerns",
        "qicore-condition-problems-health-concerns",
        "Condition",
    ),
    (
        "atrius-in-allergyintolerance",
        "qicore-allergyintolerance",
        "AllergyIntolerance",
    ),
    ("atrius-in-adverse-event", "qicore-adverseevent", "AdverseEvent"),
    (
        "atrius-in-bodystructure",
        "qicore-bodystructure",
        "BodyStructure",
    ),
    ("atrius-in-device", "qicore-device", "Device"),
    (
        "atrius-in-devicerequest",
        "qicore-devicerequest",
        "DeviceRequest",
    ),
    (
        "atrius-in-devicerequest-requested",
        "qicore-devicerequest-requested",
        "DeviceRequest",
    ),
    (
        "atrius-in-devicerequest-prohibited",
        "qicore-devicerequest-prohibited",
        "DeviceRequest",
    ),
    (
        "atrius-in-deviceusestatement",
        "qicore-deviceusestatement",
        "DeviceUseStatement",
    ),
    (
        "atrius-in-diagnosticreport-lab",
        "qicore-diagnosticreport-lab",
        "DiagnosticReport",
    ),
    (
        "atrius-in-diagnosticreport-note",
        "qicore-diagnosticreport-note",
        "DiagnosticReport",
    ),
    ("atrius-in-imagingstudy", "qicore-imagingstudy", "ImagingStudy"),
    ("atrius-in-immunization", "qicore-immunization", "Immunization"),
    (
        "atrius-in-immunization-done",
        "qicore-immunization-done",
        "Immunization",
    ),
    (
        "atrius-in-immunization-not-done",
        "qicore-immunization-not-done",
        "Immunization",
    ),
    (
        "atrius-in-immunizationrecommendation",
        "qicore-immunizationrecommendation",
        "ImmunizationRecommendation",
    ),
    (
        "atrius-in-immunizationevaluation",
        "qicore-immunizationevaluation",
        "ImmunizationEvaluation",
    ),
    ("atrius-in-location", "qicore-location", "Location"),
    (
        "atrius-in-observation",
        "qicore-observation-lab",
        "Observation",
    ),
    (
        "atrius-in-observation-body-measurement",
        "qicore-simple-observation",
        "Observation",
    ),
    (
        "atrius-in-observation-general-assessment",
        "qicore-observation-screening-assessment",
        "Observation",
    ),
    (
        "atrius-in-observation-lifestyle",
        "qicore-observation-screening-assessment",
        "Observation",
    ),
    (
        "atrius-in-observation-physical-activity",
        "qicore-simple-observation",
        "Observation",
    ),
    (
        "atrius-in-observation-vital-signs",
        "qicore-simple-observation",
        "Observation",
    ),
    (
        "atrius-in-observation-women-health",
        "qicore-observation-screening-assessment",
        "Observation",
    ),
    ("atrius-in-careplan", "qicore-careplan", "CarePlan"),
    ("atrius-in-careplan-assess-plan", "qicore-careplan", "CarePlan"),
    ("atrius-in-careteam", "qicore-careteam", "CareTeam"),
    (
        "atrius-in-communication",
        "qicore-communication",
        "Communication",
    ),
    (
        "atrius-in-communication-not-done",
        "qicore-communication-not-done",
        "Communication",
    ),
    (
        "atrius-in-communicationrequest",
        "qicore-communicationrequest",
        "CommunicationRequest",
    ),
    ("atrius-in-claim", "qicore-claim", "Claim"),
    (
        "atrius-in-claimresponse",
        "qicore-claimresponse",
        "ClaimResponse",
    ),
    ("atrius-in-medication", "qicore-medication", "Medication"),
    (
        "atrius-in-medicationrequest",
        "qicore-medicationrequest",
        "MedicationRequest",
    ),
    (
        "atrius-in-medicationrequest-requested",
        "qicore-medicationrequest-requested",
        "MedicationRequest",
    ),
    (
        "atrius-in-medicationrequest-prohibited",
        "qicore-medicationrequest-prohibited",
        "MedicationRequest",
    ),
    (
        "atrius-in-medicationstatement",
        "qicore-medicationstatement",
        "MedicationStatement",
    ),
    (
        "atrius-in-medicationadministration",
        "qicore-medicationadministration",
        "MedicationAdministration",
    ),
    (
        "atrius-in-medicationadministration-not-done",
        "qicore-medicationadministration-not-done",
        "MedicationAdministration",
    ),
    (
        "atrius-in-medicationdispense",
        "qicore-medicationdispense",
        "MedicationDispense",
    ),
    (
        "atrius-in-medicationdispense-declined",
        "qicore-medicationdispense-declined",
        "MedicationDispense",
    ),
    (
        "atrius-in-nutritionorder",
        "qicore-nutritionorder",
        "NutritionOrder",
    ),
    ("atrius-in-procedure", "qicore-procedure", "Procedure"),
    (
        "atrius-in-procedure-not-done",
        "qicore-procedure-not-done",
        "Procedure",
    ),
    (
        "atrius-in-servicerequest",
        "qicore-servicerequest",
        "ServiceRequest",
    ),
    (
        "atrius-in-servicerequest-not-requested",
        "qicore-servicerequest-not-requested",
        "ServiceRequest",
    ),
    (
        "atrius-in-questionnaireresponse",
        "qicore-questionnaireresponse",
        "QuestionnaireResponse",
    ),
    ("atrius-in-specimen", "us-core-specimen", "Specimen"),
    ("atrius-in-substance", "qicore-substance", "Substance"),
    ("atrius-in-task", "qicore-task", "Task"),
    ("atrius-in-task-rejected", "qicore-task-rejected", "Task"),
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
            .find(|p| p.atrius_profile.ends_with("atrius-in-specimen"))
            .unwrap();
        assert_eq!(specimen.evaluation_profile, us("us-core-specimen"));
    }
}

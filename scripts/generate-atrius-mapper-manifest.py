#!/usr/bin/env python3
"""Generate atrius-mapper-manifest.json from the runtime-mapper inventory.

Usage:
  python3 scripts/generate-atrius-mapper-manifest.py [--output PATH] [--validate-ig IG_OUTPUT_DIR]

Without --output, prints JSON to stdout.
With --validate-ig, checks that each atrius_profile id exists as
StructureDefinition-{id}.json in the IG build output directory.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ATRIUS_BASE = "https://atrius.in/fhir/r4/atrius-in/StructureDefinition/"
QICORE_SD = "http://hl7.org/fhir/us/qicore/StructureDefinition/"
US_CORE_SD = "http://hl7.org/fhir/us/core/StructureDefinition/"


def q(name: str) -> str:
    return f"{QICORE_SD}{name}"


def us(name: str) -> str:
    return f"{US_CORE_SD}{name}"


# Keep in sync with crates/atrius-runtime-mapper/src/inventory.rs
INVENTORY: list[tuple[str, str, str]] = [
    ("atrius-in-patient", q("qicore-patient"), "Patient"),
    ("atrius-in-encounter", q("qicore-encounter"), "Encounter"),
    ("atrius-in-familymemberhistory", q("qicore-familymemberhistory"), "FamilyMemberHistory"),
    ("atrius-in-flag", q("qicore-flag"), "Flag"),
    ("atrius-in-goal", q("qicore-goal"), "Goal"),
    ("atrius-in-practitioner", q("qicore-practitioner"), "Practitioner"),
    ("atrius-in-practitionerrole", q("qicore-practitionerrole"), "PractitionerRole"),
    ("atrius-in-organization", q("qicore-organization"), "Organization"),
    ("atrius-in-relatedperson", q("qicore-relatedperson"), "RelatedPerson"),
    ("atrius-in-coverage", q("qicore-coverage"), "Coverage"),
    (
        "atrius-in-condition-encounter-diagnosis",
        q("qicore-condition-encounter-diagnosis"),
        "Condition",
    ),
    (
        "atrius-in-condition-problems-health-concerns",
        q("qicore-condition-problems-health-concerns"),
        "Condition",
    ),
    ("atrius-in-allergyintolerance", q("qicore-allergyintolerance"), "AllergyIntolerance"),
    ("atrius-in-adverse-event", q("qicore-adverseevent"), "AdverseEvent"),
    ("atrius-in-bodystructure", q("qicore-bodystructure"), "BodyStructure"),
    ("atrius-in-device", q("qicore-device"), "Device"),
    ("atrius-in-devicerequest", q("qicore-devicerequest"), "DeviceRequest"),
    ("atrius-in-devicerequest-requested", q("qicore-devicerequest-requested"), "DeviceRequest"),
    ("atrius-in-devicerequest-prohibited", q("qicore-devicerequest-prohibited"), "DeviceRequest"),
    ("atrius-in-deviceusestatement", q("qicore-deviceusestatement"), "DeviceUseStatement"),
    ("atrius-in-diagnosticreport-lab", q("qicore-diagnosticreport-lab"), "DiagnosticReport"),
    ("atrius-in-diagnosticreport-note", q("qicore-diagnosticreport-note"), "DiagnosticReport"),
    ("atrius-in-imagingstudy", q("qicore-imagingstudy"), "ImagingStudy"),
    ("atrius-in-immunization", q("qicore-immunization"), "Immunization"),
    ("atrius-in-immunization-done", q("qicore-immunization-done"), "Immunization"),
    ("atrius-in-immunization-not-done", q("qicore-immunization-not-done"), "Immunization"),
    (
        "atrius-in-immunizationrecommendation",
        q("qicore-immunizationrecommendation"),
        "ImmunizationRecommendation",
    ),
    (
        "atrius-in-immunizationevaluation",
        q("qicore-immunizationevaluation"),
        "ImmunizationEvaluation",
    ),
    ("atrius-in-location", q("qicore-location"), "Location"),
    ("atrius-in-observation", q("qicore-observation-lab"), "Observation"),
    ("atrius-in-observation-body-measurement", q("qicore-simple-observation"), "Observation"),
    (
        "atrius-in-observation-general-assessment",
        q("qicore-observation-screening-assessment"),
        "Observation",
    ),
    (
        "atrius-in-observation-lifestyle",
        q("qicore-observation-screening-assessment"),
        "Observation",
    ),
    ("atrius-in-observation-physical-activity", q("qicore-simple-observation"), "Observation"),
    ("atrius-in-observation-vital-signs", q("qicore-simple-observation"), "Observation"),
    (
        "atrius-in-observation-women-health",
        q("qicore-observation-screening-assessment"),
        "Observation",
    ),
    ("atrius-in-careplan", q("qicore-careplan"), "CarePlan"),
    ("atrius-in-careplan-assess-plan", q("qicore-careplan"), "CarePlan"),
    ("atrius-in-careteam", q("qicore-careteam"), "CareTeam"),
    ("atrius-in-communication", q("qicore-communication"), "Communication"),
    ("atrius-in-communication-not-done", q("qicore-communication-not-done"), "Communication"),
    ("atrius-in-communicationrequest", q("qicore-communicationrequest"), "CommunicationRequest"),
    ("atrius-in-claim", q("qicore-claim"), "Claim"),
    ("atrius-in-claimresponse", q("qicore-claimresponse"), "ClaimResponse"),
    ("atrius-in-medication", q("qicore-medication"), "Medication"),
    ("atrius-in-medicationrequest", q("qicore-medicationrequest"), "MedicationRequest"),
    (
        "atrius-in-medicationrequest-requested",
        q("qicore-medicationrequest-requested"),
        "MedicationRequest",
    ),
    (
        "atrius-in-medicationrequest-prohibited",
        q("qicore-medicationrequest-prohibited"),
        "MedicationRequest",
    ),
    ("atrius-in-medicationstatement", q("qicore-medicationstatement"), "MedicationStatement"),
    (
        "atrius-in-medicationadministration",
        q("qicore-medicationadministration"),
        "MedicationAdministration",
    ),
    (
        "atrius-in-medicationadministration-not-done",
        q("qicore-medicationadministration-not-done"),
        "MedicationAdministration",
    ),
    ("atrius-in-medicationdispense", q("qicore-medicationdispense"), "MedicationDispense"),
    (
        "atrius-in-medicationdispense-declined",
        q("qicore-medicationdispense-declined"),
        "MedicationDispense",
    ),
    ("atrius-in-nutritionorder", q("qicore-nutritionorder"), "NutritionOrder"),
    ("atrius-in-procedure", q("qicore-procedure"), "Procedure"),
    ("atrius-in-procedure-not-done", q("qicore-procedure-not-done"), "Procedure"),
    ("atrius-in-servicerequest", q("qicore-servicerequest"), "ServiceRequest"),
    (
        "atrius-in-servicerequest-not-requested",
        q("qicore-servicerequest-not-requested"),
        "ServiceRequest",
    ),
    (
        "atrius-in-questionnaireresponse",
        q("qicore-questionnaireresponse"),
        "QuestionnaireResponse",
    ),
    ("atrius-in-specimen", us("us-core-specimen"), "Specimen"),
    ("atrius-in-substance", q("qicore-substance"), "Substance"),
    ("atrius-in-task", q("qicore-task"), "Task"),
    ("atrius-in-task-rejected", q("qicore-task-rejected"), "Task"),
]


def build_manifest() -> dict:
    return {
        "profile_mappings": [
            {
                "atrius_profile": f"{ATRIUS_BASE}{atrius_id}",
                "evaluation_profile": eval_url,
                "resource_type": resource_type,
            }
            for atrius_id, eval_url, resource_type in INVENTORY
        ],
        "condition_encounter_diagnosis_evaluation_profile": q(
            "qicore-condition-encounter-diagnosis"
        ),
        "condition_problems_health_concerns_evaluation_profile": q(
            "qicore-condition-problems-health-concerns"
        ),
    }


def validate_ig(output_dir: Path) -> int:
    missing: list[str] = []
    for atrius_id, _, _ in INVENTORY:
        path = output_dir / f"StructureDefinition-{atrius_id}.json"
        if not path.is_file():
            missing.append(str(path))
    if missing:
        print("Missing IG StructureDefinition files:", file=sys.stderr)
        for path in missing:
            print(f"  - {path}", file=sys.stderr)
        return 1
    print(f"Validated {len(INVENTORY)} profiles under {output_dir}", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Write manifest JSON to this path (default: stdout)",
    )
    parser.add_argument(
        "--validate-ig",
        type=Path,
        help="Validate StructureDefinition files exist in IG output directory",
    )
    args = parser.parse_args()

    if args.validate_ig:
        code = validate_ig(args.validate_ig)
        if code != 0:
            return code

    manifest = build_manifest()
    text = json.dumps(manifest, indent=2) + "\n"

    if args.output:
        args.output.write_text(text, encoding="utf-8")
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

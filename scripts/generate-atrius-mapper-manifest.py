#!/usr/bin/env python3
"""Generate atrius-mapper-manifest.json from the runtime-mapper inventory.

Usage:
  python3 scripts/generate-atrius-mapper-manifest.py [--output PATH] [--validate-ig IG_OUTPUT_DIR]

Without --output, prints JSON to stdout.
With --validate-ig, checks that each atrius_profile id exists as
StructureDefinition-atrius-*.json in the IG build output directory.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ATRIUS_BASE = "https://atrius.in/fhir/r4/atrius-core/StructureDefinition/"
QICORE_SD = "http://hl7.org/fhir/us/qicore/StructureDefinition/"
US_CORE_SD = "http://hl7.org/fhir/us/core/StructureDefinition/"


def q(name: str) -> str:
    return f"{QICORE_SD}{name}"


def us(name: str) -> str:
    return f"{US_CORE_SD}{name}"


# Keep in sync with crates/atrius-runtime-mapper/src/inventory.rs
INVENTORY: list[tuple[str, str, str]] = [
    ("atrius-patient", q("qicore-patient"), "Patient"),
    ("atrius-encounter", q("qicore-encounter"), "Encounter"),
    ("atrius-familymemberhistory", q("qicore-familymemberhistory"), "FamilyMemberHistory"),
    ("atrius-flag", q("qicore-flag"), "Flag"),
    ("atrius-goal", q("qicore-goal"), "Goal"),
    ("atrius-practitioner", q("qicore-practitioner"), "Practitioner"),
    ("atrius-practitionerrole", q("qicore-practitionerrole"), "PractitionerRole"),
    ("atrius-organization", q("qicore-organization"), "Organization"),
    ("atrius-relatedperson", q("qicore-relatedperson"), "RelatedPerson"),
    ("atrius-coverage", q("qicore-coverage"), "Coverage"),
    (
        "atrius-condition-encounter-diagnosis",
        q("qicore-condition-encounter-diagnosis"),
        "Condition",
    ),
    (
        "atrius-condition-problems-health-concerns",
        q("qicore-condition-problems-health-concerns"),
        "Condition",
    ),
    ("atrius-allergyintolerance", q("qicore-allergyintolerance"), "AllergyIntolerance"),
    ("atrius-adverseevent", q("qicore-adverseevent"), "AdverseEvent"),
    ("atrius-bodystructure", q("qicore-bodystructure"), "BodyStructure"),
    ("atrius-device", q("qicore-device"), "Device"),
    ("atrius-devicerequest", q("qicore-devicerequest"), "DeviceRequest"),
    ("atrius-devicerequest-requested", q("qicore-devicerequest-requested"), "DeviceRequest"),
    ("atrius-devicerequest-prohibited", q("qicore-devicerequest-prohibited"), "DeviceRequest"),
    ("atrius-deviceusestatement", q("qicore-deviceusestatement"), "DeviceUseStatement"),
    ("atrius-diagnosticreport-lab", q("qicore-diagnosticreport-lab"), "DiagnosticReport"),
    ("atrius-diagnosticreport-note", q("qicore-diagnosticreport-note"), "DiagnosticReport"),
    ("atrius-imagingstudy", q("qicore-imagingstudy"), "ImagingStudy"),
    ("atrius-immunization", q("qicore-immunization"), "Immunization"),
    ("atrius-immunization-done", q("qicore-immunization-done"), "Immunization"),
    ("atrius-immunization-not-done", q("qicore-immunization-not-done"), "Immunization"),
    (
        "atrius-immunizationrecommendation",
        q("qicore-immunizationrecommendation"),
        "ImmunizationRecommendation",
    ),
    (
        "atrius-immunizationevaluation",
        q("qicore-immunizationevaluation"),
        "ImmunizationEvaluation",
    ),
    ("atrius-location", q("qicore-location"), "Location"),
    ("atrius-observation", q("qicore-observation-lab"), "Observation"),
    ("atrius-observation-body-measurement", q("qicore-simple-observation"), "Observation"),
    (
        "atrius-observation-general-assessment",
        q("qicore-observation-screening-assessment"),
        "Observation",
    ),
    (
        "atrius-observation-lifestyle",
        q("qicore-observation-screening-assessment"),
        "Observation",
    ),
    ("atrius-observation-physical-activity", q("qicore-simple-observation"), "Observation"),
    ("atrius-observation-vital-signs", q("qicore-simple-observation"), "Observation"),
    (
        "atrius-observation-women-health",
        q("qicore-observation-screening-assessment"),
        "Observation",
    ),
    ("atrius-careplan", q("qicore-careplan"), "CarePlan"),
    ("atrius-careplan-assess-plan", q("qicore-careplan"), "CarePlan"),
    ("atrius-careteam", q("qicore-careteam"), "CareTeam"),
    ("atrius-communication", q("qicore-communication"), "Communication"),
    ("atrius-communication-not-done", q("qicore-communication-not-done"), "Communication"),
    ("atrius-communicationrequest", q("qicore-communicationrequest"), "CommunicationRequest"),
    ("atrius-claim", q("qicore-claim"), "Claim"),
    ("atrius-claimresponse", q("qicore-claimresponse"), "ClaimResponse"),
    ("atrius-medication", q("qicore-medication"), "Medication"),
    ("atrius-medicationrequest", q("qicore-medicationrequest"), "MedicationRequest"),
    (
        "atrius-medicationrequest-requested",
        q("qicore-medicationrequest-requested"),
        "MedicationRequest",
    ),
    (
        "atrius-medicationrequest-prohibited",
        q("qicore-medicationrequest-prohibited"),
        "MedicationRequest",
    ),
    ("atrius-medicationstatement", q("qicore-medicationstatement"), "MedicationStatement"),
    (
        "atrius-medicationadministration",
        q("qicore-medicationadministration"),
        "MedicationAdministration",
    ),
    (
        "atrius-medicationadministration-not-done",
        q("qicore-medicationadministration-not-done"),
        "MedicationAdministration",
    ),
    ("atrius-medicationdispense", q("qicore-medicationdispense"), "MedicationDispense"),
    (
        "atrius-medicationdispense-declined",
        q("qicore-medicationdispense-declined"),
        "MedicationDispense",
    ),
    ("atrius-nutritionorder", q("qicore-nutritionorder"), "NutritionOrder"),
    ("atrius-procedure", q("qicore-procedure"), "Procedure"),
    ("atrius-procedure-not-done", q("qicore-procedure-not-done"), "Procedure"),
    ("atrius-servicerequest", q("qicore-servicerequest"), "ServiceRequest"),
    (
        "atrius-servicerequest-not-requested",
        q("qicore-servicerequest-not-requested"),
        "ServiceRequest",
    ),
    (
        "atrius-questionnaireresponse",
        q("qicore-questionnaireresponse"),
        "QuestionnaireResponse",
    ),
    ("atrius-specimen", us("us-core-specimen"), "Specimen"),
    ("atrius-substance", q("qicore-substance"), "Substance"),
    ("atrius-task", q("qicore-task"), "Task"),
    ("atrius-task-rejected", q("qicore-task-rejected"), "Task"),
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

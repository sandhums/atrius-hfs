#!/usr/bin/env python3
"""Import Synthea FHIR R4 data into clinical HFS with Atrius storage profiles.

Part of the **clinical reasoning stack** — see ``docs/clinical-reasoning/data-import.md``.

Transforms plain Synthea resources (no meta.profile) into Atrius-profiled instances
before POST. Intended for dev/test datasets — not a full NDHM terminology crosswalk.

After import, the JVM sidecar reads patient data through **cr-fhir-bridge** (not raw clinical
HFS) so Atrius resources are projected to QI-Core before CQL evaluation.

Usage:
  # One Synthea patient file (Bundle with Patient + related resources)
  ./scripts/import-synthea-atrius.py ./synthea/output/fhir/Adrian_Allen1.json

  # Directory of patient bundles
  ./scripts/import-synthea-atrius.py ./synthea/output/fhir/

  # Preview counts without posting
  ./scripts/import-synthea-atrius.py --dry-run --stats ./path/to/bundle.json

  # Smaller transaction batches (default 25) for large patients
  ./scripts/import-synthea-atrius.py --batch-size 25 ./path/to/dir/

  # Import first N patient bundles from a Synthea output directory
  ./scripts/import-synthea-atrius.py --patients 4 ./synthea/output/fhir/

Environment / flags:
  --base-url     Clinical HFS base (default http://127.0.0.1:8082)
  --tenant       X-Tenant-ID header (default default)
  --validate     POST $validate before first batch (requires HFS profile manifest)

After import, read through cr-fhir-bridge (8081) to see QI-Core projection for the sidecar.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterable

ATRIUS_SD = "https://atrius.in/fhir/r4/atrius-in/StructureDefinition/"
HL7_CONDITION_CATEGORY = "http://terminology.hl7.org/CodeSystem/condition-category"
HL7_CONDITION_CLINICAL = "http://terminology.hl7.org/CodeSystem/condition-clinical"
HL7_CONDITION_VER = "http://terminology.hl7.org/CodeSystem/condition-ver-status"
HL7_OBS_CATEGORY = "http://terminology.hl7.org/CodeSystem/observation-category"
HL7_ALLERGY_CLINICAL = "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical"
HL7_ALLERGY_VER = "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification"

# Default Atrius storage profile per FHIR type (runtime-mapper.md inventory)
DEFAULT_PROFILE: dict[str, str] = {
    "Patient": f"{ATRIUS_SD}atrius-in-patient",
    "Organization": f"{ATRIUS_SD}atrius-in-organization",
    "Practitioner": f"{ATRIUS_SD}atrius-in-practitioner",
    "PractitionerRole": f"{ATRIUS_SD}atrius-in-practitionerrole",
    "Location": f"{ATRIUS_SD}atrius-in-location",
    "RelatedPerson": f"{ATRIUS_SD}atrius-in-relatedperson",
    "Coverage": f"{ATRIUS_SD}atrius-in-coverage",
    "Encounter": f"{ATRIUS_SD}atrius-in-encounter",
    "AllergyIntolerance": f"{ATRIUS_SD}atrius-in-allergyintolerance",
    "ConditionProblemsHealthConcerns": f"{ATRIUS_SD}atrius-in-condition-problems-health-concerns",
    "ConditionEncounterDiagnosis": f"{ATRIUS_SD}atrius-in-condition-encounter-diagnosis",
    "ObservationLab": f"{ATRIUS_SD}atrius-in-observation",
    "ObservationVitalSigns": f"{ATRIUS_SD}atrius-in-observation-vital-signs",
    "ObservationBodyMeasurement": f"{ATRIUS_SD}atrius-in-observation-body-measurement",
    "ObservationGeneralAssessment": f"{ATRIUS_SD}atrius-in-observation-general-assessment",
    "ObservationLifestyle": f"{ATRIUS_SD}atrius-in-observation-lifestyle",
    "ObservationPhysicalActivity": f"{ATRIUS_SD}atrius-in-observation-physical-activity",
    "Procedure": f"{ATRIUS_SD}atrius-in-procedure",
    "MedicationRequest": f"{ATRIUS_SD}atrius-in-medicationrequest",
    "MedicationRequestRequested": f"{ATRIUS_SD}atrius-in-medicationrequest-requested",
    "MedicationStatement": f"{ATRIUS_SD}atrius-in-medicationstatement",
    "Immunization": f"{ATRIUS_SD}atrius-in-immunization",
    "ImmunizationDone": f"{ATRIUS_SD}atrius-in-immunization-done",
    "DiagnosticReportLab": f"{ATRIUS_SD}atrius-in-diagnosticreport-lab",
    "DiagnosticReportNote": f"{ATRIUS_SD}atrius-in-diagnosticreport-note",
    "Claim": f"{ATRIUS_SD}atrius-in-claim",
    "ClaimResponse": f"{ATRIUS_SD}atrius-in-claimresponse",
    "Device": f"{ATRIUS_SD}atrius-in-device",
    "CareTeam": f"{ATRIUS_SD}atrius-in-careteam",
    "Goal": f"{ATRIUS_SD}atrius-in-goal",
    "ServiceRequest": f"{ATRIUS_SD}atrius-in-servicerequest",
}

# Actor-first import order (runtime-mapper.md bundle ordering)
IMPORT_ORDER: dict[str, int] = {
    "Patient": 0,
    "Organization": 1,
    "Practitioner": 2,
    "PractitionerRole": 3,
    "Location": 4,
    "RelatedPerson": 5,
    "Coverage": 6,
    "Encounter": 7,
    "AllergyIntolerance": 8,
    "Condition": 9,
    "Observation": 10,
    "Procedure": 11,
    "MedicationRequest": 12,
    "MedicationStatement": 13,
    "Immunization": 14,
    "DiagnosticReport": 15,
    "Device": 16,
    "CareTeam": 17,
    "Goal": 18,
    "ServiceRequest": 19,
    "Claim": 20,
    "ClaimResponse": 21,
    "DocumentReference": 22,
}

VITAL_SIGN_LOINC = {
    "8480-6",
    "8462-4",
    "85354-9",
    "8867-4",
    "9279-1",
    "8310-5",
    "29463-7",
    "39156-5",
    "8302-2",
    "72514-3",
}
BODY_MEASUREMENT_LOINC = {"39156-5", "8302-2", "29463-7", "3137-7"}


def set_profile(resource: dict[str, Any], profile_url: str) -> None:
    meta = resource.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        resource["meta"] = meta
    meta["profile"] = [profile_url]


def observation_category_codes(resource: dict[str, Any]) -> list[str]:
    codes: list[str] = []
    for cat in resource.get("category") or []:
        if not isinstance(cat, dict):
            continue
        for coding in cat.get("coding") or []:
            if (
                isinstance(coding, dict)
                and coding.get("system") == HL7_OBS_CATEGORY
                and coding.get("code")
            ):
                codes.append(str(coding["code"]))
    return codes


def observation_loinc_codes(resource: dict[str, Any]) -> list[str]:
    codes: list[str] = []
    code = resource.get("code")
    if not isinstance(code, dict):
        return codes
    for coding in code.get("coding") or []:
        if isinstance(coding, dict) and coding.get("code"):
            codes.append(str(coding["code"]))
    return codes


def pick_observation_profile(resource: dict[str, Any]) -> str:
    categories = observation_category_codes(resource)
    loinc = set(observation_loinc_codes(resource))

    if "vital-signs" in categories or loinc & VITAL_SIGN_LOINC:
        return DEFAULT_PROFILE["ObservationVitalSigns"]
    if loinc & BODY_MEASUREMENT_LOINC:
        return DEFAULT_PROFILE["ObservationBodyMeasurement"]
    if any(c in categories for c in ("social-history", "survey")):
        return DEFAULT_PROFILE["ObservationLifestyle"]
    if "exam" in categories:
        return DEFAULT_PROFILE["ObservationGeneralAssessment"]
    if "activity" in categories:
        return DEFAULT_PROFILE["ObservationPhysicalActivity"]
    if "laboratory" in categories or "imaging" in categories:
        return DEFAULT_PROFILE["ObservationLab"]
    return DEFAULT_PROFILE["ObservationLab"]


def pick_condition_profile(resource: dict[str, Any]) -> str:
    if resource.get("encounter", {}).get("reference"):
        return DEFAULT_PROFILE["ConditionEncounterDiagnosis"]
    for cat in resource.get("category") or []:
        for coding in (cat.get("coding") or []) if isinstance(cat, dict) else []:
            if (
                isinstance(coding, dict)
                and coding.get("system") == HL7_CONDITION_CATEGORY
                and coding.get("code") == "encounter-diagnosis"
            ):
                return DEFAULT_PROFILE["ConditionEncounterDiagnosis"]
    return DEFAULT_PROFILE["ConditionProblemsHealthConcerns"]


def ensure_condition_must_support(resource: dict[str, Any], profile_url: str) -> None:
    if not resource.get("verificationStatus"):
        resource["verificationStatus"] = {
            "coding": [{"system": HL7_CONDITION_VER, "code": "confirmed"}]
        }
    if profile_url == DEFAULT_PROFILE["ConditionEncounterDiagnosis"]:
        if not resource.get("category"):
            resource["category"] = [
                {
                    "coding": [
                        {"system": HL7_CONDITION_CATEGORY, "code": "encounter-diagnosis"}
                    ]
                }
            ]
    elif not resource.get("category"):
        resource["category"] = [
            {
                "coding": [
                    {"system": HL7_CONDITION_CATEGORY, "code": "problem-list-item"}
                ]
            }
        ]
    if not resource.get("clinicalStatus"):
        resource["clinicalStatus"] = {
            "coding": [{"system": HL7_CONDITION_CLINICAL, "code": "active"}]
        }


def ensure_allergy_must_support(resource: dict[str, Any]) -> None:
    if not resource.get("clinicalStatus"):
        resource["clinicalStatus"] = {
            "coding": [{"system": HL7_ALLERGY_CLINICAL, "code": "active"}]
        }
    if not resource.get("verificationStatus"):
        resource["verificationStatus"] = {
            "coding": [{"system": HL7_ALLERGY_VER, "code": "confirmed"}]
        }
    if not resource.get("type"):
        resource["type"] = "allergy"


def ensure_observation_must_support(resource: dict[str, Any]) -> None:
    status = resource.get("status")
    if status in (None, "registered", "preliminary", "unknown"):
        resource["status"] = "final"


def ensure_encounter_must_support(resource: dict[str, Any]) -> None:
    if not resource.get("status"):
        resource["status"] = "finished"
    if not resource.get("class"):
        resource["class"] = {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "AMB",
            "display": "ambulatory",
        }


def pick_medication_request_profile(resource: dict[str, Any]) -> str:
    status = str(resource.get("status", "")).lower()
    if status in ("active", "completed", "on-hold"):
        return DEFAULT_PROFILE["MedicationRequestRequested"]
    return DEFAULT_PROFILE["MedicationRequest"]


def pick_immunization_profile(resource: dict[str, Any]) -> str:
    if str(resource.get("status", "")).lower() == "completed":
        return DEFAULT_PROFILE["ImmunizationDone"]
    return DEFAULT_PROFILE["Immunization"]


def pick_diagnostic_report_profile(resource: dict[str, Any]) -> str:
    categories = observation_category_codes(resource)
    if "laboratory" in categories or "LAB" in categories:
        return DEFAULT_PROFILE["DiagnosticReportLab"]
    return DEFAULT_PROFILE["DiagnosticReportNote"]


TransformFn = Callable[[dict[str, Any]], None]
TRANSFORMERS: dict[str, TransformFn] = {}


def _simple(profile_key: str, extra: TransformFn | None = None) -> TransformFn:
    profile = DEFAULT_PROFILE[profile_key]

    def apply(resource: dict[str, Any]) -> None:
        if extra:
            extra(resource)
        set_profile(resource, profile)

    return apply


TRANSFORMERS.update(
    {
        "Patient": _simple("Patient"),
        "Organization": _simple("Organization"),
        "Practitioner": _simple("Practitioner"),
        "PractitionerRole": _simple("PractitionerRole"),
        "Location": _simple("Location"),
        "RelatedPerson": _simple("RelatedPerson"),
        "Coverage": _simple("Coverage"),
        "Procedure": _simple("Procedure"),
        "MedicationStatement": _simple("MedicationStatement"),
        "Device": _simple("Device"),
        "CareTeam": _simple("CareTeam"),
        "Goal": _simple("Goal"),
        "ServiceRequest": _simple("ServiceRequest"),
        "Claim": _simple("Claim"),
        "ClaimResponse": _simple("ClaimResponse"),
        "Encounter": _simple("Encounter", ensure_encounter_must_support),
        "AllergyIntolerance": _simple("AllergyIntolerance", ensure_allergy_must_support),
        "MedicationRequest": lambda r: set_profile(
            r, pick_medication_request_profile(r)
        ),
        "Immunization": lambda r: set_profile(r, pick_immunization_profile(r)),
        "DiagnosticReport": lambda r: set_profile(
            r, pick_diagnostic_report_profile(r)
        ),
        "DocumentReference": _simple("DiagnosticReportNote"),
    }
)


def _transform_condition(resource: dict[str, Any]) -> None:
    profile = pick_condition_profile(resource)
    ensure_condition_must_support(resource, profile)
    set_profile(resource, profile)


def _transform_observation(resource: dict[str, Any]) -> None:
    ensure_observation_must_support(resource)
    set_profile(resource, pick_observation_profile(resource))


TRANSFORMERS["Condition"] = _transform_condition
TRANSFORMERS["Observation"] = _transform_observation


def transform_resource(resource: dict[str, Any]) -> dict[str, Any] | None:
    rt = resource.get("resourceType")
    if not isinstance(rt, str) or rt not in TRANSFORMERS:
        return None
    if not resource.get("id"):
        return None

    out = json.loads(json.dumps(resource))
    TRANSFORMERS[rt](out)
    return out


def list_patient_bundles(directory: Path) -> list[Path]:
    """Synthea patient bundles in a directory (exclude hospital/practitioner master files)."""
    return [
        child
        for child in sorted(directory.glob("*.json"))
        if "information" not in child.name.lower()
    ]


def find_master_file(directory: Path, prefix: str) -> Path | None:
    matches = sorted(directory.glob(f"{prefix}*.json"))
    return matches[0] if matches else None


def resolve_inputs(inputs: list[Path], patients: int) -> list[Path]:
    if not patients:
        return inputs
    resolved: list[Path] = []
    for path in inputs:
        if path.is_dir():
            bundles = list_patient_bundles(path)
            if not bundles:
                raise SystemExit(f"No patient bundles found in {path}")
            for prefix in ("hospitalInformation", "practitionerInformation"):
                master = find_master_file(path, prefix)
                if master:
                    resolved.append(master)
            selected = bundles[:patients]
            resolved.extend(selected)
            print(
                f"Using {len(selected)} patient bundle(s) from {path}:",
                file=sys.stderr,
            )
            for bundle in selected:
                print(f"  {bundle.name}", file=sys.stderr)
        else:
            resolved.append(path)
    return resolved


def iter_input_paths(paths: list[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        if path.is_dir():
            for child in sorted(path.glob("**/*.json")):
                yield from iter_input_paths([child])
            continue
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".ndjson" or (
            path.suffix != ".json" and "\n{" in text.strip()[:500]
        ):
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
        else:
            doc = json.loads(text)
            if doc.get("resourceType") == "Bundle":
                for entry in doc.get("entry") or []:
                    res = entry.get("resource")
                    if isinstance(res, dict):
                        yield res
            elif isinstance(doc, dict):
                yield doc


def post_json(url: str, body: dict[str, Any], tenant: str) -> tuple[int, str]:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
            "X-Tenant-ID": tenant,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def validate_resource(base_url: str, resource: dict[str, Any], tenant: str) -> bool:
    rt = resource["resourceType"]
    status, body = post_json(f"{base_url.rstrip('/')}/{rt}/$validate", resource, tenant)
    if status >= 400:
        print(f"  $validate {rt} HTTP {status}: {body[:500]}", file=sys.stderr)
        return False
    outcome = json.loads(body)
    errors = [
        i
        for i in outcome.get("issue", [])
        if i.get("severity") in ("error", "fatal")
    ]
    if errors:
        print(f"  $validate {rt} issues: {json.dumps(errors[:3], indent=2)}", file=sys.stderr)
        return False
    return True


def dedupe_resources(resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for r in resources:
        rt = r.get("resourceType")
        rid = r.get("id")
        if not isinstance(rt, str) or not isinstance(rid, str):
            continue
        key = (rt, rid)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def print_stats(
    raw: Counter[str],
    transformed: Counter[str],
    skipped: Counter[str],
) -> None:
    print("Import summary (resourceType -> count):")
    all_types = sorted(set(raw) | set(transformed) | set(skipped))
    for rt in all_types:
        print(
            f"  {rt:22} raw={raw.get(rt, 0):4}  "
            f"atrius={transformed.get(rt, 0):4}  skipped={skipped.get(rt, 0):4}"
        )
    print(
        f"  {'TOTAL':22} raw={sum(raw.values()):4}  "
        f"atrius={sum(transformed.values()):4}  skipped={sum(skipped.values()):4}"
    )


def import_resources(
    resources: list[dict[str, Any]],
    *,
    base_url: str,
    tenant: str,
    dry_run: bool,
    stats_only: bool,
    do_validate: bool,
    batch_size: int,
    import_limit: int = 0,
) -> int:
    raw_counts: Counter[str] = Counter()
    transformed_counts: Counter[str] = Counter()
    skipped_counts: Counter[str] = Counter()

    transformed: list[dict[str, Any]] = []
    for r in resources:
        rt = r.get("resourceType")
        if isinstance(rt, str):
            raw_counts[rt] += 1
        t = transform_resource(r)
        if t is None:
            if isinstance(rt, str):
                skipped_counts[rt] += 1
            continue
        transformed_counts[t["resourceType"]] += 1
        transformed.append(t)

    transformed = dedupe_resources(transformed)
    transformed.sort(
        key=lambda r: (
            IMPORT_ORDER.get(str(r.get("resourceType")), 99),
            str(r.get("resourceType")),
            str(r.get("id")),
        )
    )

    print_stats(raw_counts, transformed_counts, skipped_counts)

    if import_limit and len(transformed) > import_limit:
        transformed = transformed[:import_limit]
        print(f"\nCapped to --import-limit {import_limit} Atrius-profiled resources")

    if not transformed:
        print("No supported resources to import.", file=sys.stderr)
        return 1

    if dry_run or stats_only:
        if not stats_only and transformed:
            print("\nSample transformed resource:")
            print(json.dumps(transformed[0], indent=2))
        return 0

    if do_validate:
        seen_types: set[str] = set()
        for res in transformed:
            rt = res["resourceType"]
            if rt in seen_types:
                continue
            seen_types.add(rt)
            if not validate_resource(base_url, res, tenant):
                print("Validation failed; aborting import.", file=sys.stderr)
                return 2

    total = 0
    base = base_url.rstrip("/")
    for i in range(0, len(transformed), batch_size):
        chunk = transformed[i : i + batch_size]
        bundle = {
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [
                {
                    "request": {
                        "method": "PUT",
                        "url": f"{r['resourceType']}/{r['id']}",
                    },
                    "resource": r,
                }
                for r in chunk
            ],
        }
        status, body = post_json(f"{base}/", bundle, tenant)
        if status >= 400:
            print(
                f"Transaction batch {i // batch_size + 1} failed HTTP {status}:\n{body}",
                file=sys.stderr,
            )
            return 1
        total += len(chunk)
        print(f"  batch {i // batch_size + 1}: {len(chunk)} resources (HTTP {status})")

    print(f"Imported {total} Atrius-profiled resources")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="Synthea JSON Bundle, NDJSON file, or directory",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8082",
        help="Clinical HFS base URL",
    )
    parser.add_argument("--tenant", default="default", help="X-Tenant-ID")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print transform counts only (no POST)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run $validate once per resource type before import",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Resources per transaction Bundle (default 25)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max raw resources to read")
    parser.add_argument(
        "--import-limit",
        type=int,
        default=0,
        help="Max Atrius-profiled resources to import (after transform/dedupe/sort)",
    )
    parser.add_argument(
        "--patients",
        type=int,
        default=0,
        help="When input is a directory, import this many patient bundle files",
    )
    args = parser.parse_args()

    input_paths = resolve_inputs(args.inputs, max(0, args.patients))

    resources: list[dict[str, Any]] = []
    for doc in iter_input_paths(input_paths):
        resources.append(doc)
        if args.limit and len(resources) >= args.limit:
            break

    return import_resources(
        resources,
        base_url=args.base_url,
        tenant=args.tenant,
        dry_run=args.dry_run,
        stats_only=args.stats,
        do_validate=args.validate,
        batch_size=max(1, args.batch_size),
        import_limit=max(0, args.import_limit),
    )


if __name__ == "__main__":
    raise SystemExit(main())

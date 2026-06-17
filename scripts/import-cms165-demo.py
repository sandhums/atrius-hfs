#!/usr/bin/env python3
"""Seed CMS165 demo terminology + clinical chart for ``cms165-demo``.

CMS165 / AdultOutpatientEncounters retrieve outpatient Encounters with
``type:in`` ValueSets whose compose lists **CPT** codes. HTS does not ship CPT
(AMA license), so ``$expand`` returns zero codes and **Qualifying Encounters**
is empty unless minimal CPT concepts are seeded for dev.

This script:
1. Seeds CPT codes referenced by AdultOutpatientEncounters ValueSets (from VSAC
   compose already in ``hts.db``).
2. Replaces ``cms165-demo`` chart data (Patient, one qualifying Encounter,
   hypertension Condition, BP Observation) aligned with the sidecar default
   measurement period (current calendar year).

Usage:
  ./scripts/import-cms165-demo.py
  ./scripts/import-cms165-demo.py --clinical-only
  ./scripts/import-cms165-demo.py --hts-url http://127.0.0.1:9091 --verify

See ``docs/clinical-reasoning/data-import.md``.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = REPO_ROOT / "data/clinical-reasoning/cms165-demo.bundle.json"
HTS_DB_DEFAULT = REPO_ROOT / "data/hts.db"
CPT_SYSTEM = "http://www.ama-assn.org/go/cpt"

# AdultOutpatientEncounters encounter-type ValueSets (from sidecar retrieve traces).
AOE_ENCOUNTER_VALUE_SETS = [
    "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.101.12.1001",
    "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.526.3.1240",
    "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.101.12.1025",
    "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.101.12.1023",
    "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.101.12.1016",
    "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.101.12.1089",
    "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.101.12.1080",
]


def collect_cpt_codes_from_hts(db_path: Path) -> dict[str, str]:
    """Walk VSAC compose graphs in HTS and collect explicit CPT concept codes."""
    if not db_path.is_file():
        raise FileNotFoundError(f"HTS database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    codes: dict[str, str] = {}
    seen: set[str] = set()

    def walk(url: str, depth: int = 0) -> None:
        if depth > 8 or url in seen:
            return
        seen.add(url)
        row = conn.execute(
            "SELECT compose_json FROM value_sets WHERE url = ?", (url,)
        ).fetchone()
        if not row:
            return
        compose = json.loads(row[0])
        for inc in compose.get("include", []):
            if inc.get("system") == CPT_SYSTEM:
                for concept in inc.get("concept", []):
                    code = concept.get("code")
                    if code:
                        codes[code] = concept.get("display") or code
            for nested in inc.get("valueSet", []) or []:
                walk(nested, depth + 1)

    for root in AOE_ENCOUNTER_VALUE_SETS:
        walk(root)
    conn.close()
    if not codes:
        raise RuntimeError(
            "No CPT codes found in HTS ValueSet compose — import VSAC terminology first"
        )
    return codes


def post_json(url: str, body: dict, *, tenant: str = "default") -> tuple[int, str]:
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


def seed_cpt(hts_url: str, db_path: Path) -> None:
    codes = collect_cpt_codes_from_hts(db_path)
    print(f"Seeding {len(codes)} CPT demo concepts to HTS ({hts_url})")

    bundle = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "resource": {
                    "resourceType": "CodeSystem",
                    "id": "cpt-cms165-demo",
                    "url": CPT_SYSTEM,
                    "version": "cms165-demo",
                    "name": "CPTCMS165Demo",
                    "title": "CPT codes for CMS165 demo (VSAC-enumerated subset)",
                    "status": "active",
                    "content": "complete",
                    "description": (
                        "Dev-only subset of CPT codes from VSAC ValueSet compose for "
                        "AdultOutpatientEncounters. Not a full CPT release."
                    ),
                    "concept": [
                        {"code": code, "display": display}
                        for code, display in sorted(codes.items())
                    ],
                },
                "request": {"method": "POST", "url": "CodeSystem"},
            }
        ],
    }
    status, body = post_json(f"{hts_url.rstrip('/')}/import", bundle)
    if status >= 400:
        print(f"HTS import failed HTTP {status}:\n{body[:2000]}", file=sys.stderr)
        raise SystemExit(1)
    print("HTS CPT seed OK:", body[:300])


def import_clinical(clinical_url: str, *, tenant: str) -> None:
    if not BUNDLE_PATH.is_file():
        raise FileNotFoundError(BUNDLE_PATH)
    bundle = json.loads(BUNDLE_PATH.read_text(encoding="utf-8"))
    print(f"Importing clinical bundle from {BUNDLE_PATH.name} -> {clinical_url}")
    status, body = post_json(f"{clinical_url.rstrip('/')}/", bundle, tenant=tenant)
    if status >= 400:
        print(f"Clinical import failed HTTP {status}:\n{body[:2000]}", file=sys.stderr)
        raise SystemExit(1)
    print("Clinical import OK")


def delete_stale_resources(clinical_url: str, patient_id: str, *, tenant: str) -> None:
    """Remove legacy cms165-demo resources not in the tuned bundle."""
    keep = {
        ("Patient", patient_id),
        ("Encounter", "cms165-demo-enc-1"),
        ("Condition", "cms165-demo-htn"),
        ("Observation", "cms165-demo-bp"),
    }
    for rtype in ("Encounter", "Condition", "Observation"):
        url = f"{clinical_url.rstrip('/')}/{rtype}?patient={patient_id}"
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/fhir+json", "X-Tenant-ID": tenant},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            bundle = json.loads(resp.read().decode("utf-8"))
        for entry in bundle.get("entry", []):
            resource = entry.get("resource") or {}
            rid = resource.get("id")
            if not rid or (rtype, rid) in keep:
                continue
            del_url = f"{clinical_url.rstrip('/')}/{rtype}/{rid}"
            del_req = urllib.request.Request(
                del_url,
                headers={"X-Tenant-ID": tenant},
                method="DELETE",
            )
            try:
                with urllib.request.urlopen(del_req, timeout=60) as del_resp:
                    print(f"Deleted stale {rtype}/{rid} HTTP {del_resp.status}")
            except urllib.error.HTTPError as e:
                print(f"Delete {rtype}/{rid} HTTP {e.code}", file=sys.stderr)


def verify(sidecar_url: str, bridge_url: str, hts_url: str, kr_url: str) -> None:
    patient = "cms165-demo"
    mp = {
        "Measurement Period": {
            "low": "2026-01-01",
            "high": "2026-12-31",
            "lowClosed": True,
            "highClosed": True,
        }
    }

    vs = "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.101.12.1001"
    expand_body = {
        "resourceType": "Parameters",
        "parameter": [{"name": "url", "valueUri": vs}],
    }
    status, expand_resp = post_json(f"{hts_url.rstrip('/')}/ValueSet/$expand", expand_body)
    total = json.loads(expand_resp).get("expansion", {}).get("total", 0)
    print(f"HTS expand office visit VS total: {total}")

    enc_search = (
        f"{bridge_url.rstrip('/')}/Encounter?patient={patient}"
        f"&type:in={vs}"
    )
    req = urllib.request.Request(enc_search, headers={"Accept": "application/fhir+json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        enc_n = len(json.loads(resp.read()).get("entry", []))
    print(f"Bridge qualifying encounter search matches: {enc_n}")

    for library, version, expr in [
        ("AdultOutpatientEncounters", "4.19.000", "Qualifying Encounters"),
        ("CMS165FHIRControllingHighBloodPressure", "0.3.000", "Initial Population"),
        ("CMS165FHIRControllingHighBloodPressure", "0.3.000", "Numerator"),
    ]:
        body = {
            "libraryId": library,
            "libraryVersion": version,
            "expression": expr,
            "hfsBaseUrl": bridge_url,
            "htsBaseUrl": hts_url,
            "libraryBaseUrl": kr_url,
            "resolveLibraryArtifactsFromFhir": True,
            "patientId": patient,
            "parameters": mp,
        }
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            f"{sidecar_url.rstrip('/')}/v1/evaluate/expression",
            data=data,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.loads(resp.read()).get("result")
                print(f"{library}/{expr}: {result}")
        except urllib.error.HTTPError as e:
            print(f"{library}/{expr}: ERROR HTTP {e.code} {e.read()[:200]!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hts-url",
        default="http://127.0.0.1:9091",
        help="HTS base URL",
    )
    parser.add_argument(
        "--clinical-url",
        default="http://127.0.0.1:8082",
        help="Clinical HFS base URL (Atrius profiles)",
    )
    parser.add_argument(
        "--bridge-url",
        default="http://127.0.0.1:8081",
        help="Bridge URL for verify searches",
    )
    parser.add_argument(
        "--sidecar-url",
        default="http://127.0.0.1:8088",
        help="JVM sidecar URL for verify",
    )
    parser.add_argument(
        "--kr-url",
        default="http://127.0.0.1:8079",
        help="KR HFS URL for verify",
    )
    parser.add_argument(
        "--hts-db",
        type=Path,
        default=HTS_DB_DEFAULT,
        help="Path to hts.db for CPT compose walk",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="X-Tenant-ID for clinical HFS",
    )
    parser.add_argument(
        "--clinical-only",
        action="store_true",
        help="Skip CPT seeding",
    )
    parser.add_argument(
        "--terminology-only",
        action="store_true",
        help="Only seed CPT to HTS",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not delete stale cms165-demo Encounters/Conditions/Observations",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run sidecar expression checks after import",
    )
    args = parser.parse_args()

    if not args.clinical_only:
        seed_cpt(args.hts_url, args.hts_db)

    if not args.terminology_only:
        if not args.no_clean:
            delete_stale_resources(args.clinical_url, "cms165-demo", tenant=args.tenant)
        import_clinical(args.clinical_url, tenant=args.tenant)

    if args.verify or (not args.terminology_only and not args.clinical_only):
        verify(
            args.sidecar_url,
            args.bridge_url,
            args.hts_url,
            args.kr_url,
        )

    print(
        "\nNext: restart HTS if it was running during CPT seed (CodeSystem cache), "
        "then curl CDS invoke or $apply for cms165-demo."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

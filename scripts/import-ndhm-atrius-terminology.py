#!/usr/bin/env python3
"""Import NDHM + Atrius IG CodeSystems and ValueSets into HTS (P0 #4).

Loads published NDHM 6.5.0 terminology from a local ndhm.in mirror and Atrius-owned
CodeSystem/ValueSet JSON from the built IG output directory, then POSTs them to HTS
``/import`` in chunked Bundles. HL7 core and THO terminology come from the HTS
bootstrap packages (``HTS_BOOTSTRAP_DIR``), SNOMED CT / LOINC / ICD from their
own loaders; this script does not stub any of them.

Usage:
  python3 scripts/import-ndhm-atrius-terminology.py \\
    --hts-url http://127.0.0.1:9091 \\
    --ndhm-dir ~/Downloads/ndhm \\
    --atrius-ig-output ../../AtriusIGDraft/output

``--fhir-version`` (default r4) checks that HTS reports the matching
``fhirVersion``; one HTS database holds one FHIR version.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ATRIUS_IN = "https://atrius.in/fhir/r4/atrius-in"
NDHM = "https://nrces.in/ndhm/fhir/r4"
EXPECTED_FHIR_PREFIX = {"r4": "4.0", "r5": "5.0"}
DEFAULT_NDHM_DIR = Path.home() / "Downloads" / "ndhm"
DEFAULT_IG_OUTPUT = Path(__file__).resolve().parents[2].parent / "AtriusIGDraft" / "output"


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def post_bundle(hts_url: str, entries: list[dict], *, finalize: bool = False) -> str:
    """POST one chunk. Non-final chunks defer the concept-closure rebuild; the
    final one (``finalize``) rebuilds every closure left missing, which on a
    large HTS outlasts its 30 s request timeout if done per chunk."""
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{"resource": resource} for resource in entries],
    }
    payload = json.dumps(bundle).encode("utf-8")
    request = urllib.request.Request(
        f"{hts_url.rstrip('/')}/import?{'finalize' if finalize else 'defer_closure'}=true",
        data=payload,
        headers={"Content-Type": "application/fhir+json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTS import failed ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"HTS not reachable at {hts_url}: {exc}") from exc

    if body.strip():
        print(body[:500])
    return body


def finalize_closures(hts_url: str) -> None:
    """Re-send ``finalize=true`` with an empty Bundle until HTS stops reporting
    the closure rebuild as pending."""
    for _ in range(60):
        body = post_bundle(hts_url, [], finalize=True)
        try:
            pending = json.loads(body).get("closures_rebuild") == "pending"
        except (ValueError, AttributeError):
            pending = False
        if not pending:
            return
        time.sleep(5)
    raise SystemExit("HTS closure rebuild still pending after 5 minutes")


def collect_terminology(directory: Path, prefix: str) -> list[dict]:
    resources: list[dict] = []
    for path in sorted(directory.glob(f"{prefix}-*.json")):
        resource = load_json(path)
        if resource.get("resourceType") not in ("CodeSystem", "ValueSet"):
            continue
        resources.append(resource)
    return resources


def require_hts_fhir_version(hts_url: str, fhir_version: str) -> None:
    request = urllib.request.Request(
        f"{hts_url.rstrip('/')}/metadata", headers={"Accept": "application/fhir+json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            reported = json.loads(response.read().decode("utf-8")).get("fhirVersion")
    except urllib.error.URLError as exc:
        raise SystemExit(f"HTS not reachable at {hts_url}: {exc}") from exc
    prefix = EXPECTED_FHIR_PREFIX[fhir_version]
    if not isinstance(reported, str) or not reported.startswith(prefix):
        raise SystemExit(
            f"HTS reports fhirVersion {reported!r}; --fhir-version {fhir_version} expects "
            f"{prefix}.x (one HTS database per FHIR version)"
        )


def chunked(items: list[dict], size: int) -> list[list[dict]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def expansion_contains(result: dict) -> list:
    """Extract expansion.contains from HTS $expand (ValueSet) or FHIR Parameters return."""
    if result.get("resourceType") == "ValueSet":
        return result.get("expansion", {}).get("contains", []) or []

    for parameter in result.get("parameter", []):
        if parameter.get("name") != "return":
            continue
        resource = parameter.get("resource")
        if isinstance(resource, dict):
            return resource.get("expansion", {}).get("contains", []) or []
    return []


def verify_expand(hts_url: str, value_set_url: str) -> None:
    payload = json.dumps(
        {
            "resourceType": "Parameters",
            "parameter": [{"name": "url", "valueUri": value_set_url}],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{hts_url.rstrip('/')}/ValueSet/$expand",
        data=payload,
        headers={"Content-Type": "application/fhir+json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
    contains = expansion_contains(result)
    if not contains:
        raise SystemExit(f"$expand returned no codes for {value_set_url}")
    print(f"  {len(contains)} codes")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hts-url", default="http://127.0.0.1:9091")
    parser.add_argument(
        "--ndhm-dir",
        default=os.environ.get("ATRIUS_NDHM_DIR", str(DEFAULT_NDHM_DIR)),
        help="Local ndhm.in 6.5.0 package mirror (env ATRIUS_NDHM_DIR)",
    )
    parser.add_argument(
        "--atrius-ig-output",
        default=os.environ.get("ATRIUS_IG_OUTPUT", str(DEFAULT_IG_OUTPUT)),
        help="Built Atrius IG output/ directory (env ATRIUS_IG_OUTPUT)",
    )
    parser.add_argument(
        "--fhir-version",
        choices=sorted(EXPECTED_FHIR_PREFIX),
        default="r4",
        help="FHIR version the target HTS was built and bootstrapped for",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10,
        help="Resources per /import request; each CodeSystem costs ~1.5 s on a "
        "SNOMED-sized HTS, and the request timeout is 30 s",
    )
    parser.add_argument("--skip-verify", action="store_true")
    args = parser.parse_args()

    ndhm_dir = Path(args.ndhm_dir).expanduser()
    ig_output = Path(args.atrius_ig_output).expanduser()
    if not ndhm_dir.is_dir():
        raise SystemExit(f"NDHM directory not found: {ndhm_dir}")
    if not ig_output.is_dir():
        raise SystemExit(f"Atrius IG output not found: {ig_output} (run ./_build.sh)")

    ndhm_cs = collect_terminology(ndhm_dir, "CodeSystem")
    ndhm_vs = collect_terminology(ndhm_dir, "ValueSet")
    atrius_cs = collect_terminology(ig_output, "CodeSystem")
    atrius_vs = collect_terminology(ig_output, "ValueSet")
    if not atrius_vs:
        authored = ig_output.parent / "input" / "resources"
        if authored.is_dir():
            authored_cs = collect_terminology(authored, "CodeSystem")
            authored_vs = collect_terminology(authored, "ValueSet")
            if authored_vs:
                print(
                    f"output/ had 0 Atrius ValueSets; using authored {authored} "
                    f"({len(authored_vs)} ValueSets, {len(authored_cs)} CodeSystems)",
                    file=sys.stderr,
                )
                atrius_cs = authored_cs or atrius_cs
                atrius_vs = authored_vs

    require_hts_fhir_version(args.hts_url, args.fhir_version)

    all_resources = ndhm_cs + ndhm_vs + atrius_cs + atrius_vs
    print(
        f"Importing {len(ndhm_cs)} NDHM CodeSystems, {len(ndhm_vs)} NDHM ValueSets, "
        f"{len(atrius_cs)} Atrius CodeSystems, {len(atrius_vs)} Atrius ValueSets "
        f"→ {args.hts_url}"
    )

    for index, batch in enumerate(chunked(all_resources, args.chunk_size), start=1):
        print(f"  batch {index}: {len(batch)} resources")
        post_bundle(args.hts_url, batch)
    print("  rebuilding concept closures")
    finalize_closures(args.hts_url)

    if args.skip_verify:
        return 0

    sample_vs = [
        f"{NDHM}/ValueSet/ndhm-identifier-type-code",
        f"{ATRIUS_IN}/ValueSet/atrius-in-visit-mode",
        f"{ATRIUS_IN}/ValueSet/atrius-in-cds-flag",
    ]
    for url in sample_vs:
        print(f"Verifying $expand: {url}")
        verify_expand(args.hts_url, url)
        print("  ok")

    print("NDHM + Atrius terminology import complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

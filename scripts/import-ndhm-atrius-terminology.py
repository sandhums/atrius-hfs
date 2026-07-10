#!/usr/bin/env python3
"""Import NDHM + Atrius IG CodeSystems and ValueSets into HTS (P0 #4).

Loads published NDHM 6.5.0 terminology from a local ndhm.in mirror and Atrius-owned
CodeSystem/ValueSet JSON from the built IG output directory, then POSTs them to HTS
``/import`` in chunked Bundles.

After this script, run ``atrius-his/scripts/seed-atrius-terminology.py`` for HL7 +
Atrius encounter/composition ValueSets used by HIS smoke tests.

Usage:
  python3 scripts/import-ndhm-atrius-terminology.py \\
    --hts-url http://127.0.0.1:9091 \\
    --ndhm-dir /Users/sandhu/Downloads/ndhm \\
    --atrius-ig-output /Users/sandhu/AtriusIGDraft/output
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

ATRIUS_IN = "https://atrius.in/fhir/r4/atrius-in"
NDHM = "https://nrces.in/ndhm/fhir/r4"


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def post_bundle(hts_url: str, entries: list[dict]) -> None:
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{"resource": resource} for resource in entries],
    }
    payload = json.dumps(bundle).encode("utf-8")
    request = urllib.request.Request(
        f"{hts_url.rstrip('/')}/import",
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


def collect_terminology(directory: Path, prefix: str) -> list[dict]:
    resources: list[dict] = []
    for path in sorted(directory.glob(f"{prefix}-*.json")):
        resource = load_json(path)
        if resource.get("resourceType") not in ("CodeSystem", "ValueSet"):
            continue
        resources.append(resource)
    return resources


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
        default="/Users/sandhu/Downloads/ndhm",
        help="Local ndhm.in 6.5.0 package mirror",
    )
    parser.add_argument(
        "--atrius-ig-output",
        default="/Users/sandhu/AtriusIGDraft/output",
        help="Built Atrius IG output/ directory",
    )
    parser.add_argument("--chunk-size", type=int, default=25)
    parser.add_argument("--skip-verify", action="store_true")
    args = parser.parse_args()

    ndhm_dir = Path(args.ndhm_dir)
    ig_output = Path(args.atrius_ig_output)
    if not ndhm_dir.is_dir():
        raise SystemExit(f"NDHM directory not found: {ndhm_dir}")
    if not ig_output.is_dir():
        raise SystemExit(f"Atrius IG output not found: {ig_output} (run ./_build.sh)")

    ndhm_cs = collect_terminology(ndhm_dir, "CodeSystem")
    ndhm_vs = collect_terminology(ndhm_dir, "ValueSet")
    atrius_cs = collect_terminology(ig_output, "CodeSystem")
    atrius_vs = collect_terminology(ig_output, "ValueSet")

    all_resources = ndhm_cs + ndhm_vs + atrius_cs + atrius_vs
    print(
        f"Importing {len(ndhm_cs)} NDHM CodeSystems, {len(ndhm_vs)} NDHM ValueSets, "
        f"{len(atrius_cs)} Atrius CodeSystems, {len(atrius_vs)} Atrius ValueSets "
        f"→ {args.hts_url}"
    )

    for index, batch in enumerate(chunked(all_resources, args.chunk_size), start=1):
        print(f"  batch {index}: {len(batch)} resources")
        post_bundle(args.hts_url, batch)

    if args.skip_verify:
        return 0

    sample_vs = [
        f"{NDHM}/ValueSet/ndhm-identifier-type-code",
        f"{ATRIUS_IN}/ValueSet/atrius-in-visit-mode",
        f"{ATRIUS_IN}/ValueSet/atrius-in-encounter-class",
    ]
    for url in sample_vs:
        print(f"Verifying $expand: {url}")
        verify_expand(args.hts_url, url)
        print("  ok")

    print("NDHM + Atrius terminology import complete.")
    print("Next: python3 ../atrius-his/scripts/seed-atrius-terminology.py --hts-url", args.hts_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())

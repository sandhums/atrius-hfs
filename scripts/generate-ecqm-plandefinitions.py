#!/usr/bin/env python3
"""Generate CDSHooksServicePlanDefinition resources from eCQM Library/Measure content.

The eCQM QICore Content IG ships Library and Measure resources but **no**
PlanDefinition. Per HL7 Clinical Reasoning ↔ CDS Hooks mapping, each CDS
**Service** should be a ``PlanDefinition`` (``CDSHooksServicePlanDefinition``
profile) with ``action.trigger`` (hook), ``action.input`` (prefetch /
DataRequirement), and ``library`` (CQL).

Typical workflow::

  # Generate JSON files (review before import)
  ./scripts/generate-ecqm-plandefinitions.py --download \\
    --output-dir manifests/plandefinitions-ecqm

  # Generate and PUT to KR HFS
  ./scripts/generate-ecqm-plandefinitions.py --kr-base-url http://127.0.0.1:8079 \\
    --upload

  # Then build CDS manifest from PlanDefinitions (spec-aligned discovery source)
  ./scripts/generate-cds-hooks-manifest.py --from-plandefinition \\
    --output manifests/cds-services-kr-ecqm.json

See ``docs/clinical-reasoning/data-import.md``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow `from ecqm_cds_common import …` when invoked as `python3 scripts/…`.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import tarfile
import tempfile
import urllib.error
from pathlib import Path
from typing import Any

from ecqm_cds_common import (
    CDS_HOOKS_SERVICE_PROFILE,
    CQF_CDS_HOOKS_ENDPOINT,
    DEFAULT_KR_BASE_URL,
    DEFAULT_SKIP_LIBRARY_IDS,
    ECQM_NPM_PACKAGE_URL,
    ECQM_POPULATION_EXPRESSIONS,
    MANIFEST_LIBRARY_PREFIX,
    PLAN_DEFINITION_TYPE_ECA,
    build_measure_index,
    decode_cql,
    default_data_requirements,
    expression_names_from_cql,
    fetch_paged_resources,
    human_title,
    kr_put_resource,
    kr_resource_count,
    library_reference,
    measure_reference,
    pick_expressions,
    population_expressions_from_measure,
    pascal_name,
    slugify,
)

DEFAULT_DOWNLOAD_URL = ECQM_NPM_PACKAGE_URL
DEFAULT_OUTPUT_DIR = Path("manifests/plandefinitions-ecqm")
DEFAULT_PLAN_URL_BASE = "https://atrius.org/PlanDefinition"
PACKAGE_PREFIX = "package/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CDSHooksServicePlanDefinition resources for eCQM libraries.",
    )
    parser.add_argument(
        "tgz",
        nargs="?",
        type=Path,
        help="Path to eCQM NPM .tgz (omit with --kr-base-url or --download)",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help=f"Download eCQM package from {DEFAULT_DOWNLOAD_URL}",
    )
    parser.add_argument(
        "--download-url",
        default=DEFAULT_DOWNLOAD_URL,
        help="Override eCQM package download URL",
    )
    parser.add_argument(
        "--kr-base-url",
        default=None,
        help=f"Read Library/Measure from KR instead of package (default: {DEFAULT_KR_BASE_URL})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Write PlanDefinition JSON files here (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--url-base",
        default=DEFAULT_PLAN_URL_BASE,
        help=f"Canonical PlanDefinition.url prefix (default: {DEFAULT_PLAN_URL_BASE})",
    )
    parser.add_argument(
        "--cds-endpoint-base",
        default=None,
        help="Base URL for cqf-cdsHooksEndpoint extension (e.g. http://127.0.0.1:8095)",
    )
    parser.add_argument(
        "--hook",
        default="patient-view",
        help="CDS Hooks hook / named-event trigger (default: patient-view)",
    )
    parser.add_argument(
        "--populations",
        action="store_true",
        help="One PlanDefinition per eCQM population expression when present",
    )
    parser.add_argument(
        "--include-manifest-libraries",
        action="store_true",
        help="Include Manifest-* NPM libraries (normally excluded)",
    )
    parser.add_argument(
        "--include-helper-libraries",
        action="store_true",
        help="Attempt PlanDefinitions for known helper libraries",
    )
    parser.add_argument(
        "--library-id",
        action="append",
        dest="library_ids",
        help="Restrict to specific Library.id (repeatable)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="PUT each PlanDefinition to KR (--kr-base-url required unless implied)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary only; do not write files or upload",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="X-Tenant-ID header for KR requests (default: default)",
    )
    return parser.parse_args()


def download_tgz(url: str, dest: Path) -> None:
    import urllib.request

    print(f"Downloading {url} -> {dest}", file=sys.stderr)
    urllib.request.urlretrieve(url, dest)


def collect_package_resources(
    tgz_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    libraries: list[dict[str, Any]] = []
    measures: list[dict[str, Any]] = []
    with tarfile.open(tgz_path, "r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            name = member.name
            if not name.startswith(PACKAGE_PREFIX) or not name.endswith(".json"):
                continue
            base = name.removeprefix(PACKAGE_PREFIX)
            if base in ("package.json", ".index.json"):
                continue
            resource_type = base.split("-", 1)[0]
            if resource_type not in ("Library", "Measure"):
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            resource = json.loads(extracted.read())
            if resource.get("resourceType") == "Library":
                libraries.append(resource)
            elif resource.get("resourceType") == "Measure":
                measures.append(resource)
    libraries.sort(key=lambda r: r.get("id", ""))
    measures.sort(key=lambda r: r.get("id", ""))
    return libraries, measures


def load_kr_resources(
    base: str,
    tenant: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    libraries = fetch_paged_resources(base, "Library", tenant)
    measures = fetch_paged_resources(base, "Measure", tenant)
    return libraries, measures


def service_id(library_id: str, expression: str) -> str:
    if expression == "Initial Population":
        return slugify(library_id)
    return slugify(f"{library_id}-{expression}")


def plan_definition_resource(
    *,
    library: dict[str, Any],
    expression: str,
    hook: str,
    url_base: str,
    cds_endpoint_base: str | None,
    measure: dict[str, Any] | None,
) -> dict[str, Any]:
    library_id = library["id"]
    version = library.get("version")
    sid = service_id(library_id, expression)
    canonical_url = f"{url_base.rstrip('/')}/{sid}"

    related_artifact: list[dict[str, Any]] = []
    if measure:
        related_artifact.append(
            {
                "type": "depends-on",
                "display": measure.get("title") or measure.get("id"),
                "resource": measure_reference(measure),
            }
        )

    extensions: list[dict[str, Any]] = []
    if cds_endpoint_base:
        extensions.append(
            {
                "url": CQF_CDS_HOOKS_ENDPOINT,
                "valueUri": f"{cds_endpoint_base.rstrip('/')}/{sid}",
            }
        )

    resource: dict[str, Any] = {
        "resourceType": "PlanDefinition",
        "id": sid,
        "meta": {"profile": [CDS_HOOKS_SERVICE_PROFILE]},
        "url": canonical_url,
        "identifier": [{"use": "official", "value": sid}],
        "version": version,
        "name": pascal_name(library_id, expression),
        "title": human_title(library_id, expression),
        "status": "active",
        "description": (
            f"CDS Hooks service evaluating CQL expression `{expression}` from "
            f"Library `{library_id}`"
            + (f" version {version}" if version else "")
            + (
                f" (Measure {measure['id']})"
                if measure and measure.get("id")
                else ""
            )
            + "."
        ),
        "type": PLAN_DEFINITION_TYPE_ECA,
        "library": [library_reference(library)],
        "action": [
            {
                "id": "cds-evaluate",
                "title": human_title(library_id, expression),
                "description": f"Evaluate `{expression}` at `{hook}` hook.",
                "trigger": [{"type": "named-event", "name": hook}],
                "input": default_data_requirements(),
                "condition": [
                    {
                        "kind": "applicability",
                        "expression": {
                            "language": "text/cql",
                            "expression": expression,
                        },
                    }
                ],
            }
        ],
    }

    if extensions:
        resource["extension"] = extensions
    if related_artifact:
        resource["relatedArtifact"] = related_artifact

    return resource


def generate_plandefinitions(
    libraries: list[dict[str, Any]],
    measures: list[dict[str, Any]],
    *,
    hook: str,
    url_base: str,
    cds_endpoint_base: str | None,
    populations_mode: bool,
    include_manifest_libraries: bool,
    include_helper_libraries: bool,
    filter_ids: set[str] | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    measure_index = build_measure_index(measures)
    plans: list[dict[str, Any]] = []
    skipped: list[str] = []
    seen_ids: set[str] = set()

    for lib in libraries:
        library_id = lib.get("id") or ""
        if not library_id:
            continue
        if filter_ids and library_id not in filter_ids:
            continue
        if not include_manifest_libraries and library_id.startswith(
            MANIFEST_LIBRARY_PREFIX
        ):
            skipped.append(f"{library_id} (Manifest NPM library)")
            continue
        if not include_helper_libraries and library_id in DEFAULT_SKIP_LIBRARY_IDS:
            skipped.append(f"{library_id} (helper/include library)")
            continue

        cql = decode_cql(lib)
        if not cql:
            skipped.append(f"{library_id} (no text/cql content)")
            continue

        defines = expression_names_from_cql(cql)
        measure = measure_index.get(library_id)
        if measure is None and lib.get("url"):
            measure = measure_index.get(lib["url"])
        measure_exprs = (
            population_expressions_from_measure(measure) if measure else None
        )
        expressions = pick_expressions(
            library_id,
            defines,
            populations_mode,
            measure_expressions=measure_exprs,
        )
        if not expressions:
            skipped.append(f"{library_id} (no evaluable define — functions only?)")
            continue

        for expression in expressions:
            if expression not in defines and expression not in ECQM_POPULATION_EXPRESSIONS:
                skipped.append(
                    f"{library_id}/{expression} (expression not in library CQL)"
                )
                continue
            plan = plan_definition_resource(
                library=lib,
                expression=expression,
                hook=hook,
                url_base=url_base,
                cds_endpoint_base=cds_endpoint_base,
                measure=measure,
            )
            if plan["id"] in seen_ids:
                plan["id"] = slugify(f"{plan['id']}-{expression}")[:80]
                plan["url"] = f"{url_base.rstrip('/')}/{plan['id']}"
                plan["identifier"] = [{"use": "official", "value": plan["id"]}]
            if plan["id"] in seen_ids:
                skipped.append(f"{library_id}/{expression} (duplicate PlanDefinition id)")
                continue
            seen_ids.add(plan["id"])
            plans.append(plan)

    return plans, skipped


def main() -> int:
    args = parse_args()
    filter_ids = set(args.library_ids) if args.library_ids else None
    kr_base = args.kr_base_url
    if args.upload and not kr_base:
        kr_base = DEFAULT_KR_BASE_URL

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    tgz_path = args.tgz

    if args.download:
        temp_dir = tempfile.TemporaryDirectory(prefix="ecqm-plandef-")
        tgz_path = Path(temp_dir.name) / "package.tgz"
        download_tgz(args.download_url, tgz_path)

    try:
        if kr_base and not tgz_path:
            libraries, measures = load_kr_resources(kr_base, args.tenant)
            if not libraries:
                print(
                    "KR has no Library resources — import the eCQM NPM package first "
                    "(it contains Library + Measure, not PlanDefinition):",
                    file=sys.stderr,
                )
                print(
                    "  ./scripts/import-ecqm-kr-libraries.py --download "
                    f"--kr-base-url {kr_base}",
                    file=sys.stderr,
                )
                return 1
        elif tgz_path and tgz_path.is_file():
            print(
                "Reading Library + Measure from eCQM package "
                "(no PlanDefinition in package — synthesizing CDSHooksServicePlanDefinition)",
                file=sys.stderr,
            )
            libraries, measures = collect_package_resources(tgz_path)
        else:
            print(
                "Provide a .tgz path, --download, or --kr-base-url",
                file=sys.stderr,
            )
            return 2
    except urllib.error.URLError as exc:
        print(f"Failed to load resources: {exc}", file=sys.stderr)
        return 1

    plans, skipped = generate_plandefinitions(
        libraries,
        measures,
        hook=args.hook,
        url_base=args.url_base,
        cds_endpoint_base=args.cds_endpoint_base,
        populations_mode=args.populations,
        include_manifest_libraries=args.include_manifest_libraries,
        include_helper_libraries=args.include_helper_libraries,
        filter_ids=filter_ids,
    )

    print(
        f"Libraries: {len(libraries)}; Measures: {len(measures)}; "
        f"PlanDefinitions generated: {len(plans)}; skipped: {len(skipped)}",
        file=sys.stderr,
    )
    for line in skipped:
        print(f"  skip: {line}", file=sys.stderr)

    if args.dry_run:
        for plan in plans[:5]:
            lib_ref = plan["library"][0]
            expr = plan["action"][0]["condition"][0]["expression"]["expression"]
            print(
                f"  example: PlanDefinition/{plan['id']} -> {lib_ref} / {expr}",
                file=sys.stderr,
            )
        if len(plans) > 5:
            print(f"  ... and {len(plans) - 5} more", file=sys.stderr)
        return 0

    if not args.upload:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for plan in plans:
            out_path = args.output_dir / f"PlanDefinition-{plan['id']}.json"
            out_path.write_text(
                json.dumps(plan, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        print(f"Wrote {len(plans)} file(s) to {args.output_dir}", file=sys.stderr)

    if args.upload:
        if not kr_base:
            print("--upload requires --kr-base-url", file=sys.stderr)
            return 2
        if not plans:
            print("No PlanDefinitions to upload.", file=sys.stderr)
            return 1
        try:
            lib_on_kr = kr_resource_count(kr_base, "Library", args.tenant)
        except urllib.error.URLError as exc:
            print(f"KR Library check failed: {exc}", file=sys.stderr)
            return 1
        if lib_on_kr == 0:
            print(
                "Warning: KR has no Library resources — $apply will fail until "
                "eCQM libraries are imported:",
                file=sys.stderr,
            )
            print(
                f"  ./scripts/import-ecqm-kr-libraries.py --download --kr-base-url {kr_base}",
                file=sys.stderr,
            )
        uploaded = 0
        failed = 0
        for plan in plans:
            try:
                kr_put_resource(kr_base, plan, args.tenant)
                uploaded += 1
                if uploaded <= 3 or uploaded == len(plans):
                    print(
                        f"  PUT PlanDefinition/{plan['id']}",
                        file=sys.stderr,
                    )
            except urllib.error.HTTPError as exc:
                failed += 1
                detail = exc.read().decode("utf-8", errors="replace")
                print(
                    f"  FAILED PlanDefinition/{plan['id']}: HTTP {exc.code} {detail[:300]}",
                    file=sys.stderr,
                )
            except urllib.error.URLError as exc:
                failed += 1
                print(
                    f"  FAILED PlanDefinition/{plan['id']}: {exc}",
                    file=sys.stderr,
                )
        print(
            f"Upload complete: {uploaded} succeeded, {failed} failed",
            file=sys.stderr,
        )
        if failed:
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

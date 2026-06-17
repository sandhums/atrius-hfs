#!/usr/bin/env python3
"""Generate a CDS Hooks service catalog from KR PlanDefinition or Library resources.

**Spec-aligned path:** generate ``PlanDefinition`` resources first (see
``generate-ecqm-plandefinitions.py``), then build the cds-server manifest from
those PlanDefinitions (``--from-plandefinition``).

**Legacy path:** read ``Library`` resources directly and infer ``libraryId`` /
``expression`` (shortcut until ``PlanDefinition/$apply`` is wired end-to-end).

Part of the clinical reasoning stack — see ``docs/clinical-reasoning/data-import.md``.

Typical workflow::

  ./scripts/generate-ecqm-plandefinitions.py --kr-base-url http://127.0.0.1:8079 --upload
  ./scripts/generate-cds-hooks-manifest.py --from-plandefinition \\
    --kr-base-url http://127.0.0.1:8079 \\
    --output manifests/cds-services-kr-ecqm.json

  CDS_SERVICES_MANIFEST_PATH=./manifests/cds-services-kr-ecqm.json \\
  CDS_HFS_BASE_URL=http://127.0.0.1:8081 \\
    cargo run -p cds-server
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path

# Allow `from ecqm_cds_common import …` when invoked as `python3 scripts/…`.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import urllib.error
from pathlib import Path
from typing import Any

from ecqm_cds_common import (
    DEFAULT_KR_BASE_URL,
    DEFAULT_SKIP_LIBRARY_IDS,
    MANIFEST_LIBRARY_PREFIX,
    data_requirements_to_prefetch,
    missing_plandefinitions_help,
    plan_definition_data_requirements,
    decode_cql,
    expression_names_from_cql,
    fetch_paged_resources,
    human_title,
    kr_put_binary,
    pick_expressions,
    slugify,
)

DEFAULT_OUTPUT = Path("manifests/cds-services-kr-ecqm.json")
CDS_HOOKS_SERVICE_PROFILE = (
    "http://hl7.org/fhir/StructureDefinition/cdshooksserviceplandefinition"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CDS Hooks manifest JSON from KR PlanDefinition or Library resources.",
    )
    parser.add_argument(
        "--kr-base-url",
        default=DEFAULT_KR_BASE_URL,
        help=f"KR HFS base URL (default: {DEFAULT_KR_BASE_URL})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output manifest path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--from-plandefinition",
        action="store_true",
        help="Build manifest from PlanDefinition resources (recommended; spec-aligned)",
    )
    parser.add_argument(
        "--hook",
        default="patient-view",
        help="CDS Hooks hook name for every service (default: patient-view; Library mode only)",
    )
    parser.add_argument(
        "--populations",
        action="store_true",
        help="Emit one service per eCQM population expression (Library mode only)",
    )
    parser.add_argument(
        "--include-manifest-libraries",
        action="store_true",
        help="Include Manifest-* NPM libraries (Library mode only)",
    )
    parser.add_argument(
        "--include-helper-libraries",
        action="store_true",
        help="Attempt hooks for known helper libraries (Library mode only)",
    )
    parser.add_argument(
        "--library-id",
        action="append",
        dest="library_ids",
        help="Restrict to specific Library.id (Library mode only; repeatable)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary only; do not write output or upload",
    )
    parser.add_argument(
        "--upload-binary-id",
        metavar="ID",
        help="POST manifest to KR as Binary/{ID} (contentType application/json)",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="X-Tenant-ID header for KR requests (default: default)",
    )
    return parser.parse_args()


def build_service_entry(
    library: dict[str, Any],
    expression: str,
    hook: str,
    service_id: str | None = None,
    plan_definition_id: str | None = None,
    prefetch: dict[str, str] | None = None,
) -> dict[str, Any]:
    library_id = library["id"]
    version = library.get("version")
    sid = service_id or slugify(library_id)
    if expression != "Initial Population":
        sid = slugify(f"{library_id}-{expression}")

    entry: dict[str, Any] = {
        "id": sid,
        "hook": hook,
        "title": human_title(library_id, expression),
        "description": (
            f"Evaluates CQL expression `{expression}` from Library `{library_id}`"
            + (f" version {version}" if version else "")
            + " via clinical reasoning sidecar."
        ),
        "prefetch": prefetch or data_requirements_to_prefetch(None),
        "libraryId": library_id,
        "libraryVersion": version,
        "expression": expression,
        "resolveFromFhir": True,
        "cdsHooksVersion": "1.0",
    }
    if plan_definition_id:
        entry["planDefinitionId"] = plan_definition_id
    return entry


def library_id_from_reference(reference: str) -> str:
    if reference.startswith("Library/"):
        return reference.removeprefix("Library/").split("|", 1)[0]
    return reference.rsplit("/", 1)[-1].split("|", 1)[0]


def plan_definition_hook(plan: dict[str, Any], default_hook: str) -> str:
    for action in plan.get("action") or []:
        for trigger in action.get("trigger") or []:
            if trigger.get("type") == "named-event" and trigger.get("name"):
                return str(trigger["name"])
    return default_hook


def plan_definition_expression(plan: dict[str, Any]) -> str | None:
    for action in plan.get("action") or []:
        for condition in action.get("condition") or []:
            expr = condition.get("expression") or {}
            if expr.get("language") == "text/cql" and expr.get("expression"):
                return str(expr["expression"])
    return None


def plan_definition_service_id(plan: dict[str, Any]) -> str:
    for ident in plan.get("identifier") or []:
        if ident.get("value"):
            return str(ident["value"])
    plan_id = plan.get("id")
    if plan_id:
        return str(plan_id)
    url = plan.get("url") or ""
    return url.rstrip("/").rsplit("/", 1)[-1]


def is_cds_hooks_plan(plan: dict[str, Any]) -> bool:
    profiles = (plan.get("meta") or {}).get("profile") or []
    if CDS_HOOKS_SERVICE_PROFILE in profiles:
        return True
    for action in plan.get("action") or []:
        for trigger in action.get("trigger") or []:
            if trigger.get("type") == "named-event":
                return True
    return bool(plan.get("library"))


def manifest_from_plans(
    plans: list[dict[str, Any]],
    libraries_by_id: dict[str, dict[str, Any]],
    *,
    default_hook: str,
) -> tuple[dict[str, Any], list[str]]:
    services: list[dict[str, Any]] = []
    skipped: list[str] = []
    seen_ids: set[str] = set()

    for plan in plans:
        if not is_cds_hooks_plan(plan):
            skipped.append(f"{plan.get('id', '?')} (not a CDS Hooks PlanDefinition)")
            continue

        expression = plan_definition_expression(plan)
        if not expression:
            skipped.append(f"{plan.get('id', '?')} (no CQL condition expression)")
            continue

        library_refs = plan.get("library") or []
        if not library_refs:
            skipped.append(f"{plan.get('id', '?')} (no library reference)")
            continue

        library_id = library_id_from_reference(str(library_refs[0]))
        library = libraries_by_id.get(library_id)
        if library is None:
            skipped.append(
                f"{plan.get('id', '?')} (Library/{library_id} not found on KR)"
            )
            continue

        sid = plan_definition_service_id(plan)
        if sid in seen_ids:
            skipped.append(f"{plan.get('id', '?')} (duplicate service id `{sid}`)")
            continue
        seen_ids.add(sid)

        hook = plan_definition_hook(plan, default_hook)
        prefetch = data_requirements_to_prefetch(
            plan_definition_data_requirements(plan)
        )
        description = plan.get("description") or (
            f"PlanDefinition/{plan.get('id')} — {expression}"
        )

        services.append(
            {
                "id": sid,
                "hook": hook,
                "title": plan.get("title") or human_title(library_id, expression),
                "description": description,
                "prefetch": prefetch,
                "libraryId": library_id,
                "libraryVersion": library.get("version"),
                "expression": expression,
                "resolveFromFhir": True,
                "cdsHooksVersion": "1.0",
                "planDefinitionId": plan.get("id"),
                "planDefinitionUrl": plan.get("url"),
            }
        )

    return {"services": services}, skipped


def generate_manifest_from_libraries(
    libraries: list[dict[str, Any]],
    *,
    hook: str,
    populations_mode: bool,
    include_manifest_libraries: bool,
    include_helper_libraries: bool,
    filter_ids: set[str] | None,
) -> tuple[dict[str, Any], list[str]]:
    services: list[dict[str, Any]] = []
    skipped: list[str] = []
    seen_ids: set[str] = set()

    for lib in libraries:
        library_id = lib.get("id") or ""
        if not library_id:
            continue
        if filter_ids and library_id not in filter_ids:
            continue
        if not include_manifest_libraries and library_id.startswith(MANIFEST_LIBRARY_PREFIX):
            skipped.append(f"{library_id} (Manifest NPM library)")
            continue
        if (
            not include_helper_libraries
            and library_id in DEFAULT_SKIP_LIBRARY_IDS
        ):
            skipped.append(f"{library_id} (helper/include library)")
            continue

        cql = decode_cql(lib)
        if not cql:
            skipped.append(f"{library_id} (no text/cql content)")
            continue

        defines = expression_names_from_cql(cql)
        expressions = pick_expressions(library_id, defines, populations_mode)
        if not expressions:
            skipped.append(f"{library_id} (no evaluable define — functions only?)")
            continue

        for expression in expressions:
            entry = build_service_entry(lib, expression, hook)
            if entry["id"] in seen_ids:
                entry["id"] = slugify(f"{entry['id']}-{expression}")[:80]
            if entry["id"] in seen_ids:
                skipped.append(f"{library_id}/{expression} (duplicate service id)")
                continue
            seen_ids.add(entry["id"])
            services.append(entry)

    return {"services": services}, skipped


def main() -> int:
    args = parse_args()
    base = args.kr_base_url.rstrip("/")
    filter_ids = set(args.library_ids) if args.library_ids else None

    try:
        libraries = fetch_paged_resources(base, "Library", args.tenant)
        if args.from_plandefinition:
            plans = fetch_paged_resources(base, "PlanDefinition", args.tenant)
            if not plans:
                print(
                    "KR has no PlanDefinition resources — cannot build "
                    "--from-plandefinition manifest.",
                    file=sys.stderr,
                )
                print(missing_plandefinitions_help(kr_base=base), file=sys.stderr)
                return 1
            libraries_by_id = {lib["id"]: lib for lib in libraries if lib.get("id")}
            manifest, skipped = manifest_from_plans(
                plans,
                libraries_by_id,
                default_hook=args.hook,
            )
            source_label = f"PlanDefinition ({len(plans)} on KR)"
            if not manifest["services"]:
                print(
                    "No CDS services built from PlanDefinitions "
                    f"({len(plans)} on KR, {len(skipped)} skipped).",
                    file=sys.stderr,
                )
                return 1
        else:
            manifest, skipped = generate_manifest_from_libraries(
                libraries,
                hook=args.hook,
                populations_mode=args.populations,
                include_manifest_libraries=args.include_manifest_libraries,
                include_helper_libraries=args.include_helper_libraries,
                filter_ids=filter_ids,
            )
            source_label = f"Library ({len(libraries)} on KR)"
    except urllib.error.URLError as exc:
        print(f"Failed to fetch resources from {base}: {exc}", file=sys.stderr)
        return 1

    print(
        f"Source: {source_label}; "
        f"CDS services generated: {len(manifest['services'])}; "
        f"skipped: {len(skipped)}",
        file=sys.stderr,
    )
    for line in skipped:
        print(f"  skip: {line}", file=sys.stderr)

    manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    print(f"Manifest size: {len(manifest_bytes):,} bytes", file=sys.stderr)

    if args.dry_run:
        for svc in manifest["services"][:5]:
            extra = ""
            if svc.get("planDefinitionId"):
                extra = f" plan={svc['planDefinitionId']}"
            print(
                f"  example: {svc['id']} -> {svc['libraryId']} / {svc['expression']}{extra}",
                file=sys.stderr,
            )
        if len(manifest["services"]) > 5:
            print(f"  ... and {len(manifest['services']) - 5} more", file=sys.stderr)
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(manifest_bytes)
    print(f"Wrote {args.output}", file=sys.stderr)

    if args.upload_binary_id:
        kr_put_binary(base, args.upload_binary_id, manifest_bytes, args.tenant)
        print(
            f"Uploaded Binary/{args.upload_binary_id} to {base} "
            f"({len(manifest_bytes):,} bytes payload)",
            file=sys.stderr,
        )
        print(
            "Configure cds-server: "
            f"CDS_KR_SERVICES_BINARY_ID={args.upload_binary_id} "
            f"CDS_LIBRARY_BASE_URL={base}",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

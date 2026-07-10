#!/usr/bin/env python3
"""Generate a CDS Hooks service catalog from KR PlanDefinition resources.

PlanDefinitions are authored in AtriusIGDraft and imported to KR
(``import-atrius-kr-libraries.py --clinical-reasoning``). This script reads
those PlanDefinitions (+ linked Libraries for version pins) and writes
``manifests/cds-services-kr.json`` for cds-server.

Prefer the orchestrator when KR may be empty::

  ./scripts/setup-plandefinition-cds-catalog.sh

Manifest-only (PlanDefinitions already on KR)::

  ./scripts/generate-cds-hooks-manifest.py \\
    --kr-base-url http://127.0.0.1:8079 \\
    --output manifests/cds-services-kr.json

See ``docs/clinical-reasoning/data-import.md``.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cds_manifest_common import (
    DEFAULT_KR_BASE_URL,
    data_requirements_to_prefetch,
    fetch_paged_resources,
    human_title,
    kr_put_binary,
    missing_plandefinitions_help,
    plan_definition_data_requirements,
)

DEFAULT_OUTPUT = Path("manifests/cds-services-kr.json")
CDS_HOOKS_SERVICE_PROFILE = (
    "http://hl7.org/fhir/StructureDefinition/cdshooksserviceplandefinition"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CDS Hooks manifest JSON from KR PlanDefinition resources.",
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
        "--hook",
        default="patient-view",
        help="Default CDS Hooks hook when PlanDefinition has no named-event trigger",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary only; do not write output or upload",
    )
    parser.add_argument(
        "--upload-binary-id",
        metavar="ID",
        help="PUT manifest to KR as Binary/{ID} (contentType application/json)",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="X-Tenant-ID header for KR requests (default: default)",
    )
    # Kept for callers that still pass the old flag; PlanDefinition mode is the only path.
    parser.add_argument(
        "--from-plandefinition",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


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


def main() -> int:
    args = parse_args()
    base = args.kr_base_url.rstrip("/")

    try:
        libraries = fetch_paged_resources(base, "Library", args.tenant)
        plans = fetch_paged_resources(base, "PlanDefinition", args.tenant)
        if not plans:
            print(
                "KR has no PlanDefinition resources — cannot build CDS manifest.",
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
        if not manifest["services"]:
            print(
                "No CDS services built from PlanDefinitions "
                f"({len(plans)} on KR, {len(skipped)} skipped).",
                file=sys.stderr,
            )
            return 1
    except urllib.error.URLError as exc:
        print(f"Failed to fetch resources from {base}: {exc}", file=sys.stderr)
        return 1

    print(
        f"Source: PlanDefinition ({len(plans)} on KR); "
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
            print(
                f"  example: {svc['id']} -> {svc['libraryId']} / {svc['expression']} "
                f"plan={svc.get('planDefinitionId')}",
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

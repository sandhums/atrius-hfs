#!/usr/bin/env python3
"""Delete legacy eCQM / synthesized CMS artifacts from KR HFS.

Keeps Atrius-authored knowledge:
  - Library: Atrius*, FHIRHelpers
  - PlanDefinition: atrius*, er-*
  - Measure: Atrius*

Removes CMS* Libraries/Measures and synthesized PlanDefinitions from the old
NPM import path. FHIR DELETE is a soft-delete (``is_deleted=true``) — rows still
appear in Postgres until ``--hard-purge-deleted``.

Usage:
  ./scripts/purge-kr-ecqm-artifacts.py --dry-run
  ./scripts/purge-kr-ecqm-artifacts.py
  ./scripts/purge-kr-ecqm-artifacts.py --hard-purge-deleted \\
      --database-url postgresql://sandhu:parsons02@localhost:5432/fhir_kr
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_KR = os.environ.get("KR_BASE_URL", "http://127.0.0.1:8079")
DEFAULT_DB = os.environ.get("HFS_DATABASE_URL") or os.environ.get("KR_DATABASE_URL")


def kr_request(
    base: str,
    method: str,
    path: str,
    *,
    tenant: str,
    body: bytes | None = None,
) -> tuple[int, Any]:
    url = f"{base.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "Accept": "application/fhir+json",
        "X-Tenant-ID": tenant,
    }
    if body is not None:
        headers["Content-Type"] = "application/fhir+json"
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"text": raw.decode("utf-8", errors="replace")}
        return exc.code, payload


def fetch_all(base: str, resource_type: str, tenant: str) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    offset = 0
    page_size = 200
    while True:
        qs = urllib.parse.urlencode({"_count": page_size, "_offset": offset})
        status, bundle = kr_request(base, "GET", f"{resource_type}?{qs}", tenant=tenant)
        if status >= 400:
            raise RuntimeError(f"GET {resource_type} failed: HTTP {status} {bundle}")
        entries = bundle.get("entry") or []
        for entry in entries:
            res = entry.get("resource")
            if res and res.get("resourceType") == resource_type:
                resources.append(res)
        if len(entries) < page_size:
            break
        offset += page_size
    return resources


def keep_library(library_id: str) -> bool:
    return library_id == "FHIRHelpers" or library_id.startswith("Atrius")


def keep_plan_definition(plan_id: str) -> bool:
    return plan_id.startswith("atrius") or plan_id.startswith("er-")


def keep_measure(measure_id: str) -> bool:
    return measure_id.startswith("Atrius")


def delete_ids(
    base: str,
    tenant: str,
    resource_type: str,
    ids: list[str],
    *,
    dry_run: bool,
) -> int:
    failed = 0
    for rid in ids:
        if dry_run:
            print(f"  would DELETE {resource_type}/{rid}", file=sys.stderr)
            continue
        status, _ = kr_request(
            base, "DELETE", f"{resource_type}/{rid}", tenant=tenant
        )
        if status not in (200, 204, 404, 410):
            print(f"FAIL DELETE {resource_type}/{rid} HTTP {status}", file=sys.stderr)
            failed += 1
        else:
            print(f"deleted {resource_type}/{rid}", file=sys.stderr)
    return failed


def hard_purge_deleted(database_url: str, tenant: str, *, dry_run: bool) -> int:
    """Physically remove soft-deleted rows via ``psql`` (no Python DB driver required)."""
    import shutil
    import subprocess

    if not shutil.which("psql"):
        print("error: psql not found on PATH for --hard-purge-deleted", file=sys.stderr)
        return 1

    count_sql = f"""
SELECT resource_type, COUNT(*)
FROM resources
WHERE tenant_id = '{tenant}' AND is_deleted = true
GROUP BY resource_type
ORDER BY resource_type;
"""
    proc = subprocess.run(
        ["psql", database_url, "-v", "ON_ERROR_STOP=1", "-c", count_sql],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout, file=sys.stderr)
        return 1
    print("soft-deleted rows:", file=sys.stderr)
    print(proc.stdout, file=sys.stderr)

    if dry_run:
        print("dry-run: no hard deletes", file=sys.stderr)
        return 0

    delete_sql = f"""
DELETE FROM resources
WHERE tenant_id = '{tenant}' AND is_deleted = true;
"""
    proc = subprocess.run(
        ["psql", database_url, "-v", "ON_ERROR_STOP=1", "-c", delete_sql],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout, file=sys.stderr)
        return 1
    print(proc.stdout, file=sys.stderr)
    print("hard-purge complete (search_index/fts cascade via FK)", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kr-base-url", default=DEFAULT_KR)
    parser.add_argument("--tenant", default="default")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--also-binary",
        action="store_true",
        help="Also DELETE Binary/cds-services-catalog (stale catalog id)",
    )
    parser.add_argument(
        "--hard-purge-deleted",
        action="store_true",
        help="Physically DELETE soft-deleted rows from Postgres (cleans DB view)",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DB,
        help="Postgres URL for --hard-purge-deleted (default: HFS_DATABASE_URL)",
    )
    args = parser.parse_args()
    base = args.kr_base_url.rstrip("/")

    libraries = fetch_all(base, "Library", args.tenant)
    plans = fetch_all(base, "PlanDefinition", args.tenant)
    measures = fetch_all(base, "Measure", args.tenant)

    lib_delete = [r["id"] for r in libraries if r.get("id") and not keep_library(r["id"])]
    plan_delete = [
        r["id"] for r in plans if r.get("id") and not keep_plan_definition(r["id"])
    ]
    measure_delete = [
        r["id"] for r in measures if r.get("id") and not keep_measure(r["id"])
    ]

    print(
        f"KR {base}: "
        f"Library keep={len(libraries) - len(lib_delete)} delete={len(lib_delete)}; "
        f"PlanDefinition keep={len(plans) - len(plan_delete)} delete={len(plan_delete)}; "
        f"Measure keep={len(measures) - len(measure_delete)} delete={len(measure_delete)}",
        file=sys.stderr,
    )

    failed = 0
    failed += delete_ids(
        base, args.tenant, "PlanDefinition", plan_delete, dry_run=args.dry_run
    )
    failed += delete_ids(
        base, args.tenant, "Measure", measure_delete, dry_run=args.dry_run
    )
    failed += delete_ids(
        base, args.tenant, "Library", lib_delete, dry_run=args.dry_run
    )

    if args.also_binary:
        failed += delete_ids(
            base,
            args.tenant,
            "Binary",
            ["cds-services-catalog"],
            dry_run=args.dry_run,
        )

    if args.hard_purge_deleted:
        if not args.database_url:
            print(
                "error: --hard-purge-deleted needs --database-url or HFS_DATABASE_URL",
                file=sys.stderr,
            )
            return 1
        failed += hard_purge_deleted(
            args.database_url, args.tenant, dry_run=args.dry_run
        )

    if args.dry_run:
        print("dry-run: no deletes", file=sys.stderr)
        return 0

    if failed:
        return 1
    print(
        "Done. Soft-deleted rows remain in Postgres until "
        "--hard-purge-deleted. Restart not required for API; "
        "regenerate catalog if needed.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Import eCQM QICore Content IG libraries (and optionally Measures) into KR HFS.

Part of the **clinical reasoning stack** — see ``docs/clinical-reasoning/data-import.md``.

Unpacks a FHIR NPM package (.tgz) from the eCQM content IG and POSTs FHIR
transaction (or batch) Bundles to a Knowledge Repository HFS instance.

The JVM sidecar loads the **primary** library from ``libraryBaseUrl`` (this KR) but resolves
CQL **include** dependencies via ``hfsBaseUrl`` — configure **cr-fhir-bridge** with
``CR_FHIR_BRIDGE_KR_URL`` pointing at this same KR.

Example:
  ./scripts/import-ecqm-kr-libraries.py --download
  ./scripts/import-ecqm-kr-libraries.py ./ecqm-content-qicore-2025.tgz
  ./scripts/import-ecqm-kr-libraries.py --download --dry-run
  ./scripts/import-ecqm-kr-libraries.py ./pkg.tgz --include Measure --batch-size 1

Default KR base URL matches deploy/kr/.env.kr.example (http://127.0.0.1:8079).
Libraries total ~61 MiB in the 2025.0.0 package; use a small --batch-size
(1–2) so each bundle stays under HFS_MAX_BODY_SIZE (default 10 MiB).
"""

from __future__ import annotations

import argparse
import json
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_DOWNLOAD_URL = (
    "https://build.fhir.org/ig/cqframework/ecqm-content-qicore-2025/package.tgz"
)
DEFAULT_KR_BASE_URL = "http://127.0.0.1:8079"
DEFAULT_RESOURCE_TYPES = ("Library",)
PACKAGE_PREFIX = "package/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import eCQM IG NPM package resources into KR HFS via FHIR Bundles.",
    )
    parser.add_argument(
        "tgz",
        nargs="?",
        type=Path,
        help="Path to gov.healthit.ecqi.ecqms NPM .tgz (omit with --download)",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help=f"Download package from {DEFAULT_DOWNLOAD_URL}",
    )
    parser.add_argument(
        "--download-url",
        default=DEFAULT_DOWNLOAD_URL,
        help="Override NPM package download URL",
    )
    parser.add_argument(
        "--kr-base-url",
        default=DEFAULT_KR_BASE_URL,
        help=f"KR HFS base URL (default: {DEFAULT_KR_BASE_URL})",
    )
    parser.add_argument(
        "--include",
        action="append",
        choices=("Library", "Measure"),
        dest="resource_types",
        help="Resource type to import (repeatable; default: Library only)",
    )
    parser.add_argument(
        "--bundle-type",
        choices=("transaction", "batch"),
        default="transaction",
        help="FHIR Bundle type (default: transaction)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Resources per Bundle POST (default: 1; keep low for large Libraries)",
    )
    parser.add_argument(
        "--method",
        choices=("PUT", "POST"),
        default="PUT",
        help="HTTP method for each entry (default: PUT for idempotent re-import)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List resources and bundle sizes without POSTing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-resource details",
    )
    return parser.parse_args()


def download_tgz(url: str, dest: Path) -> None:
    print(f"Downloading {url} -> {dest}", file=sys.stderr)
    urllib.request.urlretrieve(url, dest)


def collect_resources(
    tgz_path: Path,
    resource_types: Sequence[str],
) -> list[tuple[str, dict[str, Any], int]]:
    """Return [(filename, resource, raw_size), ...] sorted by filename."""
    allowed = set(resource_types)
    found: list[tuple[str, dict[str, Any], int]] = []

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
            if resource_type not in allowed:
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            raw = extracted.read()
            resource = json.loads(raw)
            if resource.get("resourceType") != resource_type:
                continue
            found.append((base, resource, len(raw)))

    found.sort(key=lambda item: item[0].lower())
    return found


def elm_identifier_version(resource: dict[str, Any]) -> str | None:
    """Read ELM library identifier.version from Library.content (if present)."""
    import base64

    for content in resource.get("content") or []:
        if content.get("contentType") != "application/elm+json":
            continue
        data = content.get("data")
        if not data:
            continue
        try:
            elm = json.loads(base64.b64decode(data))
        except (json.JSONDecodeError, ValueError):
            continue
        ident = elm.get("library", {}).get("identifier") or {}
        version = ident.get("version")
        if isinstance(version, str) and version.strip():
            return version.strip()
    return None


def normalize_cql_library_version(resource: dict[str, Any], elm_version: str) -> bool:
    """Align text/cql `library … version '…'` with ELM identifier.version."""
    import base64
    import re

    changed = False
    pattern = re.compile(
        r"(library\s+"
        + re.escape(resource.get("name") or resource.get("id") or "")
        + r"\s+version\s+')([^']+)(')",
        re.IGNORECASE,
    )
    for content in resource.get("content") or []:
        if content.get("contentType") != "text/cql":
            continue
        data = content.get("data")
        if not data:
            continue
        try:
            text = base64.b64decode(data).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            continue
        new_text, n = pattern.subn(rf"\g<1>{elm_version}\g<3>", text, count=1)
        if n:
            content["data"] = base64.b64encode(new_text.encode("utf-8")).decode("ascii")
            changed = True
    return changed


def normalize_library_version(resource: dict[str, Any]) -> bool:
    """Align Library.version and CQL header with ELM identifier.version for sidecar validation."""
    if resource.get("resourceType") != "Library":
        return False
    elm_version = elm_identifier_version(resource)
    if not elm_version:
        return False
    changed = False
    current = resource.get("version")
    if current != elm_version:
        resource["version"] = elm_version
        changed = True
    if normalize_cql_library_version(resource, elm_version):
        changed = True
    return changed


def bundle_entry(
    resource: dict[str, Any],
    method: str,
) -> dict[str, Any]:
    resource_type = resource["resourceType"]
    resource_id = resource.get("id")
    if method == "PUT":
        if not resource_id:
            raise ValueError(f"{resource_type} missing id; use --method POST")
        request_url = f"{resource_type}/{resource_id}"
    else:
        request_url = resource_type

    entry: dict[str, Any] = {
        "resource": resource,
        "request": {"method": method, "url": request_url},
    }
    if resource_id:
        entry["fullUrl"] = f"{resource_type}/{resource_id}"
    return entry


def build_bundle(
    resources: Sequence[dict[str, Any]],
    bundle_type: str,
    method: str,
) -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "type": bundle_type,
        "entry": [bundle_entry(r, method) for r in resources],
    }


def post_bundle(kr_base_url: str, bundle: dict[str, Any]) -> dict[str, Any]:
    url = kr_base_url.rstrip("/") + "/"
    body = json.dumps(bundle).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from KR HFS: {detail}") from exc

    if not payload:
        return {}
    return json.loads(payload)


def summarize_response(response: dict[str, Any]) -> tuple[int, int]:
    ok = 0
    failed = 0
    for entry in response.get("entry", []):
        status = entry.get("response", {}).get("status", "")
        code = status.split()[0] if status else ""
        if code.startswith(("200", "201")):
            ok += 1
        else:
            failed += 1
    return ok, failed


def chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    if size < 1:
        raise ValueError("--batch-size must be >= 1")
    for start in range(0, len(items), size):
        yield items[start : start + size]


def main() -> int:
    args = parse_args()
    resource_types = tuple(args.resource_types or DEFAULT_RESOURCE_TYPES)

    if args.tgz is None and not args.download:
        print("Provide a .tgz path or pass --download", file=sys.stderr)
        return 2

    tgz_path = args.tgz
    temp_dir: tempfile.TemporaryDirectory[str] | None = None

    if args.download:
        temp_dir = tempfile.TemporaryDirectory(prefix="ecqm-kr-import-")
        tgz_path = Path(temp_dir.name) / "package.tgz"
        download_tgz(args.download_url, tgz_path)
    elif tgz_path is None or not tgz_path.is_file():
        print(f"File not found: {tgz_path}", file=sys.stderr)
        return 2

    resources = collect_resources(tgz_path, resource_types)
    if not resources:
        print(
            f"No {', '.join(resource_types)} resources found under {PACKAGE_PREFIX} in {tgz_path}",
            file=sys.stderr,
        )
        return 1

    version_fixes = 0
    for name, resource, _size in resources:
        if normalize_library_version(resource):
            version_fixes += 1
            if args.verbose:
                elm_version = resource.get("version")
                print(
                    f"  version sync {name}: Library.version -> {elm_version} (from ELM)",
                    file=sys.stderr,
                )
    if version_fixes:
        print(
            f"Aligned Library.version with ELM for {version_fixes} resource(s)",
            file=sys.stderr,
        )

    total_bytes = sum(size for _, _, size in resources)
    print(
        f"Found {len(resources)} resource(s) "
        f"({total_bytes / (1024 * 1024):.1f} MiB raw JSON) "
        f"in {tgz_path.name}",
        file=sys.stderr,
    )

    if args.verbose or args.dry_run:
        for name, resource, size in resources:
            print(
                f"  {resource['resourceType']}/{resource.get('id', '?')} "
                f"({size / 1024:.0f} KiB) {name}",
                file=sys.stderr,
            )

    if args.dry_run:
        sample = [r for _, r, _ in resources[: args.batch_size]]
        bundle = build_bundle(sample, args.bundle_type, args.method)
        encoded = json.dumps(bundle)
        print(
            f"Dry run: would POST {len(resources)} resource(s) in "
            f"{(len(resources) + args.batch_size - 1) // args.batch_size} bundle(s); "
            f"first bundle ~{len(encoded) / (1024 * 1024):.1f} MiB",
            file=sys.stderr,
        )
        return 0

    total_ok = 0
    total_failed = 0
    resource_dicts = [r for _, r, _ in resources]
    num_bundles = (len(resource_dicts) + args.batch_size - 1) // args.batch_size

    for index, chunk in enumerate(chunked(resource_dicts, args.batch_size), start=1):
        bundle = build_bundle(chunk, args.bundle_type, args.method)
        bundle_bytes = len(json.dumps(bundle))
        ids = ", ".join(
            f"{r['resourceType']}/{r.get('id', '?')}" for r in chunk
        )
        print(
            f"[{index}/{num_bundles}] POST {args.bundle_type} bundle "
            f"({len(chunk)} resource(s), {bundle_bytes / (1024 * 1024):.1f} MiB): {ids}",
            file=sys.stderr,
        )
        response = post_bundle(args.kr_base_url, bundle)
        ok, failed = summarize_response(response)
        total_ok += ok
        total_failed += failed
        if failed:
            print(
                f"  bundle had {failed} failed entry/entries; "
                f"response saved to stderr",
                file=sys.stderr,
            )
            print(json.dumps(response, indent=2), file=sys.stderr)

    print(
        f"Done: {total_ok} succeeded, {total_failed} failed "
        f"({len(resources)} resource(s) processed)",
        file=sys.stderr,
    )
    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())

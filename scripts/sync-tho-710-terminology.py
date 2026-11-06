#!/usr/bin/env python3
"""Sync HTS to HL7 THO package hl7.terminology.r4#7.1.0 only.

Older THO rows (from VSAC, hl7.terminology 6.x, duplicate v2 table versions, etc.)
are removed using an allowlist extracted from the 7.1.0 NPM package. Individual
CodeSystem/ValueSet resources keep their canonical version (e.g. v3-ActCode 10.0.0),
not the package version string.

FHIR R4 core 4.0.1 also defines terminology.hl7.org resources whose URL THO
dropped (medication-admin-status, medicationdispense-status,
v3-ConfidentialityClassification, older v2 tables). R4 profiles bind them, and
an HTS without them answers `result=false` for every code, so they stay on the
allowlist and are imported from hl7.fhir.r4.core#4.0.1 alongside THO.

Usage:
  # Dry run (default)
  python3 scripts/sync-tho-710-terminology.py

  # Apply prune + re-import (stop HTS first to avoid SQLite lock contention)
  python3 scripts/sync-tho-710-terminology.py --execute

  # After prune, compact the DB (optional, can take minutes on large DBs)
  python3 scripts/sync-tho-710-terminology.py --execute --vacuum
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

THO_PACKAGE_ID = "hl7.terminology.r4"
THO_PACKAGE_VERSION = "7.1.0"
THO_DOWNLOAD_URL = f"https://packages.fhir.org/{THO_PACKAGE_ID}/{THO_PACKAGE_VERSION}"
THO_PREFIX = "http://terminology.hl7.org/"
CORE_PACKAGE_ID = "hl7.fhir.r4.core"
CORE_PACKAGE_VERSION = "4.0.1"
CORE_DOWNLOAD_URL = f"https://packages.fhir.org/{CORE_PACKAGE_ID}/{CORE_PACKAGE_VERSION}"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def terminology_resources(tgz: Path):
    """Yield (archive member name, resource) for every CodeSystem and ValueSet."""
    with tarfile.open(tgz) as archive:
        for name in archive.getnames():
            base = name.rsplit("/", 1)[-1]
            if not name.endswith(".json") or not base.startswith(("CodeSystem-", "ValueSet-")):
                continue
            resource = json.load(archive.extractfile(name))
            if resource.get("resourceType") in ("CodeSystem", "ValueSet") and resource.get("url"):
                yield name, resource


def load_allowlist(tgz: Path) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    code_systems: set[tuple[str, str]] = set()
    value_sets: set[tuple[str, str]] = set()
    for _, resource in terminology_resources(tgz):
        target = code_systems if resource["resourceType"] == "CodeSystem" else value_sets
        target.add((resource["url"], resource.get("version") or ""))
    return code_systems, value_sets


def core_only_members(
    core_tgz: Path,
    cs_allow: set[tuple[str, str]],
    vs_allow: set[tuple[str, str]],
) -> list[tuple[str, dict]]:
    """R4 core terminology.hl7.org resources whose URL THO does not define."""
    tho_urls = {url for url, _ in cs_allow | vs_allow}
    return [
        (name, resource)
        for name, resource in terminology_resources(core_tgz)
        if resource["url"].startswith(THO_PREFIX) and resource["url"] not in tho_urls
    ]


def write_core_subset(core_tgz: Path, members: list[tuple[str, dict]], dest: Path) -> Path:
    """An NPM-shaped tgz of `members` plus the core manifest, for `hts import`."""
    keep = {name for name, _ in members} | {"package/package.json"}
    with tarfile.open(core_tgz) as source, tarfile.open(dest, "w:gz") as target:
        for info in source.getmembers():
            if info.name in keep:
                target.addfile(info, source.extractfile(info))
    return dest


def ensure_tgz(dest: Path, label: str, url: str) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size > 0:
        return dest
    print(f"Downloading {label} …")
    tmp = dest.with_suffix(".tgz.part")
    with urllib.request.urlopen(url, timeout=300) as response:
        tmp.write_bytes(response.read())
    tmp.rename(dest)
    return dest


def write_allowlist_manifest(
    path: Path,
    code_systems: set[tuple[str, str]],
    value_sets: set[tuple[str, str]],
) -> None:
    payload = {
        "package": f"{THO_PACKAGE_ID}#{THO_PACKAGE_VERSION}",
        "core_only_from": f"{CORE_PACKAGE_ID}#{CORE_PACKAGE_VERSION}",
        "code_systems": sorted(
            ({"url": u, "version": v} for u, v in code_systems),
            key=lambda item: (item["url"], item["version"]),
        ),
        "value_sets": sorted(
            ({"url": u, "version": v} for u, v in value_sets),
            key=lambda item: (item["url"], item["version"]),
        ),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def analyze(conn: sqlite3.Connection, allowlist: set[tuple[str, str]], table: str) -> tuple[int, int, int]:
    cur = conn.cursor()
    cur.execute(f"SELECT url, COALESCE(version, '') FROM {table} WHERE url LIKE ?", (f"{THO_PREFIX}%",))
    rows = cur.fetchall()
    keep = sum(1 for url, version in rows if (url, version) in allowlist)
    delete = len(rows) - keep
    return len(rows), keep, delete


def prune_table(
    conn: sqlite3.Connection,
    table: str,
    allowlist: set[tuple[str, str]],
    execute: bool,
) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT id, url, COALESCE(version, '') FROM {table} WHERE url LIKE ?", (f"{THO_PREFIX}%",))
    to_delete = [row[0] for row in cur.fetchall() if (row[1], row[2]) not in allowlist]
    if execute and to_delete:
        for index in range(0, len(to_delete), 500):
            batch = to_delete[index : index + 500]
            placeholders = ",".join("?" for _ in batch)
            cur.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", batch)
        conn.commit()
    return len(to_delete)


def import_package(hts_db: Path, tgz: Path) -> None:
    root = repo_root()
    cmd = [
        "cargo",
        "run",
        "-q",
        "--bin",
        "hts",
        "--",
        "import",
        str(tgz),
        "--database-url",
        str(hts_db),
    ]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, cwd=root, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=str(repo_root() / "data" / "hts.db"),
        help="Path to HTS SQLite database",
    )
    parser.add_argument(
        "--tgz",
        default=str(repo_root() / "crates" / "hts" / "terminology-data" / f"{THO_PACKAGE_ID}-{THO_PACKAGE_VERSION}.tgz"),
        help="Local THO 7.1.0 package path",
    )
    parser.add_argument(
        "--core-tgz",
        default=str(
            repo_root() / "crates" / "hts" / "terminology-data" / f"{CORE_PACKAGE_ID}-{CORE_PACKAGE_VERSION}.tgz"
        ),
        help="Local FHIR R4 core 4.0.1 package path",
    )
    parser.add_argument(
        "--allowlist-out",
        default=str(repo_root() / "manifests" / "tho-710-terminology-allowlist.json"),
        help="Write extracted allowlist JSON for audit",
    )
    parser.add_argument("--execute", action="store_true", help="Apply deletes and re-import THO 7.1.0")
    parser.add_argument("--skip-import", action="store_true", help="Prune only; do not run hts import")
    parser.add_argument("--vacuum", action="store_true", help="VACUUM SQLite after prune (with --execute)")
    args = parser.parse_args()

    db_path = Path(args.database_url)
    if not db_path.is_file():
        raise SystemExit(f"HTS database not found: {db_path}")

    tgz = ensure_tgz(Path(args.tgz), f"{THO_PACKAGE_ID}#{THO_PACKAGE_VERSION}", THO_DOWNLOAD_URL)
    core_tgz = ensure_tgz(Path(args.core_tgz), f"{CORE_PACKAGE_ID}#{CORE_PACKAGE_VERSION}", CORE_DOWNLOAD_URL)
    cs_allow, vs_allow = load_allowlist(tgz)
    core_only = core_only_members(core_tgz, cs_allow, vs_allow)
    for _, resource in core_only:
        target = cs_allow if resource["resourceType"] == "CodeSystem" else vs_allow
        target.add((resource["url"], resource.get("version") or ""))
    write_allowlist_manifest(Path(args.allowlist_out), cs_allow, vs_allow)
    print(
        f"THO allowlist: {len(cs_allow)} CodeSystems, {len(vs_allow)} ValueSets "
        f"(package {THO_PACKAGE_ID}#{THO_PACKAGE_VERSION}, plus {len(core_only)} "
        f"from {CORE_PACKAGE_ID}#{CORE_PACKAGE_VERSION} that THO does not define)"
    )

    conn = sqlite3.connect(db_path)
    try:
        cs_total, cs_keep, cs_delete = analyze(conn, cs_allow, "code_systems")
        vs_total, vs_keep, vs_delete = analyze(conn, vs_allow, "value_sets")
        print(f"code_systems: {cs_total} THO rows → keep {cs_keep}, delete {cs_delete}")
        print(f"value_sets:   {vs_total} THO rows → keep {vs_keep}, delete {vs_delete}")

        if not args.execute:
            print("\nDry run only. Re-run with --execute after stopping HTS.")
            return 0

        print("\nPruning stale THO rows …")
        deleted_vs = prune_table(conn, "value_sets", vs_allow, execute=True)
        deleted_cs = prune_table(conn, "code_systems", cs_allow, execute=True)
        print(f"Deleted {deleted_vs} ValueSets, {deleted_cs} CodeSystems")

        if args.vacuum:
            print("Running VACUUM (this may take a while) …")
            conn.execute("VACUUM")
            print("VACUUM complete")
    finally:
        conn.close()

    if args.skip_import:
        return 0

    print("\nRe-importing THO 7.1.0 …")
    import_package(db_path, tgz)
    print(f"\nImporting {len(core_only)} R4 core resources THO does not define …")
    with tempfile.TemporaryDirectory() as scratch:
        subset = write_core_subset(core_tgz, core_only, Path(scratch) / "r4-core-tho-only.tgz")
        import_package(db_path, subset)
    print("Done. Restart HTS and verify $expand on pinned ValueSets.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

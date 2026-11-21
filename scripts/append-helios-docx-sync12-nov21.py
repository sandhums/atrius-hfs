#!/usr/bin/env python3
"""Append Sync 12 (21 Nov 2026) to HELIOS vs Atrius.docx."""

from __future__ import annotations

import shutil
import sys
from datetime import datetime
from pathlib import Path

from docx import Document
from docx.enum.text import WD_BREAK

DOCX = Path(
    "/Users/sandhu/Library/Mobile Documents/com~apple~CloudDocs/"
    "FHIR Stuff/HELIOS vs Atrius.docx"
)
MARKER = "Sync 12 — 21 Nov 2026"


def add_heading(doc: Document, text: str, level: int = 2) -> None:
    doc.add_heading(text, level=level)


def add_bullets(doc: Document, items: list[str]) -> None:
    for item in items:
        doc.add_paragraph(f"• {item}")


def add_table2(doc: Document, rows: list[tuple[str, str]]) -> None:
    for k, v in rows:
        p = doc.add_paragraph()
        p.add_run(f"{k}: ").bold = True
        p.add_run(v)


def doc_contains(doc: Document, needle: str) -> bool:
    for par in doc.paragraphs:
        if needle in par.text:
            return True
    return False


def main() -> int:
    if not DOCX.is_file():
        print(f"missing: {DOCX}", file=sys.stderr)
        return 1

    doc = Document(str(DOCX))
    if doc_contains(doc, MARKER):
        print(f"already contains {MARKER!r}; no changes", file=sys.stderr)
        return 0

    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    backup = DOCX.with_name(f"HELIOS vs Atrius.docx.bak-pre-sync12-{stamp}")
    shutil.copy2(DOCX, backup)
    print(f"backup: {backup}")

    p = doc.add_paragraph()
    p.add_run().add_break(WD_BREAK.PAGE)

    add_heading(doc, "Sync 12 — 21 Nov 2026", level=2)
    doc.add_paragraph(
        "Feat branch: merge origin/main → feat-clinical-reasoning. "
        "Merge commit 1661a21b6; docs stamp 3ea197f71. "
        "Companion: docs/helios-feat-merge-oct-nov2026.md (Sync 12) and "
        "docs/clinical-reasoning/upstream-merge.md (Last feat sync)."
    )

    add_table2(
        doc,
        [
            ("atrius main", "1f5b886a5 (Helios 1fa9a352b, 0.2.9)"),
            ("Rollback tag", "pre-merge-main-2026-11-21 → 62e7fe42f"),
            ("Distance", "116 on main / 68 non-merge (since 18 Nov tag)"),
            ("SQLite / Postgres ledger", "50 / 59 (unchanged)"),
            ("MSRV", "1.95"),
        ],
    )

    doc.add_heading("Helios themes", level=3)
    add_bullets(
        doc,
        [
            "FHIRPath #2716: decimals beyond rust_decimal kept as out-of-range values.",
            "#2653: curated 503 client text (sqlite busy, postgres connection shutdown).",
            "#2654: search tiebreak on _lastUpdated + _id (sqlite/postgres/mongo).",
            "#2695: Patient $export _type narrowing stays inside compartment.",
            "Subscriptions: rest-hook/messaging egress hardening; messaging bearer allowlist (#2685).",
            "Bulk-submit: OAuth cached token Result path, CREATE scope check, manifest/body ceilings, kickoff ownership.",
            "#2666: chain-hop confinement for chained search.",
            "Docker: /data volume paths for HTS, bulk-export/submit; hadolint CI.",
        ],
    )

    doc.add_heading("Conflicts resolved (10)", level=3)
    add_bullets(
        doc,
        [
            "Subscription + docker skills (mirrored): Helios messaging env vars + fork HFS_SUBSCRIPTION_OUTBOX_*.",
            "Docker skill: Helios /data paths + fork CDS_SERVER_HOST / port 8095.",
            ".dockerignore: union db fixtures and secret patterns.",
            "auth/README.md: HFS_TENANT_STRICT_VALIDATION + fork HFS_AUTH_REQUIRE_TENANT_CLAIM.",
            "fhir/src/lib.rs: #2716 out-of-range decimal evaluation path.",
            "persistence/error.rs: connection_failed / *_unavailable + fork postgres_error_chain.",
            "bulk_submit_oauth.rs: Ok(tok) for cache hit; bulk_submit.rs: SmartPermissions::CREATE on submit scope.",
        ],
    )

    add_heading(doc, "Keep-list (unchanged highlights)", level=3)
    add_bullets(
        doc,
        [
            "bundle_patch_format, durable subscription outbox on store_update, IngestValidator on bulk-submit.",
            "$events: Helios IG range OR outbox cursor when no range params.",
            "helios-auth redis; helios-terminology-client on rest/ui; named schema ledger; crates/fhir/src/r6/.",
        ],
    )

    add_heading(doc, "HIS pin reminders (after push)", level=3)
    add_bullets(
        doc,
        [
            "Move HIS pins for helios-persistence, helios-rest, helios-auth, helios-subscriptions, "
            "helios-fhirpath (and co-versioned Helios crates) to merge SHA 1661a21b6 or feat tip 3ea197f71.",
        ],
    )

    add_heading(doc, "Verification (21 Nov)", level=3)
    add_bullets(
        doc,
        [
            "cargo build; cargo build -p cds-server.",
            "helios-rest --features R4,sqlite (lib 1086).",
            "helios-rest --features R4,sqlite,subscriptions (1098).",
            "helios-hts --features R4,sqlite (lib 857).",
        ],
    )

    doc.save(str(DOCX))
    print(f"updated: {DOCX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
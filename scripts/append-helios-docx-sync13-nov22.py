#!/usr/bin/env python3
"""Append Sync 13 (22 Nov 2026) to HELIOS vs Atrius.docx."""

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
MARKER = "Sync 13 — 22 Nov 2026"


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
    backup = DOCX.with_name(f"HELIOS vs Atrius.docx.bak-pre-sync13-{stamp}")
    shutil.copy2(DOCX, backup)
    print(f"backup: {backup}")

    p = doc.add_paragraph()
    p.add_run().add_break(WD_BREAK.PAGE)

    add_heading(doc, "Sync 13 — 22 Nov 2026", level=2)
    doc.add_paragraph(
        "Merge origin/main → feat-clinical-reasoning. Merge commit a6975c23f; docs stamp a33c915a8. "
        "Includes prior fork fix 3c29d4bd9 (fhirpath_triggers_integration allow_private_endpoints)."
    )

    add_table2(
        doc,
        [
            ("atrius main", "cd5ed3a01 (Helios e9fd09de3, 0.2.9)"),
            ("Rollback tag", "pre-merge-main-2026-11-22 → 3c29d4bd9"),
            ("Distance", "48 on main / 25 non-merge (since 21 Nov)"),
            ("SQLite / Postgres ledger", "51 / 60 (+ search_index_quantity_high, #2602)"),
            ("MSRV", "1.95"),
        ],
    )

    doc.add_heading("Helios themes", level=3)
    add_bullets(
        doc,
        [
            "#2602 quantity intervals: value_quantity_high, range partial indexes, Postgres backfill trigger on upgrade.",
            "#2689 JWKS: single-flight refresh on unknown kid.",
            "#2649 tenant claim must be JSON string or null (other types → 401).",
            "MongoDB quantity gt/ge/eb on quantity index (#2730).",
            "FHIRPath octofhir-ucum 0.9.3; CI Inferno US Quality Core pins.",
        ],
    )

    doc.add_heading("Conflicts resolved (6)", level=3)
    add_bullets(
        doc,
        [
            "work-with-subscriptions skills (mirrored): Helios WebSocket 10s bind / 256 queue / 30s frame window + fork durable outbox, SQLite claim, broker-readiness bullets.",
            "auth/README.md: keep fork HFS_AUTH_JTI_* rows and HFS_AUTH_REQUIRE_TENANT_CLAIM; take Helios JWKS min-refresh wording and non-string tenant claim paragraph.",
            "jwks_bearer.rs (merged cleanly): Helios typed tenant-claim match (json_type_name) replaces plain as_str.",
            "sqlite/schema.rs + postgres/schema.rs: keep named schema_migrations ledger (not integer migrate_schema); insert step search_index_quantity_high before fork dead-letter / slot-2; SCHEMA 50→51 / 59→60.",
        ],
    )

    add_heading(doc, "HIS pin reminders (after push)", level=3)
    add_bullets(
        doc,
        [
            "helios-persistence, helios-auth, helios-fhirpath (and co-versioned crates) → merge SHA a6975c23f or tip a33c915a8.",
        ],
    )

    add_heading(doc, "Verification (22 Nov)", level=3)
    add_bullets(
        doc,
        [
            "cargo build; cargo build -p cds-server.",
            "helios-rest --features R4,sqlite (lib 1090).",
            "helios-rest --features R4,sqlite,subscriptions (1102).",
            "helios-hts --features R4,sqlite (lib 857).",
            "helios-subscriptions fhirpath_triggers_integration (4 tests).",
        ],
    )

    doc.save(str(DOCX))
    print(f"updated: {DOCX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
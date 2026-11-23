#!/usr/bin/env python3
"""Append Sync 14 (23 Nov 2026) to HELIOS vs Atrius.docx."""

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
MARKER = "Sync 14 — 23 Nov 2026"


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
    backup = DOCX.with_name(f"HELIOS vs Atrius.docx.bak-pre-sync14-{stamp}")
    shutil.copy2(DOCX, backup)
    print(f"backup: {backup}")

    p = doc.add_paragraph()
    p.add_run().add_break(WD_BREAK.PAGE)

    add_heading(doc, "Sync 14 — 23 Nov 2026", level=2)
    doc.add_paragraph(
        "Merge origin/main → feat-clinical-reasoning. Merge commit 837c2236c. "
        "Fork tip before merge: scripts/reindex.sh (6f0398f58) — tenant-aware poll fix."
    )

    add_table2(
        doc,
        [
            ("atrius main", "3036336d4 (Helios db893fbbc, 0.2.10)"),
            ("Rollback tag", "pre-merge-main-2026-11-23 → 6f0398f58"),
            ("Distance", "32 on main / 18 non-merge (since 22 Nov)"),
            ("SQLite / Postgres ledger", "51 / 60 (unchanged)"),
            ("MSRV", "1.95"),
        ],
    )

    doc.add_heading("Helios themes", level=3)
    add_bullets(
        doc,
        [
            "Release 0.2.10.",
            "#2761 bulk-submit: idle-tenant claim order when workers claim together.",
            "#2759 S3: keep SDK/service text out of 503 OperationOutcome.",
            "#2757 SOF MongoDB: render join() separator as $literal.",
            "#2755 output store: per-tenant path segment.",
            "#2753 bulk-submit: bound NDJSON line buffered on ingest.",
            "#2749 cap per-tenant SearchParameter registry cache.",
            "#2747 / #2746 PostgreSQL sslmode + TLS (tokio-postgres-rustls).",
            "#2745 transaction GET judged on scope segment from the entry.",
            "#2737 FHIRPath repeat()/round() upstream defensive cases.",
            "#2738 CI extended: build outside checkout + cargo-target-gc.",
            "RUSTSEC-2026-0592: url 2.5.9; Inferno Specimen SNOMED bundle fix.",
        ],
    )

    doc.add_heading("Conflicts resolved (6)", level=3)
    add_bullets(
        doc,
        [
            "crates/{hfs,rest,ui,persistence}/Cargo.toml: pins 0.2.10 with fork helios-auth redis + helios-terminology-client on rest/ui.",
            "persistence/Cargo.toml: Postgres TLS feature deps + cassandra/neo4j optional backends (merged dep lines for cdrs-tokio / neo4rs post-merge).",
            "postgres/mod.rs: fork SCHEMA_VERSION + PostgresSubscriptionOutbox exports and Helios PostgresTlsSettings.",
        ],
    )

    add_heading(doc, "HIS pin reminders (after push)", level=3)
    add_bullets(
        doc,
        [
            "helios-persistence, helios-rest, helios-fhirpath (and co-versioned crates) → merge SHA 837c2236c or feat tip.",
        ],
    )

    add_heading(doc, "Verification (23 Nov)", level=3)
    add_bullets(
        doc,
        [
            "cargo build; cargo build -p cds-server.",
            "helios-rest --features R4,sqlite (lib 1089).",
            "helios-rest --features R4,sqlite,subscriptions (1101).",
            "helios-hts --features R4,sqlite (lib 857).",
            "helios-subscriptions fhirpath_triggers_integration (4 tests).",
        ],
    )

    doc.save(str(DOCX))
    print(f"updated: {DOCX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
#!/usr/bin/env python3
"""Append Sync 15 (25 Nov 2026) to HELIOS vs Atrius.docx."""

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
MARKER = "Sync 15 — 25 Nov 2026"


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
    backup = DOCX.with_name(f"HELIOS vs Atrius.docx.bak-pre-sync15-{stamp}")
    shutil.copy2(DOCX, backup)
    print(f"backup: {backup}")

    p = doc.add_paragraph()
    p.add_run().add_break(WD_BREAK.PAGE)

    add_heading(doc, "Sync 15 — 25 Nov 2026", level=2)
    doc.add_paragraph(
        "Merge origin/main → feat-clinical-reasoning. Merge commit 7b48a630f; "
        "docs stamp b8c0003c7. Clean merge (0 conflicts)."
    )

    add_table2(
        doc,
        [
            ("atrius main", "a118f9feb (Helios 7ade40f51, 0.2.10)"),
            ("Rollback tag", "pre-merge-main-2026-11-25 → 8c5ca9cef"),
            ("Distance", "11 on main / 5 non-merge (since 23 Nov merge tip)"),
            ("SQLite / Postgres ledger", "51 / 60 (unchanged)"),
            ("MSRV", "1.95"),
        ],
    )

    doc.add_heading("Helios themes", level=3)
    add_bullets(
        doc,
        [
            "#2593 CapabilityStatement: list only SearchParameters the active storage serves (serves_search_param per backend).",
            "#2781 HTS: refuse an import that would replace owner-ranked terminology content.",
            "#2777 HTS UI: refuse cross-site form posts to the import page.",
            "RUSTSEC-2026-0594: cookie 0.18.3.",
        ],
    )

    doc.add_heading("Conflicts resolved (0)", level=3)
    add_bullets(
        doc,
        [
            "Clean ort merge: capabilities.rs, sqlite/postgres/mongo/es query builders, composite serves_search_param, HTS import authority + UI router tests.",
        ],
    )

    add_heading(doc, "HIS pin reminders (after push)", level=3)
    add_bullets(
        doc,
        [
            "helios-persistence, helios-rest, helios-hts (and co-versioned crates) → merge SHA 7b48a630f or feat tip b8c0003c7.",
        ],
    )

    add_heading(doc, "Verification (25 Nov)", level=3)
    add_bullets(
        doc,
        [
            "cargo build; cargo build -p cds-server.",
            "helios-rest --features R4,sqlite (lib 1092).",
            "helios-rest --features R4,sqlite,subscriptions (1104).",
            "helios-hts --features R4,sqlite (lib 857).",
            "helios-subscriptions fhirpath_triggers_integration (4 tests).",
        ],
    )

    doc.save(str(DOCX))
    print(f"updated: {DOCX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
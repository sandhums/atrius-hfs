#!/usr/bin/env python3
"""Append Sync 16 (26 Nov 2026) to HELIOS vs Atrius.docx."""

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
MARKER = "Sync 16 — 26 Nov 2026"


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
    backup = DOCX.with_name(f"HELIOS vs Atrius.docx.bak-pre-sync16-{stamp}")
    shutil.copy2(DOCX, backup)
    print(f"backup: {backup}")

    p = doc.add_paragraph()
    p.add_run().add_break(WD_BREAK.PAGE)

    add_heading(doc, "Sync 16 — 26 Nov 2026", level=2)
    doc.add_paragraph(
        "Merge origin/main → feat-clinical-reasoning. Merge commit 5240cca91. "
        "Playbook stamp in merge commit."
    )

    add_table2(
        doc,
        [
            ("atrius main", "215d03ed9 (Helios fb7d12ed5, 0.2.10)"),
            ("Rollback tag", "pre-merge-main-2026-11-26 → 928d13ce8"),
            ("Distance", "19 on main / 10 non-merge (since 25 Nov)"),
            ("SQLite / Postgres ledger", "51 / 60 (unchanged)"),
            ("MSRV", "1.95"),
        ],
    )

    doc.add_heading("Helios themes", level=3)
    add_bullets(
        doc,
        [
            "FHIRPath: isKnown(), hasData(), sameSubject() on Bundle document sections.",
            "#2796 UI: tenant and bulk-import admin routes require a system scope.",
            "#2788 HTS: only splice valid element names into XML rendering.",
            "#2783 HTS: do not record bootstrap file rows refused by owner rank.",
            "#2779 HTS: cap tree expansions; floor header ceiling; expansion_too_costly helper.",
            "SQLite system-id cache keyed by database file path (not shared across DBs).",
            "CI: remove @claude mention workflow (fork claude.yml deleted on merge).",
        ],
    )

    doc.add_heading("Conflicts resolved (2)", level=3)
    add_bullets(
        doc,
        [
            ".github/workflows/claude.yml — accept Helios deletion (fork gate was HeliosSoftware-only).",
            "hts/src/backends/sqlite/value_set.rs — took main (DB-file cs_id_cache + expansion ceiling); fork per-backend cs_system_id_cache field kept on SqliteTerminologyBackend.",
        ],
    )

    add_heading(doc, "HIS pin reminders (after push)", level=3)
    add_bullets(
        doc,
        [
            "helios-hts, helios-fhirpath, helios-ui (and co-versioned crates) → merge SHA 5240cca91.",
        ],
    )

    add_heading(doc, "Verification (26 Nov)", level=3)
    add_bullets(
        doc,
        [
            "cargo build; cargo build -p cds-server.",
            "helios-rest --features R4,sqlite (lib 1092).",
            "helios-rest --features R4,sqlite,subscriptions (1104).",
            "helios-hts --features R4,sqlite (lib 853).",
            "helios-subscriptions fhirpath_triggers_integration (4 tests).",
        ],
    )

    doc.save(str(DOCX))
    print(f"updated: {DOCX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
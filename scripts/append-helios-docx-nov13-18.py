#!/usr/bin/env python3
"""Append 13–18 Nov 2026 feat sync entries to HELIOS vs Atrius.docx."""

from __future__ import annotations

import sys
from pathlib import Path

from docx import Document
from docx.enum.text import WD_BREAK
from docx.shared import Pt


DOCX = Path(
    "/Users/sandhu/Library/Mobile Documents/com~apple~CloudDocs/"
    "FHIR Stuff/HELIOS vs Atrius.docx"
)


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


def main() -> int:
    if not DOCX.is_file():
        print(f"missing: {DOCX}", file=sys.stderr)
        return 1

    doc = Document(str(DOCX))

    # Page break before new material (avoids run-on from prior section).
    p = doc.add_paragraph()
    p.add_run().add_break(WD_BREAK.PAGE)

    add_heading(doc, "Feat sync addendum — 13 Nov through 18 Nov 2026", level=1)
    doc.add_paragraph(
        "Paste companion to the repo file docs/helios-feat-merge-oct-nov2026.md "
        "(Sync 9–11). Authoritative long form: docs/clinical-reasoning/upstream-merge.md "
        "(Last feat sync). Feat branch tip after 18 Nov merge: aa3227500."
    )

    add_heading(doc, "Cumulative state (after 18 Nov 2026)", level=2)
    add_table2(
        doc,
        [
            ("Workspace", "0.2.9"),
            ("MSRV", "1.95"),
            ("SQLite SCHEMA_VERSION", "50"),
            ("Postgres SCHEMA_VERSION", "59"),
            ("Rollback tags (new)", "pre-merge-main-2026-11-13, -11-15, -11-18"),
        ],
    )

    add_heading(doc, "Keep-list delta (still true)", level=2)
    add_bullets(
        doc,
        [
            "prepare_write (#2554) + persistence prefetch_resolve on validation.rs (13 Nov).",
            "store_update (#2562) with maybe_enqueue_* outbox on same transaction (13 Nov).",
            "Subscription $events: Helios IG range OR outbox after/count when outbox attached (12 Nov fix 6d1269fe9).",
            "helios-auth features = [redis]; helios-terminology-client on rest/ui; batch ifNoneMatch + bundle_patch_format.",
            "Named schema ledger 50/59; r6/ directory layout (no flat r6.rs).",
        ],
    )

    add_heading(doc, "Sync 9 — 13 Nov 2026", level=2)
    add_table2(
        doc,
        [
            ("atrius main", "6927bc473 (Helios a547196c0, 0.2.8)"),
            ("Rollback tag", "pre-merge-main-2026-11-13 → be4c16ab4"),
            ("Merge", "5876ad96b area + follow-ups on feat"),
            ("Distance", "29 on main / 15 non-merge"),
        ],
    )
    doc.add_heading("Helios themes", level=3)
    add_bullets(
        doc,
        [
            "#2566 REST $meta / $meta-add / $meta-delete.",
            "#2554 prepare_write + drop_empty_elements on writes and bulk-submit merge.",
            "#2562 merge_update_meta + store_update refactor.",
            "#2556 chain-ending _has; #2563 unknown nested _has params.",
        ],
    )
    doc.add_paragraph("Conflicts (8): Cargo.lock rebuild; sqlite/postgres store_update + outbox; S3 content_to_write + ingest_validation_error; validation prepare_write + prefetch_resolve; batch/validation tests combined.")

    add_heading(doc, "Sync 10 — 15 Nov 2026", level=2)
    add_table2(
        doc,
        [
            ("atrius main", "a19148379 (Helios 3cf136209, 0.2.8, MSRV 1.95)"),
            ("Rollback tag", "pre-merge-main-2026-11-15 → eab3521fe"),
            ("Merge commit", "5cb8c4c60"),
            ("Post-merge fix", "46e93675a — ES token :in path returns Ok(...) after #2510"),
            ("Distance", "15 / 9 non-merge"),
        ],
    )
    add_bullets(
        doc,
        [
            "#2509 empty compartment refs; #2508 refuse unbuildable search parameters.",
            "#2574 SoF Postgres detoast-once + sqlquery projection.",
            "ES search: less cloning on hits/bodies.",
            "#2580 MSRV 1.95.",
        ],
    )
    doc.add_paragraph("Conflicts: 0.")

    add_heading(doc, "Sync 11 — 18 Nov 2026 (0.2.9)", level=2)
    add_table2(
        doc,
        [
            ("atrius main", "89ebf7384 (Helios c52ff36ad, 0.2.9)"),
            ("Rollback tag", "pre-merge-main-2026-11-18 → 853013773"),
            ("Merge commit", "aa3227500"),
            ("Distance", "9 / 4 non-merge"),
        ],
    )
    add_bullets(
        doc,
        [
            "Release 0.2.9.",
            "#2585 MongoDB fewer commands on create with minted id.",
            "#2579 / #2584 CI (benchmark WiredTiger 3.5 GB; Windows nightly PDB trim).",
        ],
    )
    doc.add_paragraph(
        "Conflicts (3): hfs/rest/ui Cargo.toml — pins 0.2.9 with redis auth and "
        "helios-terminology-client on rest/ui."
    )
    doc.add_paragraph(
        "Operator: GitHub origin/main had lagged upstream; feat merged synced local main. "
        "Push origin main from 89ebf7384 before next sync."
    )

    add_heading(doc, "HIS pin reminders", level=2)
    add_bullets(
        doc,
        [
            "After 12 Nov: helios-observability (+ feat tip with $events fix).",
            "After 13 Nov: helios-persistence, helios-rest.",
            "After 15 Nov: helios-persistence (search/SoF).",
            "After 18 Nov: helios-persistence (Mongo create path); workspace 0.2.9.",
        ],
    )

    add_heading(doc, "Verification snapshot (18 Nov)", level=2)
    add_bullets(
        doc,
        [
            "cargo build; cargo build -p cds-server.",
            "helios-rest --features R4,sqlite (lib 1034).",
            "helios-rest --features R4,sqlite,subscriptions (1046).",
            "helios-hts --features R4,sqlite — green.",
        ],
    )

    doc.save(str(DOCX))
    print(f"updated: {DOCX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
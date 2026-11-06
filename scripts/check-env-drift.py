#!/usr/bin/env python3
"""Fail when deploy/env templates mention keys the workspace never reads.

Scans `deploy/env/*.env.example` (and `deploy/clinical/.env*.example`).
Each KEY= (including commented assignments) must appear somewhere else in
the repo — crates, scripts, systemd units, docs, skills — unless it is on
the JVM-sidecar allowlist (consumed by JVMsidecar, not this workspace).

Live `deploy/env/*.env` files (gitignored) are also checked for removed
`HFS_PROFILE_*` keys that now refuse to start HFS.

Uses a Python file walk (no ripgrep) so Atrius CI on ubuntu-latest works.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Consumed by the JVM CQL sidecar, not by atrius-hfs Rust/scripts.
JVM_SIDECAR_ONLY = {
    "SIDECAR_FHIR_HTTP_LOG",
    "SIDECAR_ENV",
    "SIDECAR_ALLOWED_FHIR_BASES",
}

REMOVED_HFS_PROFILE = {
    "HFS_PROFILE_MANIFEST",
    "HFS_PROFILE_VALIDATION_MODE",
    "HFS_PROFILE_VALIDATION_ADDONS",
}

KEY_RE = re.compile(r"^\s*#?\s*([A-Z][A-Z0-9_]{2,})\s*=")

SKIP_DIR_NAMES = {
    "target",
    ".git",
    "node_modules",
    "fhir-terminology",
}

SKIP_SUFFIXES = {
    ".gz",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".wasm",
    ".so",
    ".dylib",
    ".rlib",
    ".a",
    ".o",
    ".parquet",
    ".db",
    ".sqlite",
    ".lock",
    ".pyc",
}

SKIP_FILE_NAMES = {
    "Cargo.lock",
}


def example_files() -> list[Path]:
    files = sorted(ROOT.glob("deploy/env/*.env.example"))
    files.extend(sorted(ROOT.glob("deploy/clinical/.env*.example")))
    return files


def keys_in(path: Path) -> list[str]:
    keys: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = KEY_RE.match(line)
        if match:
            keys.append(match.group(1))
    return keys


def skip_dir(dirpath: Path) -> bool:
    rel = dirpath.relative_to(ROOT)
    parts = rel.parts
    if any(part in SKIP_DIR_NAMES for part in parts):
        return True
    return parts[:2] == ("deploy", "env")


def skip_file(path: Path) -> bool:
    name = path.name
    if name in SKIP_FILE_NAMES:
        return True
    if name.endswith(".env.example"):
        return True
    if name.startswith(".env") and name.endswith(".example"):
        return True
    if path.suffix.lower() in SKIP_SUFFIXES:
        return True
    return False


def load_corpus() -> list[str]:
    """File texts that may mention env keys (excludes env templates)."""
    texts: list[str] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        current = Path(dirpath)
        if skip_dir(current):
            dirnames.clear()
            continue
        dirnames[:] = [
            name
            for name in dirnames
            if name not in SKIP_DIR_NAMES
            and not skip_dir(current / name)
        ]
        for filename in filenames:
            path = current / filename
            if skip_file(path):
                continue
            try:
                texts.append(path.read_text(encoding="utf-8"))
            except (UnicodeDecodeError, OSError):
                continue
    return texts


def referenced(key: str, corpus: list[str]) -> bool:
    return any(key in text for text in corpus)


def main() -> int:
    errors: list[str] = []
    corpus = load_corpus()

    for path in example_files():
        for key in keys_in(path):
            if key in JVM_SIDECAR_ONLY or key in REMOVED_HFS_PROFILE:
                continue
            if not referenced(key, corpus):
                rel = path.relative_to(ROOT)
                errors.append(f"{rel}: {key} is not referenced outside env templates")

    for path in sorted((ROOT / "deploy/env").glob("*.env")):
        if path.name.endswith(".example"):
            continue
        present = set(keys_in(path)) & REMOVED_HFS_PROFILE
        if present:
            rel = path.relative_to(ROOT)
            errors.append(
                f"{rel}: removed vars still set: {', '.join(sorted(present))} "
                "(use HFS_VALIDATION_MODE + HFS_FHIR_PACKAGES)"
            )

    if errors:
        print("env drift:")
        for err in errors:
            print(f"  {err}")
        return 1
    print("env drift: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())

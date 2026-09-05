#!/usr/bin/env python3
"""Compare FHIR model signatures: this fork's directories vs Helios flat files.

Helios still emits one file per version (`crates/fhir/src/r4.rs`). This fork
emits a module directory (`crates/fhir/src/r4/` with `mod.rs`, `primitives/`,
`complex_types/`, `resources/`). Raw diffs are unreadable (layout + rustdoc +
buildId). This script compares struct fields, enum variants, and `fhir_*`
attribute payloads so a sync cannot silently drop a shape change.

Exit 0 when signatures match. Exit 1 on mismatches or if Helios files are
missing.

Usage (repo root):

  ./scripts/diff-fhir-model-signatures.py
  ./scripts/diff-fhir-model-signatures.py --helios-ref origin/main --version r4
  ./scripts/diff-fhir-model-signatures.py --helios-root /path/to/hfs
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
VERSIONS = ("r4", "r4b", "r5", "r6")

IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
PUB_STRUCT = re.compile(r"\bpub\s+struct\s+(" + IDENT.pattern + r")")
PUB_ENUM = re.compile(r"\bpub\s+enum\s+(" + IDENT.pattern + r")")
PAIRS = {"{": "}", "(": ")", "[": "]"}


def _skip_ws_comments(src: str, i: int) -> int:
    n = len(src)
    while i < n:
        if src[i] in " \t\r\n":
            i += 1
            continue
        if src.startswith("//", i):
            nl = src.find("\n", i)
            i = n if nl < 0 else nl + 1
            continue
        if src.startswith("/*", i):
            end = src.find("*/", i + 2)
            i = n if end < 0 else end + 2
            continue
        break
    return i


def _read_delimited(src: str, open_idx: int) -> tuple[str, int]:
    """Return (inner, index_after_close). `open_idx` points at `{`, `(`, or `[`."""
    opener = src[open_idx]
    closer = PAIRS[opener]
    depth = 0
    i = open_idx
    n = len(src)
    while i < n:
        ch = src[i]
        if ch == '"':
            i += 1
            while i < n:
                if src[i] == "\\":
                    i += 2
                    continue
                if src[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if src.startswith("//", i):
            nl = src.find("\n", i)
            i = n if nl < 0 else nl + 1
            continue
        if ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return src[open_idx + 1 : i], i + 1
        i += 1
    raise ValueError(f"unbalanced {opener}")


def _norm_type(ty: str) -> str:
    s = re.sub(r"\s+", "", ty.strip().rstrip(","))
    # rustfmt trailing commas inside generics: Option<Vec<Foo>,>
    while True:
        n = s.replace(",>", ">").replace(",)", ")").replace(",]", "]")
        if n == s:
            return n
        s = n


def _parse_struct_fields(inner: str) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    i = 0
    n = len(inner)
    while i < n:
        i = _skip_ws_comments(inner, i)
        if i >= n:
            break
        if inner.startswith("pub", i):
            i += 3
            i = _skip_ws_comments(inner, i)
            if inner.startswith("(", i):
                close = inner.find(")", i)
                if close < 0:
                    break
                i = _skip_ws_comments(inner, close + 1)
            m = IDENT.match(inner, i)
            if not m:
                i += 1
                continue
            name = m.group(0)
            i = _skip_ws_comments(inner, m.end())
            if i >= n or inner[i] != ":":
                continue
            i = _skip_ws_comments(inner, i + 1)
            start = i
            depth = 0
            while i < n:
                ch = inner[i]
                if ch in "<([{":
                    depth += 1
                elif ch in ">)]}":
                    depth = max(0, depth - 1)
                elif ch == "," and depth == 0:
                    break
                elif ch == "\n" and depth == 0:
                    # generated fields are one line; allow newline before comma
                    nxt = _skip_ws_comments(inner, i + 1)
                    if nxt < n and inner[nxt] == ",":
                        i = nxt
                        break
                    if nxt >= n or inner.startswith("pub", nxt) or inner[nxt] == "}":
                        break
                i += 1
            ty = _norm_type(inner[start:i])
            if name and ty:
                fields.append((name, ty))
            if i < n and inner[i] == ",":
                i += 1
            continue
        i += 1
    return fields


def _parse_enum_variants(inner: str) -> list[tuple[str, str]]:
    variants: list[tuple[str, str]] = []
    i = 0
    n = len(inner)
    while i < n:
        i = _skip_ws_comments(inner, i)
        if i >= n:
            break
        m = IDENT.match(inner, i)
        if not m:
            i += 1
            continue
        name = m.group(0)
        i = _skip_ws_comments(inner, m.end())
        payload = ""
        if i < n and inner[i] == "(":
            body, i = _read_delimited(inner, i)
            payload = _norm_type(body)
            i = _skip_ws_comments(inner, i)
        elif i < n and inner[i] == "{":
            body, i = _read_delimited(inner, i)
            payload = "{" + _norm_type(body) + "}"
            i = _skip_ws_comments(inner, i)
        if i < n and inner[i] == ",":
            i += 1
        variants.append((name, payload))
    return variants


def extract_types(src: str) -> dict[str, dict]:
    """Map type name -> {kind, fields|variants, fhir_attrs}."""
    out: dict[str, dict] = {}
    i = 0
    n = len(src)
    pending: list[str] = []
    while i < n:
        i = _skip_ws_comments(src, i)
        if i >= n:
            break
        if src.startswith("#[", i):
            end = src.find("]", i)
            if end < 0:
                break
            attr = src[i : end + 1]
            if attr.startswith("#[fhir_"):
                pending.append(re.sub(r"\s+", "", attr))
            i = end + 1
            continue
        if src.startswith("pub struct ", i) or src.startswith("pub enum ", i):
            kind = "struct" if src.startswith("pub struct ", i) else "enum"
            m = PUB_STRUCT.match(src, i) if kind == "struct" else PUB_ENUM.match(src, i)
            if not m:
                i += 1
                pending = []
                continue
            name = m.group(1)
            j = _skip_ws_comments(src, m.end())
            if j < n and src[j] == "<":
                # skip generics
                depth = 0
                while j < n:
                    if src[j] == "<":
                        depth += 1
                    elif src[j] == ">":
                        depth -= 1
                        if depth == 0:
                            j += 1
                            break
                    j += 1
                j = _skip_ws_comments(src, j)
            if j >= n or src[j] != "{":
                pending = []
                i = m.end()
                continue
            inner, after = _read_delimited(src, j)
            item = {"kind": kind, "fhir_attrs": tuple(pending)}
            if kind == "struct":
                item["fields"] = tuple(_parse_struct_fields(inner))
            else:
                item["variants"] = tuple(_parse_enum_variants(inner))
            if name in out and out[name] != item:
                raise ValueError(f"duplicate type {name} with different signatures")
            out[name] = item
            pending = []
            i = after
            continue
        pending = []
        i += 1
    return out


def load_directory(version_dir: Path) -> dict[str, dict]:
    merged: dict[str, dict] = {}
    for path in sorted(version_dir.rglob("*.rs")):
        text = path.read_text(encoding="utf-8")
        for name, item in extract_types(text).items():
            if name in merged and merged[name] != item:
                raise ValueError(
                    f"{path}: {name} already defined with a different signature"
                )
            merged[name] = item
    return merged


def git_show(ref: str, path: str) -> str | None:
    r = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        return None
    return r.stdout


def load_helios(version: str, helios_ref: str | None, helios_root: Path | None) -> dict[str, dict]:
    flat = f"crates/fhir/src/{version}.rs"
    directory = REPO / "crates/fhir/src" / version
    if helios_root is not None:
        candidate = helios_root / flat
        if candidate.is_file():
            return extract_types(candidate.read_text(encoding="utf-8"))
        d = helios_root / "crates/fhir/src" / version
        if d.is_dir():
            return load_directory(d)
        raise FileNotFoundError(f"no Helios {version} models under {helios_root}")
    assert helios_ref is not None
    text = git_show(helios_ref, flat)
    if text is not None:
        return extract_types(text)
    # Helios adopted directories.
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", helios_ref, f"crates/fhir/src/{version}"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if listing.returncode != 0 or not listing.stdout.strip():
        raise FileNotFoundError(
            f"{helios_ref}:{flat} not found (and no crates/fhir/src/{version}/ tree)"
        )
    merged: dict[str, dict] = {}
    for rel in listing.stdout.splitlines():
        if not rel.endswith(".rs"):
            continue
        body = git_show(helios_ref, rel)
        if body is None:
            continue
        merged.update(extract_types(body))
    return merged


def fmt_item(item: dict) -> str:
    attrs = ",".join(item.get("fhir_attrs") or ())
    if item["kind"] == "struct":
        fields = "; ".join(f"{n}: {t}" for n, t in item["fields"])
        return f"struct attrs=[{attrs}] fields=[{fields}]"
    variants = "; ".join(f"{n}({p})" if p else n for n, p in item["variants"])
    return f"enum attrs=[{attrs}] variants=[{variants}]"


def diff_version(ours: dict[str, dict], theirs: dict[str, dict]) -> list[str]:
    lines: list[str] = []
    only_ours = sorted(set(ours) - set(theirs))
    only_theirs = sorted(set(theirs) - set(ours))
    for name in only_ours:
        lines.append(f"  only in fork: {name}")
    for name in only_theirs:
        lines.append(f"  only in Helios: {name}")
    for name in sorted(set(ours) & set(theirs)):
        if ours[name] != theirs[name]:
            lines.append(f"  {name} differs")
            lines.append(f"    fork:   {fmt_item(ours[name])}")
            lines.append(f"    Helios: {fmt_item(theirs[name])}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--helios-ref",
        default="upstream/main",
        help="git ref for Helios flat models (default: upstream/main)",
    )
    parser.add_argument(
        "--helios-root",
        type=Path,
        help="filesystem checkout of Helios instead of --helios-ref",
    )
    parser.add_argument(
        "--version",
        action="append",
        dest="versions",
        choices=VERSIONS,
        help="limit to one version (repeatable). Default: all four",
    )
    args = parser.parse_args()
    versions = tuple(args.versions) if args.versions else VERSIONS

    helios_ref = None if args.helios_root else args.helios_ref
    if helios_ref:
        probe = subprocess.run(
            ["git", "rev-parse", "--verify", helios_ref],
            cwd=REPO,
            capture_output=True,
            text=True,
        )
        if probe.returncode != 0:
            alt = "origin/main"
            probe2 = subprocess.run(
                ["git", "rev-parse", "--verify", alt],
                cwd=REPO,
                capture_output=True,
                text=True,
            )
            if probe2.returncode != 0:
                print(f"error: neither {helios_ref} nor {alt} exists", file=sys.stderr)
                return 2
            print(f"note: {helios_ref} missing, using {alt}", file=sys.stderr)
            helios_ref = alt

    failed = False
    for version in versions:
        ours_dir = REPO / "crates/fhir/src" / version
        if not ours_dir.is_dir():
            print(f"{version}: missing fork directory {ours_dir}", file=sys.stderr)
            failed = True
            continue
        try:
            ours = load_directory(ours_dir)
            theirs = load_helios(version, helios_ref, args.helios_root)
        except (FileNotFoundError, ValueError) as e:
            print(f"{version}: {e}", file=sys.stderr)
            failed = True
            continue
        lines = diff_version(ours, theirs)
        print(
            f"{version}: fork={len(ours)} types, Helios={len(theirs)} types"
        )
        if lines:
            failed = True
            print("\n".join(lines))
        else:
            print("  signatures match")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

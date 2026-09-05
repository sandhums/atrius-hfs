# Regenerating directory-layout FHIR models

This fork’s `helios-fhir-gen` writes **module directories**, not Helios’s
flat files:

| Helios (`upstream/main`) | This fork |
|--------------------------|-----------|
| `crates/fhir/src/r4.rs` | `crates/fhir/src/r4/` (`mod.rs`, `primitives/`, `complex_types/`, `resources/`) |
| same for `r4b.rs`, `r5.rs`, `r6.rs` | `r4b/`, `r5/`, `r6/` |

`pub mod r4;` in `crates/fhir/src/lib.rs` is the same on both sides; rustc
resolves `r4.rs` or `r4/mod.rs`. Nothing Atrius-specific depends on the
layout. The cost of keeping it is **silent model staleness**: a merge that
`git rm`s Helios’s `r4.rs` discards every upstream body change. Regen after
those syncs is the only way the directory tree picks them up.

Terminology (`crates/fhir-terminology`) is a different generator
(`helios-fhir-valueset-gen`) and does not follow this layout.

## When to regenerate

After merging `main` / `upstream/main`, if Helios touched any of:

- `crates/fhir/src/r4.rs`
- `crates/fhir/src/r4b.rs`
- `crates/fhir/src/r5.rs`
- `crates/fhir/src/r6.rs`

beyond buildId / rustdoc churn, run this runbook. Also run it after changing
`crates/fhir-gen/src/` itself.

Do **not** take Helios’s flat `r*.rs` onto this branch (compile error E0761
next to `r4/`). Keep the directory; regenerate.

## Commands (repo root)

```bash
# Enable every version you want emitted. Default features are R4 only.
# Do not use --all-features (that turns on skip-r6-download).
cargo build -p helios-fhir-gen --features R4,R4B,R5,R6

# Paths are hardcoded: input crates/fhir-gen/resources/{VERSION}/,
# output crates/fhir/src/{r4,r4b,r5,r6}/. Must run from the workspace root.
./target/debug/helios-fhir-gen --all

cargo fmt --all

# Shape check vs Helios flat files (ignores layout, rustdoc, timestamps).
./scripts/diff-fhir-model-signatures.py
```

One-liner:

```bash
cargo run -p helios-fhir-gen --features R4,R4B,R5,R6 -- --all
```

Single version (smoke test):

```bash
cargo run -p helios-fhir-gen --features R4 -- R4
```

Checked-in R6 JSON, no `build.fhir.org` fetch:

```bash
cargo build -p helios-fhir-gen --features R4,R4B,R5,R6,skip-r6-download
./target/debug/helios-fhir-gen --all
```

`--compartments-only` refreshes `crates/fhir/src/compartment_expressions/`
and skips the per-type files.

## What to commit vs discard

**Commit**

- `crates/fhir/src/r4/` `r4b/` `r5/` `r6/` (and `compartment_expressions/` if that ran)
- Generator source changes under `crates/fhir-gen/src/` if you made them

**Do not commit** unless you intend a spec-fixture bump

- `crates/fhir-gen/resources/R6/*` (live download during `cargo build -p helios-fhir-gen --features R6`)
- `crates/fhir/tests/data/{json,xml}/R6/*` (same class of churn)

A leftover `crates/fhir/src/r4.rs` beside `r4/` is deleted by the generator.

## Confirming upstream shape actually landed

`./scripts/diff-fhir-model-signatures.py` compares struct fields, enum
variants, and `#[fhir_*]` payloads to `upstream/main`’s flat files (falls
back to `origin/main`). Exit 0 means the directory tree matches Helios
shape. Exit 1 prints the type names that drifted — regenerate or inspect
before merging.

```bash
./scripts/diff-fhir-model-signatures.py --version r4
./scripts/diff-fhir-model-signatures.py --helios-ref origin/main
```

Spot-check after a ViewDefinition-class change: base type in
`crates/fhir/src/r4/resources/view_definition.rs` (and r4b/r5/r6) should
match Helios’s comment on `ViewDefinition` in `crates/fhir/src/r4.rs`.

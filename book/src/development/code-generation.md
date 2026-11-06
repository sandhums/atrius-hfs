# Code Generation

The `helios-fhir` crate contains auto-generated Rust types for FHIR resources. These types are produced by `helios-fhir-gen` from official HL7 StructureDefinition JSON schemas.

You only need to run this if you are updating the FHIR spec version or changing how models are generated.

This fork writes **directories** (`crates/fhir/src/r4/`, not `r4.rs`). Full
runbook: [fhir-model-regen.md](../../../docs/fhir-model-regen.md).

## Regenerating FHIR Models

```bash
# 1. Build the generator
#    (Note: use --features R4,R4B,R5,R6 here — not --all-features)
cargo build -p helios-fhir-gen --features R4,R4B,R5,R6

# 2. Run the generator from the workspace root
#    Input:  crates/fhir-gen/resources/{R4,R4B,R5,R6}/
#    Output: crates/fhir/src/{r4,r4b,r5,r6}/
./target/debug/helios-fhir-gen --all

# 3. Format the generated files
cargo fmt --all

# 4. Confirm shape vs Helios flat files (not a raw diff)
./scripts/diff-fhir-model-signatures.py
```

> **R6 specs** are automatically downloaded from the HL7 build server (`https://build.fhir.org/`) during the build. This requires internet access. To keep the checked-in R6 JSON, add the `skip-r6-download` feature. Do not commit accidental `resources/R6` or `fhir/tests/data/**/R6` churn unless you intend a spec bump.

> **Build time:** Building `helios-fhir-gen` can take 5–10 minutes due to the large generated files.

## How It Works

1. The generator reads FHIR StructureDefinition JSON files for each version
2. It transforms them into Rust struct and enum definitions
3. Output is a module directory per version (`crates/fhir/src/r4/` with `mod.rs`, `primitives/`, `complex_types/`, `resources/`). Helios still emits a single `r4.rs`; do not take that file on this branch.
4. Feature flags (`R4`, `R4B`, `R5`, `R6`) gate which versions are compiled

## The `skip-r6-download` Feature

When running `cargo clippy --all-features`, the `skip-r6-download` feature is activated, which prevents `helios-fhir-gen` from attempting to download R6 specs during the build. This makes CI linting safe to run without network access or a long build.

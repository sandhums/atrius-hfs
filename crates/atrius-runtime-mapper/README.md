# atrius-runtime-mapper

Projects **Atrius/ABDM clinical** FHIR resources into **QI-Core evaluation shapes** before CQL execution.

## Scope

- **In scope:** clinical data read from the clinical HFS instance (Atrius authoring/storage profiles).
- **Out of scope:** Knowledge Repository (KR) — a separate HFS instance holding eCQM `Library` / `Measure` artifacts. KR reads use `libraryBaseUrl` on the sidecar and are **not** mapped.

Specification: Atrius IG `runtime-mapper.md`.

## Usage

```rust
use atrius_runtime_mapper::{MapperManifest, RuntimeMapper};

let manifest = MapperManifest::default_v0_1();
let mapper = RuntimeMapper::new(manifest);
let projected = mapper.project_bundle(clinical_bundle)?;
```

Load a manifest generated from the Atrius IG build:

```rust
let manifest = MapperManifest::from_json_file("atrius-mapper-manifest.json")?;
```

## Status

v0.1 implements **Condition** projection (encounter-diagnosis vs problems-health-concerns). Additional resource types follow the same pattern.

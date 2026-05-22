---
name: Sidecar Rust client sync
overview: Align `atrius-clinical-reasoning` DTOs with JVM MVP (optional elm, libraryBaseUrl, resolveLibraryArtifactsFromFhir, optional resultType); add result normalization and `EvaluateExpressionRequestBuilder` from tenant endpoints.
todos:
  - id: dto-sync
    content: Update dto.rs (request/response + IncludedLibrary.elm optional); fix tests and http_smoke
    status: completed
  - id: normalized-result
    content: Add normalized_result.rs + EvaluateExpressionResponse::normalized_result()
    status: completed
  - id: request-builder
    content: Add request_builder.rs (FhirServiceEndpoints, builder + validation when resolve=false)
    status: completed
  - id: lib-example-readme
    content: Export modules from lib.rs; update live_sidecar_roundtrip + README
    status: completed
isProject: true
---

# JVM sidecar MVP → `atrius-clinical-reasoning` sync

**Execution:** apply in **Agent mode** (Rust edits blocked under Plan mode).

## Request (`EvaluateExpressionRequest`)

Wire these Kotlin fields:

| JVM field | Rust |
|-----------|------|
| optional `elm` / `elmJson` | `elm: Option<String>` |
| `elmFormat` | `elm_format` (unchanged) |
| `libraryBaseUrl` | `library_base_url: Option<String>` |
| `resolveLibraryArtifactsFromFhir` default `true` | `resolve_library_artifacts_from_fhir: bool` with `#[serde(default = "...")]` |
| `includedLibraries[].elm` optional | `IncludedLibrary.elm: Option<String>` |

Serialization: `skip_serializing_if` for `None` on optional fields; always serialize `resolve_library_artifacts_from_fhir` so `false` reaches the sidecar.

Validation rule (Rust builder): when `resolve_library_artifacts_from_fhir == false`, non-blank inline `elm` is required.

## Response (`EvaluateExpressionResponse`)

- `result_type: Option<String>` — `#[serde(default)]` for missing/null.
- Add `fn normalized_result(&self) -> NormalizedSidecarResult` delegating to [`normalize_sidecar_result`](crates/atrius-clinical-reasoning/src/normalized_result.rs).

## Result normalization

New module `normalized_result.rs`:

- Enum: `Null`, `Bool`, `Number`, `String`, `Array`, `Object`, `FhirResource(Value)`.
- Top-level `result` JSON **string** that parses to object with non-empty `resourceType` → `FhirResource`.
- No deep recursion into nested arrays/objects for FHIR strings (document limitation).

## Request builder

New `request_builder.rs`:

- `FhirServiceEndpoints { hfs_base_url, hts_base_url, library_base_url: Option<_> }` with `new`, `with_library_base_url`.
- `EvaluateExpressionRequestBuilder` + `build() -> Result<_, EvaluateExpressionRequestBuildError>`.

## Files to touch

- [`crates/atrius-clinical-reasoning/src/dto.rs`](crates/atrius-clinical-reasoning/src/dto.rs)
- [`crates/atrius-clinical-reasoning/src/normalized_result.rs`](crates/atrius-clinical-reasoning/src/normalized_result.rs) (new)
- [`crates/atrius-clinical-reasoning/src/request_builder.rs`](crates/atrius-clinical-reasoning/src/request_builder.rs) (new)
- [`crates/atrius-clinical-reasoning/src/lib.rs`](crates/atrius-clinical-reasoning/src/lib.rs)
- [`crates/atrius-clinical-reasoning/tests/http_smoke.rs`](crates/atrius-clinical-reasoning/tests/http_smoke.rs)
- [`crates/atrius-clinical-reasoning/examples/live_sidecar_roundtrip.rs`](crates/atrius-clinical-reasoning/examples/live_sidecar_roundtrip.rs)
- [`crates/atrius-clinical-reasoning/README.md`](crates/atrius-clinical-reasoning/README.md) (short note on builder + normalization)

## Verify

```bash
cargo fmt --all
cargo test -p atrius-clinical-reasoning
cargo build -p atrius-clinical-reasoning --example live_sidecar_roundtrip --features integration-demo
cargo clippy -p atrius-clinical-reasoning --all-targets --all-features -- …
```

Also refresh [`.cursor/plans/library_knowledge_artifacts.plan.md`](.cursor/plans/library_knowledge_artifacts.plan.md) “Evaluate” row to mention `libraryBaseUrl` + `resolveLibraryArtifactsFromFhir` once Rust ships.

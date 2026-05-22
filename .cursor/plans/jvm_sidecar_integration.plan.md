---
name: JVM sidecar integration
overview: Align Rust `atrius-clinical-reasoning` with JVMsidecar `/v1/evaluate/expression`, refresh HTS terminology doc field names, then validate with smoke tests. CDS Hooks wiring and `$evaluate-measure` / `$apply` remain deferred.
todos:
  - id: align-dto-path
    content: Update atrius-clinical-reasoning DTOs + POST path + tests to match JVMsidecar EvaluateExpressionRequest/Response
    status: completed
  - id: hts-doc-fields
    content: Refresh crates/hts/docs/cqframework_terminology.md examples to hfsBaseUrl/htsBaseUrl after DTO alignment
    status: completed
  - id: e2e-smoke
    content: Document or script HFS+HTS+sidecar smoke using ClinicalReasoningClient with tenant-correct base URLs
    status: pending
  - id: artifact-rules-doc
    content: Cross-repo doc — ELM hydration requirement, includedLibraries vs IncludeDef, identifier validation
    status: pending
  - id: cds-wire-later
    content: After contract stable — connect cds-hooks prefetch/context to sidecar evaluate call
    status: pending
isProject: true
---

# JVM sidecar ↔ Helios Rust stack

## Contract

- **POST** `/v1/evaluate/expression` — body `EvaluateExpressionRequest` (camelCase JSON).
- **Response** `EvaluateExpressionResponse`: `expression`, `resultType`, `result`.

Rust structs live in [`crates/atrius-clinical-reasoning/src/dto.rs`](crates/atrius-clinical-reasoning/src/dto.rs). Legacy JSON keys `fhirDataUrl`, `fhirTerminologyUrl`, and `elmJson` deserialize via serde `alias`.

## Terminology

[`crates/hts/docs/cqframework_terminology.md`](crates/hts/docs/cqframework_terminology.md) — configure **`htsBaseUrl`** (and **`hfsBaseUrl`** for retrieves) to match JVM sidecar env wiring.

## Follow-ups

- E2E smoke with live HFS + HTS + JVMsidecar.
- Cross-repo documentation for ELM hydration and include rules (`ElmLibraryHydration.kt` on JVM).
- CDS Hooks integration when product-ready.

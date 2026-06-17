# Atrius clinical reasoning

Rust façade for invoking the JVM **clinical reasoning sidecar** (CQL→ELM evaluation, CDS-oriented calls, future `$evaluate-measure` / `PlanDefinition/$apply` pass-through).

**Architecture & startup:** [docs/clinical-reasoning/README.md](../../docs/clinical-reasoning/README.md)

HTTP contract mirrors JVM **`POST /v1/evaluate/expression`** (`EvaluateExpressionRequest` / `EvaluateExpressionResponse` — see `src/dto.rs`). Default sidecar base URL in config matches **`SIDECAR_PORT`** default **8088** on the JVM service.

Crate name: `atrius-clinical-reasoning`  
Rust import path: `atrius_clinical_reasoning`

See the workspace clinical reasoning plans under `.cursor/plans/`.

## Live roundtrip example

Manual check against a running JVM sidecar (and HFS / HTS when your ELM needs them):

```bash
LIBRARY_ID=MyLib EXPRESSION=MyDefine ELM_PATH=/path/to/MyLib.elm.json \
  cargo run -p atrius-clinical-reasoning \
  --example live_sidecar_roundtrip --features integration-demo
```

Defaults: sidecar `http://127.0.0.1:8088`, HFS `http://127.0.0.1:8080`, HTS `http://127.0.0.1:8090`. Override with `CR_SIDECAR_URL`, `HFS_BASE_URL`, `HTS_BASE_URL`, optional `LIBRARY_BASE_URL`, `RESOLVE_FROM_FHIR`. Full env list is in [`examples/live_sidecar_roundtrip.rs`](examples/live_sidecar_roundtrip.rs).

## Request builder and result normalization

- **`FhirServiceEndpoints`** + **`EvaluateExpressionRequestBuilder`** ([`src/request_builder.rs`](src/request_builder.rs)) assemble tenant-aware `hfsBaseUrl` / `htsBaseUrl` / optional `libraryBaseUrl` and enforce “inline ELM required when `resolveLibraryArtifactsFromFhir` is false”.
- **`EvaluateExpressionResponse::normalized_result`** and **`normalize_sidecar_result`** ([`src/normalized_result.rs`](src/normalized_result.rs)) classify the JVM `result` JSON, including FHIR resources returned as **double-encoded JSON strings** (`resourceType` heuristic).

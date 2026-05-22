# HTS terminology operations vs CQFramework `R4FhirTerminologyProvider`

Clinical reasoning JVM sidecars often use CQFramework's
[`R4FhirTerminologyProvider`](https://github.com/cqframework/clinical_quality_language/blob/main/Src/java/engine-fhir/src/main/kotlin/org/opencds/cqf/cql/engine/fhir/terminology/R4FhirTerminologyProvider.kt)
with a HAPI `IGenericClient` pointed at a **single FHIR base URL** (see product plan: **HTS direct** vs **HFS proxy**).

This document records how HTS lines up with that provider so **`htsBaseUrl`** (terminology) and **`hfsBaseUrl`** (clinical retrieves) in the JVM sidecar / [`atrius-clinical-reasoning`](../../atrius-clinical-reasoning/README.md) request body can be aimed at the right hosts. The Rust client still accepts legacy JSON keys `fhirTerminologyUrl` and `fhirDataUrl` when deserializing.

## Supported call patterns (verified by `tests/cqframework_terminology_compatibility.rs`)

| Operation | CQFramework behavior (HAPI client) | HTS support |
|-----------|------------------------------------|-------------|
| **Lookup** | `POST /CodeSystem/$lookup` with `code` (CodeType) and `system` (Uri) | Yes — type-level POST; parameter parsing accepts `valueUri` / `valueCode` / `valueString`. |
| **ValueSet membership** | `GET /ValueSet/{id}/$validate-code?code=…&system=…` | Yes — instance-level GET injects canonical `url` from stored `ValueSet`. |
| **Expand** | `GET /ValueSet/{id}/$expand` (no body) | Yes — returns a **ValueSet** with `expansion` (FHIR R4 shape). |
| **Resolve ValueSet id** | 1) `GET /ValueSet?url=…` 2) `GET /ValueSet?identifier=…` 3) `GET /ValueSet/{id}` | **Partial** — (1) and (3) work; (2) not implemented (see gap). |

## Gaps and configuration guidance

### 1. ValueSet search by `identifier`

`R4FhirTerminologyProvider.resolveValueSetId` falls back to:

`GET /ValueSet?identifier=<token>`

HTS **`GET /ValueSet`** accepts `url`, `version`, `name`, `title`, `status`, `_count`, `_offset` only. Unknown query parameters are ignored by Serde deserialization, so an **`identifier`** search is **not** implemented: the request may behave like an unfiltered search and can violate CQFramework's expectation that at most **one** ValueSet is returned.

**Mitigation:** In CQL and published libraries, prefer **canonical ValueSet URLs** (first resolution path). If you must resolve by identifier, extend HTS search or preload ValueSets reachable by **`url`** only.

### 2. ValueSet versioning in CQL ValueSet refs

CQFramework implementation throws if `version` or code-system bindings are set on `ValueSetInfo` (**not supported at this time** in that provider class). HTS versioning behavior is unaffected; this is primarily a client-side limitation.

### 3. `$expand` HTTP response envelope

FHIR specifies `$expand` returns a **ValueSet**. HTS returns that directly. Confirm your HAPI `IGenericClient` version accepts this for `execute()` when wiring the terminology client from the JVM sidecar; if not, normalize in the sidecar or adjust client configuration rather than departing from the FHIR response shape without cause.

### 4. Terminology URL layout

Recommended for the JVM sidecar / Helios integration:

- Set **`htsBaseUrl`** (sidecar request body) to HTS, e.g. `http://localhost:8090` — same URL you pass into **`IGenericClient`** for terminology; trailing-slash conventions should match your HTTP client.
- Optionally route through HFS **only if** `ValueSet/$expand`, `ValueSet/$validate-code`, and `CodeSystem/$lookup` are faithfully proxied to HTS without losing query parameters used in GET `$validate-code`.

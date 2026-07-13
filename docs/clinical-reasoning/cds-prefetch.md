# CDS Hooks Prefetch — Client vs Backend

This document explains **who does what** for CDS Hooks prefetch in the Atrius stack, and **what backend code actually implements** (as opposed to the CDS client / EHR).

For local testing (Postman, smoke script), see [startup-guide.md — CDS client responsibilities](./startup-guide.md#cds-client-responsibilities-prefetch--invoke).

## Summary

**Prefetch resolution is a CDS client responsibility.** The Atrius backend does not replace the EHR.

| Phase | CDS client (EHR) | Atrius backend |
|-------|------------------|----------------|
| Discovery | Reads **templates** from `GET /cds-services` | Advertises templates from manifest / KR `Binary` |
| Invoke | Resolves templates → FHIR resources; sends populated `prefetch` | **Pass-through** to sidecar; no FHIR fetch for prefetch |
| CQL evaluation | — | Sidecar **consumes** populated prefetch; REST fallback for gaps |

Backend prefetch work means **honoring the CDS Hooks contract end-to-end**, not implementing template resolution on the server.

## CDS client (EHR) — standard contract

Per [CDS Hooks](https://cds-hooks.org/):

1. **`GET /cds-services`** — each service may include a `prefetch` object whose values are **FHIR query templates** (strings), e.g. `"patient": "Patient/{{context.patientId}}"`.
2. **Before invoke** — the client substitutes hook context placeholders and executes those queries against **its** FHIR server (using the client’s authorization).
3. **`POST /cds-services/{id}`** — the client sends:
   - `context` (`patientId`, `userId`, `measurementPeriod` for eCQM, …)
   - `prefetch` — **populated** data: `patient` is the Patient **resource JSON**; other keys are typically searchset `Bundle`s.

The smoke script (`scripts/cds-cms165-prefetch-smoke.sh`) and Postman collection (`postman/atrius-cds-cms165.postman_collection.json`) **simulate** this client behavior for local dev. They are not production backend paths.

## What the backend implements

### 1. cds-server — templates on discovery, pass-through on invoke

**Discovery** copies prefetch **templates** from the manifest into each `CdsService` returned by `GET /cds-services`. Templates are not resolved server-side.

Sources:

- `manifests/cds-services-kr.json` (generated)
- `scripts/cds_manifest_common.py` — `STANDARD_PATIENT_CHART_PREFETCH` (fallback **only** when PlanDefinition has no `action.input`); authored inputs map 1:1 to discovery templates
- `crates/cds-server/src/kr_manifest.rs` — same standard templates when building from KR

**Invoke** receives the client’s populated `prefetch` and forwards it to the JVM sidecar on **both** evaluation paths:

- `PlanDefinition/$apply` (when manifest has `planDefinitionId`)
- Legacy `evaluate/expression` (most generated eCQM services)

Implementation: `crates/cds-server/src/services/mod.rs` — `prefetch_map_for_sidecar()` strips null entries and omits empty maps; no FHIR logic.

```text
POST /cds-services/{id}
  request.prefetch  →  prefetch_map_for_sidecar()
                    →  EvaluateExpressionRequestBuilder::prefetch()  OR  ApplyPlanDefinitionRequestBuilder::prefetch()
                    →  POST sidecar /v1/evaluate/expression  OR  /v1/plandefinition/apply
```

**cds-server does not:**

- Substitute `{{context.patientId}}` in templates
- Call clinical HFS or cr-fhir-bridge to fill prefetch
- Require prefetch (empty `{}` is valid — see REST fallback below)

**Related (not prefetch):** `context.measurementPeriod` is parsed and forwarded as CQL `parameters["Measurement Period"]` (`crates/cds-server/src/measurement_period.rs`). That is the eCQM **reporting window**, also supplied by the client on each invoke.

### 2. atrius-clinical-reasoning — HTTP plumbing

Optional `prefetch` on sidecar request DTOs and builders:

- `EvaluateExpressionRequest` / `EvaluateExpressionRequestBuilder`
- `ApplyPlanDefinitionRequest` / `ApplyPlanDefinitionRequestBuilder`

This is transport only: serialize the client-supplied map into the JSON body the sidecar expects.

### 3. JVM sidecar (external repo) — consume prefetch

This is where backend behavior affects **performance and REST traffic** after the client has done its job.

When `prefetch` is present on `evaluate/expression`:

| Component | Role |
|-----------|------|
| `PrefetchRetrieveSupport` | Flatten prefetch (Patient resource + searchset `Bundle`s) into a resource list |
| `SidecarPrefetchRetrieveProvider` | Answer CQL `retrieve` from prefetched resources (profile type aliasing, `:in` / ValueSet filters via HTS) |
| `PriorityRetrieveProvider` | Prefetch first; REST against `hfsBaseUrl` (cr-fhir-bridge) only for misses |
| `CachedR4FhirTerminologyProvider` | Process-wide ValueSet `$expand` cache per HTS base |

When `prefetch` is empty or omitted, behavior is unchanged: the sidecar retrieves all clinical data via REST through the bridge.

**cr-fhir-bridge** is unchanged for prefetch specifically. It remains the FHIR base for sidecar REST fallback (`CDS_HFS_BASE_URL`) and for local client-simulated testing.

### 4. Manifest generation — wider templates, not wider server fetch

`scripts/generate-cds-hooks-manifest.py` uses `data_requirements_to_prefetch()` from each PlanDefinition’s `action.input`. When inputs are present, discovery prefetch is **exactly those types**. The standard chart pack is used only if `action.input` is empty (legacy). That controls **what the client is told to fetch** at discovery time.

## End-to-end flow

```text
┌─────────────────────────────────────────────────────────────────┐
│ CDS client (EHR)                                                │
│  GET /cds-services → read prefetch TEMPLATES                    │
│  FHIR queries against EHR server → populated prefetch           │
│  POST /cds-services/{id} + context.measurementPeriod            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ cds-server                                                      │
│  Discovery: return templates (from manifest)                    │
│  Invoke:    forward prefetch + Measurement Period → sidecar     │
│             (no server-side prefetch retrieval)                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ JVM sidecar                                                     │
│  Prefetch overlay for CQL retrieve                              │
│  REST fallback via hfsBaseUrl (bridge) for anything not in      │
│  prefetch                                                       │
└─────────────────────────────────────────────────────────────────┘
```

## Invoke modes (testing vs production)

| Mode | `prefetch` on POST | Backend behavior |
|------|-------------------|------------------|
| **Production (EHR)** | Populated by client per discovery templates | Sidecar uses overlay; minimal REST |
| **Context-only smoke** | `{}` or omitted | Sidecar uses REST only — correct but slower |
| **Client-simulated smoke** | Script/Postman fetches from bridge | Same as production path for prefetch |

## Key source files

| Area | Path |
|------|------|
| CDS Hooks models (templates vs populated) | `crates/cds-hooks/src/models.rs` |
| Forward prefetch on invoke | `crates/cds-server/src/services/mod.rs` |
| Measurement Period → CQL | `crates/cds-server/src/measurement_period.rs` |
| Sidecar request DTOs | `crates/atrius-clinical-reasoning/src/dto.rs`, `request_builder.rs` |
| Standard prefetch templates | `scripts/cds_manifest_common.py` |
| Generated manifest | `manifests/cds-services-kr.json` |
| Client simulation (local) | `scripts/cds-cms165-prefetch-smoke.sh`, `scripts/cds-er-chest-pain-smoke.sh` |
| Sidecar prefetch consume | JVM repo: `PrefetchRetrieveSupport.kt`, `SidecarPrefetchRetrieveProvider.kt` |
| BFF prefetch resolver | `atrius-bff/src/prefetch.rs`, `handlers/cds.rs` |

## BFF as CDS client (atrius-clinical-ui)

The clinical UI does **not** call cds-server directly. **atrius-bff** acts as the CDS Hooks client for SMART-launched sessions:

1. **`GET cds-server/cds-services`** — load prefetch templates for `er-chest-pain-pathway`
2. **Resolve** `{{context.patientId}}` / `{context.patientId}` in each template
3. **Fetch** populated resources from clinical HFS using the session SMART bearer token
4. **`POST cds-server/cds-services/{id}`** — invoke with populated `prefetch`, `fhirServer`, and `fhirAuthorization`

Optional prefetch keys that fail (404, etc.) become empty searchset Bundles; **`patient`** must succeed.

This matches the smoke script behavior and avoids relying on sidecar REST fallback for chart data during `$apply`.

## SMART `fhirAuthorization` (beyond prefetch)

When prefetch is incomplete or REST fallback is needed, production CDS clients pass
`fhirServer` + `fhirAuthorization` per the CDS Hooks spec. cds-server validates the object,
sets sidecar `hfsBaseUrl` to `fhirServer`, and forwards the bearer token for **clinical FHIR
only** (HTS and KR stay on cds-server config).

See [cds-server README](../../crates/cds-server/README.md#smart-fhirauthorization-production-fhir-access)
and [postman/cms165-invoke-smart-auth.json](./postman/cms165-invoke-smart-auth.json).

## See also

- [startup-guide.md](./startup-guide.md) — run stack, Postman, smoke script
- [README.md](./README.md) — architecture and routing (`hfsBaseUrl` → bridge)
- [crates/cds-server/README.md](../../crates/cds-server/README.md) — cds-server config and Measurement Period

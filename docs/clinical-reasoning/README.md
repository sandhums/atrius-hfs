# Clinical Reasoning & CDS Stack

This document describes how the **Atrius clinical reasoning stack** fits together: CDS Hooks, the JVM CQL sidecar, dual FHIR servers (clinical + knowledge repository), terminology, and runtime Atrius→QI-Core projection.

## Overview

Helios splits responsibilities across Rust services (orchestration, FHIR storage, terminology, projection) and an **external JVM clinical reasoning sidecar** (CQFramework CQL engine). Clinical data is stored **Atrius-profiled**; eCQM logic runs against **QI-Core** semantics after projection through `cr-fhir-bridge`.

| Layer | Role |
|-------|------|
| **cds-server** | CDS Hooks discovery + invocation; `$apply` when manifest has `planDefinitionId`, else legacy evaluate |
| **atrius-clinical-reasoning** | Rust HTTP client for `POST /v1/evaluate/expression`, `POST /v1/plandefinition/apply`, and `POST /v1/activitydefinition/apply` |
| **JVM sidecar** (external) | CQFramework CQL + **CQF Clinical Reasoning** (`PlanDefinition/$apply`, `ActivityDefinition/$apply`); FHIR retrieve + terminology |
| **cr-fhir-bridge** | Proxy + Atrius→QI-Core projection for sidecar `hfsBaseUrl`; FHIR REST **`PlanDefinition/$apply`** and **`ActivityDefinition/$apply`** (Parameters ↔ sidecar) |
| **Clinical HFS** | Patient chart data (Conditions, Observations, Encounters, …) |
| **KR HFS** | Knowledge Repository: `Library`, `Measure`, CDS manifest `Binary` |
| **HTS** | Terminology: `$expand`, `$validate-code`, `$lookup` for ValueSets |

## Architecture diagram

```text
┌─────────────┐     GET/POST /cds-services     ┌──────────────┐
│ EHR / client│ ─────────────────────────────► │  cds-server  │
└─────────────┘                                │   :8095      │
                                               └──────┬───────┘
                                                      │ POST /v1/evaluate/expression
                                                      ▼
                                               ┌──────────────┐
                                               │ JVM sidecar  │
                                               │   :8088      │
                                               └───┬───┬───┬──┘
                    hfsBaseUrl (8081) ────────────┘   │   └──────── htsBaseUrl
                    libraryBaseUrl (8079) ────────────┘
                                                       │
         ┌─────────────────────────────────────────────┼────────────────────────┐
         ▼                                             ▼                        ▼
  ┌─────────────┐                              ┌─────────────┐          ┌─────────────┐
  │cr-fhir-bridge│                             │   KR HFS    │          │    HTS      │
  │   :8081     │                              │   :8079     │          │  :9091*     │
  └──────┬──────┘                              └─────────────┘          └─────────────┘
         │ /Library* ──proxy──► KR (pass-through)
         │ /* clinical ──proxy──► Clinical HFS + QI-Core projection
         ▼
  ┌─────────────┐
  │ Clinical HFS│
  │   :8082     │
  └─────────────┘

* HTS default port is 8090; align `HFS_TERMINOLOGY_SERVER` and `CDS_HTS_BASE_URL` with your listen port.
```

## Critical routing: `hfsBaseUrl` vs `libraryBaseUrl`

The JVM sidecar uses **two FHIR bases**:

| URL field | Used for |
|-----------|----------|
| **`libraryBaseUrl`** | Primary CQL library: `GET /Library/{id}` for the measure/library under evaluation |
| **`hfsBaseUrl`** | Clinical data **and** CQL **`include`** dependencies (e.g. `FHIRHelpers`) |

In the Atrius stack, **`hfsBaseUrl` must point at `cr-fhir-bridge`**, not raw clinical HFS:

1. Clinical reads (`Patient`, `Condition`, …) are projected Atrius→QI-Core before the sidecar sees them.
2. `/Library/*` requests on `hfsBaseUrl` are proxied to KR when `CR_FHIR_BRIDGE_KR_URL` is set.

```bash
# Sidecar / cds-server
CDS_HFS_BASE_URL=http://127.0.0.1:8081      # bridge, NOT 8082
CDS_LIBRARY_BASE_URL=http://127.0.0.1:8079   # KR
CDS_HTS_BASE_URL=http://127.0.0.1:9091       # match your HTS listen port

# Bridge
CR_FHIR_BRIDGE_UPSTREAM_URL=http://127.0.0.1:8082   # clinical HFS
CR_FHIR_BRIDGE_KR_URL=http://127.0.0.1:8079          # KR for /Library/*
```

## End-to-end flows

### 1. CDS Hooks `patient-view`

1. Client: `GET /cds-services` → catalog + **prefetch templates** (FHIR query strings) from KR `Binary` or local manifest.
2. Client: resolves templates against its FHIR server → **populated `prefetch`**; `POST /cds-services/{id}` with context (`patientId`, `userId`, `measurementPeriod`, …).
3. **cds-server** forwards context + prefetch (pass-through only — no server-side prefetch fetch); builds sidecar request (`libraryId`, `expression`, FHIR bases, Measurement Period).
4. **Sidecar** loads ELM from KR; uses prefetch overlay for CQL retrieve when present, else REST via bridge; calls HTS for ValueSet membership.
5. **cds-server** maps sidecar result → CDS `Card`s (extension `https://atrius.dev/cds-clinical-reasoning-eval-result`).

Prefetch client vs backend split: **[cds-prefetch.md](./cds-prefetch.md)**.

### 2. Direct sidecar evaluate (development / eCQM testing)

```bash
curl -s -X POST http://127.0.0.1:8088/v1/evaluate/expression \
  -H "Content-Type: application/json" \
  -d '{
    "libraryId": "CMS165FHIRControllingHighBloodPressure",
    "libraryVersion": "0.3.000",
    "expression": "Numerator",
    "hfsBaseUrl": "http://127.0.0.1:8081",
    "htsBaseUrl": "http://127.0.0.1:9091",
    "libraryBaseUrl": "http://127.0.0.1:8079",
    "resolveLibraryArtifactsFromFhir": true,
    "patientId": "cms165-demo",
    "parameters": {
      "Measurement Period": {
        "low": "2025-01-01", "high": "2025-12-31",
        "lowClosed": true, "highClosed": true
      }
    }
  }'
```

During evaluation the sidecar drives HFS searches with `:in` modifiers; **clinical HFS** (via bridge) calls **HTS** `$expand` for each referenced ValueSet.

### 3. Terminology during CQL

CQL `InValueSet` / retrieve with ValueSet filters → sidecar → HTS `POST /ValueSet/$expand`.

Clinical HFS must have `HFS_TERMINOLOGY_SERVER` set so `:in` search modifiers expand ValueSets before querying the index. Point this at the **same HTS instance** the sidecar uses.

## Recommended local port map

| Service | Port | Env / flag |
|---------|------|------------|
| Clinical HFS | 8082 | `HFS_SERVER_PORT`, `deploy/clinical/.env.atrius` |
| KR HFS | 8079 | `deploy/kr/.env.kr`, `scripts/run-kr-hfs.sh` |
| cr-fhir-bridge | 8081 | `CR_FHIR_BRIDGE_PORT` |
| HTS | 9091 or 8090 | `HTS_SERVER_PORT` — **use one everywhere** |
| JVM sidecar | 8088 | `SIDECAR_PORT` (sidecar repo) |
| cds-server | 8095 | `CDS_SERVER_PORT` |

Use **`127.0.0.1` consistently** (not `localhost`) if the sidecar runs in Docker or a JVM that resolves `localhost` differently from Rust services.

## What must be in place for everything to work

Checklist before expecting CMS165 (or similar eCQM) to evaluate cleanly:

### Processes (startup order)

1. **HTS** — terminology loaded (see [data-import.md](./data-import.md))
2. **Clinical HFS** — Atrius profile manifest, `HFS_TERMINOLOGY_SERVER` → HTS
3. **KR HFS** — eCQM `Library` resources imported
4. **cr-fhir-bridge** — upstream clinical + KR URLs configured
5. **JVM sidecar** — listening on 8088
6. **cds-server** (optional for CDS Hooks) — sidecar URL + manifest

### Data

| Data | Where | Required for |
|------|-------|--------------|
| VSAC ValueSets (compose JSON) | HTS (bundled in terminology package) | ValueSet identity |
| SNOMED CT concepts | HTS `concepts` table | Most eCQM expansions, clinical codes |
| ICD-10-CM | HTS | Diagnosis ValueSets |
| RxNorm | HTS | Medication exclusion ValueSets |
| CPT / HCPCS | HTS (optional) | Some exclusion ValueSets only |
| Atrius-profiled patient chart | Clinical HFS | CQL retrieve |
| eCQM Libraries (+ ELM) | KR HFS | CQL evaluation |
| CDS service manifest | KR `Binary` or local JSON | cds-server discovery |

### Configuration alignment

- `CDS_HFS_BASE_URL` = bridge URL (**8081**)
- Sidecar evaluate requests use the same `hfsBaseUrl` / `htsBaseUrl` / `libraryBaseUrl`
- `CR_FHIR_BRIDGE_KR_URL` set so `FHIRHelpers` and other includes resolve
- `Library.version` on KR matches ELM identifier version (re-import with `import-ecqm-kr-libraries.py` if mismatched)
- CDS catalog generated from KR Libraries: `scripts/generate-cds-hooks-manifest.py` → `manifests/cds-services-kr-ecqm.json` (see [data-import.md](./data-import.md))

## Crates & entry points

| Crate | Binary / lib | Doc |
|-------|----------------|-----|
| `cds-server` | `cargo run -p cds-server` | [crates/cds-server/README.md](../../crates/cds-server/README.md) |
| `cr-fhir-bridge` | `cargo run --bin cr-fhir-bridge` | [crates/cr-fhir-bridge/README.md](../../crates/cr-fhir-bridge/README.md) |
| `atrius-clinical-reasoning` | library | [crates/atrius-clinical-reasoning/README.md](../../crates/atrius-clinical-reasoning/README.md) |
| `atrius-runtime-mapper` | library | [crates/atrius-runtime-mapper/README.md](../../crates/atrius-runtime-mapper/README.md) |
| `helios-cds-hooks` | library (protocol types) | [crates/cds-hooks/README.md](../../crates/cds-hooks/README.md) |
| `helios-hfs` | `cargo run --bin hfs` | CLAUDE.md |
| `helios-hts` | `cargo run --bin hts` | [crates/hts/README.md](../../crates/hts/README.md) |

## Related docs

- [Roadmap](./roadmap.md) — **stack status, slice 1 done, slice 2→3→authoring plan**
- [Startup guide](./startup-guide.md) — step-by-step local stack
- [Production deployment](./production-deployment.md) — systemd units, `/etc/atrius` env files
- [Observability](./observability.md) — invoke logs, sidecar `GET /metrics` (minimal v1)
- [KR library pinning](./kr-library-pinning.md) — version pins, sidecar cache flush
- [Data import](./data-import.md) — terminology, clinical, **KR libraries (eCQM + Atrius)**
- [CDS prefetch](./cds-prefetch.md) — client vs backend responsibilities
- [Troubleshooting](./troubleshooting.md) — empty ValueSet expansions, 404s, version mismatches
- [HTS ↔ CQFramework](../../crates/hts/docs/cqframework_terminology.md) — terminology provider compatibility

## JVM sidecar (external)

The sidecar is **not built in this repository**. It exposes:

- `POST /v1/evaluate/expression` — named CQL expression (legacy fallback)
- `POST /v1/plandefinition/apply` — FHIR **`PlanDefinition/$apply`** via `cqf-fhir-cr` 4.7.0
- `POST /v1/activitydefinition/apply` — FHIR **`ActivityDefinition/$apply`** (draft request resource from `kind` + dynamicValue)
- `GET /metrics` — process counters (evaluate/apply totals, library cache hits/misses, KR fetches)

Request/response types: `atrius-clinical-reasoning/src/dto.rs`.

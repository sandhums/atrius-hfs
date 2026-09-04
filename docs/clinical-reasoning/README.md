# Clinical Reasoning & CDS Stack

This document describes how the **Atrius clinical reasoning stack** fits together: CDS Hooks, the JVM CQL sidecar, dual FHIR servers (clinical + knowledge repository), and terminology.

## Overview

Helios splits responsibilities across Rust services (orchestration, FHIR storage, terminology) and an **external JVM clinical reasoning sidecar** (CQFramework CQL engine). Atrius-authored CQL reads clinical profiles directly via `hfsBaseUrl`; **`libraryBaseUrl` (KR) serves primary and include `Library` artifacts**.

| Layer | Role |
|-------|------|
| **cds-server** | CDS Hooks discovery + invocation; `$apply` when manifest has `planDefinitionId`, else legacy evaluate |
| **cds-server::clinical_reasoning** | Rust HTTP client for `POST /v1/evaluate/expression`, `POST /v1/plandefinition/apply`, and `POST /v1/activitydefinition/apply` |
| **JVM sidecar** (external) | CQFramework CQL + **CQF Clinical Reasoning** (`PlanDefinition/$apply`, `ActivityDefinition/$apply`); Library includes from KR |
| **Clinical HFS** | Patient chart data (Conditions, Observations, Encounters, …) |
| **KR HFS** | Knowledge Repository: `Library` (primary + includes), `Measure`, CDS manifest `Binary` |
| **HTS** | Terminology: `$expand`, `$validate-code`, `$lookup` for ValueSets |

## Architecture diagram

```text
┌─────────────┐     GET/POST /cds-services     ┌──────────────┐
│ EHR / client│ ─────────────────────────────► │  cds-server  │
└─────────────┘                                │   :8095      │
                                               └──────┬───────┘
                                                      │ POST /v1/plandefinition/apply
                                                      │   (or /v1/evaluate/expression)
                                                      ▼
                                               ┌──────────────┐
                                               │ JVM sidecar  │
                                               │   :8088      │
                                               └───┬───┬───┬──┘
                    hfsBaseUrl (8082) ────────────┘   │   └──────── htsBaseUrl
                    libraryBaseUrl (8079) ────────────┘
                                                       │
         ┌─────────────────────────────────────────────┼────────────────────────┐
         ▼                                             ▼                        ▼
  ┌─────────────┐                              ┌─────────────┐          ┌─────────────┐
  │ Clinical HFS│                              │   KR HFS    │          │    HTS      │
  │   :8082     │                              │   :8079     │          │  :9091*     │
  └─────────────┘                              └─────────────┘          └─────────────┘

* HTS default port is 8090; align `HFS_TERMINOLOGY_SERVER` and `CDS_HTS_BASE_URL` with your listen port.
```

## Critical routing: `hfsBaseUrl` vs `libraryBaseUrl`

The JVM sidecar uses **two FHIR bases**:

| URL field | Used for |
|-----------|----------|
| **`libraryBaseUrl`** | **Required** KR base: primary CQL `Library` **and** all CQL **`include`** libraries (`FHIRHelpers`, `AtriusCommon`, …) |
| **`hfsBaseUrl`** | Clinical data only (`Patient`, `Condition`, …) |

Includes do **not** load from `hfsBaseUrl`.

```bash
# Sidecar / cds-server
CDS_HFS_BASE_URL=http://127.0.0.1:8082      # clinical HFS
CDS_LIBRARY_BASE_URL=http://127.0.0.1:8079   # KR (primary + includes)
CDS_HTS_BASE_URL=http://127.0.0.1:9091       # match your HTS listen port
```

## End-to-end flows

### 1. CDS Hooks `patient-view`

1. Client: `GET /cds-services` → catalog + **prefetch templates** (FHIR query strings) from KR `Binary` or local manifest.
2. Client: resolves templates against its FHIR server → **populated `prefetch`**; `POST /cds-services/{id}` with context (`patientId`, `userId`, `measurementPeriod`, …).
3. **cds-server** forwards context + prefetch (pass-through only — no server-side prefetch fetch); builds sidecar request (`libraryId`, `expression`, FHIR bases, Measurement Period).
4. **Sidecar** loads ELM from KR; uses prefetch overlay for CQL retrieve when present, else REST against clinical HFS; calls HTS for ValueSet membership.
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
    "hfsBaseUrl": "http://127.0.0.1:8082",
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

During evaluation the sidecar drives HFS searches with `:in` modifiers; **clinical HFS** calls **HTS** `$expand` for each referenced ValueSet.

### 3. Terminology during CQL

CQL `InValueSet` / retrieve with ValueSet filters → sidecar → HTS `POST /ValueSet/$expand`.

Clinical HFS must have `HFS_TERMINOLOGY_SERVER` set so `:in` search modifiers expand ValueSets before querying the index. Point this at the **same HTS instance** the sidecar uses.

## Recommended local port map

| Service | Port | Start with |
|---------|------|------------|
| HTS | 9091 | `./scripts/run-hts.sh` → `deploy/env/hts.env` |
| Clinical HFS | 8082 | `./scripts/run-hfs.sh` → `deploy/env/hfs-clinical.env` |
| KR HFS | 8079 | `./scripts/run-kr-hfs.sh` → `deploy/env/hfs-kr.env` |
| JVM sidecar | 8088 | `./scripts/run-cql-sidecar.sh` |
| cds-server | 8095 | `./scripts/run-cds-server.sh` → `deploy/env/cds-server.env` |

Build once: `./scripts/build-clinical-reasoning.sh`. Full map: [scripts/README.md](../../scripts/README.md).

Use **`127.0.0.1` consistently** (not `localhost`) if the sidecar runs in Docker or a JVM that resolves `localhost` differently from Rust services.

## What must be in place for everything to work

Checklist before expecting CMS165 (or similar eCQM) to evaluate cleanly:

### Processes (startup order)

1. **HTS** — terminology loaded (see [data-import.md](./data-import.md))
2. **Clinical HFS** — `HFS_FHIR_PACKAGES` overlay + `HFS_VALIDATION_MODE`, `HFS_TERMINOLOGY_SERVER` → HTS
3. **KR HFS** — eCQM / Atrius `Library` resources imported
4. **JVM sidecar** — listening on 8088
5. **cds-server** (optional for CDS Hooks) — sidecar URL + manifest

### Data

| Data | Where | Required for |
|------|-------|--------------|
| VSAC ValueSets (compose JSON) | HTS (bundled in terminology package) | ValueSet identity |
| SNOMED CT concepts | HTS `concepts` table | Most eCQM expansions, clinical codes |
| ICD-10-CM | HTS | Diagnosis ValueSets |
| RxNorm | HTS | Medication exclusion ValueSets |
| CPT / HCPCS | HTS (optional) | Some exclusion ValueSets only |
| Atrius-profiled patient chart | Clinical HFS | CQL retrieve |
| eCQM / Atrius Libraries (+ ELM) | KR HFS | CQL evaluation |
| CDS service manifest | KR `Binary` or local JSON | cds-server discovery |

### Configuration alignment

- `CDS_HFS_BASE_URL` = clinical HFS (**8082**)
- `CDS_LIBRARY_BASE_URL` = KR (**8079**) for primary + include libraries
- Sidecar evaluate / `$apply` requests use the same `hfsBaseUrl` / `htsBaseUrl` / `libraryBaseUrl`
- `Library.version` on KR matches ELM identifier version (re-import with AtriusIGDraft `import-atrius-kr-libraries.py` if mismatched)
- CDS catalog generated from KR PlanDefinitions: `scripts/generate-cds-hooks-manifest.py` → `manifests/cds-services-kr.json` (see [data-import.md](./data-import.md))

## Crates & entry points

| Crate | Local start | Doc |
|-------|-------------|-----|
| `helios-hts` | `./scripts/run-hts.sh` | [crates/hts/README.md](../../crates/hts/README.md) |
| `helios-hfs` | `./scripts/run-hfs.sh` / `./scripts/run-kr-hfs.sh` | CLAUDE.md |
| `cds-server` | `./scripts/run-cds-server.sh` | [crates/cds-server/README.md](../../crates/cds-server/README.md) |
| JVM sidecar | `./scripts/run-cql-sidecar.sh` | external JVMsidecar repo |
| `cds-server::clinical_reasoning` | module | sidecar HTTP client (in cds-server) |
| `helios-cds-hooks` | library (protocol types) | [crates/cds-hooks/README.md](../../crates/cds-hooks/README.md) |

Build all Rust binaries: `./scripts/build-clinical-reasoning.sh`. Script map: [scripts/README.md](../../scripts/README.md).

## Related docs

- [startup-guide.md](./startup-guide.md) — ordered local startup
- [cds-prefetch.md](./cds-prefetch.md) — prefetch templates vs pass-through
- [kr-library-pinning.md](./kr-library-pinning.md) — library versions + cache flush
- [production-deployment.md](./production-deployment.md) — systemd install
- [troubleshooting.md](./troubleshooting.md)
- [data-import.md](./data-import.md)

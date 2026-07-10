# Clinical Reasoning Stack — Startup Guide

Step-by-step local setup for CDS Hooks + eCQM evaluation with the Atrius profile stack.

## Prerequisites

- Rust toolchain (see root `rust-version`)
- Python 3.11+ (import scripts)
- JVM clinical reasoning sidecar built and runnable (external repo)
- Licensed terminology as needed: SNOMED RF2, RxNorm RRF (see [data-import.md](./data-import.md))

## 1. Import data (once per database)

Complete [data-import.md](./data-import.md) before starting services. Minimum for CMS165 demo:

```bash
# HTS terminology + VSAC ValueSets
cargo run --bin hts -- import ./crates/hts/terminology-data --database-url ./data/hts.db
cargo run --bin hts -- import ./SnomedCT_InternationalRF2_*.zip --format snomed-rf2 --database-url ./data/hts.db
cargo run --bin hts -- import ./icd10cm_tabular_2025.xml --database-url ./data/hts.db
cargo run --bin hts -- import ./RxNorm_full_current/rrf/ --format rxnorm --database-url ./data/hts.db

# Clinical demo patient (Atrius profiles)
./scripts/import-cms165-demo.py --verify
# Or Synthea: ./scripts/import-synthea-atrius.py --patients 1 ./path/to/synthea/output/fhir/

# KR libraries (AtriusIGDraft)
IMPORT_ATRIUS=1 ./scripts/setup-plandefinition-cds-catalog.sh
# or: (cd $ATRIUS_IG && ./scripts/translate-cql.sh && ./scripts/import-atrius-kr-libraries.py --clinical-reasoning)
```

Verify concept counts:

```bash
sqlite3 ./data/hts.db "SELECT COUNT(*) FROM concepts;"   # expect 400k+ with SNOMED+ICD+RxNorm
```

## 2. Start services

Use **one terminal per service**, or **systemd** for production — see [production-deployment.md](./production-deployment.md). **Order matters**: HTS and both HFS instances before bridge and sidecar.

```bash
./scripts/build-clinical-reasoning.sh   # once after pulling / changing Rust code
```

Script inventory: [scripts/README.md](../../scripts/README.md).

### HTS (terminology)

```bash
./scripts/run-hts.sh
# GET http://127.0.0.1:9091/health
```

### Clinical HFS (Atrius chart data)

```bash
./scripts/run-hfs.sh
# GET http://127.0.0.1:8082/health
# Env: deploy/env/hfs-clinical.env (HFS_DEFAULT_TENANT must match chart data / bridge default tenant)
```

### KR HFS (libraries)

```bash
./scripts/run-kr-hfs.sh
# GET http://127.0.0.1:8079/health
# Env: deploy/env/hfs-kr.env (same release `hfs` binary as clinical, different DB/port)
```

### cr-fhir-bridge

```bash
./scripts/run-cr-fhir-bridge.sh
# GET http://127.0.0.1:8081/health
# Sets CR_FHIR_BRIDGE_DEFAULT_TENANT to match clinical HFS when callers omit X-Tenant-ID
```

### JVM sidecar

```bash
./scripts/run-cql-sidecar.sh
# GET http://127.0.0.1:8088/health
# Uses JVMSIDECAR_HOME (default ~/IdeaProjects/JVMsidecar) or SIDECAR_JAR
```

Never bind the sidecar to **8081** (that port is the bridge). Restart the sidecar after KR re-import or sidecar code changes (`mvn -q -DskipTests compile` in JVMsidecar).

### cds-server (CDS Hooks)

```bash
./scripts/run-cds-server.sh
# GET http://127.0.0.1:8095/cds-services
# Env: deploy/env/cds-server.env — CDS_HFS_BASE_URL must be the bridge (:8081)
```

### Atrius CMS165 (after Atrius IG libraries imported to KR)

```bash
# From AtriusIGDraft (KR must be up):
# ./scripts/translate-cql.sh && ./scripts/import-atrius-kr-libraries.py

curl -s http://127.0.0.1:8095/cds-services \
  | jq '.services[] | select(.id=="atriuscms165controllinghighbp") | .prefetch'

./scripts/cds-cms165-prefetch-smoke.sh
# ./scripts/cds-cms165-prefetch-smoke.sh --apply   # eCQM PlanDefinition service id

# Expect cards for cms165-demo when demo data + period align:
# ./scripts/import-cms165-demo.py --verify

# Context-only (empty prefetch — REST fallback):
curl -s -X POST http://127.0.0.1:8095/cds-services/atriuscms165controllinghighbp \
  -H 'Content-Type: application/json' \
  -d @docs/clinical-reasoning/postman/cms165-invoke-context-only.json | jq .
```

cds-server returns plain text (not JSON) on 502/412 — check HTTP status before piping to `jq`.

### ER chest pain pathway (encounter-start)

After importing Atrius KR libraries (PlanDefinition `er-chest-pain-pathway`, Library `AtriusERChestPainPathway`):

```bash
# cds-server must load manifests/cds-services-kr.json (includes er-chest-pain-pathway)
./scripts/run-cds-server.sh

./scripts/cds-er-chest-pain-smoke.sh
./scripts/cds-er-chest-pain-smoke.sh --bridge-apply   # optional FHIR REST $apply

TEST_PATIENT_ID=my-ed-patient TEST_ENCOUNTER_ID=Encounter/xyz ./scripts/cds-er-chest-pain-smoke.sh
```

Zero cards usually means CQL **In Scope** is false — confirm ED encounter with chest pain chief complaint exists for the test patient.

Full stack documentation: **`AtriusIGDraft/docs/clinical-reasoning-stack.md`**.

## CDS client responsibilities (prefetch & invoke)

CDS Hooks split **discovery** from **invoke**. The Atrius stack follows the standard contract: **cds-server advertises prefetch templates; the CDS client (EHR) resolves them and sends populated resources on each invoke.** cds-server does not fetch clinical data for prefetch.

```text
GET  /cds-services
       → each service includes prefetch TEMPLATES (FHIR search URLs with {{context.patientId}})

CDS client (EHR)
       → resolves templates against its FHIR server (or cr-fhir-bridge in local dev)
       → builds hook context (patientId, userId, measurementPeriod, …)
       → POST /cds-services/{id} with populated prefetch object

cds-server → forwards context + prefetch to JVM sidecar evaluate path
sidecar    → prefetch overlay first; REST fallback only for gaps
```

| Responsibility | Owner | Notes |
|----------------|-------|-------|
| Prefetch **templates** on service definition | cds-server (from manifest) | e.g. `"patient": "Patient/{{context.patientId}}"` |
| Resolve templates → FHIR resources | **CDS client** | Patient resource JSON, search `Bundle`s per key |
| **Measurement Period** (eCQM reporting window) | **CDS client** on each invoke | `context.measurementPeriod.low` / `.high` (aliases `start` / `end`) |
| Forward prefetch + context to CQL | cds-server → sidecar | No server-side prefetch retrieval |

### Two invoke modes (testing)

| Mode | `prefetch` on POST | Use when |
|------|-------------------|----------|
| **Context-only** | `{}` or omitted | Quick smoke; sidecar uses REST against `CDS_HFS_BASE_URL` (bridge) |
| **Client-simulated** | Full resources per manifest keys | Production-like; fewer REST calls, faster eval |

**Context-only** — minimum fields for CMS165 demo:

```json
POST http://127.0.0.1:8095/cds-services/atriuscms165controllinghighbp
{
  "hook": "patient-view",
  "hookInstance": "postman-context-only",
  "context": {
    "patientId": "cms165-demo",
    "userId": "Practitioner/example",
    "measurementPeriod": { "low": "2026-01-01", "high": "2026-12-31" }
  },
  "prefetch": {}
}
```

Saved copy: [postman/cms165-invoke-context-only.json](./postman/cms165-invoke-context-only.json).

**Client-simulated** — same as an EHR after discovery:

1. `GET` each template URL from discovery (against the client’s `fhirServer` / bridge).
2. Place results under matching prefetch keys (`patient`, `conditions`, `encounters`, …).
3. `patient` must be the **Patient resource JSON**, not an id string.

Generate a full body locally (no POST):

```bash
./scripts/cds-cms165-prefetch-smoke.sh --print-payload-only > /tmp/cms165-invoke.json
```

### Postman

Import [postman/atrius-cds-cms165.postman_collection.json](./postman/atrius-cds-cms165.postman_collection.json):

| Request | What it demonstrates |
|---------|---------------------|
| **Discovery — CMS165 prefetch templates** | `GET /cds-services` → `.prefetch` on `atriuscms165controllinghighbp` |
| **Invoke — context only** | `measurementPeriod` + empty prefetch (REST fallback) |
| **Invoke — client-simulated prefetch** | Pre-request script fetches from `bridge_base`, builds populated `prefetch` |

Collection variables default to local ports (`cds_base` 8095, `bridge_base` 8081). Prerequisites: stack running + `./scripts/import-cms165-demo.py --verify`.

### What the backend implements (vs the client)

Prefetch **resolution** is not implemented on the server — only **advertisement** (discovery templates), **pass-through** (invoke → sidecar), and **consumption** (sidecar CQL overlay + REST fallback). See **[cds-prefetch.md](./cds-prefetch.md)** for component-by-component detail (cds-server, `atrius-clinical-reasoning`, JVM sidecar, manifest generation) and what was explicitly *not* built.

## 3. Smoke tests

### Bridge projection

```bash
curl -s http://127.0.0.1:8081/Patient/cms165-demo | jq '.meta.profile'
# Expect QI-Core or projected profiles, not raw Atrius-only shapes for mapped types
```

### HTS expansion

```bash
curl -s -X POST http://127.0.0.1:9091/ValueSet/\$expand \
  -H 'Content-Type: application/fhir+json' \
  -d '{"resourceType":"Parameters","parameter":[{"name":"url","valueUri":"http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.113883.3.464.1003.113.12.1074"}]}' \
  | jq '.expansion.total'
# Expect non-zero when SNOMED/ICD concepts are loaded
```

### Sidecar PlanDefinition `$apply`

When the manifest row includes `planDefinitionId`, **cds-server** calls **`POST /v1/plandefinition/apply`** (CQF Clinical Reasoning via [cqframework/clinical-reasoning](https://github.com/cqframework/clinical-reasoning)) instead of raw CQL expression evaluation:

```bash
curl -s -X POST http://127.0.0.1:8088/v1/plandefinition/apply \
  -H 'Content-Type: application/json' \
  -d '{
    "planDefinitionId": "cms165fhircontrollinghighbloodpressure",
    "patientId": "cms165-demo",
    "practitionerId": "Practitioner/example",
    "hfsBaseUrl": "http://127.0.0.1:8081",
    "htsBaseUrl": "http://127.0.0.1:9091",
    "libraryBaseUrl": "http://127.0.0.1:8079",
    "useServerData": false
  }' | jq '.requestGroup.status'
```

### Bridge FHIR REST `$apply` (Parameters in/out)

**cr-fhir-bridge** exposes spec-shaped **`PlanDefinition/$apply`** and **`ActivityDefinition/$apply`** when **`CR_FHIR_BRIDGE_SIDECAR_URL`** is set (default `http://127.0.0.1:8088`). Request and response bodies are FHIR **Parameters**; the bridge translates to/from the sidecar JSON API.

```bash
# PlanDefinition instance apply → CarePlan in Parameters.return
curl -sS -X POST http://127.0.0.1:8081/PlanDefinition/cms165fhircontrollinghighbloodpressure/\$apply \
  -H 'Content-Type: application/fhir+json' \
  -d '{
    "resourceType": "Parameters",
    "parameter": [
      { "name": "subject", "valueString": "Patient/cms165-demo" },
      { "name": "practitioner", "valueString": "Practitioner/example" }
    ]
  }' | jq '.parameter[] | select(.name=="return") | .resource.resourceType'

# ActivityDefinition type apply → draft ServiceRequest (etc.) in Parameters.return
curl -sS -X POST http://127.0.0.1:8081/ActivityDefinition/\$apply \
  -H 'Content-Type: application/fhir+json' \
  -d '{
    "resourceType": "Parameters",
    "parameter": [
      { "name": "subject", "valueString": "Patient/cms165-demo" },
      { "name": "activityDefinition", "resource": {
        "resourceType": "ActivityDefinition",
        "id": "your-activity-id"
      }}
    ]
  }' | jq '.parameter[0].resource.resourceType'
```

`GET http://127.0.0.1:8081/metadata` lists `$apply` on PlanDefinition and ActivityDefinition when the sidecar is configured.

### Sidecar evaluate (legacy / debugging)

```bash
curl -s -X POST http://127.0.0.1:8088/v1/evaluate/expression \
  -H 'Content-Type: application/json' \
  -d '{
    "libraryId":"CMS165FHIRControllingHighBloodPressure",
    "libraryVersion":"0.3.000",
    "expression":"Numerator",
    "hfsBaseUrl":"http://127.0.0.1:8081",
    "htsBaseUrl":"http://127.0.0.1:9091",
    "libraryBaseUrl":"http://127.0.0.1:8079",
    "resolveLibraryArtifactsFromFhir":true,
    "patientId":"cms165-demo",
    "parameters":{"Measurement Period":{"low":"2025-01-01","high":"2025-12-31","lowClosed":true,"highClosed":true}}
  }' | jq .
```

### CDS Hooks

```bash
curl -s http://127.0.0.1:8095/cds-services | jq '.services[].id'

# Invoke via PlanDefinition/$apply when manifest has planDefinitionId (requires context.userId + context.patientId)
curl -s -X POST http://127.0.0.1:8095/cds-services/cms165fhircontrollinghighbloodpressure \
  -H 'Content-Type: application/json' \
  -d '{
    "hook": "patient-view",
    "hookInstance": "550e8400-e29b-41d4-a716-446655440000",
    "fhirServer": "http://127.0.0.1:8081",
    "context": {
      "userId": "Practitioner/example",
      "patientId": "cms165-demo"
    }
  }' | jq .
```

## 4. Environment file reference

| File | Service |
|------|---------|
| `deploy/env/hts.env` | HTS — created by `run-hts.sh` if missing |
| `deploy/env/hfs-clinical.env` | Clinical HFS (`run-hfs.sh`) |
| `deploy/env/hfs-kr.env` | KR HFS (`run-kr-hfs.sh`) |
| `deploy/env/cr-fhir-bridge.env` | Bridge (`run-cr-fhir-bridge.sh`) |
| `deploy/env/cds-server.env` | cds-server (`run-cds-server.sh`) |
| `deploy/env/cql-sidecar.env` | Sidecar (`run-cql-sidecar.sh`) |
| `deploy/env/*.env.example` | Templates (also used for `/etc/atrius/*.env` in production) |
| `deploy/clinical/.env.atrius.example` | Legacy sqlite-oriented notes — prefer `deploy/env/hfs-clinical.env` |

See [scripts/README.md](../../scripts/README.md) for the full script map.

## Port consistency checklist

- [ ] `HFS_TERMINOLOGY_SERVER` (clinical HFS) = `CDS_HTS_BASE_URL` = sidecar `htsBaseUrl`
- [ ] `CDS_HFS_BASE_URL` = bridge (**not** clinical HFS direct)
- [ ] `CR_FHIR_BRIDGE_UPSTREAM_URL` = clinical HFS
- [ ] All URLs use the same host (`127.0.0.1` vs `localhost`)

See [troubleshooting.md](./troubleshooting.md) if any step fails.

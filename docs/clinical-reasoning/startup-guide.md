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

# KR eCQM libraries
./scripts/import-ecqm-kr-libraries.py --download
```

Verify concept counts:

```bash
sqlite3 ./data/hts.db "SELECT COUNT(*) FROM concepts;"   # expect 400k+ with SNOMED+ICD+RxNorm
```

## 2. Start services

Use separate terminals for local dev, or **systemd** for production — see [production-deployment.md](./production-deployment.md). **Order matters**: HTS and both HFS instances before bridge and sidecar.

### HTS (terminology)

```bash
HTS_DATABASE_URL=./data/hts.db HTS_SERVER_PORT=9091 cargo run --bin hts
# GET http://127.0.0.1:9091/health
```

### Clinical HFS (Atrius chart data)

```bash
# From repo root; uses deploy/clinical/.env.atrius or equivalent
HFS_SERVER_PORT=8082 \
HFS_DATABASE_URL=./data/clinical-fhir.db \
HFS_PROFILE_MANIFEST=manifests/atrius-r4-profile-manifest-core.json \
HFS_TERMINOLOGY_SERVER=http://127.0.0.1:9091 \
  cargo run --bin hfs
# GET http://127.0.0.1:8082/health
```

### KR HFS (libraries)

```bash
./scripts/run-kr-hfs.sh
# Or: source deploy/kr/.env.kr && cargo run --bin hfs
# GET http://127.0.0.1:8079/health
```

### cr-fhir-bridge

```bash
CR_FHIR_BRIDGE_PORT=8081 \
CR_FHIR_BRIDGE_UPSTREAM_URL=http://127.0.0.1:8082 \
CR_FHIR_BRIDGE_KR_URL=http://127.0.0.1:8079 \
  cargo run --bin cr-fhir-bridge
# GET http://127.0.0.1:8081/health
```

### JVM sidecar

Build and run from the **JVMsidecar** project (port **8088** only — never bind the sidecar to **8081**).

```bash
cd /path/to/JVMsidecar
mvn package
SIDECAR_PORT=8088 mvn exec:java -Dexec.mainClass=com.atrius.sidecar.MainKt
# GET http://127.0.0.1:8088/health
```

The sidecar includes **AtriusIn modelinfo** support and **process-wide library caching** (see `AtriusIGDraft/docs/clinical-reasoning-stack.md`). Restart after KR re-import.

Ensure the sidecar can reach bridge **8081**, KR **8079**, and HTS **9091**.

### cds-server (CDS Hooks)

```bash
CDS_SERVER_PORT=8095 \
CDS_CLINICAL_REASONING_URL=http://127.0.0.1:8088 \
CDS_HFS_BASE_URL=http://127.0.0.1:8081 \
CDS_HTS_BASE_URL=http://127.0.0.1:9091 \
CDS_LIBRARY_BASE_URL=http://127.0.0.1:8079 \
CDS_SERVICES_MANIFEST_PATH=./manifests/cds-services-kr-ecqm.json \
  cargo run -p cds-server
# GET http://127.0.0.1:8095/cds-services
```

### Atrius CMS165 (after Atrius IG libraries imported to KR)

```bash
# From AtriusIGDraft (KR must be up):
# ./scripts/translate-cql.sh && ./scripts/import-atrius-kr-libraries.py

# Discovery advertises standard chart prefetch (Patient + compartment searches for common QI-Core types).
curl -s http://127.0.0.1:8095/cds-services | jq '.services[] | select(.id=="atriuscms165controllinghighbp") | .prefetch'

# EHR fills prefetch from those templates; cds-server forwards to the sidecar evaluate path.
# JVM sidecar caches ValueSet $expand per HTS base (process-wide) to avoid duplicate url-search + expand calls.
# Use the helper script (builds JSON with jq — avoids shell interpolation bugs):
./scripts/cds-cms165-prefetch-smoke.sh

# PlanDefinition/$apply path (slice 3): upload PlanDefinitions + regenerate manifest first:
#   ./scripts/setup-plandefinition-cds-catalog.sh
# Then restart cds-server and smoke with populated prefetch:
#   ./scripts/cds-cms165-prefetch-smoke.sh --apply

# Pass Measurement Period on each CDS invoke (hook context — the eCQM reporting window):
#   "context": { "patientId": "...", "userId": "...", "measurementPeriod": {"low":"2026-01-01","high":"2026-12-31"} }
# Expect `"Initial Population: true"` for cms165-demo when demo data + period align (`import-cms165-demo.py --verify`).

### ER chest pain pathway (encounter-start)

After importing Atrius KR libraries (PlanDefinition `er-chest-pain-pathway`, Library `AtriusERChestPainPathway`):

```bash
# cds-server must load manifests/cds-services-kr-ecqm.json (includes er-chest-pain-pathway service)
CDS_SERVICES_MANIFEST_PATH=./manifests/cds-services-kr-ecqm.json cargo run -p cds-server

# Populated prefetch + encounter-start invoke (PlanDefinition/$apply path)
./scripts/cds-er-chest-pain-smoke.sh

# Optional: also exercise bridge FHIR REST PlanDefinition/$apply
./scripts/cds-er-chest-pain-smoke.sh --bridge-apply

# Set patient/encounter when your clinical data uses different ids
TEST_PATIENT_ID=my-ed-patient TEST_ENCOUNTER_ID=Encounter/xyz ./scripts/cds-er-chest-pain-smoke.sh
```

Zero cards usually means CQL **In Scope** is false — confirm ED encounter with chest pain chief complaint exists for the test patient.

# Avoid inline `$(curl ...)` JSON interpolation — it breaks easily and can confuse `jq`.
# Prefer `./scripts/cds-cms165-prefetch-smoke.sh` (same prefetch, safe JSON assembly).

# Minimal invoke without prefetch (sidecar fetches clinical data via REST):
curl -s -X POST http://127.0.0.1:8095/cds-services/atriuscms165controllinghighbp \
  -H 'Content-Type: application/json' \
  -d @docs/clinical-reasoning/postman/cms165-invoke-context-only.json | jq .

# cds-server returns plain text (not JSON) on 502/412 errors — if jq fails, check HTTP status:
# curl -s -o /tmp/cds.out -w "%{http_code}\n" ... && cat /tmp/cds.out
```

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
| `deploy/clinical/.env.atrius.example` | Clinical HFS (local dev) |
| `deploy/kr/.env.kr.example` | KR HFS (local dev) |
| `deploy/env/*.env.example` | Production templates → `/etc/atrius/*.env` via [production-deployment.md](./production-deployment.md) |
| `CDS_*` env vars | cds-server — see `crates/cds-server/src/config.rs` |
| `CR_FHIR_BRIDGE_*` | bridge — see `crates/cr-fhir-bridge/src/config.rs` |

## Port consistency checklist

- [ ] `HFS_TERMINOLOGY_SERVER` (clinical HFS) = `CDS_HTS_BASE_URL` = sidecar `htsBaseUrl`
- [ ] `CDS_HFS_BASE_URL` = bridge (**not** clinical HFS direct)
- [ ] `CR_FHIR_BRIDGE_UPSTREAM_URL` = clinical HFS
- [ ] All URLs use the same host (`127.0.0.1` vs `localhost`)

See [troubleshooting.md](./troubleshooting.md) if any step fails.

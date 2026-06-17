# Clinical Reasoning — Data Import

What to load, where it lives, and in what order for CDS / eCQM evaluation.

## Import order

```text
1. HTS bundled terminologies (VSAC ValueSets, ICD-10-CM tabular, THO, UCUM, …)
2. SNOMED CT RF2 (licensed)
3. ICD-10-CM XML (optional if bundled copy suffices)
4. RxNorm RRF (NLM ToS; needed for medication ValueSets)
5. LOINC (optional; Synthea vitals/labs)
6. Atrius IG → profile manifest for clinical HFS validation
7. Clinical patient data → clinical HFS
8. eCQM Libraries → KR HFS (`import-ecqm-kr-libraries.py`)
9. Atrius IG Libraries → KR HFS (AtriusIGDraft: `translate-cql.sh` + `import-atrius-kr-libraries.py`)
10. eCQM PlanDefinitions → KR HFS (generated — not in NPM package)
11. CDS manifest → KR Binary or local JSON (from PlanDefinition or Library)
```

## HTS (terminology server)

Database: `HTS_DATABASE_URL` (default `./data/hts.db`).

### Bundled package

```bash
cargo run --bin hts -- import ./crates/hts/terminology-data --database-url ./data/hts.db
```

Includes VSAC ValueSet **definitions** (compose JSON), HL7 THO CodeSystems, bundled ICD-10-CM, and more. **ValueSet compose alone is not enough** for full `$expand` — see below.

### Licensed / external terminologies

| Source | Command | Notes |
|--------|---------|-------|
| SNOMED RF2 ZIP | `hts import ./SnomedCT_*.zip --format snomed-rf2` | NRC license; ~350k+ concepts |
| ICD-10-CM XML | `hts import ./icd10cm_tabular_2025.xml` | Free (CMS) |
| RxNorm RRF folder | `hts import ./RxNorm_full_current/rrf/ --format rxnorm` | NLM ToS |
| LOINC ZIP | `hts import ./Loinc_*.zip --format loinc` | Regenstrief registration |

Verify:

```bash
sqlite3 ./data/hts.db "
SELECT cs.url, COUNT(c.id) FROM code_systems cs
LEFT JOIN concepts c ON c.system_id = cs.id
WHERE cs.url IN (
  'http://snomed.info/sct',
  'http://hl7.org/fhir/sid/icd-10-cm',
  'http://www.nlm.nih.gov/research/umls/rxnorm'
) GROUP BY cs.url;"
```

### Why `concepts` matter

HTS full `$expand` (no `count` parameter — what HFS `:in` search uses) joins enumerated ValueSet codes against the **`concepts`** table. VSAC packages store explicit code lists in `compose_json`, but expansion still validates against imported CodeSystem content unless the paginated fast path applies.

| Code system | Typical eCQM use | Import required for full `$expand` |
|-------------|------------------|-------------------------------------|
| SNOMED | Diagnoses, procedures, frailty | Yes |
| ICD-10-CM | Diagnosis ValueSets | Yes (often bundled) |
| RxNorm | Dementia meds, opioid exclusions | Yes |
| CPT | ESRD outpatient, some frailty encounters | Optional (exclusion paths only) |
| HCPCS | Home health / skilled nursing codes | Optional (exclusion paths only) |

### Stale or empty SNOMED rows

Re-importing SNOMED creates a versioned row (e.g. `snomed-ct|20260501`) while VSAC may leave an empty `current` stub. HTS prefers rows **with concepts** when resolving CodeSystems. After major re-imports, restart HTS to clear the in-process CodeSystem id cache.

If `concepts` dropped to zero unexpectedly, check:

- Pointing HTS at a different `hts.db` file
- `DELETE FROM code_systems` (cascades to concepts)
- Importing only VSAC without re-running SNOMED RF2

## Clinical HFS (patient data)

Script: [`scripts/import-synthea-atrius.py`](../../scripts/import-synthea-atrius.py)

- Adds Atrius `meta.profile` to Synthea resources before POST
- Default base URL: `http://127.0.0.1:8082`
- Requires clinical HFS with profile manifest for strict validation

```bash
./scripts/import-synthea-atrius.py --base-url http://127.0.0.1:8082 ./synthea/output/fhir/
```

**Read patient data through the bridge** (`8081`) when testing sidecar projection — not raw clinical HFS.

### Patient reference search

Compartment searches (`?patient={id}`) require bare-id reference matching in persistence (see `reference_match.rs` / SQLite reference handler). Ensure clinical data uses consistent patient ids (e.g. `cms165-demo`).

### CMS165 demo patient (`cms165-demo`)

CMS165 **Initial Population** requires:

- Essential hypertension Condition (SNOMED `59621000` in VS `…104.12.1011`)
- **Qualifying outpatient Encounter** during the measurement period (`AdultOutpatientEncounters` uses **CPT** `type:in` ValueSets)
- BP Observation during the period (LOINC panel `85354-9` / components `8480-6`, `8462-4`) linked to the encounter

HTS ships VSAC **compose** for CPT ValueSets but not CPT **concepts** (AMA license). Without CPT in HTS, `$expand` is empty and qualifying encounters never match.

Import tuned demo data (CPT seed + chart bundle):

```bash
./scripts/import-cms165-demo.py --verify
# Restart HTS if it was already running when CPT was seeded (CodeSystem id cache)
```

Bundle: [`data/clinical-reasoning/cms165-demo.bundle.json`](../../data/clinical-reasoning/cms165-demo.bundle.json) — clinical dates fall in **2026**; set `CDS_MEASUREMENT_PERIOD_LOW=2026-01-01` and `CDS_MEASUREMENT_PERIOD_HIGH=2026-12-31` on cds-server when evaluating.

Verify:

```bash
curl -s -X POST http://127.0.0.1:8088/v1/plandefinition/apply \
  -H 'Content-Type: application/json' \
  -d '{"planDefinitionId":"cms165fhircontrollinghighbloodpressure","patientId":"cms165-demo","practitionerId":"Practitioner/example","hfsBaseUrl":"http://127.0.0.1:8081","htsBaseUrl":"http://127.0.0.1:9091","libraryBaseUrl":"http://127.0.0.1:8079"}' \
  | jq '.requestGroup.action | length'   # expect >= 1
```

## KR HFS (knowledge libraries)

The **Knowledge Repository** is a dedicated HFS instance (default port **8079**, separate DB from clinical HFS). It stores FHIR **`Library`** resources (CQL + ELM attachments), optional **`Measure`** / **`PlanDefinition`**, and the CDS catalog **`Binary`**.

```text
eCQM NPM package (.tgz)          Atrius IG (external repo)
        │                                  │
        ▼                                  ▼
import-ecqm-kr-libraries.py      translate-cql.sh + import-atrius-kr-libraries.py
        │                                  │
        └──────────────┬───────────────────┘
                       ▼
              KR HFS (8079)  —  Postgres/SQLite resources + search_index
                       │
                       ▼
        generate-cds-hooks-manifest.py  →  cds-server pins (libraryId + libraryVersion)
                       │
                       ▼
              JVM sidecar (libraryBaseUrl) + cds-server KR probe (GET /Library/{id})
```

### Start KR

```bash
# Configure deploy/kr/.env.kr (DB URL, port 8079) — see scripts/run-kr-hfs.sh
./scripts/run-kr-hfs.sh
# GET http://127.0.0.1:8079/health
```

Local example uses Postgres (`fhir_kr` in `deploy/kr/.env.kr`). Clinical chart data stays on **8082**; never mix KR and clinical in one DB.

### eCQM libraries (HL7 NPM package)

Script: [`scripts/import-ecqm-kr-libraries.py`](../../scripts/import-ecqm-kr-libraries.py)

Source: [ecqm-content-qicore NPM package](https://build.fhir.org/ig/cqframework/ecqm-content-qicore-2025/package.tgz) (Library + Measure JSON under `package/`).

```bash
# KR must be running on 8079 (or pass --kr-base-url)
./scripts/import-ecqm-kr-libraries.py --download
# Or local file:
./scripts/import-ecqm-kr-libraries.py ./ecqm-content-qicore-2025.tgz --batch-size 1
```

| Flag / detail | Notes |
|---------------|-------|
| `--batch-size 1` | Libraries are large (~61 MiB total); keep each transaction bundle under `HFS_MAX_BODY_SIZE` (default 10 MiB) |
| `--method PUT` (default) | Idempotent re-import by `Library/{id}` |
| `--include Measure` | Optional; links populations on generated PlanDefinitions |
| `--verbose` | Log per-resource version alignment |

**What the script does:**

1. Unpacks `package/Library-*.json` (and optional `Measure-*.json`) from the `.tgz`.
2. For each Library, reads **`identifier.version`** from the **`application/elm+json`** attachment (`elm_identifier_version()`).
3. **`normalize_library_version()`** sets `Library.version` (and the `text/cql` header) to that ELM version so sidecar binary-compatibility checks succeed. NPM JSON may show a different version string (e.g. `0.4.000` in the file, `0.4.001` in ELM) — **ELM wins** on import.
4. POSTs FHIR transaction/batch Bundles to `POST {kr}/` (each entry `PUT Library/{id}`).

**Verify eCQM import:**

```bash
curl -s "http://127.0.0.1:8079/Library?_count=0" -H 'Accept: application/fhir+json' | jq '.total'
curl -s http://127.0.0.1:8079/Library/CMS2FHIRPCSDepressionScreenAndFollowUp \
  -H 'Accept: application/fhir+json' | jq '{id, name, version}'
```

Expect `version` to match ELM, not necessarily the raw version string in the NPM JSON file.

### Atrius IG libraries (CMS165 and profile CQL)

Atrius measure libraries (e.g. **`AtriusCMS165ControllingHighBP`**) and **`AtriusIn-ModelInfo`** are maintained in the **AtriusIGDraft** repository (not in the eCQM NPM package). Import **after** eCQM base libraries (`FHIRHelpers`, ValueSet helpers, etc.) are on KR.

From **AtriusIGDraft** (KR HFS on **8079** must be up):

```bash
# Translate CQL → ELM and build FHIR Library resources
./scripts/translate-cql.sh

# POST Libraries to KR (same HFS transaction pattern as eCQM import)
./scripts/import-atrius-kr-libraries.py
```

Full stack notes: **`AtriusIGDraft/docs/clinical-reasoning-stack.md`**.

**Why both eCQM and Atrius:**

| Source | Examples on KR | Used for |
|--------|----------------|----------|
| eCQM NPM | `CMS2FHIR…`, `FHIRHelpers`, `SupplementalDataElements` | CMS measure logic, includes |
| Atrius IG | `AtriusCMS165ControllingHighBP`, `AtriusIn-ModelInfo` | Atrius-profiled measures, custom modelinfo |

Sidecar **`include`** libraries load via **`hfsBaseUrl`** → **cr-fhir-bridge** → KR (`CR_FHIR_BRIDGE_KR_URL`). The **primary** library loads from **`libraryBaseUrl`** (KR direct, **8079**).

### After KR library import (checklist)

1. **Regenerate CDS manifest** so pins match deployed KR (recommended — avoids probing libraries you did not import):

   ```bash
   ./scripts/generate-cds-hooks-manifest.py \
     --kr-base-url http://127.0.0.1:8079 \
     --output manifests/cds-services-local.json
   ```

   Or upload catalog Binary: `--upload-binary-id cds-services-catalog` and set `CDS_KR_SERVICES_BINARY_ID` on cds-server.

2. **Spot-check a pin** (direct read — same check cds-server uses):

   ```bash
   curl -s http://127.0.0.1:8079/Library/AtriusCMS165ControllingHighBP \
     -H 'Accept: application/fhir+json' | jq '{id, version}'
   ```

3. **Optional — search index** (sidecar fallback only; can drift — see [kr-library-pinning.md](./kr-library-pinning.md)):

   ```bash
   curl -s "http://127.0.0.1:8079/Library?name=AtriusCMS165ControllingHighBP&version=0.1.0" \
     -H 'Accept: application/fhir+json' | jq '.entry | length'
   ```

   If search returns 0 but direct read works, re-PUT the resource through HFS (export + PUT) to rebuild `search_index`.

4. **Enable pinning on cds-server** (`CDS_VALIDATE_KR_LIBRARIES=true`) and confirm `GET /ready` — see [kr-library-pinning.md](./kr-library-pinning.md).

5. **After any KR content change**, flush sidecar cache: `POST /v1/admin/cache/libraries/clear`.

### Version discipline

| Layer | Source of truth |
|-------|-----------------|
| ELM attachment | `library.identifier.version` inside `application/elm+json` |
| KR `Library.version` | Set by `import-ecqm-kr-libraries.py` / Atrius import to match ELM |
| CDS manifest `libraryVersion` | From `generate-cds-hooks-manifest.py` reading KR `Library.version` |
| cds-server probe | `GET /Library/{libraryId}` → compare `Library.version` to pin |

Re-import when sidecar reports **`libraryVersion does not match ELM identifier version`** ([troubleshooting.md](./troubleshooting.md)).

## eCQM PlanDefinitions (CDS Hooks services)

> **Important:** The [eCQM QICore Content NPM package](https://build.fhir.org/ig/cqframework/ecqm-content-qicore-2025/package.tgz) contains **Library** and **Measure** JSON only — **no `PlanDefinition`**. Slice 3 does **not** import PlanDefinitions from the package; [`generate-ecqm-plandefinitions.py`](../../scripts/generate-ecqm-plandefinitions.py) **synthesizes** `CDSHooksServicePlanDefinition` resources from Library CQL + Measure populations, then PUTs them to KR.

Per [HL7 Clinical Reasoning ↔ CDS Hooks](https://build.fhir.org/ig/HL7/cds-hooks-clinical-reasoning/en/specification.html), each CDS **Service** maps to a **`PlanDefinition`** (`CDSHooksServicePlanDefinition` profile):

| CDS Hooks | PlanDefinition (R4 `CDSHooksServicePlanDefinition`) |
|-----------|----------------|
| `Service.id` | `PlanDefinition.url` (last path segment / identifier) |
| `Service.hook` | `action.trigger` (`named-event`) |
| `Service.prefetch` | `action.input` (`DataRequirement` — not a root-level field in R4) |
| Evaluation | `library` + CQL condition expression (future: `$apply`) |

Script: [`scripts/generate-ecqm-plandefinitions.py`](../../scripts/generate-ecqm-plandefinitions.py)

```bash
# Preview (~61 services from a typical 74-Library / 53-Measure import)
python3 scripts/generate-ecqm-plandefinitions.py --download --dry-run

# Write JSON for review
python3 scripts/generate-ecqm-plandefinitions.py --download \\
  --output-dir manifests/plandefinitions-ecqm

# PUT to KR (after Libraries are imported)
python3 scripts/generate-ecqm-plandefinitions.py --kr-base-url http://127.0.0.1:8079 --upload

# Optional: tag CDS server endpoint on each PlanDefinition
python3 scripts/generate-ecqm-plandefinitions.py --kr-base-url http://127.0.0.1:8079 \\
  --cds-endpoint-base http://127.0.0.1:8095 --upload

# All four population expressions per measure (~181 PlanDefinitions)
python3 scripts/generate-ecqm-plandefinitions.py --download --populations --upload \\
  --kr-base-url http://127.0.0.1:8079
```

Import **Measures** to KR when you want population criteria linked on each PlanDefinition (`relatedArtifact` → `Measure/...`):

```bash
./scripts/import-ecqm-kr-libraries.py --download --include Measure --batch-size 5
```

## CDS service manifest

For **cds-server**, define services in JSON (see `crates/cds-server/cds-services.manifest.example.json`):

```json
{
  "services": [{
    "id": "cms165-numerator",
    "hook": "patient-view",
    "description": "CMS165 numerator check",
    "libraryId": "CMS165FHIRControllingHighBloodPressure",
    "libraryVersion": "0.3.000",
    "expression": "Numerator",
    "resolveFromFhir": true,
    "prefetch": {
      "patient": "Patient/{{context.patientId}}",
      "conditions": "Condition?patient={{context.patientId}}",
      "encounters": "Encounter?patient={{context.patientId}}",
      "observations": "Observation?patient={{context.patientId}}",
      "procedures": "Procedure?patient={{context.patientId}}",
      "medicationRequests": "MedicationRequest?patient={{context.patientId}}",
      "immunizations": "Immunization?patient={{context.patientId}}",
      "diagnosticReports": "DiagnosticReport?patient={{context.patientId}}",
      "serviceRequests": "ServiceRequest?patient={{context.patientId}}",
      "allergies": "AllergyIntolerance?patient={{context.patientId}}",
      "coverage": "Coverage?beneficiary=Patient/{{context.patientId}}"
    }
  }]
}
```

Store on KR as a FHIR `Binary` (`contentType: application/json`, base64 payload) and set `CDS_KR_SERVICES_BINARY_ID`, or use `CDS_SERVICES_MANIFEST_PATH` locally.

### Generate manifest from KR PlanDefinitions (recommended)

One-shot (import libraries if KR is empty, synthesize PlanDefinitions, regenerate manifest):

```bash
# KR running; imports eCQM libraries when none are present
./scripts/setup-plandefinition-cds-catalog.sh

# Force library import from NPM, then synthesize + manifest
ECQM_IMPORT_LIBS=1 ./scripts/setup-plandefinition-cds-catalog.sh
```

Or step by step:

```bash
python3 scripts/generate-ecqm-plandefinitions.py --kr-base-url http://127.0.0.1:8079 --upload

python3 scripts/generate-cds-hooks-manifest.py --from-plandefinition \\
  --kr-base-url http://127.0.0.1:8079 \\
  --output manifests/cds-services-kr-ecqm.json
```

The manifest includes `planDefinitionId` / `planDefinitionUrl` for **`PlanDefinition/$apply`** (preferred). Legacy `libraryId` / `expression` remain for direct `evaluate/expression` fallback when no PlanDefinition is declared.

### Generate manifest from KR Libraries (legacy shortcut)

After importing eCQM libraries to KR, generate one CDS Hook per evaluable Library:

```bash
# Writes manifests/cds-services-kr-ecqm.json (~62 services from a typical 76-Library import)
python3 scripts/generate-cds-hooks-manifest.py --kr-base-url http://127.0.0.1:8079

# Optional: all four population expressions per CMS measure (~181 services)
python3 scripts/generate-cds-hooks-manifest.py --populations --output manifests/cds-services-kr-ecqm-populations.json

# Upload catalog to KR for cds-server discovery
python3 scripts/generate-cds-hooks-manifest.py --upload-binary-id cds-services-catalog

# cds-server
CDS_SERVICES_MANIFEST_PATH=./manifests/cds-services-kr-ecqm.json \
CDS_HFS_BASE_URL=http://127.0.0.1:8081 \
  cargo run -p cds-server
```

The generator skips NPM `Manifest-*` libraries and helper/include libraries (`FHIRHelpers`, `SupplementalDataElements`, …) that have no standalone CQL expression. Each service uses `patient-view`, `libraryId`, `libraryVersion`, and the best population expression (`Initial Population`, then `Numerator`, …).

## Authoring workflow (after roadmap slice 2–3)

When adding **new** measures (not bulk eCQM import), follow **[roadmap.md § Phase 4](./roadmap.md#phase-4--authoring-more-libraries-and-plandefinitions)**:

1. Author CQL in **AtriusIGDraft** → `translate-cql.sh` → `import-atrius-kr-libraries.py`
2. Create/upload **PlanDefinition** to KR (generated or hand-authored)
3. Regenerate CDS manifest `--from-plandefinition`
4. Restart cds-server, flush sidecar cache, smoke invoke

## Atrius runtime mapper

Projection rules: `atrius-runtime-mapper` (v0.1: Condition). Optional custom manifest:

```bash
ATRIUS_MAPPER_MANIFEST=path/to/manifest.json cargo run --bin cr-fhir-bridge
```

Generate inventory from Atrius IG: `scripts/generate-atrius-mapper-manifest.py`.

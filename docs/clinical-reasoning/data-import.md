# Clinical Reasoning — Data Import

What to load, where it lives, and in what order for CDS / measure evaluation.

## Import order

```text
1. HTS bundled terminologies (VSAC ValueSets, ICD-10-CM tabular, THO, UCUM, …)
2. SNOMED CT RF2 (licensed)
3. ICD-10-CM XML (optional if bundled copy suffices)
4. RxNorm RRF (NLM ToS; needed for medication ValueSets)
5. LOINC (optional; Synthea vitals/labs)
6. Atrius IG → `HFS_FHIR_PACKAGES` overlay for clinical HFS validation
7. Clinical patient data → clinical HFS
8. Atrius IG Libraries + PlanDefinitions → KR HFS
   (AtriusIGDraft: translate-cql.sh + import-atrius-kr-libraries.py --clinical-reasoning)
9. CDS manifest → local JSON or KR Binary
   (setup-plandefinition-cds-catalog.sh → manifests/cds-services-kr.json)
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
- Requires clinical HFS with `HFS_FHIR_PACKAGES` + `HFS_VALIDATION_MODE=enforce`

```bash
./scripts/import-synthea-atrius.py --base-url http://127.0.0.1:8082 ./synthea/output/fhir/
```

**Read patient data from clinical HFS** (`8082`) for sidecar evaluation.

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
  -d '{"planDefinitionId":"cms165fhircontrollinghighbloodpressure","patientId":"cms165-demo","practitionerId":"Practitioner/example","hfsBaseUrl":"http://127.0.0.1:8082","htsBaseUrl":"http://127.0.0.1:9091","libraryBaseUrl":"http://127.0.0.1:8079"}' \
  | jq '.requestGroup.action | length'   # expect >= 1
```

## KR HFS (knowledge libraries)

The **Knowledge Repository** is a dedicated HFS instance (default port **8079**, separate DB from clinical HFS). It stores FHIR **`Library`** resources (CQL + ELM attachments), **`Measure`** / **`PlanDefinition`** / **`ActivityDefinition`**, and optionally the CDS catalog **`Binary`**.

```text
AtriusIGDraft (CQL + FSH PlanDefinitions)
        │
        ▼
translate-cql.sh + import-atrius-kr-libraries.py --clinical-reasoning
        │
        ▼
KR HFS (8079)  —  Libraries + PlanDefinitions
        │
        ▼
setup-plandefinition-cds-catalog.sh
  └─ generate-cds-hooks-manifest.py  →  manifests/cds-services-kr.json
        │
        ▼
cds-server (pins) + JVM sidecar (libraryBaseUrl)
```

### Start KR

```bash
# Configure deploy/env/hfs-kr.env (DB URL, port 8079)
./scripts/build-clinical-reasoning.sh   # once
./scripts/run-kr-hfs.sh
# GET http://127.0.0.1:8079/health
```

Local example uses Postgres (`fhir_kr` in `deploy/env/hfs-kr.env`). Clinical chart data stays on **8082**; never mix KR and clinical in one DB.

### Atrius IG libraries and PlanDefinitions

Measure libraries (e.g. **`AtriusCMS165ControllingHighBP`**), helpers (**`FHIRHelpers`**, **`AtriusIn-ModelInfo`**), and CDS **`PlanDefinition`** / **`ActivityDefinition`** resources are maintained in **AtriusIGDraft**.

From **AtriusIGDraft** (KR HFS on **8079** must be up):

```bash
./scripts/translate-cql.sh
./scripts/import-atrius-kr-libraries.py --clinical-reasoning
```

Or from atrius-hfs (auto-detects `ATRIUS_IG_ROOT`):

```bash
IMPORT_ATRIUS=1 ./scripts/setup-plandefinition-cds-catalog.sh
```

Full stack notes: **`AtriusIGDraft/docs/clinical-reasoning-stack.md`**.

**Verify import:**

```bash
curl -s "http://127.0.0.1:8079/Library?_count=0" -H 'Accept: application/fhir+json' | jq '.total'
curl -s "http://127.0.0.1:8079/PlanDefinition?_count=0" -H 'Accept: application/fhir+json' | jq '.total'
curl -s http://127.0.0.1:8079/Library/AtriusCMS165ControllingHighBP \
  -H 'Accept: application/fhir+json' | jq '{id, name, version}'
```

Expect `Library.version` to match the ELM `identifier.version` inside `application/elm+json`.

### After KR import

1. Confirm direct read:

   ```bash
   curl -s http://127.0.0.1:8079/Library/AtriusCMS165ControllingHighBP \
     -H 'Accept: application/fhir+json' | jq '{id, version}'
   ```

2. **Optional — search index** (sidecar fallback only; can drift — see [kr-library-pinning.md](./kr-library-pinning.md)):

   ```bash
   curl -s "http://127.0.0.1:8079/Library?name=AtriusCMS165ControllingHighBP&version=0.1.0" \
     -H 'Accept: application/fhir+json' | jq '.entry | length'
   ```

   If search returns 0 but direct read works, re-PUT the resource through HFS (export + PUT) to rebuild `search_index`.

3. **Enable pinning on cds-server** (`CDS_VALIDATE_KR_LIBRARIES=true`) and confirm `GET /ready` — see [kr-library-pinning.md](./kr-library-pinning.md).

4. **After any KR content change**, flush sidecar cache: `POST /v1/admin/cache/libraries/clear`.

### Version discipline

| Layer | Source of truth |
|-------|-----------------|
| ELM attachment | `library.identifier.version` inside `application/elm+json` |
| KR `Library.version` | Set by Atrius import to match ELM |
| CDS manifest `libraryVersion` | From `generate-cds-hooks-manifest.py` reading KR `Library.version` |
| cds-server probe | `GET /Library/{libraryId}` → compare `Library.version` to pin |

Re-import when sidecar reports **`libraryVersion does not match ELM identifier version`** ([troubleshooting.md](./troubleshooting.md)).

## CDS service manifest

For **cds-server**, services come from KR **PlanDefinitions** (see toy schema in `crates/cds-server/cds-services.manifest.example.json`).

Per [HL7 Clinical Reasoning ↔ CDS Hooks](https://build.fhir.org/ig/HL7/cds-hooks-clinical-reasoning/en/specification.html), each CDS **Service** maps to a **`PlanDefinition`** (`CDSHooksServicePlanDefinition` profile):

| CDS Hooks | PlanDefinition (R4 `CDSHooksServicePlanDefinition`) |
|-----------|----------------|
| `Service.id` | `PlanDefinition.url` (last path segment / identifier) |
| `Service.hook` | `action.trigger` (`named-event`) |
| `Service.prefetch` | `action.input` (`DataRequirement` — not a root-level field in R4) |
| Evaluation | `library` + CQL condition → **`PlanDefinition/$apply`** |

Store the generated catalog on disk (`CDS_SERVICES_MANIFEST_PATH`) or as a FHIR `Binary` (`CDS_KR_SERVICES_BINARY_ID`).

### Generate catalog

**Orchestrator** (import from AtriusIGDraft if KR is empty, then write the JSON):

```bash
./scripts/setup-plandefinition-cds-catalog.sh
IMPORT_ATRIUS=1 ./scripts/setup-plandefinition-cds-catalog.sh   # force re-import
```

**Writer only** (PlanDefinitions already on KR):

```bash
python3 scripts/generate-cds-hooks-manifest.py \
  --kr-base-url http://127.0.0.1:8079 \
  --output manifests/cds-services-kr.json
```

| Script | Role |
|--------|------|
| `setup-plandefinition-cds-catalog.sh` | Ensure KR has Atrius Libraries/PlanDefinitions, then call the writer |
| `generate-cds-hooks-manifest.py` | Read KR PlanDefinitions → `manifests/cds-services-kr.json` |

The manifest includes `planDefinitionId` / `planDefinitionUrl` for **`PlanDefinition/$apply`**, plus `libraryId` / `libraryVersion` / `expression` pins.

## Authoring workflow

When adding **new** measures / pathways, follow **[roadmap.md § Phase 4](./roadmap.md#phase-4--authoring-more-libraries-and-plandefinitions)**:

1. Author CQL in **AtriusIGDraft** → `translate-cql.sh` → `import-atrius-kr-libraries.py --clinical-reasoning`
2. Ensure **PlanDefinition** (and related ActivityDefinitions) are in the IG and imported to KR
3. Regenerate CDS catalog: `./scripts/setup-plandefinition-cds-catalog.sh`
4. Restart cds-server, flush sidecar cache, smoke invoke

## Atrius runtime mapper


```bash
ATRIUS_MAPPER_MANIFEST=path/to/manifest.json ./scripts/run-clinical HFS.sh
```


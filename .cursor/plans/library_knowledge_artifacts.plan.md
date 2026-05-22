---
name: Library knowledge artifacts
overview: FHIR Library profiles vs one CQL library with many definitions; MVP profile priorities; where and how to store and invoke Libraries in Helios (HFS REST + persistence tenancy); optional dedicated KR service and DB split.
todos:
  - id: kr-library-crud-search
    content: Expose Library CRUD + search (url, version, status) on chosen server (HFS or KR service)
    status: pending
  - id: library-content-conventions
    content: Standardize Library.content MIME types for ELM (+ optional CQL); relatedArtifact for includes
    status: pending
  - id: profile-phase-a
    content: Document meta.profile stack (LogicLibrary + ELMLibrary); optional warn-only validation
    status: pending
  - id: profile-phase-b
    content: Load CR IG StructureDefinitions into fhir-validation ProfileRegistry; validate on write
    status: pending
  - id: resolver-to-sidecar
    content: Resolve Library → ELM bytes + identifiers → atrius-clinical-reasoning / JVM sidecar
    status: pending
isProject: true
---

# Library / knowledge artifacts

## One FHIR Library vs many “logics” (CQL)

- A **CQL document** has one top-level **`library ... version`** → one **ELM Library identifier**.
- That artifact contains **many** `define` / function / include constructs.
- **One FHIR `Library` resource** usually packages **one** CQL/ELM library; execution picks a **named expression** inside it (or resolves **included** libraries via additional FHIR `Library` instances + `relatedArtifact`).

## Profiles — implement all minimally?

No. They are **profiles on the same [`Library`](crates/fhir/src/r4/resources/library.rs) resource**. Priority: **LogicLibrary + ELMLibrary** (execution path); **CQLLibrary** when storing source; **PublishableLibrary** when publishing metadata matters; defer **FHIRPathLibrary**, **ModelInfoLibrary**, **ModuleDefinitionLibrary** until needed.

Implementation shape: **one storage + validation path**, assert conformance via **`meta.profile`** and [`helios-fhir-validation`](crates/fhir-validation) when ready — not seven parallel Rust models.

---

## Where to store Libraries

### Default recommendation: **Helios FHIR server (HFS)**

- [`crates/rest`](crates/rest) already exposes **generic type routes**: `GET/POST /Library`, `GET /Library/{id}`, search, history — same as any other resource type (see [`fhir_routes.rs`](crates/rest/src/routing/fhir_routes.rs): `/{resource_type}`).
- **Persistence** is whatever backs HFS today (SQLite / PostgreSQL / composites per [`helios-persistence`](crates/persistence)); **no separate “Library table”** — Libraries are rows/documents like other resources.

### Tenancy (important for “same DB as clinical?”)

[`DefaultResourceTenancy`](crates/persistence/src/tenant/tenancy.rs) classifies **`Library`** with **`Measure`**, **`PlanDefinition`**, **`ActivityDefinition`**, **`Questionnaire`** as **Knowledge resources — often `TenancyModel::Shared`**.

- Meaning: in default configuration, knowledge artifacts are treated as **cross-tenant shared**, not isolated per patient tenant — appropriate when libraries are **canonical** and reused everywhere.
- If product requires **tenant-private libraries**, use **`CustomResourceTenancy`** with an override for `"Library"` → **`TenantScoped`** (or **`Configurable`**) without changing REST routing.

So: **physical DB** is usually the **same database instance as HFS clinical data**, but **logical isolation** follows tenancy rules (shared vs tenant-scoped).

### When to use a **dedicated Knowledge Artifact (KR) service**

Split into a **second FHIR deployment** (own base URL, optionally own DB) if you need:

- **Independent scaling / blast radius** (large ELM blobs, heavy CI publishing traffic vs clinical reads).
- **Different authz model** (publishers vs clinicians; public canonical libs vs internal-only).
- **Different lifecycle** (immutable published packages vs mutable clinical data).
- **Operational separation** (upgrade KR without touching clinical HFS).

Tradeoff: callers must **configure two base URLs** (`hfsBaseUrl` clinical vs KR base for library fetch), or you add a **BFF/gateway** that merges visibility.

---

## How to “call” Libraries

Libraries are **not** invoked like RPC endpoints by resource id alone; **execution** happens elsewhere.

| Step | Mechanism |
|------|-----------|
| **Discover / resolve** | FHIR **`GET /Library?url=…&version=…`** (and/or id read) against HFS or KR base — depends on search parameter coverage in your deployment. |
| **Load payload** | Parse **`Library.content`** → ELM string (and optional CQL); resolve **`relatedArtifact`** / includes to other `Library` instances if multi-library ELM. |
| **Evaluate** | Build [`EvaluateExpressionRequest`](crates/atrius-clinical-reasoning/src/dto.rs) (`elm` optional when resolving from FHIR, `libraryBaseUrl`, `resolveLibraryArtifactsFromFhir`, `libraryId`, `libraryVersion`, `expression`, `includedLibraries`, `hfsBaseUrl`, `htsBaseUrl`) → **JVM sidecar** via [`ClinicalReasoningClient`](crates/atrius-clinical-reasoning/src/client/http.rs). Use [`EvaluateExpressionRequestBuilder`](crates/atrius-clinical-reasoning/src/request_builder.rs) from tenant bases; normalize `result` via [`normalized_result`](crates/atrius-clinical-reasoning/src/normalized_result.rs) (see [.cursor/plans/atrius_clinical_reasoning_sidecar_client.plan.md](atrius_clinical_reasoning_sidecar_client.plan.md)). |
| **Higher-level CR ops (later)** | **`Measure/$evaluate-measure`**, **`PlanDefinition/$apply`** orchestrate evaluation and may pull **`Measure.library`** / canonical references automatically — still ultimately CQL/ELM execution. |

Future CDS Hooks flow: hook handler → (optional) **read Library from HFS** → sidecar evaluate → map to cards.

---

## Same database vs separate database

| Approach | When it fits |
|----------|----------------|
| **Same DB as HFS** | Simplest ops; shared tenancy default treats libraries as global knowledge; good for MVP and single-tenant-style deployments. |
| **Same DB engine, separate schema/database name** | Logical separation, backup policies; still two connection strings if two services. |
| **Separate KR service + DB** | Strong isolation, publishing SLA, or regulatory separation from PHI-heavy stores (libraries often low PHI but attachments policy may differ). |

**Helios note:** switching DB does not require changing **`Library`** FHIR shape — only **which base URL** clients use for CRUD/search vs clinical data.

---

## Phased implementation (unchanged summary)

1. **KR on HFS**: Library CRUD + search; document **`meta.profile`** and content MIME conventions.
2. **Resolver**: canonical/url/version → `Library` → ELM (+ includes) → sidecar request.
3. **Validation**: optional ProfileRegistry loading for LogicLibrary / ELMLibrary.
4. **Split KR** only when scaling or governance demands it.

---

## Diagram

```mermaid
flowchart LR
  subgraph store [Storage_options]
    HFS[HFS_same_stack]
    KR[Dedicated_KR_FHIR]
  end
  subgraph call [Invocation]
    REST[FHIR_Read_Search_Library]
    Res[Resolver_ELMincludes]
    Sidecar[JVM_sidecar_CqlEngine]
  end
  HFS --> REST
  KR --> REST
  REST --> Res
  Res --> Sidecar
  Sidecar --> Clin[HFS_clinical_data]
  Sidecar --> Term[HTS_terminology]
```

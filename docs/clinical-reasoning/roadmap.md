# Clinical Reasoning Stack — Status & Roadmap

Where the CDS + eCQM stack stands, what ops has already enabled, and the planned build sequence before expanding **authoring** (more Libraries and PlanDefinitions).

Related: [README.md](./README.md), [forward-plan.md](./forward-plan.md), [kr-library-pinning.md](./kr-library-pinning.md), [production-deployment.md](./production-deployment.md), [data-import.md](./data-import.md).

---

## Current status (slices 1–3 infrastructure complete)

The stack is **ready for single-tenant production** with manual KR upgrade runbook when ops flags are on. **ER chest pain** is the reference authored pathway (PlanDefinition `$apply`, CQL 0.4.2, smoke + clinical UI).

| Capability | Status | Notes |
|------------|--------|-------|
| CDS Hooks discovery + invoke | Done | `cds-server` :8095 |
| Prefetch pass-through | Done | Client/BFF resolves templates — [cds-prefetch.md](./cds-prefetch.md) |
| BFF prefetch resolver | Done | `atrius-bff` fetches discovery templates + clinical FHIR for ER chest pain |
| SMART `fhirAuthorization` | Done | Clinical bearer on bridge path — [cds-server README](../../crates/cds-server/README.md) |
| KR library version pinning (slice 1) | Done | Manifest pins, `GET /ready`, direct-read KR probe |
| Sidecar content-aware cache (slice 2) | Done | `Library.meta.versionId` or ELM hash in cache keys (JVM sidecar) |
| PlanDefinition-first catalog (slice 3) | Done | 68 services with `planDefinitionId`; `/ready` `planDefinitionPins` |
| Sidecar cache flush | Done | `POST /v1/admin/cache/libraries/clear` (rare after slice 2) |
| Minimal observability | Done | `cds_invoke_metrics` logs, sidecar `GET /metrics` — [observability.md](./observability.md) |
| systemd / production docs | Done | [production-deployment.md](./production-deployment.md) |
| clinical HFS + sidecar evaluate/`$apply` | Done | CMS165 + ER chest pain validated |
| ER chest pain pathway (phase 4 pilot) | Done | AtriusIGDraft + `er-chest-pain-pathway` manifest + smoke script |

### Ops enablement (you enabled these)

| Flag / setting | Purpose |
|----------------|---------|
| `CDS_REQUIRE_LIBRARY_VERSION=true` | Manifest must declare `libraryVersion` per evaluate service |
| `CDS_VALIDATE_KR_LIBRARIES=true` | Startup KR probe; `GET /ready` = 200 when pins exist |
| `SIDECAR_ADMIN_TOKEN` | Protect cache flush endpoint |
| `CDS_SERVICES_MANIFEST_PATH` or KR Binary | Catalog source aligned with imported KR |

**Verify after any change:**

```bash
curl -s http://127.0.0.1:8095/ready | jq .
./scripts/cds-cms165-prefetch-smoke.sh
curl -s http://127.0.0.1:8088/metrics | jq '{evaluateTotal, libraryStackCacheHits, krLibraryFetches}'
```

**Note:** `GET /cds-services` does **not** expose `libraryId` / `libraryVersion` — those are server-internal (CDS Hooks spec). Pins live in the manifest and are validated by `/ready`.

---

## Planned sequence (your next steps)

```text
Slice 1 (done)     Ops flags + pinning + observability + smoke
Slice 2 (done)     Content-aware sidecar cache keys (versionId / ELM hash)
Slice 3 (done)     PlanDefinition-first CDS catalog + `$apply` probe
Phase A (in progress)  BFF prefetch + card indicator + clinical UI polish
Phase 4 (next)     More Atrius CQL → Library → PlanDefinition → manifest
```

See [forward-plan.md](./forward-plan.md) for the full component roadmap.

---

## Slice 2 — Content-aware cache invalidation (done)

Implemented in JVM sidecar: cache keys include `Library.meta.versionId` or ELM SHA-256 fallback so same-version KR re-import auto-invalidates without manual flush.

Manual `POST /v1/admin/cache/libraries/clear` remains for emergencies.

---

## Slice 3 — PlanDefinition-first catalog (done)

**Status (atrius-hfs):** infrastructure in place. Author PlanDefinitions in AtriusIGDraft → `import-atrius-kr-libraries.py --clinical-reasoning` → `./scripts/setup-plandefinition-cds-catalog.sh`. Smoke with `./scripts/cds-cms165-prefetch-smoke.sh --apply`. `/ready` reports `planDefinitionPins` when `CDS_VALIDATE_KR_LIBRARIES=true`.

### Background

Today most manifest rows use the **legacy evaluate path**:

```text
CDS invoke → cds-server → sidecar POST /v1/evaluate/expression
           libraryId + libraryVersion + expression (e.g. "Initial Population")
```

HL7 **CDS Hooks ↔ Clinical Reasoning** alignment uses a **`PlanDefinition`** per CDS service:

```text
CDS invoke → cds-server → sidecar POST /v1/plandefinition/apply
           planDefinitionId (e.g. cms165fhircontrollinghighbloodpressure)
           → CQFramework PlanDefinitionProcessor → RequestGroup + actions
```

### Why move to PlanDefinition-first?

| Legacy `libraryId` + `expression` | PlanDefinition `$apply` |
|-----------------------------------|-------------------------|
| One CQL expression per CDS card | Full measure logic: populations, recommendations, actions |
| Good for debugging / smoke | Spec-aligned CDS Hooks + Clinical Reasoning |
| Pins one Library version | PlanDefinition references libraries + data requirements |
| cds-server builds evaluate request | Sidecar runs CQF Clinical Reasoning processor |

**cds-server already supports both:** if manifest row has `planDefinitionId` (or `planDefinitionUrl`), invoke uses **`$apply`**; otherwise it falls back to **evaluate/expression**.

### What slice 3 adds (engineering)

1. **Author/upload PlanDefinitions to KR** (AtriusIGDraft FSH + `--clinical-reasoning` import).
2. **Regenerate CDS manifest from PlanDefinitions**:

   ```bash
   ./scripts/setup-plandefinition-cds-catalog.sh
   # or writer-only:
   python3 scripts/generate-cds-hooks-manifest.py \
     --kr-base-url http://127.0.0.1:8079 \
     --output manifests/cds-services-kr.json
   ```

3. **cds-server KR probe for PlanDefinitions** — `kr_readiness.rs` probes `GET /PlanDefinition/{id}`; `/ready` exposes `planDefinitionPins` + `krPlanDefinitions`.
4. **Smoke / docs** — `scripts/setup-plandefinition-cds-catalog.sh`, `cds-cms165-prefetch-smoke.sh --apply`.

### Slice 3 vs authoring (phase 4)

| Slice 3 | Authoring (phase 4) |
|---------|---------------------|
| **Infrastructure** — tooling + probes + manifest shape | **Content** — new CQL measures / pathways you write |
| Catalog from KR PlanDefinitions | Grows KR via **AtriusIGDraft** translate + import |

---

## Phase 4 — Authoring more Libraries and PlanDefinitions

After slice 2 + 3, expand **knowledge content** on KR.

**Full IG authoring plan (profiles, NDHM/QI-Core, authored PlanDefinitions, incremental CMS migration):** [AtriusIGDraft `docs/authoring-plan.md`](../../AtriusIGDraft/docs/authoring-plan.md).

### Atrius-profiled measures (primary path for you)

From **AtriusIGDraft** (external repo):

```bash
# 1. Author/edit CQL + PlanDefinitions in the IG project
# 2. Translate to ELM + FHIR Library resources
./scripts/translate-cql.sh

# 3. Import Libraries + PlanDefinitions/ActivityDefinitions to KR (8079)
./scripts/import-atrius-kr-libraries.py --clinical-reasoning
```

See [data-import.md § KR HFS](./data-import.md#kr-hfs-knowledge-libraries).

### PlanDefinitions

Author `PlanDefinition` resources in the IG following `CDSHooksServicePlanDefinition` — hook, prefetch as `action.input`, library reference + condition expression. Import with `--clinical-reasoning`.

### Wire into cds-server

```bash
./scripts/setup-plandefinition-cds-catalog.sh
# or:
python3 scripts/generate-cds-hooks-manifest.py \
  --kr-base-url http://127.0.0.1:8079 \
  --output manifests/cds-services-kr.json

# Restart cds-server; confirm /ready; flush sidecar cache if libraries changed
curl -s -X POST http://127.0.0.1:8088/v1/admin/cache/libraries/clear \
  -H "Authorization: Bearer $SIDECAR_ADMIN_TOKEN"
```

### Authoring checklist (per new measure)

1. CQL compiles; ELM attached to `Library.content`
2. `Library.version` matches ELM `identifier.version` (import scripts enforce for eCQM)
3. `PUT` Library to KR → verify `GET /Library/{id}`
4. PlanDefinition on KR references library + populations
5. Manifest row with `planDefinitionId` (preferred) or `libraryId`+`expression` (legacy)
6. Smoke: sidecar `$apply` or cds-server `POST /cds-services/{id}`
7. After KR change: cache flush (until slice 2 lands)

Full import order: [data-import.md](./data-import.md).

---

## Component map — done vs later

| Component | Slice 1 (done) | Slice 2–3 | Later |
|-----------|----------------|-----------|-------|
| **cds-server** | Pinning, prefetch, SMART, metrics | PlanDefinition probe, maybe auto-flush | Per-tenant URLs |
| **cds-hooks** | Protocol types | — | New hooks if needed |
| **atrius-clinical-reasoning** | Client + cache flush API | — | Resilience helpers |
| **JVM sidecar** | Evaluate, `$apply`, metrics, cache admin | Content-aware cache keys | Prometheus |
| **KR HFS** | eCQM + Atrius import | PlanDefinition upload | Search index hardening (optional) |
| **HTS** | Terminology import | — | — |

---

## Backlog (not in current sequence)

Lower priority until multi-tenant or ops scale demands it:

- Per-tenant clinical / KR / bridge URLs (SaaS)
- Prometheus / OTLP + Grafana dashboards
- `/ready` live re-probe (not just startup)
- CI job: manifest pins ⊆ KR for release tag
- cds-server admin API for manifest pins (optional; discovery stays spec-pure)

---

## See also

- [kr-library-pinning.md](./kr-library-pinning.md) — pinning detail + slice 1 runbook
- [observability.md](./observability.md) — logs and `/metrics`
- [startup-guide.md](./startup-guide.md) — local stack + `$apply` curl examples
- [troubleshooting.md](./troubleshooting.md) — KR probe, search index drift, stale cache

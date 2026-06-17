# KR Library Version Pinning & Sidecar Cache Policy

How the clinical reasoning stack pins **which CQL ELM** runs, validates it against the Knowledge Repository (KR), and invalidates JVM sidecar caches after knowledge updates.

Part of **production hardening step 8** (`AtriusIGDraft/docs/clinical-reasoning-stack.md` §8). Related: [production-deployment.md](./production-deployment.md), [troubleshooting.md](./troubleshooting.md).

## Problem

| Risk without pinning / invalidation | Example |
|-------------------------------------|---------|
| Wrong measure logic | cds-server evaluates “latest” `Library` on KR, not the certified build |
| Stale ELM after re-import | KR `Library` 0.1.0 updated in place; sidecar still serves old compiled stack |
| Silent drift | Manifest `libraryVersion` ≠ imported KR resource |

Prefetch and SMART `fhirAuthorization` control **patient data**; this controls **measure logic**.

## Architecture (today)

```text
manifests/cds-services-kr-ecqm.json
  libraryId + libraryVersion  (per CDS service)
        ↓
cds-server  — validate manifest, probe KR at startup (GET /Library/{id}), GET /ready
        ↓
sidecar evaluate/expression  — cache key (libraryBase, libraryId, version, includes)
        ↓
KR HFS  — GET /Library/{id} (primary); search name+version (fallback)
```

| Component | Role |
|-----------|------|
| **Manifest** | Pins `(libraryId, libraryVersion)` per legacy CQL evaluate service |
| **cds-server** | Enforces pins, probes KR, forwards version on each invoke |
| **JVM sidecar** | Loads ELM from KR; caches compiled stacks process-wide |
| **Admin flush** | `POST /v1/admin/cache/libraries/clear` drops caches without JVM restart |

---

## First slice (implemented)

Enough for **production KR upgrades** when ops follow the runbook.

### cds-server

| Feature | Config / endpoint | Behavior |
|---------|-------------------|----------|
| Manifest version required | `CDS_REQUIRE_LIBRARY_VERSION=true` | Services with `libraryId` + `expression` must declare `libraryVersion`; invoke rejected if missing |
| KR startup probe | `CDS_VALIDATE_KR_LIBRARIES=true` | Unique pins probed via **`GET /Library/{libraryId}`** (primary); search `name`+`version` only if read returns 404; **process exits** if any fail |
| Readiness | `GET /ready` | **200** if startup probe ok; **503** with message if probe failed; `krLibraries: not_checked` in demo / when validation off |
| Invoke pass-through | (always) | `libraryVersion` from manifest → sidecar `EvaluateExpressionRequest` |

Implementation: `crates/cds-server/src/library_version.rs`, wired in `services/mod.rs` and `main.rs`.

Defaults: both flags **off** (local dev). Enable in production — see `deploy/env/cds-server.env.example`.

### JVM sidecar

| Feature | Endpoint / env | Behavior |
|---------|----------------|----------|
| Cache flush | `POST /v1/admin/cache/libraries/clear` | Clears compiled ELM stacks, cached KR `Library` resources, ValueSet expansion buckets |
| Admin auth | `SIDECAR_ADMIN_TOKEN` (optional) | When set, requires `Authorization: Bearer <token>` |

Implementation: `SidecarLibraryCacheAdmin.kt`, `EvaluationLibraryCache.clear()`, `FhirLibraryResourceCaches.clearAll()`, `ValueSetExpansionCache.clear()`.

### atrius-clinical-reasoning

`ClinicalReasoningClient::clear_library_cache()` — HTTP wrapper for the sidecar admin endpoint.

### Documentation & ops

- Upgrade runbook: [production-deployment.md § KR library version pinning](./production-deployment.md#kr-library-version-pinning-production)
- Stale CQL: [troubleshooting.md § Stale CQL after KR re-import](./troubleshooting.md#stale-cql-after-kr-re-import-same-version-string)
- KR import (eCQM + Atrius): [data-import.md § KR HFS](./data-import.md#kr-hfs-knowledge-libraries)

### KR probe vs FHIR search (important)

cds-server pinning validation uses **direct read**, same as the JVM sidecar:

1. `GET {CDS_LIBRARY_BASE_URL}/Library/{libraryId}`
2. Confirm `Library.name` or `id` matches the pin and `Library.version` matches `libraryVersion` (empty version on resource matches any pin version).

FHIR **`Library?name=…&version=…` search is not used for pinning** except as a fallback when direct read returns 404.

**Search index can drift** from stored `Library.version` (e.g. `search_index.value_token_code` = `0.4.001` while `resources.data.version` = `0.4.000`). That breaks combined search but **does not block pinning** with the direct-read probe. Typical cause: re-import or edit changed the resource JSON without a full HFS re-index pass.

**Fix search index for a library** (optional — sidecar also prefers `GET /Library/{id}`):

```bash
curl -s "http://127.0.0.1:8079/Library/{libraryId}" -H 'Accept: application/fhir+json' > /tmp/lib.json
curl -s -X PUT "http://127.0.0.1:8079/Library/{libraryId}" \
  -H 'Content-Type: application/fhir+json' -d @/tmp/lib.json
```

Verify in Postgres:

```sql
SELECT data->>'version' AS resource_version
FROM resources WHERE resource_type = 'Library' AND id = '{libraryId}';

SELECT value_token_code FROM search_index
WHERE resource_type = 'Library' AND resource_id = '{libraryId}' AND param_name = 'version';
```

Both should match the manifest pin. See [troubleshooting.md § KR library probe](./troubleshooting.md#kr-library-probe-failed--search-returns-empty-but-library-exists).

### Production upgrade loop (summary)

1. Import new ELM to KR — **prefer a new `Library.version`** (e.g. `0.1.1`). See [data-import.md § KR HFS](./data-import.md#kr-hfs-knowledge-libraries).
2. Regenerate manifest: `./scripts/generate-cds-hooks-manifest.py`.
3. Restart **cds-server** with `CDS_VALIDATE_KR_LIBRARIES=true` (re-probes pins).
4. Flush sidecar cache (see curl in [production-deployment.md](./production-deployment.md)).
5. Smoke invoke (e.g. `./scripts/cds-cms165-prefetch-smoke.sh`).

---

## Not in the first slice (roadmap)

The items below are **planned** hardening; not required for the first production slice but worth tracking.

| Gap | Impact | Likely next step |
|-----|--------|------------------|
| **Same-version re-import auto-invalidation** | Re-importing `0.1.0` with new ELM still serves stale cache until manual flush | Include `Library.meta.versionId` or ELM content hash in sidecar cache key |
| **Cache TTL** | Long-lived process may serve old ELM if KR changes without ops action | Optional TTL on `EvaluationLibraryCache` / terminology cache |
| **Deploy-time auto-flush** | Ops must remember `curl` after KR deploy | cds-server or install script calls sidecar flush after successful KR probe |
| **`/ready` live re-probe** | `/ready` reflects **startup** only; KR delete after boot not detected | Periodic or on-demand KR re-probe in readiness handler |
| **PlanDefinition KR validation** | **Slice 3 done:** `GET /PlanDefinition/{id}` for each manifest `planDefinitionId` at startup + `GET /ready` | Live re-probe on `/ready` (still startup-only today) |
| **CI manifest ↔ KR pinning** | Manifest can drift from deployed KR between releases | CI job: manifest pins ⊆ KR search results for release tag |
| **Cache / eval metrics** | **v1 done:** sidecar `GET /metrics` + per-request logs; cds-server `cds_invoke_metrics` logs — see [observability.md](./observability.md) | Prometheus / OTLP export + dashboards (roadmap) |

**Highest-value next slice:** content-aware cache keys (`versionId` / ELM hash) so same-version re-import invalidates without manual flush.

Full stack sequence (slice 2 → PlanDefinition-first → authoring): **[roadmap.md](./roadmap.md)**.

---

## Configuration reference

### cds-server

| Variable | Default | Production |
|----------|---------|------------|
| `CDS_REQUIRE_LIBRARY_VERSION` | `false` | `true` |
| `CDS_VALIDATE_KR_LIBRARIES` | `false` | `true` |
| `CDS_LIBRARY_BASE_URL` | (KR URL) | Required for KR probe |

### JVM sidecar

| Variable | Default | Production |
|----------|---------|------------|
| `SIDECAR_ADMIN_TOKEN` | (unset = open) | Set; pass on cache flush |

### Key source files

| Area | Path |
|------|------|
| Pinning + KR probe | `crates/cds-server/src/library_version.rs` |
| Invoke wiring | `crates/cds-server/src/services/mod.rs` |
| Manifest generation | `scripts/generate-cds-hooks-manifest.py` |
| Catalog | `manifests/cds-services-kr-ecqm.json` |
| Sidecar cache | JVM: `EvaluationLibraryCache.kt`, `SidecarLibraryCacheAdmin.kt` |
| HTTP client flush | `crates/atrius-clinical-reasoning/src/client/http.rs` |

## See also

- [cds-prefetch.md](./cds-prefetch.md) — patient data prefetch (client vs backend)
- [data-import.md](./data-import.md) — KR library import
- [crates/cds-server/README.md](../../crates/cds-server/README.md) — env vars summary

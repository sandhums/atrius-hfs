# Clinical Reasoning — Troubleshooting

Common failures when wiring CDS Hooks, the JVM sidecar, bridge, dual HFS, and HTS.

## cds-server 502: `Duplicate key null` … merging `Bundle`

**Symptom:** `POST /cds-services/{id}` with populated prefetch returns HTTP 502; sidecar body contains:

```text
Duplicate key null (attempted merging values org.hl7.fhir.r4.model.Bundle@… and org.hl7.fhir.r4.model.Bundle@…)
```

**Cause:** PlanDefinition-first manifests include `planDefinitionId`, so cds-server invokes **`$apply`** (not legacy `evaluate/expression`). CDS prefetch sends searchset `Bundle`s per key (`conditions`, `encounters`, …). Older sidecar builds nested those Bundles inside the apply data bundle; CQF `InMemoryFhirRepository` indexes by resource id — multiple `Bundle` resources with null id collide.

**Fix:** Rebuild/restart **JVM sidecar** with prefetch flattening in `SidecarPlanDefinitionApplier` (same logic as evaluate `PrefetchRetrieveSupport`). Regression test: `SidecarPlanDefinitionApplierPrefetchTest`.

**Workaround until sidecar is updated:** invoke with empty prefetch (`"prefetch": {}`) so the sidecar uses REST fallback via cr-fhir-bridge (slower, but avoids the bug).

## cds-server 502 / warning card: `Expected a list with at most one element`

**Symptom:** `POST /cds-services/atriuscms165controllinghighbp` returns HTTP 200 with a warning card; RequestGroup `OperationOutcome` contains:

```text
Condition expression Initial Population encountered exception: Expected a list with at most one element, but found a list with multiple elements.
```

**Cause:** FHIR `PlanDefinition/$apply` defines a single required **`subject`** parameter (`Patient/{id}`). The sidecar passes `patientId` as that subject; CQF resolves it from the clinical repository. CDS Hooks prefetch also includes a `patient` resource (chart template). Overlaying both puts two Patient instances in the evaluation context and breaks CQL `context Patient`. Direct `evaluate/expression` is unaffected (sidecar sets `contextParameter = Patient/{id}`, the same subject binding).

**Fix:** Rebuild/restart **JVM sidecar** with Patient omitted from the apply prefetch overlay (`SidecarPlanDefinitionApplier.prefetchToBundle`). Regression test: `SidecarPlanDefinitionApplierPrefetchTest`.

**Workaround:** empty prefetch (`"prefetch": {}`) until sidecar is updated.

## cds-server warning card: `MethodOutcome.getResource()` … `IRepository.invoke` is null

**Symptom:** After fixing duplicate-Patient prefetch, RequestGroup `OperationOutcome` contains:

```text
Cannot invoke "ca.uhn.fhir.rest.api.MethodOutcome.getResource()" because the return value of "ca.uhn.fhir.repository.IRepository.invoke(...)" is null
```

**Cause:** With prefetch present, CQF adds an in-memory retrieve provider (disabling server-side `:in` ValueSet searches). ValueSet membership then uses `ValueSet/{id}/$expand` via `IRepository.invoke(id, "$expand", …)`. CQF `ProxyRepository` returns **null** from that overload, so expansion fails. Without prefetch, REST `:in` searches avoid `$expand`.

**Fix:** Rebuild/restart **JVM sidecar** with `SidecarRoutingRepository` (routes `invoke` by resource type to HTS/KR/clinical REST). `$apply` uses it via `LibraryEngine` instead of CQF `ProxyRepository`. CQF calls `invoke(id, "$expand", null)` — the router accepts null parameters and substitutes empty FHIR `Parameters` before delegating to REST.

**Workaround:** empty prefetch until sidecar is updated.

## HFS WARN: `ValueSet $expand returned empty expansion for :in modifier`

**Symptom:** Clinical HFS logs warnings; `:in` searches match nothing.

**Cause:** HTS `$expand` returned zero codes. HFS injects sentinel `__hts_empty_expansion__` so no resources falsely match.

**Checks:**

1. **HTS has concepts for the ValueSet's code systems**

   ```bash
   sqlite3 ./data/hts.db "SELECT COUNT(*) FROM concepts;"
   curl -s -X POST http://127.0.0.1:9091/ValueSet/\$expand \
     -H 'Content-Type: application/fhir+json' \
     -d '{"resourceType":"Parameters","parameter":[{"name":"url","valueUri":"YOUR_VS_URL"}]}' \
     | jq '.expansion.total'
   ```

2. **SNOMED/ICD/RxNorm imported** — VSAC compose JSON alone is insufficient for full expand (see [data-import.md](./data-import.md)).

3. **CPT/HCPCS-only ValueSets** — Some exclusion ValueSets (ESRD outpatient, frailty encounters) need CPT/HCPCS concepts. Numerator paths often don't hit these; Denominator Exclusions may still warn.

4. **`HFS_TERMINOLOGY_SERVER`** on clinical HFS must point at the same HTS the sidecar uses.

5. **Restart HTS** after large imports (CodeSystem id cache).

6. **Clear stale empty expansion cache** (rare):

   ```sql
   DELETE FROM value_set_expansions WHERE value_set_id IN (
     SELECT id FROM value_sets WHERE url LIKE '%problem_oid%'
   );
   ```

## Bridge `upstream=http://127.0.0.1:8080` or clinical 404 on `:8081`

**Symptom:** Bridge log shows default upstream **8080**; `GET /metadata` on **8081** returns 404 or clinical paths fail.

**Cause:** `CR_FHIR_BRIDGE_UPSTREAM_URL` not passed to the bridge process (common when env vars are on separate shell lines).

**Fix:**

```bash
CR_FHIR_BRIDGE_UPSTREAM_URL=http://127.0.0.1:8082 \
CR_FHIR_BRIDGE_KR_URL=http://127.0.0.1:8079 \
  cargo run --bin cr-fhir-bridge
```

## Sidecar bound to port 8081

**Symptom:** `GET http://127.0.0.1:8081/metadata` returns 404 with Spring-style JSON; CDS fails with HAPI-1357 metadata errors.

**Cause:** JVM sidecar started on **8081** instead of **cr-fhir-bridge**.

**Fix:** Sidecar on **8088** only; bridge on **8081**. `lsof -ti :8081 | xargs kill` then restart bridge.

## `Could not load model information for model AtriusIn`

**Cause:** Sidecar build without AtriusIn modelinfo support, or `Library/AtriusIn-ModelInfo` missing on KR.

**Fix:** Use current **JVMsidecar** build; import Atrius libraries: `AtriusIGDraft/scripts/import-atrius-kr-libraries.py`. See `AtriusIGDraft/docs/clinical-reasoning-stack.md`.

## Sidecar 404 with Spring-style JSON (`"path":"/Condition"`)

**Symptom:** Evaluate fails; body looks like Spring Boot, not FHIR `OperationOutcome`.

**Cause:** Sidecar JVM client hit wrong host/port/path — not Helios HFS.

**Fix:**

- Set `hfsBaseUrl` to **bridge** URL reachable from sidecar process
- Use `127.0.0.1` not `localhost` if sidecar runs in Docker
- Match path prefix (no extra `/fhir` unless HFS is mounted there)

## `libraryVersion does not match ELM identifier version`

**Cause:** KR `Library.version` out of sync with ELM content.

**Fix:** Re-import libraries:

```bash
./scripts/import-ecqm-kr-libraries.py --download
```

Ensure manifest `libraryVersion` matches imported resource.

Re-import with a **new** version string when ELM content changes; then flush sidecar cache:

```bash
curl -s -X POST http://127.0.0.1:8088/v1/admin/cache/libraries/clear
```

See [kr-library-pinning.md](./kr-library-pinning.md) and [production-deployment.md](./production-deployment.md).

## KR library probe failed / search returns empty but library exists

**Symptom:** `cds-server` exits with `KR library probe failed` and `Library search returned no entries`, or `curl Library?name=…&version=…` returns 0 entries, but `GET /Library/{id}` returns the resource.

**Cause:** Two different issues:

1. **Old cds-server probe (search-only)** — fixed in `library_version.rs`: probe now uses **`GET /Library/{libraryId}`** first. Rebuild cds-server if you still see search-only failures.
2. **Search index drift** — `resources.data.version` and `search_index` disagree (e.g. resource `0.4.000`, index token `0.4.001`). Common after partial re-import; ELM-normalized version vs stale index. **Pinning validation uses direct read and is unaffected** once probe is updated.

**Checks:**

```bash
# Authoritative (cds-server + sidecar)
curl -s http://127.0.0.1:8079/Library/CMS2FHIRPCSDepressionScreenAndFollowUp \
  -H 'Accept: application/fhir+json' | jq '{id, name, version}'

# Search (optional)
curl -s "http://127.0.0.1:8079/Library?name=CMS2FHIRPCSDepressionScreenAndFollowUp&version=0.4.000" \
  -H 'Accept: application/fhir+json' | jq '.entry | length'
```

**Fix search index** (export + PUT through HFS):

```bash
curl -s "http://127.0.0.1:8079/Library/{libraryId}" -H 'Accept: application/fhir+json' > /tmp/lib.json
curl -s -X PUT "http://127.0.0.1:8079/Library/{libraryId}" \
  -H 'Content-Type: application/fhir+json' -d @/tmp/lib.json
```

**Fix manifest pins** — regenerate from KR so you do not probe libraries that were never imported:

```bash
./scripts/generate-cds-hooks-manifest.py --kr-base-url http://127.0.0.1:8079 \
  --output manifests/cds-services-local.json
```

See [data-import.md § KR HFS](./data-import.md#kr-hfs-knowledge-libraries) and [kr-library-pinning.md § KR probe vs FHIR search](./kr-library-pinning.md#kr-probe-vs-fhir-search-important).

## Stale CQL after KR re-import (same version string)

**Cause:** JVM sidecar caches compiled ELM by `(libraryBase, libraryId, libraryVersion)`; re-importing the same version does not change the cache key.

**Fix:** `POST /v1/admin/cache/libraries/clear` on the sidecar, or restart `atrius-cql-sidecar`.

**Note:** Auto-invalidation on same-version re-import is **not** implemented yet (planned: cache key includes `versionId` / ELM hash). See [kr-library-pinning.md § Roadmap](./kr-library-pinning.md#not-in-the-first-slice-roadmap).

## `FHIRHelpers` / include library not found

**Cause:** Sidecar loads CQL **`include`** via **`hfsBaseUrl`**, not `libraryBaseUrl`.

**Fix:**

- `hfsBaseUrl` → `cr-fhir-bridge` (**8081**)
- `CR_FHIR_BRIDGE_KR_URL=http://127.0.0.1:8079`
- Verify: `curl http://127.0.0.1:8081/Library/FHIRHelpers`

## Patient retrieve returns no data

**Checks:**

- Patient exists on clinical HFS: `curl http://127.0.0.1:8082/Patient/{id}`
- Same id through bridge: `curl http://127.0.0.1:8081/Patient/{id}`
- Compartment search: `curl 'http://127.0.0.1:8081/Observation?patient={id}'`
- Reference index: bare `?patient=cms165-demo` must match `Patient/{id}` and `urn:uuid:{id}` forms

## Empty Numerator / false Denominator Exclusions

**Checks:**

- Hook invoke includes `context.measurementPeriod` with the intended reporting window (`low` / `high` dates)
- Essential HT Condition present for patient (CMS165 IP)
- BP observations with correct LOINC codes in measurement period
- HTS expands HT and BP ValueSets (non-zero `expansion.total`)

## cds-server demo mode (no ELM evaluation)

**Cause:** `CDS_CLINICAL_REASONING_URL` empty or sidecar unreachable.

**Fix:** Set sidecar URL and provide manifest via `CDS_SERVICES_MANIFEST_PATH` or `CDS_KR_SERVICES_BINARY_ID`.

## HTS port mismatch

Defaults differ across files (`8090` vs `9091`). Align:

- `HTS_SERVER_PORT`
- `HFS_TERMINOLOGY_SERVER` (clinical HFS)
- `CDS_HTS_BASE_URL` (cds-server)
- Sidecar `htsBaseUrl` in evaluate requests

## SNOMED concepts disappeared

**Common causes:**

- HTS using a different database file than expected
- Only VSAC re-imported (ValueSets without RF2 concepts)
- Empty `snomed-ct|current` stub row — concepts on versioned row (`20260501`); verify with:

  ```sql
  SELECT cs.id, cs.version, COUNT(c.id)
  FROM code_systems cs LEFT JOIN concepts c ON c.system_id = cs.id
  WHERE cs.url = 'http://snomed.info/sct' GROUP BY cs.id;
  ```

Re-run SNOMED RF2 import; restart HTS.

## Useful log targets

```bash
RUST_LOG=helios_rest::handlers::search=debug cargo run --bin hfs   # :in expansion warnings
RUST_LOG=cr_fhir_bridge=debug cargo run --bin cr-fhir-bridge       # projection stats
RUST_LOG=cds_server=debug cargo run -p cds-server                    # sidecar invoke
```

HTS: `EX_PROBE` lines show expand cache hits/misses and result sizes.

# CDS / Clinical Reasoning Stack — Forward Plan

Saved from architecture review (2026-06). See also [roadmap.md](./roadmap.md) for slice status.

## Current state

The stack is **functionally complete for slice 1–3 infrastructure** with a **reference ER chest pain pathway E2E**:

| Layer | Status |
|-------|--------|
| **cds-server** | 68 services, `/ready` pins, `$apply` + legacy evaluate, all library hooks, SMART pass-through |
| **JVM sidecar** | evaluate, PlanDefinition/ActivityDefinition `$apply`, prefetch dedupe, content-aware cache keys (**slice 2 done**) |
| **clinical HFS** | Proxy, KR `/Library*` passthrough, FHIR REST `$apply` façade |
| **ER chest pain** | AtriusIGDraft CQL 0.4.2, smoke script, RequestGroup with ACS + result-loop actions |
| **Clinical UI + BFF** | SMART via Keycloak, classification QR, order proposals, fulfillment simulation |

**Strategic split:**

- **Atrius-authored pathways** (ER chest pain): CQL uses `AtriusIn` — runtime mapper optional.
- **Imported CMS eCQMs**: prefer Atrius-authored CQL against storage profiles (no runtime mapper).

## Priority sequence

```text
Phase A  Close ER chest pain production loop (UI + BFF + card UX)     ← in progress
Phase B  Harden ops (auth, observability, CI pins, docs sync)
Phase C  Expand clinical pathways + order hooks (authoring in AtriusIGDraft)
Phase D  Legacy eCQM quality (mapper v0.2) — only if CMS cards must run on Atrius storage
Phase E  Platform scale (multi-tenant, optional server-side prefetch)
```

## Phase A — ER chest pain production loop

| Item | Owner | Status |
|------|-------|--------|
| A1 BFF prefetch resolver | atrius-bff | **Done** — `src/prefetch.rs` |
| A2 Card indicator semantics | cds-server | **Done** — `request_group_cards.rs` |
| A3 RequestGroup UI panels | atrius-clinical-ui | Partial — orders + fulfillment; indicator styling added |
| A4 Smoke script consolidation | docs | AtriusIGDraft = seed + phased; atrius-hfs = CI curl |
| B3 Manifest KR pin validation | atrius-hfs | **Done** — `scripts/validate-manifest-kr-pins.sh` |

## Phase B — Production hardening

- B1: `CDS_REQUIRE_FHIR_AUTHORIZATION`, disable BFF `enable_internal_launch` in staging
- B2: Prometheus export for sidecar `/metrics`
- B3: CI manifest↔KR pin validation (`scripts/validate-manifest-kr-pins.sh`)
- B4: roadmap + cds-prefetch docs synced

## Phase C — Authoring (AtriusIGDraft)

- **ActivityDefinition catalog (v0.1)** — [activity-definition-catalog-authoring.md](../../../AtriusIGDraft/docs/activity-definition-catalog-authoring.md); ER chest pain ACS orders compose catalog ADs
- Complete non-ACS branch ActivityDefinitions
- Second reference pathway (CMS165 Atrius-profiled or adjacent ED protocol)
- Generic RequestGroup renderer in clinical UI
- Order hooks when CPOE integration starts

## Phase D — Runtime mapper (conditional)

Extend Observation/Encounter projection for legacy CMS eCQMs on Atrius storage profiles.

## Architecture decisions (preserve)

1. cds-server speaks **CDS Hooks only** — FHIR `$apply` REST on clinical HFS
2. Sidecar `hfsBaseUrl` → **clinical HFS (8082)**; `libraryBaseUrl` → KR
3. KR `/Library*` passthrough — no projection on knowledge reads
4. **Prefetch: client/BFF resolves**, cds-server pass-through
5. **PlanDefinition-first invoke** when manifest has `planDefinitionId`

## Component ownership

| Repo / crate | Next work |
|--------------|-----------|
| cds-server | Card indicator, optional JWT introspection |
| clinical HFS | Broader mapper hooks when phase D |
| atrius-bff | Prefetch resolver (done), production auth config |
| atrius-clinical-ui | Generic pathway renderer |
| AtriusIGDraft | Pathway content, translate/import |
| JVMsidecar | Prometheus, maintain prefetch dedupe |

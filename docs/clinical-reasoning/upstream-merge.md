# Merging Helios `main` into Atrius feature branches

This guide describes how to keep **Atrius clinical-reasoning feature branches** aligned with [Helios HFS](https://github.com/HeliosSoftware/hfs) without repeating the painful merge we hit when `feat-clinical-reasoning` fell ~250 commits behind upstream.

Use it whenever you sync `upstream/main` → your fork `main` → `feat-clinical-reasoning` (or related feat branches).

There is **one** validation engine: `helios-fhir-validator`, reached through `ValidationService`. Atrius IG profiles load as listed `HFS_FHIR_PACKAGES` overlays. The `fhir-validation*` crates, `HFS_PROFILE_MANIFEST`, `ProfileValidationService`, and `enforce_profile_on_write` were removed on 1 Aug 2026 (`f257914a8`). Do not restore them on conflict. Operator config: [validation-cutover.md](../validation-cutover.md).

---

## Branch roles

| Branch | Purpose | Allowed merges |
|--------|---------|----------------|
| **`main`** | Helios sync + fork hygiene only | `upstream/main` → `main` |
| **`feat-clinical-reasoning`** (and siblings) | All Atrius product code | `main` → feat only |
| **`feat-clinical-reasoning-v2`** | Clean rebuild base (optional) | Same as feat |

**Rules**

1. Never merge `upstream/main` directly into a feature branch — always through your synced `main`.
2. Do not land Atrius feature crates on `main` (keep `main` mergeable with Helios).
3. Sync **weekly** while a feature branch is active; if you are **> ~80 commits** behind `main`, prefer a **rebuild** (see below) over a heroic merge.

---

## Weekly sync workflow

### Step 1 — Update fork `main`

```bash
git fetch upstream origin
git checkout main
git merge upstream/main
# Expect tiny diffs: .gitattributes, .gitignore, occasional generated FHIR test fixtures.
cargo build
git push origin main
```

### Step 2 — Merge `main` into the feature branch

```bash
git checkout feat-clinical-reasoning
git fetch origin
git merge origin/main
```

If Git reports conflicts, use the [conflict resolution table](#conflict-resolution-cheat-sheet) below.

### Step 3 — Verify

```bash
cargo build
cargo build -p cds-server
cargo test -p helios-rest --test validation_enforcement_tests
cargo test -p helios-rest --test validate_operation_tests
```

Optional (requires running stack):

```bash
./scripts/cds-cms165-prefetch-smoke.sh
./scripts/cds-er-chest-pain-smoke.sh
```

### Step 4 — How far behind am I?

```bash
git fetch origin
git log --oneline origin/main..HEAD | wc -l    # commits ahead (your work)
git log --oneline HEAD..origin/main | wc -l    # commits behind main — keep this small
```

| Commits behind `main` | Recommended action |
|----------------------|-------------------|
| 0–30 | Normal merge |
| 30–80 | Merge + careful conflict triage |
| **> 80** | **Rebuild** feature branch on `main` (see [Rebuild procedure](#rebuild-procedure-when-merge-is-too-painful)) |

---

## What is Atrius-only vs Helios-owned

### Atrius-only (keep yours on conflict)

These paths rarely conflict with Helios and should stay on feature branches:

```
.github/workflows/atrius-ci.yml
crates/cds-server/
crates/fhir-valueset-gen/
crates/fhir-terminology/
docs/clinical-reasoning/
docs/his/
docs/validation-cutover.md
manifests/
deploy/
scripts/cds-*.sh
scripts/cds_manifest_common.py
scripts/generate-*.py
scripts/import-*.py
scripts/run-kr-hfs.sh
scripts/setup-plandefinition-cds-catalog.sh
scripts/setup-atrius-profile-registry.sh
scripts/validate-manifest-kr-pins.sh
data/clinical-reasoning/
```

**Do not commit:** `org/opencds/**/*.class` (JVM bytecode belongs in the sidecar JAR, not git).

**Do not resurrect:** `crates/fhir-validation/`, `crates/fhir-validation-gen/`, `crates/fhir-validation-types/`, `crates/rest/src/profile_validation.rs`. Those trees are gone.

### Helios-owned (take `main` on conflict)

Default to **Helios `main`** for shared infrastructure you did not intentionally extend:

```
crates/rest/src/handlers/bulk_*.rs
crates/rest/src/handlers/sof/
crates/rest/src/bulk_submit_*.rs
crates/rest/src/export/
crates/persistence/
crates/sof/
crates/hts/          # except tiny CDS-related tweaks — re-apply manually if needed
crates/hfs/          # ValidationService is constructed inside helios-rest build_app
crates/fhirpath/     # except terminology_client extensions listed below
crates/audit/
crates/subscriptions/
docker/
.github/
```

Taking an old feat version of these files **removes** upstream features (bulk-submit, SOF REST, FHIRPath `resolve()`, HTS import performance, etc.).

---

## Surgical integration in shared crates

After taking `main` for a conflicted shared file, re-apply only these **Atrius touch points**.

### `crates/rest`

One engine. Writes go through `ValidationService::check_write` (`HFS_VALIDATION_MODE`: `off` / `log` / `enforce`). `$validate` uses `ValidationService::validate_resource`. Atrius IG profiles are **not** a second validator; they are extra `SchemaRegistry` layers on `CompositeResolver`.

| File | Atrius change |
|------|----------------|
| `src/validation.rs` | `package_layers` from `HFS_FHIR_PACKAGES` (listed packages only; do not walk `package.json` deps) on `CompositeResolver` with the tenant overlay and embedded core pack |
| `src/config.rs` | `HFS_FHIR_PACKAGE_CACHE`, `HFS_FHIR_PACKAGES` (Helios already owns `HFS_VALIDATION_MODE`) |
| `src/state.rs` | `validation: Arc<ValidationService>` — **not** a `profile_validation` field |
| `src/handlers/create.rs` / `update.rs` / `patch.rs` | `state.validation().check_write(...)` before persist |
| `src/handlers/batch.rs` | `check_write` on batch POST/PUT/PATCH; transaction pre-flight for POST/PUT/PATCH and DELETE existence |
| `src/handlers/validate.rs` | `$validate` `mode` enforcement (create/update/delete/profile); do not restore the deleted Atrius handler |
| `tests/validation_enforcement_tests.rs` | Write-path `HFS_VALIDATION_MODE` tests |

`check_write` **does** run on batch and transaction POST/PUT/PATCH. Transaction
DELETE entries fail the bundle if the instance is missing. Bulk-submit ingest
calls `IngestValidator` (`check_write`) when the worker is wired with
`ValidationService`. Do not restore the old crates.

`$validate` `mode` changes enforcement: `create` (duplicate id), `update` (id
required / not found), `delete` (id, existence, AuditEvent immutability; no
referential integrity), `profile` (ignore `meta.profile`).

Slice `type` / `profile` / `binding` / `exists` / `extension` matchers are
evaluated. Remaining limitations (not a second engine):

- `resolve()` only follows References already in the instance (`contained`,
  Bundle entries). It does not hit storage; an unresolved reference does not
  match.
- Binding discriminators do not expand a ValueSet at mark time.
- Conditional PATCH inside a Bundle is refused (instance PATCH and Bundle
  instance-url PATCH are implemented).

### `crates/persistence`

| File | Atrius change |
|------|----------------|
| `src/core/schema_ledger.rs` | Named `schema_migrations` ledger; fork vs upstream integer classification |
| `src/backends/{sqlite,postgres}/schema.rs` | Dispatch by step **name**; `subscription_outbox` is `OUTBOX_STEP`; `subscription_outbox_dead_letter` is the tip step. Do not restore a pure integer `migrate_schema` loop |
| `src/backends/*/subscription_outbox.rs` | Durable outbox store (`mark_dead` / `dead_at`; claim skips dead rows; SQLite process mutex + `BEGIN IMMEDIATE` + CAS so one file cannot double-claim — not a cluster outbox) |

`SCHEMA_VERSION` (22 SQLite / 39 Postgres) is an operator stamp. Clinical restart after the ledger lands creates `schema_migrations` and backfills names; it must not replay the full Postgres index ladder. SQLite 20/21 are Helios `#903` `idx_resources_reindex` and `#944` partial family indexes (Helios numbered those v19/v20). SQLite 22 / Postgres 39 add `subscription_outbox.dead_at` (`OUTBOX_DEAD_LETTER_STEP`). Do not restore hourly retry of exhausted outbox rows.

### `crates/hts`

SQLite URL→`system_id` and CodeSystem language memos live on `SqliteTerminologyBackend` (`cs_system_id_cache`, `cs_language_cache`). `invalidate_caches` is exhaustive. Do not restore process-wide `OnceLock` maps.

### `crates/fhirpath`

| File | Atrius change |
|------|----------------|
| `src/lib.rs` | `pub mod terminology_client;` |
| `src/terminology_client.rs` | `validate_code_with_parameters`, local ValueSet URL routing |
| `src/handlers.rs` | `pub fn json_value_to_evaluation_result` |
| `src/evaluator.rs` | `pub fn convert_resource_to_result` |

Merge carefully — do **not** replace the whole file with an old feat copy (you will lose `resolve()` and resource visibility fixes from Helios).

### `crates/fhir-validator`

| File | Atrius change |
|------|----------------|
| `src/packages/resolve.rs` | Listed `HFS_FHIR_PACKAGES` only — do not walk `package.json` dependencies (that pulls `ndhm.in` and fails offline) |
| `src/converter/slicing.rs` | Discriminator coverage: `type` / `profile` / `binding` / `exists` / `extension('url')`; `resolve()` is in-scope only (`contained` / Bundle entries). Ordered slice ordinals. |

### `crates/cds-hooks`

| File | Atrius change |
|------|----------------|
| `src/hooks.rs` | `MeasurementPeriodContext`, `measurement_period` on `PatientViewContext`, `LIBRARY_HOOK_NAMES`, `is_library_hook` |
| `src/lib.rs` | Re-exports for the above |
| `src/service.rs` | `CdsHooksError::BadGateway` |

### `crates/auth`

| File | Atrius change |
|------|----------------|
| `src/discovery.rs` | Expanded SMART scopes/capabilities when authorize endpoint is configured |
| `src/principal.rs` | Keep `fhir_user`. Struct is `#[non_exhaustive]`; Helios tests must use `Principal::stub(subject, scopes)` (then `.with_issuer` / `.with_tenant_id` as needed). Do not restore field-by-field literals. |
| `src/jti/revocation.rs` | Keep Redis deny-list: `ConnectionManager`, per-request timeout, `requires_jti`, fail-closed `RevocationUnavailable`. Do not restore a replay cache. |

### `crates/subscriptions`

Keep Atrius outbox, heartbeat, and `fhirPathCriteria` evaluation. Take Helios `with_status_store` — do not restore `SubscriptionStatusStore`.

### Root workspace

| File | Atrius change |
|------|----------------|
| `Cargo.toml` | Keep Atrius `default-members` (`fhir-valueset-gen`, `fhir-terminology`, `cds-server` is a workspace member via `crates/*` but not a default member). Helios version / serde pins from `main` |

**Note:** `ValidationService` is wired in `helios-rest` `AppState` from `ServerConfig`. `crates/hfs/src/main.rs` also constructs one for bulk-submit workers (`IngestValidator`). Do not restore a second engine.

---

## Conflict resolution cheat sheet

| Conflict location | Resolution |
|-------------------|------------|
| `crates/fhir/src/r4.rs` (Helios) vs `crates/fhir/src/r4/` (this fork) | Keep the **directory**. Do not take Helios's flat `r4.rs` (E0761 if it sits beside `r4/`). There is no `r4/terminology/`. If Helios's `r*.rs` changed more than buildId, regenerate — [fhir-model-regen.md](../fhir-model-regen.md) |
| `crates/rest/**` | Take **`main`**, re-apply [REST touch points](#cratesrest) |
| `crates/persistence/**` | Take **`main`**, re-apply named ledger + outbox |
| `crates/hfs/**` | Take **`main`** |
| `crates/cds-hooks/**` | **Merge** — keep measurement period + BadGateway |
| `Cargo.toml` | Take **`main`** pins/version, keep Atrius `default-members` |
| `Cargo.lock` | Do not hand-merge — fix `Cargo.toml`, run `cargo build` |
| `.gitattributes` | Keep fork rule: `helios_fhir/** merge=ours` |
| `.gitignore` | Keep fork entries (e.g. `/crates/fhir/tests/`, `/data/fhir-packages/`) |

---

## Rebuild procedure (when merge is too painful)

Use when `HEAD..origin/main` is large (e.g. **> 80 commits**) or merges re-delete bulk-submit / SOF / HTS fixes.

```bash
git fetch origin upstream
git checkout origin/main -b feat-clinical-reasoning-v2

# 1. Copy Atrius-only trees from the old branch
git checkout origin/feat-clinical-reasoning -- \
  crates/cds-server \
  crates/fhir-valueset-gen \
  crates/fhir-terminology \
  docs/clinical-reasoning \
  docs/his \
  docs/validation-cutover.md \
  manifests \
  data/clinical-reasoning \
  deploy \
  scripts/cds-cms165-prefetch-smoke.sh \
  scripts/cds-er-chest-pain-smoke.sh \
  scripts/cds_manifest_common.py \
  scripts/generate-cds-hooks-manifest.py \
  scripts/import-cms165-demo.py \
  scripts/import-synthea-atrius.py \
  scripts/run-kr-hfs.sh \
  scripts/setup-plandefinition-cds-catalog.sh \
  scripts/setup-atrius-profile-registry.sh \
  scripts/validate-manifest-kr-pins.sh

# 2. Do NOT copy fhir-validation*, profile_validation.rs, or HFS_PROFILE_* config.

# 3. Re-port surgical shared-crate patches (see tables above) onto current main files
#    — or diff against feat-clinical-reasoning-v2 from a prior successful rebuild.
#    Persistence: named schema_migrations ledger + subscription_outbox.
#    REST: HFS_FHIR_PACKAGES overlay on ValidationService.

# 4. Build and test
cargo build
cargo build -p cds-server
cargo test -p helios-rest --test validation_enforcement_tests
cargo test -p helios-rest --test validate_operation_tests

# 5. When validated, rename branches
git branch -m feat-clinical-reasoning feat-clinical-reasoning-archive
git branch -m feat-clinical-reasoning-v2 feat-clinical-reasoning
```

---

## `.gitattributes` (fork hygiene on `main`)

On `main` you should keep:

```gitattributes
helios_fhir/** merge=ours
```

Use `merge=ours` only for **generated or fork-owned** paths — not for hand-written integration in `rest/` or `cds-hooks/`. Do not add a `fhir-validation-gen/generated/**` rule; that crate is gone.

---

## Splitting work across branches (optional)

To reduce merge conflict surface:

| Branch | Scope |
|--------|--------|
| `feat/cds-stack` | `cds-server`, terminology/valueset generators |
| `feat/clinical-reasoning` | REST/HFS package overlay, docs, manifests, smokes |

Merge order: `main` → cds-stack → clinical-reasoning integration.

---

## Pre-merge checklist

**Before merging `main` into feat:**

- [ ] `origin/main` is up to date with `upstream/main`
- [ ] Clean working tree on feature branch
- [ ] Checked commits behind: `git log --oneline HEAD..origin/main | wc -l`
- [ ] If behind > 80, scheduled a rebuild instead of merge

**After merge:**

- [ ] `cargo build` (default workspace)
- [ ] `cargo build -p cds-server`
- [ ] `cargo test -p helios-rest --test validation_enforcement_tests`
- [ ] `cargo test -p helios-rest --test validate_operation_tests`
- [ ] No accidental deletion of `handlers/sof/`, `bulk_submit`, or `persistence/src/sof/`
- [ ] No resurrection of `fhir-validation*` or `HFS_PROFILE_MANIFEST`
- [ ] `schema_migrations` dispatch still present in SQLite and Postgres schema.rs

---

## Anti-patterns (what went wrong before)

1. **Merging `upstream/main` directly into feat** — skips fork hygiene; doubles conflict noise.
2. **Letting feat sit months behind** — forces rebuild; old feat deleted bulk-submit and SOF handlers.
3. **Replacing entire `rest/` or `hfs/` from old feat** — regresses Helios 0.2.0 features.
4. **Hand-editing `Cargo.lock`** — regenerate via `cargo build`.
5. **Committing `org/*.class` or local terminology zips** — huge diffs and merge noise.
6. **Keeping “dual validators” on the sync keep-list** — there is no second engine; restoring `fhir-validation` undoes the 1 Aug cutover.

---

## Related docs

- [Clinical reasoning stack overview](./README.md)
- [Single-engine validation cutover](../validation-cutover.md)
- [Startup guide](./startup-guide.md) — local stack and smoke scripts
- [Troubleshooting](./troubleshooting.md) — runtime issues after sync

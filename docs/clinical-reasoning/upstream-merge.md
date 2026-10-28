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
# Matches `.github/workflows/atrius-ci.yml`. Includes `--lib`: Helios
# PATCH-501 unit tests can merge independently of feat's three-entry
# bundle fixture (14 Sep 2026).
cargo test -p helios-rest --features R4,sqlite
# Full HTS suite, not `--lib`: Helios's instance-identity and CRUD contract
# tests are integration tests (25 Oct 2026).
cargo test -p helios-hts --features R4,sqlite
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
| `src/handlers/batch.rs` | `check_write` on batch POST/PUT/PATCH; transaction pre-flight for instance POST/PUT/PATCH and DELETE existence. Conditional URL criteria are Helios `#859` (resolved inside the transaction; do not 400 them). Take Helios `write_event::report` (including PATCH success). Do **not** take Helios's 501 PATCH-in-bundle arm. Accept Helios's delete of `handlers/subscription_event.rs` once batch no longer calls it |
| `src/handlers/validate.rs` | `$validate` `mode` enforcement (create/update/delete/profile); do not restore the deleted Atrius handler |
| `tests/validation_enforcement_tests.rs` | Write-path `HFS_VALIDATION_MODE` tests |

`check_write` **does** run on batch and transaction POST/PUT/PATCH. Transaction
DELETE of a missing instance fails the bundle. A type-level DELETE or PATCH
that already carries criteria is Helios `#859` and is not refused up front.
Bulk-submit ingest calls `IngestValidator` (`check_write`) when the worker is
wired with `ValidationService`. Do not restore the old crates.

`$validate` `mode` changes enforcement: `create` (duplicate id), `update` (id
required / not found), `delete` (id, existence, AuditEvent immutability; no
referential integrity), `profile` (ignore `meta.profile`).

Slice `type` / `profile` / `binding` / `exists` / `extension` matchers are
evaluated. Remaining limitations (not a second engine):

- `resolve()` only follows References already in the instance (`contained`,
  Bundle entries). It does not hit storage; an unresolved reference does not
  match.
- Binding discriminators do not expand a ValueSet at mark time.
- Conditional PATCH is **resolved in a batch** (one match `200`, none `404`,
  several `412`) and **in a transaction** (Helios `#859`: criteria resolve
  inside the open transaction). Keep feat `bundle_patch_format` and the
  unsupported-capability fallback to `conditional_patch`. Instance PATCH and
  Bundle instance-url PATCH are implemented; do not take Helios's 501
  PATCH-in-bundle arm.

### `crates/persistence`

| File | Atrius change |
|------|----------------|
| `src/core/schema_ledger.rs` | Named `schema_migrations` ledger; fork vs upstream integer classification |
| `src/backends/{sqlite,postgres}/schema.rs` | Dispatch by step **name**; `subscription_outbox` is `OUTBOX_STEP`; `subscription_outbox_dead_letter` is the tip step. Do not restore a pure integer `migrate_schema` loop |
| `src/backends/*/subscription_outbox.rs` | Durable outbox store (`mark_dead` / `dead_at`; claim skips dead rows; SQLite process mutex + `BEGIN IMMEDIATE` + CAS so one file cannot double-claim — not a cluster outbox) |
| `src/composite/storage.rs` | After search, resolve `_include`/`_revinclude` with `resolve_includes_iterative` (Helios `#1103` is the shared resolver; do not keep a search backend's partial `included` list). Do not invent a persistence `TerminologySearchProvider` — REST expands `:in` via `HFS_TERMINOLOGY_SERVER`. After Helios `#1406`, do **not** keep a feat `conditional_patch` override — the trait default uses composite `update` so secondary sync + durable `secondary_sync_failures` stay on the write path. Keep `conditional_matches` → `resolve_conditional_matches` for Bundle PATCH. |
| `src/core/patch.rs` | Take Helios `#1393` FHIRPath via `apply_patch_for_version` (a version is required; `apply_patch` without one still returns `UnsupportedFormat`). Keep feat JSON-array and merge-object Bundle PATCH in `bundle_patch_format`. `decode_bundle_patch_resource` stays Parameters / R5–R6 Binary. |

`SCHEMA_VERSION` (43 SQLite / 55 Postgres) is an operator stamp. Clinical restart after the ledger lands creates `schema_migrations` and backfills names; it must not replay the full Postgres index ladder. SQLite 20/21 are Helios `#903` `idx_resources_reindex` and `#944` partial family indexes (Helios numbered those v19/v20). SQLite 22–24 are Helios `#947`/`#967` (drop `idx_search_resource`, late partial indexes, `resource_fts_map`; Helios numbered those v21–v23). SQLite 25–26 are Helios `#959`/`#953` (`idx_resources_live_type`, manifest `phase`/`files_*`; Helios numbered those v24–v25). SQLite 27–28 are Helios manifest publication and export `types_*` (Helios numbered those v26–v27). SQLite 29 is Helios v28 (partial `idx_search_string_folded`). SQLite 30 is Helios v29 (`idx_search_token_display` drop, `#945`). SQLite 31 is Helios v30 (`bulk_manifests.index_pending`, `#1125`). SQLite 32 is Helios v31 (`search_index.resource_key`, `#945`). SQLite 33 is Helios v32 (`bulk_manifest_file_progress` / `skipped_entries`, `#1127`). SQLite 34 is Helios v33 (`bulk_export_jobs.attempts`, `#1041`). SQLite 35 is Helios v34 (`secondary_sync_failures`, `#1334`). SQLite 36 is Helios v35 (`search_index_date_range`, `#1391`). SQLite 37 is Helios v36 (`login_sessions`, `#1481`). SQLite 38 is Helios v37 (`bulk_manifest_file_progress_completed`, `#1610`). SQLite 39 is Helios v38 (`search_index_composite_slot`, `#1407`). SQLite 40 is Helios v39 (`search_index_date_start`, `#1506`). SQLite 41 is Helios v40 (`history_updated_index`, `#2107`). SQLite 42 is Helios v41 (`sof_terminology_tables`, `#1824`). `OUTBOX_DEAD_LETTER_STEP` is the SQLite tip (v43) so an upstream-numbered Helios v41 DB does not imply `dead_at` already applied (`implied_applied_indices` maps Helios v29 onto fork indices 16..=28, Helios v30 onto 16..=29, Helios v31 onto 16..=30, Helios v32 onto 16..=31, Helios v33 onto 16..=32, Helios v34 onto 16..=33, Helios v35 onto 16..=34, Helios v36 onto 16..=35, Helios v37 onto 16..=36, Helios v38 onto 16..=37, Helios v39 onto 16..=38, Helios v40 onto 16..=39, and Helios v41 onto 16..=40). New Helios SQLite steps must sit **immediately after** `sof_terminology_tables` and **before** `OUTBOX_DEAD_LETTER_STEP`. `migrate_v38_to_v39` / `migrate_v39_to_v40` / `migrate_v40_to_v41` (SQLite) must **not** `set_schema_version` to 39, 40, or 41. Postgres 37–38 are the same publication/`types_*` steps (Helios numbered those v37–v38), Postgres 39 is Helios v39 file-progress, Postgres 40 is Helios v40 attempts (`#1041`, already on feat as `bulk_export_jobs_attempts`), Postgres 41 is Helios v41 `secondary_sync_failures` (`#1334`), Postgres 42 is Helios v42 `idx_search_reference_target_id` (`#1414`), Postgres 43 is Helios v43 `search_index_date_range` (`#1391`), Postgres 44 is Helios v44 `login_sessions` (`#1481`), Postgres 45 is Helios v45 `patient_export_references`, Postgres 46 is Helios v46 `bulk_manifest_file_progress_completed` (`#1610`), Postgres 47 is Helios v47 `resources_reindex_id` (`#1767`), Postgres 48 is Helios v48 `search_index_composite_slot` (`#1407`), Postgres 49 is Helios v49 `resources_live_tenant` (`#1911`, `idx_resources_live_tenant`), Postgres 50 is Helios v50 `search_index_date_recent_end` (`idx_search_date_recent_end`), then fork-only slot-2, `bulk_manifests_phase_progress`, and `dead_at` (tip v54). New Helios named steps must sit **immediately after** `search_index_date_recent_end` and **before** fork-only slot-2/phase, otherwise Upstream implied-applied would mark slot-2 as the next Helios version. `migrate_v42_to_v43` / `migrate_v43_to_v44` / `migrate_v44_to_v45` / `migrate_v46_to_v47` / `migrate_v47_to_v48` / `migrate_v48_to_v49` / `migrate_v49_to_v50` must **not** `set_schema_version` to Helios's 43, 44, 45, 47, 48, 49, or 50. Do not restore hourly retry of exhausted outbox rows.

### `crates/hts`

SQLite URL→`system_id` and CodeSystem language memos live on `SqliteTerminologyBackend` (`cs_system_id_cache`, `cs_language_cache`). `invalidate_caches` is exhaustive. Do not restore process-wide `OnceLock` maps.

### `crates/fhirpath`

| File | Atrius change |
|------|----------------|
| `src/lib.rs` | `pub mod terminology_client;` |
| `src/terminology_client.rs` | `validate_code_with_parameters`, local ValueSet URL routing. Gateway retries (HTTP 502/503/504/530) live in `helios-terminology-client` (`send_with_retry`). Map `HtsError::ServerError` with `reqwest::StatusCode` display so Helios tests see `504 Gateway Timeout` |
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

Keep Atrius outbox, heartbeat, and `fhirPathCriteria` evaluation. Take Helios `with_status_store` — do not restore `SubscriptionStatusStore`. Take Helios `SubscriptionWriteObserver` (`#1078`) **and** keep the outbox: `on_write` must call `SubscriptionEngine::enqueue_resource_event` (same-TX outbox row already written; enqueue only `notify_one()`). Do **not** spawn `on_resource_event` from the observer — that double-notifies or skips the outbox. Without a Tokio runtime, the in-process fallback is dropped rather than panicked.

### `crates/hfs`

`build_bulk_submit` takes `(config, jobs, storage, reindex_hook, write_observer)`. Keep the `storage` clone for `IngestValidator`. Take Helios `automatic_reindex_hook` / `with_bulk_index_rebuild`. `spawn_submit_workers` takes **both** `ingest_validator` and `write_observer`; the worker chains `.with_ingest_validator` and `.with_write_observer` as separate methods.

### Root workspace

| File | Atrius change |
|------|----------------|
| `Cargo.toml` | Keep Atrius `default-members` (`cds-server`, `crates/terminology-client`; do **not** restore `fhir-valueset-gen` / `fhir-terminology` — they live on `backup/fhir-terminology-and-valueset-gen`). Helios version / serde pins from `main` |

**Note:** `ValidationService` is wired in `helios-rest` `AppState` from `ServerConfig`. `crates/hfs/src/main.rs` also constructs one for bulk-submit workers (`IngestValidator`). Do not restore a second engine.

---

## Conflict resolution cheat sheet

| Conflict location | Resolution |
|-------------------|------------|
| `crates/fhir/src/r4.rs` (Helios) vs `crates/fhir/src/r4/` (this fork) | Keep the **directory**. Do not take Helios's flat `r4.rs` (E0761 if it sits beside `r4/`). There is no `r4/terminology/`. If Helios's `r*.rs` changed more than buildId, regenerate — [fhir-model-regen.md](../fhir-model-regen.md) |
| `crates/rest/**` | Take **`main`**, re-apply [REST touch points](#cratesrest) |
| `crates/persistence/**` | Take **`main`**, re-apply named ledger + outbox |
| `crates/hfs/**` | Take **`main`**, re-apply [HFS touch points](#crateshfs) (`storage` + ingest validator + write observer) |
| `crates/cds-hooks/**` | **Merge** — keep measurement period + BadGateway |
| `crates/subscriptions/src/observer.rs` | Take Helios observer; `on_write` must `enqueue_resource_event`, not spawn `on_resource_event` |
| `crates/rest/src/handlers/subscription_event.rs` | Accept Helios **delete** once batch uses `write_event` |
| `Cargo.toml` | Take **`main`** pins/version, keep Atrius `default-members` |
| `Cargo.lock` | Do not hand-merge — take feat, run `cargo build`. If Helios only bumped the lock (e.g. rustls), `cargo update -p rustls@OLD --precise NEW`; `cargo build` will not pick a newer locked crate by itself |
| `crates/persistence/src/backends/sqlite/schema.rs` | Keep named `SQLITE_STEPS`. Insert new Helios migrate fns **immediately after** `sof_terminology_tables` and **before** `OUTBOX_DEAD_LETTER_STEP`; bump `SCHEMA_VERSION` to `len+1`. Discard Helios integer `migrate_schema`. Helios v29 is `search_index_drop_token_display` (`#945`); v30 is `bulk_manifests_index_pending` (`#1125`); v31 is `search_index_resource_key` (`#945`); v32 is `bulk_manifest_file_progress` (`#1127`); v33 is `bulk_export_jobs_attempts` (`#1041`); v34 is `secondary_sync_failures` (`#1334`); v35 is `search_index_date_range` (`#1391`); v36 is `login_sessions` (`#1481`); v37 is `bulk_manifest_file_progress_completed` (`#1610`); v38 is `search_index_composite_slot` (`#1407`); v39 is `search_index_date_start` (`#1506`); v40 is `history_updated_index` (`#2107`); v41 is `sof_terminology_tables` (`#1824`). `migrate_v38_to_v39` / `migrate_v39_to_v40` / `migrate_v40_to_v41` must not stamp integers 39, 40, or 41. Postgres Helios v39 is file-progress (feat already used `migrate_v38_to_v39` for dead-letter — rename the Helios fn); v40 is attempts (already on feat — do **not** duplicate); v41 is `secondary_sync_failures`; v42 is `search_index_reference_target_id` (`#1414`); v43 is `search_index_date_range`; v44 is `login_sessions`; v45 is `patient_export_references`; v46 is `bulk_manifest_file_progress_completed` (`#1610`); v47 is `resources_reindex_id` (`#1767`); v48 is `search_index_composite_slot` (`#1407`); v49 is `resources_live_tenant` (`#1911`); v50 is `search_index_date_recent_end`; v51 is `sof_terminology_tables` (`#2185`). Place new Helios steps **immediately after** `sof_terminology_tables` and **before** fork-only slot-2/phase/dead-letter (sqlite v42 `search_index_covering_value_indexes` through v46 `search_index_token_code_first`; postgres v52 `bulk_manifests_added_at_index_pending`, v53 `search_index_composite_partial_fast_path`). Replay tests that lower `schema_version` call `unrecord_steps_completing_after` before `initialize_schema`. Do not stamp Helios integers inside `migrate_v42_to_v43`, `migrate_v43_to_v44`, `migrate_v44_to_v45`, `migrate_v45_to_v46`, `migrate_v46_to_v47`, `migrate_v49_to_v50`, `migrate_v50_to_v51`, or `migrate_v51_to_v52`. Fresh init runs the named steps so `create_indexes` may still CREATE `idx_search_token_display` / composite-on-`resource_id` and later named steps drop/rekey them. Integer stamp collision (feat tip vs Helios tip) is expected — names distinguish |
| `crates/persistence/src/core/patch.rs` | Take Helios `#1393` FHIRPath (`apply_patch_for_version`). Keep feat JSON/merge Bundle PATCH via `bundle_patch_format`. Refused-methods fixture stays three codes `["not-supported", "value", "required"]` |
| `crates/persistence/src/composite/storage.rs` | Take Helios `#1406` (drop feat `conditional_patch` override). Keep `conditional_matches` + `resolve_includes_iterative` |
| `crates/persistence/src/backends/sqlite/storage.rs` | Take Helios `resource_key` search-index DELETE SQL **and** the `#1197` `tenant_id`/`resource_type` prefix so the delete seeks `idx_search_composite`; keep feat **`tx.execute`**, not Helios `conn.execute`, so the delete and `maybe_enqueue_on_conn` share one transaction. Purge-after-row-delete still uses `resource_id` (Helios does too — the subquery would miss) |
| `crates/persistence/src/backends/sqlite/bulk_submit.rs` | Keep **both**: feat `ingest_validation_error` **then** Helios `EntryClone` span around `entry.resource.clone()`. Taking only feat leaves `resource` undefined on `create_prepared`. Validation failure returns `(failed, None, None)` (Helios 3-tuple). Do not redeclare `content` after Helios `skip_unchanged` — it is already bound |
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
cargo test -p helios-rest --features R4,sqlite

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
- [ ] `cargo test -p helios-rest --features R4,sqlite` (includes `--lib`; do not skip — Helios 501 PATCH unit tests)
- [ ] `cargo test -p helios-hts --features R4,sqlite` (full suite, not `--lib`; Helios HTS contract tests are integration tests)
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
- [SQL-on-FHIR layering (`persistence` vs `sof`)](../sof-layering.md)
- [Startup guide](./startup-guide.md) — local stack and smoke scripts
- [Troubleshooting](./troubleshooting.md) — runtime issues after sync

---

## Last feat sync

28 Oct 2026: `main` `0e3e6aaad` (Helios `c0325e265` / 0.2.6; `#2171` per-node FHIRPath constraint evaluation and empty nested invariant pass, `#2240` HTS 409 on url+version clash, transaction Bundle PUT restores deleted id, `$sql-run` gzip streaming fix, zstd/crypto-bigint RustSec bumps, fhir-benchmark CI/sql-run suite) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-28` → `ba0690311`. Ledger unchanged SQLite **48** / Postgres **57**. Workspace **0.2.6**. 49 behind / 30 non-merge since `dac7a73a9`, merged rather than rebuilt. Conflicts (4): `Cargo.lock` from feat then `cargo build`; validator — take Helios two-phase `fhirpath_effects` (#2171) and ref-1 integration tests, keep fork limitation docs and `resolution_resources` prefetch pool + `resolve_sees_prefetched_store_resources`. Feat-only since last sync: Redis JTI cfg gating (`ba0690311`), orphan Postgres WriteTx drop, prior HTS/subscriptions/ci fixes. Verification: fmt 0, ledger 38, postgres check 0, hfs check 0, validator **171**, rest **2198**, persistence lib 1814, hts 1066, cds 104, env-drift 0. **`helios-fhir-validator`** library changed; after push, move HIS validator pin with the other Helios crates to the merge SHA if not already on it. Prior: 26 Oct merge, tag `pre-merge-main-2026-10-26`.

26 Oct 2026: `main` `dac7a73a9` (Helios `4e23e5c01` / **0.2.6** release; `#2234` queued audit sink, `#2235` subscriptions tracing pin, Postgres pool budget / composite partial index v52–v53, SQLite search-index v42–v46 covering indexes and code-first token index, Mongo `:missing=true` fingerprint perf, bulk-submit deferred reindex resume, FHIRPath empty `memberOf`) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-26` → `87c964037`. Ledger SQLite **48** / Postgres **57** (Helios sqlite 46; Helios postgres 53; Mongo 14). Workspace **0.2.6**. 64 behind / 33 non-merge on `main` since `8cfaf249a`, merged rather than rebuilt. Conflicts (8): audit middleware keeps Helios `PatientResolver` + response override; Cargo pins 0.2.6 with `helios-auth` `features = ["redis"]` and `helios-terminology-client`; named Postgres ledger — insert `bulk_manifests_added_at_index_pending` (Helios v52) and `search_index_composite_partial_fast_path` (Helios v53) immediately after `sof_terminology_tables` and before fork slot-2; discard integer `migrate_schema`; `migrate_v51_to_v52` does not stamp 52. SQLite ledger — five Helios steps after `sof_terminology_tables` through `search_index_token_code_first`; `sqlite/storage.rs` keeps Helios prepared-index create/restore **and** fork `begin_immediate_tx` + outbox enqueue. Subscriptions dispatch test deduped `pin_tracing_interest` (#2235). Feat-only commits survived: `AuditEventBuilder` setters (`cf8e32e94`) and profile manifest (`085ddf3d6`). Post-merge test fix: Helios v44/v45 replay tests call `unrecord_steps_completing_after` under named dispatch. Verification: fmt 0, ledger 38, postgres check 0, hfs check 0, rest 968, validator 80, persistence lib 1814, hts 1066, cds 104, env-drift 0. **`helios-audit`** and **`helios-observability`** libraries changed; after push, move HIS `helios-fhir`, `helios-auth`, `helios-observability`, `helios-fhir-validator`, and **`helios-audit`** pins together to the merge SHA. Prior: 25 Oct `e6700603e`, tag `pre-merge-main-2026-10-25`.

25 Oct 2026: `main` `8cfaf249a` (Helios `89d7e0703` / 0.2.5; `#2185` SoF terminology tables on Postgres/Mongo/S3, Mongo missing_params fingerprint and metadata-only `$reindex` `#1999`/`#2004`–`#2008`, HTS instance identity and transactional CRUD, FHIRPath upstream cases from 2026-10-23, RUSTSEC-2026-0385/0387) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-25` → `e6700603e`. Ledger SQLite 43 / Postgres 55 (Helios sqlite still 41; Helios postgres 51; Mongo 14). Workspace stays 0.2.5. 67 behind / 33 non-merge, merged rather than rebuilt. Conflicts (13): named Postgres ledger kept — insert `sof_terminology_tables` (Helios v51, `#2185`) immediately after `search_index_date_recent_end` and before fork-only slot-2; discard integer `migrate_schema`; `migrate_v50_to_v51` does not stamp 51. Transactional `update` now calls `write_revision` and still enqueues the durable outbox. HTS keeps `code_system_concept_codes`, BCP 47, `concept_closure` hierarchy checks, and per-instance language memos; takes Helios instance identity (`selected_*_id`, instance-scoped cache keys, transactional delete). Follow-up fix: drop the old BCP 13 MIME early return at the top of ValueSet `$validate-code` (Helios moved it behind the no-stored-definition check), and take Helios's 404 for DELETE of an absent id, which retires fork-doc item 4 (import-only resources stay deletable, 204). Feat work already on the branch survived with no overlap: `AuditEventBuilder` `id` / `subtype` / `extension` setters (`cf8e32e94`) and the Atrius profile-manifest refresh (`085ddf3d6`). Both bulk-submit worker setters and SQLite `:in` `INDEX_OR_CHUNK` 250 survived. Verification: fmt 0, ledger 36, postgres check 0, hfs check 0, rest lib 968, validator 80, converter 19, persistence lib 1793, hts 857, cds 89, env-drift 0. No `helios-fhir` / `helios-auth` / `helios-observability` / `helios-fhir-validator` library change this wave. HIS pins are still on `d3c803037`; after this commit is pushed, move those four together to it, and move `helios-audit` to the same SHA so the `AuditEventBuilder` setters land. Prior: 22 Oct `e6700603e`, tag `pre-merge-main-2026-10-22`.

22 Oct 2026: `main` `20b0ca9f3` (Helios `e0bd5cb35` / 0.2.5; Postgres `idx_search_date_recent_end` schema v50, SearchParameter fingerprint TTL opt-in, eligibility-fact cache, bounded Elasticsearch contained-document sweep, fast-path page drops the pre-LIMIT live-resource EXISTS) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-22` → `cf8e32e94`. Ledger SQLite 43 / Postgres 54 (Helios sqlite still 41; Helios postgres 50). Workspace stays 0.2.5. 31 behind / 19 non-merge. Conflict (1): named Postgres ledger kept — insert `search_index_date_recent_end` (Helios v50) immediately after `resources_live_tenant` and before fork-only slot-2; discard integer `migrate_schema`; `migrate_v49_to_v50` does not stamp 50. Replay tests delete the `search_index_date_recent_end` ledger row before re-running the step. Feat work since 21 Oct survived with no overlap: `AuditEventBuilder` `id` / `subtype` / `extension` setters (`cf8e32e94`) and the Atrius profile-manifest refresh (`085ddf3d6`). Both bulk-submit worker setters and SQLite `:in` `INDEX_OR_CHUNK` 250 survived. No `helios-fhir` / `helios-auth` / `helios-observability` / `helios-fhir-validator` library change this wave. HIS pins are still on `d3c803037`; after this commit is pushed, move those four together to it (covers the 20 Oct and 21 Oct library changes), and move `helios-audit` to the same SHA so the new `AuditEventBuilder` setters land. Prior: 21 Oct `a1a4daf47`, tag `pre-merge-main-2026-10-21`.

21 Oct 2026: `main` `a10182590` (Helios `5c51850ae` / 0.2.5; SoF `$sql-run` json/csv/fhir streaming, row batching and memory trim, configurable MongoDB SoF cursor batch, `#1947` unionAll column types, `#2186` bounded dashboard snapshot cache, MongoDB chunked transaction-Bundle creates, `rustls-webpki` / `webpki-roots` / `neo4rs` RustSec bumps, CI `DOCKER_HOST_IP` derived from `DOCKER_HOST`) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-21` → `de00da1ab`. Ledger unchanged SQLite 43 / Postgres 53 (Helios still 41 / 49). Workspace stays 0.2.5. 79 behind / 49 non-merge, merged rather than rebuilt. Conflicts (3), all dependency files: the fork has no Neo4j or Cassandra backend, so `crates/persistence/Cargo.toml` keeps no `neo4rs` and `audit.toml` keeps no `cdrs-tokio` ignores; `Cargo.lock` kept from feat, then `rustls-webpki` 0.103.16 and `webpki-roots` 1.0.10. Workflow owner guards unchanged in all 25 touched workflows. The 20 Oct If-None-Match gate survived Helios's MongoDB `storage.rs` rewrite. Both bulk-submit worker setters, SQLite `:in` `INDEX_OR_CHUNK` 250, `$sql-run` / `$sql-export`, and owner-scoped `/export-jobs` survived. `helios-observability` library changed (`dashboard.rs`). HIS pins are still on `d3c803037`; after this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, `helios-observability`, and `helios-fhir-validator` pins together to it in one move (covers the 20 Oct `helios-auth` change too). Prior: 20 Oct `39de7806a`, tag `pre-merge-main-2026-10-20`.

20 Oct 2026: `main` `8bb84fdab` (Helios `678fc5061` / 0.2.5; `#2150` secret redaction, `#2142` database URL logging, `#2174` FHIRPath quantity equality, `#2138` CI extended-container cleanup) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-20` → `d3c803037`. Ledger unchanged SQLite 43 / Postgres 53 (no inbound persistence schema steps). Workspace stays 0.2.5. Conflicts (7): keep `helios_terminology_client` wrappers and apply `#2149` redaction there (`transport_error_text`); keep `propagation` and take `redact`; keep `IngestValidator`, `reindex_enabled` Debug, JTI/Redis Debug fields, RF2 `--extends` on `ImportArgs` Debug, and the Atrius `9091` terminology example with Helios `hide_env_values`; CI cleanup needs `ci-extended` and the HeliosSoftware owner guard. Both bulk-submit worker setters, Redis, SQLite `:in` `INDEX_OR_CHUNK` 250, durable outbox, BCP 47, ECL/FTS, and item 4 DELETE-204 survived. `helios-auth` and `helios-observability` libraries changed. After this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, `helios-observability`, and `helios-fhir-validator` pins together to it. HIS ICD loaders already pass `?defer_closure=true` on non-final chunks and poll `finalize=true` until a 202 `pending` clears. Prior: 19 Oct `d3c803037`, tag `pre-merge-main-2026-10-19`.

19 Oct 2026: `main` `a8297ab56` (Helios `d3976b4bd` / 0.2.5; `#2103` ES search-secondary mirrors the primary version, `#2119` ValueSet `$validate-code` versioned canonical, `#2125` hierarchical `$expand` cycle guard and `/import?finalize=true` closures, plus smunini follow-ups `#2127`–`#2135`; tip is the 0.2.5 release) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-19` → `d8fc0faad`. Ledger unchanged SQLite 43 / Postgres 53 (no inbound persistence schema steps). Workspace is now 0.2.5. Conflicts (16): take Helios `nest_expansion` for sqlite/postgres `value_set.rs` (fork-doc item 2) and Helios `/import` (`import_bundle.rs`, `fhir_bundle.rs`, `?defer_closure=true`, 202 `closures_rebuild: "pending"`); keep `code_system_concept_codes` for RF2 `--extends`. Drop Helios flat `r6.rs`. Cargo pins move to 0.2.5 and keep `helios-terminology-client` plus `helios-auth` `features = ["redis"]`. ES `#2103` takes Helios `is_new = version == "1"` and `write_synced_entries_page`. Fork-doc items 2 and 3 are now upstream. Both bulk-submit worker setters, Redis, SQLite `:in` `INDEX_OR_CHUNK` 250, durable outbox, BCP 47, ECL/FTS, and item 4 DELETE-204 survived. No `helios-fhir` / `helios-auth` / `helios-observability` library change this wave (`helios-fhir-validator` `terminology.rs` did). After this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, `helios-observability`, and `helios-fhir-validator` pins together to it. HIS ICD loaders still need `?defer_closure=true` on non-final chunks and must poll `finalize=true` until a 202 `pending` clears. Prior: 18 Oct `0c325b252`, tag `pre-merge-main-2026-10-18`.

18 Oct 2026: `main` `c71593cab` (Helios `ead2ab103` / 0.2.4; `#1824` SoF ValueSet/ConceptMap SQL tables and `#2107` dashboard history index; tip `#2118` is a Mongo concurrent-repair test) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-18` → `f1b40050a`. Ledger SQLite 43 / Postgres 53. Workspace stays 0.2.4. Conflicts (7): named SQLite ledger kept — insert `history_updated_index` (Helios v40, `#2107`) and `sof_terminology_tables` (Helios v41, `#1824`) immediately after `search_index_date_start` and before `OUTBOX_DEAD_LETTER_STEP`; discard integer `migrate_schema`; `migrate_v39_to_v40` / `migrate_v40_to_v41` do not stamp 40/41. `#1961` logging still wraps the named loop. SQLite/composite keep both `subscription_outbox_store` and `sof_terminology_store`. HTS keeps the ICD dotted-code `is-a` fast path and takes Helios `#2114` `evaluate_hierarchy` for remaining hierarchy ops. REST SoF `$expand` paging and ConceptMap lookup go through `helios_terminology_client`. UI `$expand` keeps the terminology-client path and reuses Helios's shared reqwest client (`#1019`). Cargo.lock kept from feat, then `zip` moved to 9.0.5 (RUSTSEC-2026-0367/0368/0369). Zip 9 `name()` Result and `SimpleFileOptions` taken on the leftover SNOMED importer path. Both bulk-submit worker setters, Redis on `helios-auth`, SQLite `:in` `INDEX_OR_CHUNK` 250, and the durable outbox (`enqueue_resource_event` when attached) survived. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. `helios-observability` library changed (`dashboard_counters.rs`). After this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, `helios-observability`, and `helios-fhir-validator` pins together to it. Prior: 16 Oct `35111b511`, tag `pre-merge-main-2026-10-16`.

16 Oct 2026: `main` `a92079ce8` (Helios `42cd0985b` / 0.2.4; `#2064` refuse an implicit default FHIR version on non-R4 builds, `#2020` return heap after a large Bundle, `#2048` SoF 503 on an unavailable store, `#2063` shared S3 read limit; tip `#2082` is Postgres reindex type listing) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-16` → `1d01d5683`. Ledger unchanged SQLite 41 / Postgres 53 (no inbound SQLite/Postgres schema steps). Workspace stays 0.2.4. Conflict (1): `hfs` keeps `info!("starting HFS server")` and `effective_ui_enabled`, and takes `try_from_env_with_provenance` so `#2064` can refuse an implicit default. Both bulk-submit worker setters survived. Redis on `helios-auth`, SQLite `:in` `INDEX_OR_CHUNK` 250, and the durable outbox (`enqueue_resource_event` when attached) survived. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. No `helios-fhir` / `helios-auth` / `helios-observability` library change this wave. HIS pins stay on `272e4d60b`. Prior: 15 Oct `7e85667b3`, tag `pre-merge-main-2026-10-15`.

15 Oct 2026: `main` `1f71f5765` (Helios `8997ddf66` / 0.2.4; `#2025` bounded SQLite WAL, `#1955` value-index search, tip `#2057` is a Mongo test flake) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-15` → `085130c54`. Ledger unchanged SQLite 41 / Postgres 53 (no inbound schema steps). Workspace stays 0.2.4. Conflicts (9): `Cargo.lock` kept from feat, then `rustls` moved to 0.23.46. `sqlite/mod.rs` exports both `SqliteSubscriptionOutbox` and `WalMaintenance`. Subscriptions take Helios's bounded scheduler and tenant-scoped topics. The durable outbox stays: `on_write` calls `enqueue_resource_event` when an outbox is attached (wake only; the row is already in the resource transaction) and `observe_write` otherwise. Heartbeat delivery and `fhirPathCriteria` evaluation stay. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. No `helios-fhir` / `helios-auth` / `helios-observability` library change this wave. HIS pins are still `8676db5cd`; after this commit is pushed, move all three together to it so the unpinned 14 Oct `helios-auth` change lands in one move. Prior: 14 Oct `085130c54`, tag `pre-merge-main-2026-10-14`.

14 Oct 2026: `main` `c172a77fb` (Helios `4eff68803` / 0.2.4; `#1618` patient-compartment grant and `#1506` date-start index; tip `#2016` is CI only) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-14` → `8676db5cd`. Ledger SQLite 41 / Postgres 53. Workspace stays 0.2.4. Conflicts (14): named SQLite ledger kept — insert `search_index_date_start` (Helios v39, `#1506`) immediately after `search_index_composite_slot` and before `OUTBOX_DEAD_LETTER_STEP`; discard integer `migrate_schema`; `migrate_v38_to_v39` does not stamp 39. Postgres v43 date-end rebuild is taken in place (autocommit, ctid batches, `CREATE INDEX CONCURRENTLY`) and does not stamp 43 or add a step. `#1961` logging still wraps the named loop. Auth skill map keeps JTI revocation and takes named-operation scopes. Discovery keeps the fork `scopes_supported` list. REST keeps `helios-auth` `features = ["redis"]` and adds `parking_lot` (`#1618`). Patient-compartment checks use `routed_path` and audit events use `audit_agent_identity`. SQLite `:in` membership stays chunked at `INDEX_OR_CHUNK` 250 on the new `ParameterCondition` API. `hfs` keeps `reindex_enabled` and `effective_ui_enabled`, and both `with_ingest_validator` and `with_independent_file_tasks`. HTS `$validate-code` takes VCL. FHIRPath takes `leading_type_matches` for abstract bases. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. After this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, and `helios-observability` pins together to it (`helios-auth` library changed). If the 13 Oct pin move to `8676db5cd` has not landed yet, move once to this commit. Prior: 13 Oct `8676db5cd`, tag `pre-merge-main-2026-10-13`.

13 Oct 2026: `main` `c0a873d4f` (Helios `cf1af0544` / 0.2.4, `#1973` / `#1341` native SQL chain builders removed) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-13` → `b6a380a79`. Ledger unchanged SQLite 40 / Postgres 53 (no inbound schema steps). Workspace stays 0.2.4. Conflicts (10): named SQLite ledger kept — `#1961` logs an existing-database migration around the named step loop and does not restore integer `migrate_schema`. Composite storage drops `ChainedSearchProvider` (`#1341`) and keeps `resolve_includes_iterative`. Postgres module keeps `SCHEMA_VERSION` and `PostgresSubscriptionOutbox` and takes `PoolStatus` (`#1907`). Observability keeps `propagation` and takes `pool_metrics`. FHIRPath choice suffixes must be real FHIR types. A `<url>#A.b` content reference names that canonical's schema. Terminology does not judge a core value set pinned to another release; `CoreTerminology::member` stays public. Persistence README keeps the fork backend list and takes `HFS_S3_TENANCY=bucket-per-tenant` (`#1598`). Inbound also covers S3 `_history` paging (`#1356`), a one-pass SQLite `_has` hop (`#1930`), reindex prefetch (`#1250`), and paged PostgreSQL view export. No inbound HTS schema. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. After this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, and `helios-observability` pins together to it (`helios-observability` library changed). Prior: 12 Oct `b6a380a79`, tag `pre-merge-main-2026-10-12`.

12 Oct 2026: `main` `b548ad244` (Helios `21927cf57` / 0.2.4, `#1911` Postgres live-tenant count index) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-12` → `271040354`. Ledger SQLite 40 / Postgres 53. Workspace stays 0.2.4. Conflict (1): named ledger kept — insert Postgres `resources_live_tenant` (`#1911`, Helios v49, `idx_resources_live_tenant`, built `CONCURRENTLY`) **immediately after** `search_index_composite_slot` and **before** fork-only slot-2; discard integer `migrate_schema`; `migrate_v48_to_v49` does not stamp integer 49. Inbound also covers admin tenant listings via discovery (`#1912`/`#1913`), MongoDB tenant counts from an index (`#1910`), and the PostgreSQL `count_by_tenant` statement budget (`#1911`). No inbound HTS / fhir / auth / observability. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins unchanged this wave. Prior: 11 Oct `271040354`, tag `pre-merge-main-2026-10-11`.

11 Oct 2026: `main` `e810791e8` (Helios `e3f654542` / 0.2.4, `#1886` / `#1407` composite search slots) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-11` → `843136367`. Ledger SQLite 40 / Postgres 52. Workspace stays 0.2.4. Conflicts (6): named ledger kept — insert `search_index_composite_slot` (Helios sqlite v38 / postgres v48, `#1407`) immediately after the last Helios-aligned step and before fork-only dead-letter (sqlite) or slot-2 (postgres); discard integer `migrate_schema`; `migrate_v47_to_v48` does not stamp integer 48. `core/mod.rs` keeps `subscription_outbox` and takes `tenant_discovery`. JWKS keeps JTI revocation and takes Entra `roles`/`scp` scope parsing (`#724`). FHIRPath stays on `helios_terminology_client` and classifies transient HTTP failures so they are not cached (`#1847`). Inbound also covers S3 tenant discovery (`#1672`), SQLite work off the async runtime (`#1827`/`#1930`), `count_by_tenant` budgets (`#1828`), and the tenants inventory (`#1850`/`#1851`). No inbound HTS schema. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. After this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, and `helios-observability` pins together to it (`helios-auth` and `helios-observability` libraries changed). Prior: 8 Oct `843136367`, tag `pre-merge-main-2026-10-08`.

8 Oct 2026: `main` `4d8391b62` (Helios `a5e74a360` / 0.2.4, `#1121` SMART patient launch context) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-08` → `16586dac3`. Ledger unchanged SQLite 39 / Postgres 51 (no inbound schema steps). Workspace stays 0.2.4. Conflicts (22): keep `#[non_exhaustive]`, `Principal::stub` / `with_fhir_user`, top-level `fhir_user`, and JTI revocation (3-arg provider). Take Helios `LaunchContext` (`with_launch_context`, claim names, Patient/$everything narrowed to the launch patient). Outside-crate `Principal { ..Default::default() }` literals become `stub()` chains. Inbound also covers pooled Postgres connect retry (`#1793`), SQL-export cancel stopping the S3 scan (`#1823`), SQL query graph walk `max_vds` (`#1804`), standalone S3 conformance seed (`#1838`), composite arity mismatch as 400, and MongoDB 500 too-costly when a Bundle overflows the cache. No inbound HTS schema. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. After this commit is pushed, move the HIS `helios-fhir`, `helios-auth`, and `helios-observability` pins together to it (`helios-auth` library changed). Prior: 7 Oct `16586dac3`, tag `pre-merge-main-2026-10-07`.

7 Oct 2026: `main` `085fc5a33` (Helios `69ab711c6` / 0.2.4, `#1806` MongoDB Bundle gate weights and deadline) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-07` → `99b98d1ca`. Ledger SQLite 39 / Postgres 51. Workspace stays 0.2.4. Conflicts (2): named ledger kept — insert Postgres `resources_reindex_id` (`#1767`, Helios v47, `idx_resources_reindex_id`) **immediately after** `bulk_manifest_file_progress_completed` and **before** fork-only slot-2; discard integer `migrate_schema`; `migrate_v46_to_v47` does not stamp integer 47. `export/in_memory.rs` takes Helios's cancel checkpoints and generic server-fault text (`#1703`/`#1704`). Inbound also covers id-range `$reindex` (`#1739`), a read-only SQL authorizer (`#1799`), and FHIRPath terminology-call caps. No inbound HTS / fhir / auth / observability. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins unchanged this wave. Prior: 5 Oct `99b98d1ca`, tag `pre-merge-main-2026-10-05`.

5 Oct 2026: `main` `cc8b074a7` (Helios `9875f7bb2` / 0.2.4, `#1764` MongoDB SearchParameter reload per tenant) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-05` → `ebbda93cb`. Ledger unchanged SQLite 39 / Postgres 50 (no inbound schema steps). Workspace stays 0.2.4. Conflict (1): `core/mod.rs` re-exports keep feat `IngestValidator` and take Helios `ReceiptListingCursor`. Inbound also covers transaction Bundle lock planning instead of the exclusive tenant gate (`#1637`), the patient-export index on search-offloaded PostgreSQL (`#1663`), unindexed bulk-submit resources reported as warnings when reindex is deferred (`#1666`), `_tag`/`_profile`/`_security`/`_source` sort (`#1711`), and `hasValue()` false for complex types. No inbound HTS. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins unchanged this wave (`helios-auth` gained a test only). Prior: 3 Oct `ebbda93cb`, tag `pre-merge-main-2026-10-03`.

3 Oct 2026: `main` `3d18fcea8` (Helios `acab4363d` / 0.2.4, `#1636`/`#1637` transaction begin answers 503) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-03` → `ccb66f8ed`. Ledger unchanged SQLite 39 / Postgres 50 (no inbound schema steps). Workspace stays 0.2.4. Conflict (1): `postgres_tests.rs` keeps the fork subscription-outbox helpers and isolated-schema tests and takes Helios's gate-timeout test (`postgres_integration_transaction_bundle_gate_timeout_is_transient`). Inbound also covers MongoDB `_id` as the last `_sort` key (`#1693`) and cursor reads instead of `distinct` (`#1697`), plus the SQL-export job-id copy staying available after polling (`#1645`). No inbound HTS / fhir / auth / observability. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins unchanged this wave. Prior: 2 Oct `ccb66f8ed`, tag `pre-merge-main-2026-10-02`.

2 Oct 2026: `main` `810ff5873` (Helios `bcd7b84fb` / 0.2.4, `#859` conditional URLs inside transactions) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-02` → `10ed47b9a`. Ledger unchanged SQLite 39 / Postgres 50 (no inbound schema steps). Workspace version is now 0.2.4. Conflicts (6): Cargo pins move to 0.2.4 and keep `helios-terminology-client` plus `helios-auth` `features = ["redis"]`. `batch.rs` takes Helios `transaction_conditional_interaction` and `transaction_failure_description`, and keeps `bundle_patch_format`, `ConditionalPatchResult`, and `is_unsupported_capability`. The transaction pre-check lets a type-level DELETE or PATCH that already has criteria through (`#859`); instance DELETE of a missing resource still fails the bundle. Composite storage keeps `BundleEntryEffect` and `resolve_includes_iterative`. HTS inbound is `map_err` only. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins: bump all three (`helios-fhir`, `helios-auth`, `helios-observability`) to this merge SHA after push — `helios-fhir` Cargo.toml moved to 0.2.4. Prior: 1 Oct `10ed47b9a`, tag `pre-merge-main-2026-10-01`.

1 Oct 2026: `main` `6179b9302` (Helios `e063ef8ab` / 0.2.3, `#1627` scope `$reindex` `clearExisting`) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-10-01` → `871c87c0d`. Ledger SQLite 39 / Postgres 50. Workspace still 0.2.3. Conflicts (5): named ledger kept — insert SQLite `bulk_manifest_file_progress_completed` (`#1610`, Helios v37) and Postgres `patient_export_references` (Helios v45) plus the same completed step (Helios v46) **immediately after** `login_sessions` and **before** fork-only slot-2/phase/dead-letter; discard integer `migrate_schema`; `migrate_v44_to_v45` does not stamp integer 45. `postgres_tests` keeps the subscription-env guard and Helios's quoted parameter-name test. Persistence README keeps the fork backend list and takes Helios's S3 tenancy sentence. `fhir-benchmark.yml` keeps the `HeliosSoftware` owner guard and `needs: setup`. List-export `_since` uses `parse_instant_param`. The unmatched-path test builds `Principal::stub`. No inbound HTS. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins: bump all three (`helios-fhir`, `helios-auth`, `helios-observability`) to this merge SHA after push — `helios-fhir` search loader changed. Prior: 28 Sep `871c87c0d`, tag `pre-merge-main-2026-09-28`.

28 Sep 2026: `main` `36ebab2fc` (Helios `0eb99c238` / 0.2.3, `#1538` release export/submit leases on shutdown) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-09-28` → `24964e3bb`. Ledger unchanged SQLite 38 / Postgres 48 (no inbound schema steps). Workspace still 0.2.3. Conflicts (2): `hfs` `main.rs` keeps feat `with_ingest_validator` and Helios `pools.spawn` / `with_shutdown` (`#1531`); `.github/workflows/ci.yml` drops `runs-on` on the reusable `deploy` job, takes `needs: [release, deploy-hts]`, and keeps `github.repository_owner == 'HeliosSoftware'` on both `deploy` and the new `deploy-hts` job (`#1534`). No inbound HTS / fhir / auth / observability. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins unchanged this wave. Prior: 27 Sep `24964e3bb`, tag `pre-merge-main-2026-09-27`.

27 Sep 2026: `main` `e77262e4b` (Helios `654d65cc8` / 0.2.3, `#1541` REST README Bundle section) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-09-27` → `1ff4f6e83`. Ledger unchanged SQLite 38 / Postgres 48 (no inbound schema steps). Workspace still 0.2.3. Conflict (1): `crates/rest/README.md` keeps feat JSON-array and merge-object Bundle PATCH, takes Helios Parameters/Binary notes and the R4/R4B Binary `501` row, and records that a transaction resolves conditional references (`#459`) while a batch leaves them. Inbound: MongoDB `$reindex` id-order walk, byte cap, and writer overlap (`#1403`, `#1499`); Elasticsearch dashboard counts when S3 cannot; S3 identifier-scoped conditional create; bulk-export Patient-compartment membership from `CompartmentDefinition`; HTS search `Bundle.total` and `_summary=count`; SIGINT/SIGTERM drain and OTLP flush. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins: bump all three (`helios-fhir`, `helios-auth`, `helios-observability`) to this merge SHA after push — `helios-observability` gained shutdown flush. Prior: 26 Sep `12216c82e`, tag `pre-merge-main-2026-09-26`.

26 Sep 2026: `main` `96b29a2de` (Helios `8a9303510` / 0.2.3, `#1507` bulk-export `meta.lastUpdated`) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-09-26` → `0c5d2a129`. Ledger SQLite 38 / Postgres 48. Workspace still 0.2.3. Conflicts (10): named ledger kept — insert Helios `search_index_date_range` (`#1391`) and `login_sessions` (`#1481`) **immediately after** `search_index_reference_target_id` / sqlite `secondary_sync_failures` and **before** fork-only slot-2/phase/dead-letter; discard integer `migrate_schema`; `migrate_v42_to_v43` and `migrate_v43_to_v44` do not stamp integers 43/44. Bundle PATCH takes Helios `#1393` validation and real FHIRPath (`apply_patch_for_version`) and keeps feat JSON-array and merge-object documents (`bundle_patch_format`). Refused-methods fixture stays `["not-supported", "value", "required"]`. Conditional PATCH falls back to `conditional_patch` when `prepare_conditional_patch` reports an unsupported capability. Storage transaction PATCH uses `prepare_bundle_patch` (AuditEvent 405, candidate validator, same-TX outbox enqueue unchanged). HTS inbound is test-container cleanup only; ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins: bump all three (`helios-fhir`, `helios-auth`, `helios-observability`) to this merge SHA after push — `helios-auth` session storage changed. Prior: 24 Sep `f8a5e0b0f`, tag `pre-merge-main-2026-09-24`.

24 Sep 2026: `main` `57f0eeed1` (Helios `a90564c9e` / 0.2.3, `#1487` postgres reindex groups) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-09-24` → `bdc064137`. Ledger unchanged SQLite 36 / Postgres 46 (no inbound schema steps). Workspace still 0.2.3. Conflicts (5): `hfs` `main.rs` keeps feat `with_ingest_validator` and Helios `with_independent_file_tasks`; `bulk_submit_worker` keeps both `ingest_validator` and `owned_file_executor`; postgres `search/writer.rs` takes Helios SQL-cause mapping (`#1463`); bundle `transaction.rs` delete keeps Helios search/FTS deletes and feat `maybe_enqueue_on_client`. Postgres `storage.rs` takes Helios guarded advisory-lock transactions (`#1460`–`#1487`) and re-inserts `maybe_enqueue_on_client` on that transaction (`insert_on_client` is generic over `GenericClient`). Delete reads the tombstone body for the outbox only when `HFS_SUBSCRIPTIONS_ENABLED` is on. No inbound HTS / fhir-gen / auth / observability. HTS ICD, BCP 47, ECL/FTS, and IG Publisher fork docs survived. HIS pins unchanged this wave (`helios-fhir`, `helios-auth`, `helios-observability` not in the diff). Prior: 23 Sep `d0accdef4`, tag `pre-merge-main-2026-09-23`.

23 Sep 2026: `main` `5bc35f320` (Helios `4ea298227` / 0.2.3, `#1469` postgres reindex byte limit) → `feat-clinical-reasoning`. Rollback tag `pre-merge-main-2026-09-23` → `68c34fd21`. Ledger SQLite 36 / Postgres 46. Workspace still 0.2.3. Conflicts (13): named ledger kept — insert Helios sqlite `secondary_sync_failures` (`#1334`) and postgres `secondary_sync_failures` + `search_index_reference_target_id` (`#1414`) **immediately after** `bulk_export_jobs_attempts` and **before** fork-only slot-2/phase/dead-letter; discard integer `migrate_schema`; `migrate_v41_to_v42` does not stamp integer 42. Helios `#1406` shared `apply_patch` taken (instance FHIRPath stays 501); feat JSON/merge Bundle PATCH kept (`["not-supported", "value", "required"]`). Composite dropped feat `conditional_patch` override (trait default syncs via composite `update`) and kept `conditional_matches` for Bundle PATCH. Auth keeps Redis JTI revocation **and** Helios PKCE `session` re-exports; inbound UI tests use `Principal::stub`. SQLite/Postgres `soft_delete` keeps `maybe_enqueue_on_conn`. No inbound HTS / flat `r*.rs` / fhir-gen. ES `:in` per-system terms, token-OR `INDEX_OR_CHUNK`, `:in` `$expand` paging, HTS ICD items 1–5 and BCP 47 fork docs survived. HIS pins: bump `helios-auth` (PKCE session + `#[non_exhaustive]` Principal) and `helios-observability` (`composite_metrics`) after this merge is pushed; `helios-fhir` unchanged. Prior: 21 Sep `eace579d0`, tag `pre-merge-main-2026-09-21`.

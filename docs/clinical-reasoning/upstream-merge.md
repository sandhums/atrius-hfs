# Merging Helios `main` into Atrius feature branches

This guide describes how to keep **Atrius clinical-reasoning feature branches** aligned with [Helios HFS](https://github.com/HeliosSoftware/hfs) without repeating the painful merge we hit when `feat-clinical-reasoning` fell ~250 commits behind upstream.

Use it whenever you sync `upstream/main` → your fork `main` → `feat-clinical-reasoning` (or related feat branches).

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
cargo build -p cds-server -p cr-fhir-bridge -p atrius-clinical-reasoning -p atrius-runtime-mapper
cargo test -p fhir-validation --lib
cargo test -p helios-rest --test profile_validation_integration
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
crates/atrius-clinical-reasoning/
crates/atrius-runtime-mapper/
crates/cds-server/
crates/cr-fhir-bridge/
crates/fhir-validation/
crates/fhir-validation-gen/
crates/fhir-validation-types/
crates/fhir-valueset-gen/
docs/clinical-reasoning/
docs/his/
manifests/
deploy/
scripts/activity-definition-apply-smoke.sh
scripts/cds-*.sh
scripts/ecqm_cds_common.py
scripts/generate-*.py
scripts/import-*.py
scripts/run-kr-hfs.sh
scripts/setup-plandefinition-cds-catalog.sh
scripts/validate-manifest-kr-pins.sh
data/clinical-reasoning/
```

**Do not commit:** `org/opencds/**/*.class` (JVM bytecode belongs in the sidecar JAR, not git).

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
crates/hfs/          # v2 loads profile validation inside helios-rest build_app
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

| File | Atrius change |
|------|----------------|
| `src/profile_validation.rs` | **New file** — NDHM/ABDM profile validation service |
| `src/handlers/validate.rs` | **New file** — `$validate` handler |
| `src/config.rs` | `ProfileValidationMode`, `HFS_PROFILE_MANIFEST`, `HFS_PROFILE_VALIDATION_MODE`, `HFS_PROFILE_VALIDATION_ADDONS` |
| `src/state.rs` | `profile_validation` field, `with_profile_validation`, `enforce_profile_on_write` |
| `src/lib.rs` | `pub mod profile_validation`, load manifest in `build_app`, re-exports |
| `src/error.rs` | `RestError::ValidationOutcome { outcome }` + `into_response` arm |
| `src/routing/fhir_routes.rs` | Routes for `/{type}/$validate` and `/{type}/{id}/$validate` |
| `src/handlers/mod.rs` | `pub mod validate` |
| `src/handlers/create.rs` | `state.enforce_profile_on_write(...)` before persist |
| `src/handlers/update.rs` | same |
| `src/handlers/patch.rs` | same |
| `src/handlers/batch.rs` | same + `batch_validation_error_result` |
| `tests/profile_validation_integration.rs` | Integration tests (config-driven; no extra arg to `create_app_with_auth`) |
| `Cargo.toml` | `fhir-validation` dependency + feature passthrough on R4/R4B/R5/R6 |

### `crates/fhir`

| File | Atrius change |
|------|----------------|
| `src/error.rs` | **New file** — `TerminologyValidationError` |
| `src/lib.rs` | `mod error; pub use error::TerminologyValidationError;` |
| `src/r4.rs` (and `r4b.rs`, `r5.rs`, `r6.rs`) | Append: `pub mod terminology;` |
| `src/r4/terminology/**` (etc.) | **Generated** ValueSet/CodeSystem binding tables (~1k files per version) — keep Atrius copy |

When merging Helios updates to `r4.rs`, **take Helios generated body**, then re-add the single line `pub mod terminology;` at the end.

### `crates/fhirpath`

| File | Atrius change |
|------|----------------|
| `src/lib.rs` | `pub mod terminology_client;` |
| `src/terminology_client.rs` | `validate_code_with_parameters`, local ValueSet URL routing |
| `src/handlers.rs` | `pub fn json_value_to_evaluation_result` |
| `src/evaluator.rs` | `pub fn convert_resource_to_result` |

Merge carefully — do **not** replace the whole file with an old feat copy (you will lose `resolve()` and resource visibility fixes from Helios).

### `crates/fhir-gen`

| File | Atrius change |
|------|----------------|
| `src/lib.rs` | `pub fn make_rust_safe` (used by `fhir-validation-gen`) |

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

### Root workspace

| File | Atrius change |
|------|----------------|
| `Cargo.toml` | Add Atrius crates to `default-members` (keep `version = "0.2.0"` and Helios serde pins from `main`) |

**Note:** Profile validation is wired in `helios-rest` `build_app()` from `ServerConfig` — **`crates/hfs/src/main.rs` does not need Atrius patches** in the v2 layout.

---

## Conflict resolution cheat sheet

| Conflict location | Resolution |
|-------------------|------------|
| `crates/fhir/src/r4.rs` (huge generated file) | Take **`main`**, re-add `pub mod terminology;` at EOF |
| `crates/fhir/src/r*/terminology/**` | Keep **Atrius** tree |
| `crates/fhir/src/error.rs` | Keep **Atrius** |
| `crates/rest/**` except profile files | Take **`main`**, re-apply [REST touch points](#cratesrest) |
| `crates/persistence/**`, `crates/sof/**` | Take **`main`** |
| `crates/hfs/**` | Take **`main`** |
| `crates/cds-hooks/**` | **Merge** — keep measurement period + BadGateway |
| `Cargo.toml` | Take **`main`** pins/version, add Atrius `default-members` |
| `Cargo.lock` | Do not hand-merge — fix `Cargo.toml`, run `cargo build` |
| `.gitattributes` | Keep fork rule: `helios_fhir/** merge=ours` |
| `.gitignore` | Keep fork entries (e.g. `/crates/fhir/tests/`) |

---

## Rebuild procedure (when merge is too painful)

Use when `HEAD..origin/main` is large (e.g. **> 80 commits**) or merges re-delete bulk-submit / SOF / HTS fixes.

```bash
git fetch origin upstream
git checkout origin/main -b feat-clinical-reasoning-v2

# 1. Copy Atrius-only trees from the old branch
git checkout origin/feat-clinical-reasoning -- \
  crates/atrius-clinical-reasoning \
  crates/atrius-runtime-mapper \
  crates/cds-server \
  crates/cr-fhir-bridge \
  crates/fhir-validation \
  crates/fhir-validation-gen \
  crates/fhir-validation-types \
  crates/fhir-valueset-gen \
  docs/clinical-reasoning \
  docs/his \
  manifests \
  data/clinical-reasoning \
  deploy \
  scripts/activity-definition-apply-smoke.sh \
  scripts/cds-cms165-prefetch-smoke.sh \
  scripts/cds-er-chest-pain-smoke.sh \
  scripts/ecqm_cds_common.py \
  scripts/generate-atrius-mapper-manifest.py \
  scripts/generate-cds-hooks-manifest.py \
  scripts/generate-ecqm-plandefinitions.py \
  scripts/import-cms165-demo.py \
  scripts/import-ecqm-kr-libraries.py \
  scripts/import-synthea-atrius.py \
  scripts/run-kr-hfs.sh \
  scripts/setup-plandefinition-cds-catalog.sh \
  scripts/validate-manifest-kr-pins.sh

# 2. Copy new REST validation files
git checkout origin/feat-clinical-reasoning -- \
  crates/rest/src/profile_validation.rs \
  crates/rest/src/handlers/validate.rs \
  crates/rest/tests/profile_validation_integration.rs

# 3. Copy generated terminology + fhir error (if not already present)
git checkout origin/feat-clinical-reasoning -- \
  crates/fhir/src/error.rs \
  crates/fhir/src/r4/terminology \
  crates/fhir/src/r4b/terminology \
  crates/fhir/src/r5/terminology \
  crates/fhir/src/r6/terminology

# 4. Re-port surgical shared-crate patches (see tables above) onto current main files
#    — or diff against feat-clinical-reasoning-v2 from a prior successful rebuild.

# 5. Build and test
cargo build
cargo test -p fhir-validation --lib
cargo test -p helios-rest --test profile_validation_integration

# 6. When validated, rename branches
git branch -m feat-clinical-reasoning feat-clinical-reasoning-archive
git branch -m feat-clinical-reasoning-v2 feat-clinical-reasoning
```

---

## `.gitattributes` (fork hygiene on `main`)

On `main` you should keep:

```gitattributes
helios_fhir/** merge=ours
```

Optional (only if merges keep clobbering generated Atrius trees):

```gitattributes
crates/fhir/src/r4/terminology/** merge=ours
crates/fhir-validation-gen/generated/** merge=ours
```

Use `merge=ours` only for **generated or fork-owned** paths — not for hand-written integration in `rest/` or `cds-hooks/`.

---

## Splitting work across branches (optional)

To reduce merge conflict surface:

| Branch | Scope |
|--------|--------|
| `feat/fhir-validation` | `fhir-validation*`, `fhir-valueset-gen`, fhir `terminology/` trees |
| `feat/cds-stack` | `cds-server`, `cr-fhir-bridge`, `atrius-clinical-reasoning`, `atrius-runtime-mapper` |
| `feat/clinical-reasoning` | REST/HFS integration, docs, manifests, smokes |

Merge order: `main` → validation → cds-stack → clinical-reasoning integration.

---

## Pre-merge checklist

**Before merging `main` into feat:**

- [ ] `origin/main` is up to date with `upstream/main`
- [ ] Clean working tree on feature branch
- [ ] Checked commits behind: `git log --oneline HEAD..origin/main | wc -l`
- [ ] If behind > 80, scheduled a rebuild instead of merge

**After merge:**

- [ ] `cargo build` (default workspace)
- [ ] `cargo build -p cds-server -p cr-fhir-bridge`
- [ ] `cargo test -p fhir-validation --lib`
- [ ] `cargo test -p helios-rest --test profile_validation_integration`
- [ ] No accidental deletion of `handlers/sof/`, `bulk_submit`, or `persistence/src/sof/`

---

## Anti-patterns (what went wrong before)

1. **Merging `upstream/main` directly into feat** — skips fork hygiene; doubles conflict noise.
2. **Letting feat sit months behind** — forces rebuild; old feat deleted bulk-submit and SOF handlers.
3. **Replacing entire `rest/` or `hfs/` from old feat** — regresses Helios 0.2.0 features.
4. **Hand-editing `Cargo.lock`** — regenerate via `cargo build`.
5. **Committing `org/*.class` or local terminology zips** — huge diffs and merge noise.

---

## Related docs

- [Clinical reasoning stack overview](./README.md)
- [Startup guide](./startup-guide.md) — local stack and smoke scripts
- [Troubleshooting](./troubleshooting.md) — runtime issues after sync

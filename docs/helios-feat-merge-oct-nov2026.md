# Helios follow-on sync: main → feat-clinical-reasoning (Oct–Nov 2026)

Paste-ready addendum for *HELIOS vs Atrius.docx*. Three consecutive feat syncs after
the 29 Oct entry. Authoritative detail and test counts live in
`docs/clinical-reasoning/upstream-merge.md` (*Last feat sync*).

**Direction:** `upstream/main` → atrius `main` → merge `origin/main` into
`feat-clinical-reasoning`. Rollback: `pre-merge-main-YYYY-MM-DD` tags on feat before
each merge.

**Still true on the keep-list (unchanged across these three syncs):**

- Single validation engine: `helios-fhir-validator` via `ValidationService` +
  `HFS_FHIR_PACKAGES` overlay — no `fhir-validation*` / dual validators.
- Named `schema_migrations` ledger (not integer `migrate_schema` replay on feat).
- Atrius-only: `cds-server`, clinical-reasoning docs/manifests, durable subscription
  **outbox** + heartbeat, FHIRPath **`fhirPathCriteria`**, JTI revocation
  (`helios-auth` **`features = ["redis"]`**), `AuditEventBuilder` **`id` /
  `subtype` / `extension`**, bulk export **`GET /export-jobs`**, REST/UI
  **`helios-terminology-client`** (SoF expand, editor flows — not necessarily inside
  `helios-fhirpath` after 0.2.7).
- FHIR model layout: **`crates/fhir/src/r4/`** and **`crates/fhir/src/r6/`**
  directory trees — do **not** take Helios flat `r4.rs` / `r6.rs` beside `mod.rs`.

---

## Sync 1 — 30 Oct 2026

| Ref | Value |
|-----|--------|
| atrius `main` | `cf0cdfd44` (Helios `d1c9bb09d`, workspace **0.2.6**) |
| Rollback tag | `pre-merge-main-2026-10-30` → `a519b0268` |
| Merge commit | `7cd7b181e` |
| Distance | 73 commits on `main` / **39** non-merge since `f892949fe` |

### Helios themes adopted

- `#2280` bulk-submit expired-submission purge, paged sweep,
  `idx_bulk_submissions(updated_at)` (new named step **`bulk_submissions_updated_at`**
  before fork dead-letter / slot-2).
- `#2277` composite write only after primary commit.
- `#2262` MongoDB composite `_id` page windows; `#2260` compartment probe key;
  `#2303` / `#2301` MongoDB 503 without driver text.
- `#2045` patient-compartment Mongo tests; SMART / `Principal::stub` test hygiene.
- Deploy CI **`Deploy`** runner label; HTS/auth config doc tweaks.

### Schema ledger

- SQLite tip **49**, Postgres tip **58** (one new Helios index step each backend).

### Conflicts resolved

| Location | Decision |
|----------|----------|
| `sqlite/schema.rs`, `postgres/schema.rs` | Keep named ledger; insert Helios `#2280` step; discard integer `migrate_schema`; bump tips. |
| (follow-up commit) | Replay tests: `unrecord_steps_completing_after`; purge test excludes fork `subscription_outbox` + `login_sessions`; reindex tenant test uses `Principal::stub`. |

### HIS / pins

- **`helios-auth`** changed — move HIS `helios-fhir`, `helios-auth`,
  `helios-observability`, `helios-fhir-validator` pins together to merge SHA when
  feat is pushed.

### Verification (recorded)

`cargo fmt` 0; ledger tests 39; postgres check 0; hfs check 0; validator **161**;
rest **2203**; persistence lib **1840**; hts **1066**; cds **104**.

---

## Sync 2 — 31 Oct 2026

| Ref | Value |
|-----|--------|
| atrius `main` | `86371cdab` (Helios `8c4eb74e7`, workspace **0.2.6**) |
| Rollback tag | `pre-merge-main-2026-10-31` → `67d01c4c0` |
| Merge commit | `3fa7eda29` |
| Distance | 148 on `main` / **88** non-merge since `cf0cdfd44` (merged, not rebuilt) |

### Helios themes adopted

- `#2365` / `#2367` refuse DBs **newer** than binary `SCHEMA_VERSION`.
- `#1515` audit **tenant** `meta.tag` + full **`AuditAgent`** identity in REST
  (`crates/rest/src/audit.rs`, `.principal()` on `AuditEventBuilder`).
- `#2382` / `#2402` / `#2398` instance **history `_cursor`** paging, cursor validation,
  Postgres bigint/date bindings.
- `#2285` MongoDB `:missing` complement caps; `#2308` MongoDB transactional hello /
  cache-write topology; `#2342` WriteConflict replay on Mongo update/delete.
- `#2381` history search strictness, `_count` saturation; `#2404` testcontainers
  namespace sweep; bulk-submit receipt docs `#2385`.

### Schema ledger

- Tips unchanged **49** / **58** — ported newer-schema **guards** only; no new named
  migration steps.

### Conflicts resolved

| Location | Decision |
|----------|----------|
| `sqlite/schema.rs`, `postgres/schema.rs` | Named ledger + Helios `require_supported_schema_version`, ORDER BY DESC version read, non-lowering stamps. |
| `audit/builder.rs` | Helios `AuditAgent` + `tenant()` **and** Atrius `id` / `subtype` / `extension`. |
| `rest/middleware/auth.rs`, `handlers/batch.rs` | Helios `.principal()` / `audit_agent()`. |
| `handlers/bulk_export.rs` | Keep feat **`export_jobs_list_handler`**; lifecycle audit uses **tenant tag** + `crate::audit::audit_agent` (not subject-only persistence helper). |

### HIS / pins

- **`helios-audit`** and **`helios-auth`** — refresh HIS pins with other Helios crates
  to **`3fa7eda29`**.

### Verification (recorded)

fmt 0; hfs check 0; postgres check 0; validator **161**; rest **2279**; persistence
lib **1869**; hts **1066**; cds **89**.

---

## Sync 3 — 2 Nov 2026

| Ref | Value |
|-----|--------|
| atrius `main` | `326e1585a` (Helios `699caaeba`, workspace **0.2.7**) |
| Rollback tag | `pre-merge-main-2026-11-02` → `3fa7eda29` |
| Merge commit | `c909cdbb3` |
| Distance | 158 on `main` / **63** non-merge since `86371cdab` |

### Helios themes adopted

- Release **0.2.7**; MSRV **1.91.1**.
- R6 fixtures refresh to build **v5.0.0-7924** (~1.6k JSON/XML test files).
- CI: **`CARGO_TARGET_DIR`** outside checkout; split lint/test matrix; nightly
  Windows/macOS legs.
- HTS bundled terminology version bumps (ICD-10 2027 table zip, NUCC 261, Thesaurus
  26.10d, hl7.terminology / US Core / IPS package tgz updates).
- FHIRPath terminology **`Retry-After`** / rate-limit cooldown (`RequestError`);
  `#2149` URL redaction on transport errors.
- Batch **transaction**: deferred non-read GETs after commit (`#2380`); shared
  `execute_vread` for Bundle history entries.
- MongoDB pool metrics / topology events / commit deadline helpers.

### Schema ledger

- Unchanged **49** / **58** (no inbound persistence schema steps this wave).

### Conflicts resolved

| Location | Decision |
|----------|----------|
| R6 `crates/fhir/tests/data/{json,xml}/R6/**` | Take `main`; stage with **`git add -f`** (paths ignored by fork `.gitignore`). |
| HTS `terminology-data/*` | Take `main` blobs (feat had delete/rename conflicts). |
| Workspace `Cargo.toml` | **0.2.7** pins; keep **`helios-auth` + `redis`**; keep **`helios-terminology-client`** on **`rest`** and **`ui`**. |
| `fhirpath/terminology_client.rs` | Helios **`RequestError`** / `Retry-After` **plus** Atrius **`validate_code_with_parameters`** (validator `$validate-code` Parameters body). |
| `crates/fhir/src/r6.rs` | **Removed** merged flat file; keep **`crates/fhir/src/r6/`** module tree only. |
| `handlers/batch.rs`, `vread.rs` | Take Helios (deferred GET + `#2380` parity). |
| `.github/workflows/ci.yml` | Take Helios matrix layout (jobs still gated `repository_owner == 'HeliosSoftware'` on upstream; fork uses same file). |

### Fork `main` hygiene (not feat merge, same week)

Merging **`upstream/main` → atrius `main`** blocked when ~1.6k **R6 test fixtures**
were dirty locally (typically `cargo build` with R6 **without**
`skip-r6-download`). Fix: `git restore` + `git clean` on R6 test paths, then merge.
On **`main`**, prefer builds with **`skip-r6-download`** so HL7 build.fhir.org
timestamps do not dirty the tree.

### HIS / pins

- Full workspace **0.2.7** + **`helios-fhir`** layout — move **all** HIS Helios crate
  pins to **`c909cdbb3`** (fhir, auth, audit, observability, validator, persistence as
  you track them).

### Verification (recorded)

`cargo build`; `cargo build -p cds-server`; rest `--lib` **1000**. Full HTS / full
rest integration suite not re-run in merge session — run playbook suite before
production push.

### Open follow-up (docx backlog)

- **R6 generated model:** upstream 0.2.7 regen is shipped as flat `r6.rs`; feat keeps
  **`r6/`** directory output. If R6 compile/tests drift, regen per
  `docs/fhir-model-regen.md` into the **directory** layout — do not copy flat `r6.rs`
  beside `r6/mod.rs`.

---

## Cumulative ledger (after 2 Nov 2026)

| Backend | `SCHEMA_VERSION` | Notes |
|---------|------------------|--------|
| SQLite | **49** | Named steps; Helios `#2280` = `bulk_submissions_updated_at`; fork outbox dead-letter step before slot-2. |
| Postgres | **58** | Same naming discipline; no integer replay on feat. |

---

## Word doc — suggested section headings to paste under

1. **Sync log** — three subsections (30 Oct, 31 Oct, 2 Nov) with tables above.
2. **Keep-list delta** — bulk export jobs list; audit builder extensions; fhirpath
   `validate_code_with_parameters`; r6/ not r6.rs; 0.2.7 workspace.
3. **Rollback tags** — `pre-merge-main-2026-10-30`, `-10-31`, `-11-02`.
4. **HIS pin reminders** — auth (30 Oct), auth+audit (31 Oct), full stack (2 Nov).
5. **Operator notes** — R6 dirty tree on `main` sync; `git add -f` for R6 tests on feat merge.

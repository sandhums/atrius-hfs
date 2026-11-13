# Helios follow-on sync: main → feat-clinical-reasoning (Oct–Nov 2026)

Paste-ready addendum for *HELIOS vs Atrius.docx*. Covers feat syncs **30 Oct through
12 Nov 2026** (the Word doc in iCloud was last saved **2 Nov 2026** — paste from here
through Sync 8). Authoritative detail and test counts live in
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

### Follow-up commit (same week, before next sync)

| Ref | Value |
|-----|--------|
| Commit | `3f5f7cab9` — *Restore fork batch behaviour lost in the 2 Nov Helios merge* |

Helios `batch.rs` had been taken whole; restored fork **`bundle_patch_format`**, transaction
DELETE/PATCH preflight, **`check_entry_if_none_match`** (PUT `ifNoneMatch` create-if-absent),
and the `refused_methods…` unit test. Made FHIRPath **`RequestError`** `pub`; HTS
`token_code_system` test asserts a single cached `$expand`.

---

## Sync 4 — 4 Nov 2026

| Ref | Value |
|-----|--------|
| atrius `main` | `301ad27ec` (Helios `5ec98f887`, workspace **0.2.7**) |
| Rollback tag | `pre-merge-main-2026-11-04` → `a7fa0a007` |
| Merge commit | `4f3b91009` |
| Distance | 74 on `main` / **29** non-merge since `3f5f7cab9` |

### Helios themes adopted

- `#1654` top-level `/docs/` **gitignored on upstream** — fork keeps tracked `docs/` with
  `git add -f`.
- Mongo search `#2294`, ES filter-source `#2286`, reindex transient retry `#2290`,
  FHIRPath document functions `#2245`, CI gate entries `#2173`.

### Schema ledger

- Unchanged **49** / **58** (Mongo metadata **16**).

### Conflicts resolved

| Location | Decision |
|----------|----------|
| `docs/` rename detection | Git tried to move Atrius docs into `crates/persistence/`; restored **`git checkout HEAD -- docs/`** + `git add -f` (196 tracked files). |

### Verification (recorded)

`cargo build`, `cds-server`, rest **R4,sqlite** all green (lib **1000**), hts **857**.

---

## Sync 5 — 6 Nov 2026

| Ref | Value |
|-----|--------|
| atrius `main` | `c788d77b6` (Helios `be76ebead`, workspace **0.2.7**) |
| Rollback tag | `pre-merge-main-2026-11-06` → `4f3b91009` |
| Merge commit | `fde065ffb` |
| Distance | 57 on `main` / **29** non-merge |

### Helios themes adopted

- Named step **`search_index_value_reference_target`** (Helios v47→v48 / v54→v55) for Mongo
  reference normalize `#2394`, Postgres modifier reference search `#2494`.

### Schema ledger

- SQLite **50** / Postgres **59** (step inserted after `bulk_submissions_updated_at`, before
  fork dead-letter / slot-2).

### Conflicts resolved

| Location | Decision |
|----------|----------|
| `sqlite/schema.rs`, `postgres/schema.rs` | Named ledger only. |

### Verification (recorded)

`cargo build`, `cds-server`, rest lib **1000**, hts **857**.

---

## Sync 6 — 7–9 Nov 2026 (0.2.8)

| Ref | Value |
|-----|--------|
| 7 Nov merge | `32c183235` ← `main` `ea9d0caca` (Helios `1e6bb3013`, **0.2.7**) |
| 9 Nov merge | `b59e4e1b4` ← `main` `b5ff5e173` (Helios `f39f48276`, **0.2.8** release) |
| Rollback tags | `pre-merge-main-2026-11-07` → `e78d94024`; `pre-merge-main-2026-11-09` → `32c183235` |

### Helios themes adopted

- `#2527` batch **`If-Match`** compare-and-swap; `#862` conditional write observer;
  `#863` PATCH format negotiation refusal; `#922` transaction entry index mapping;
  **`#2523` delete-absent → 204** (supersedes fork “decline missing DELETE” test).
- Traceparent propagation + tracestate cap (`helios-observability`); `#2517` versioned Location.
- 7 Nov: SQLite transaction delete clears search index `#864` **with** fork outbox enqueue on
  same connection.

### Feat-only fixes on branch (not Helios merges)

| Commit | Note |
|--------|------|
| `63616ed00` | Align transaction preflight with Helios **#2523** |
| `7031004e2` | Drop fork-only missing-DELETE transaction test (upstream owns behaviour) |
| `b877a0dec` | Remove unused fork-only **`extract_trace_context`**; keep **`inject_trace_context`** for BFF |

### Conflicts resolved (9 Nov)

| Location | Decision |
|----------|----------|
| `batch.rs` | Helios `HeldIfMatch` + **keep** `bundle_patch_format` + `check_entry_if_none_match` |
| `observability` | Helios adopt-remote-parent + tracestate cap; keep `LOG_FORMAT` test |

### HIS / pins

- **`helios-observability`** (9 Nov) — refresh HIS Helios crate pins after push.

### Verification (recorded)

schema_ledger **39**; rest lib **1001** (7 Nov) → **1003** (9 Nov); hts **857**.

---

## Sync 7 — 11 Nov 2026

| Ref | Value |
|-----|--------|
| atrius `main` | `8cf78b534` (Helios `684cae51c`, **0.2.8**) |
| Rollback tag | `pre-merge-main-2026-11-11` → `b877a0dec` |
| Merge commit | `dfa164904` (+ docs stamp `575d40822`) |
| Distance | 3 on `main` / **1** non-merge |

### Helios themes adopted

- `#2539` fhir-benchmark every release + cross-version charts (**CI only**).

### Conflicts

- **0** — clean merge.

### Verification (recorded)

rest lib **1003**, hts **857**; no library pin move.

---

## Sync 8 — 12 Nov 2026 (+ audit fix)

| Ref | Value |
|-----|--------|
| atrius `main` | `eb9c746f9` (Helios `a2df4fd3a`, **0.2.8**) |
| Rollback tag | `pre-merge-main-2026-11-12` → `575d40822` |
| Merge commit | `5876ad96b` |
| Post-audit fix | `6d1269fe9` — outbox **`$events`** cursor restored |
| Feat tip (unpushed) | **`6d1269fe9`** |
| Distance | 142 on `main` / **100** non-merge since 11 Nov tag |

### Helios themes adopted

- `#1941` bulk-submit throughput + reindex streams; `#869` **`ifNoneMatch` on Bundle GET**;
  `#868` refuse operation entry URLs; **`#2547` Subscription `$events`** (IG range query over
  in-memory history); `#514` metadata `searchRevInclude` only when storage searches; `#2510` ES
  unevaluable parameters; `#2146` MongoDB purge transaction; deps `#2549`/`#2557`; benchmark
  refresh `#2559`.

### Conflicts resolved (high signal)

| Location | Decision |
|----------|----------|
| `batch.rs` | **Keep** fork PUT `ifNoneMatch` + `bundle_patch_format`; **take** `bundle_read_result` for GET `#869` |
| `bulk_submit` (sqlite) | Keep **`ingest_validation_error`** on Helios move-into-`create_prepared` path |
| Worker / `hfs` | **Both** `with_ingest_validator` and `with_deferred_reindex_scoping` |
| `subscriptions.rs` | Took Helios IG `$events` — **audit found regression** (see below) |
| `SubscriptionManager` | Heartbeat map **and** `event_history_size` |

### Post-merge audit (12 Nov)

- One-sided files match their side; keep-list spot checks passed (`r6/` layout, Redis auth,
  terminology client, batch fork markers).
- **Regression:** merge resolution dropped Atrius **outbox-backed `$events`** (`after`/`count`
  paging for HIS inbox + BFF). **Fixed** in `6d1269fe9`: when range parameters absent and an
  outbox is attached → `outbox_events`; otherwise Helios `#2547` range query. Tests:
  `subscription_events_outbox.rs`. Playbook verification now includes
  `cargo test -p helios-rest --features R4,sqlite,subscriptions`.

### HIS / pins

- **`helios-observability`** deferred-index metrics — move pins with other Helios crates after push.

### Verification (recorded)

`cargo build`, `cds-server`, rest **R4,sqlite** lib **1024**; rest **+subscriptions** full suite green; hts lib **857**; persistence lib **1986** (audit pass).

---

## Cumulative ledger (after 12 Nov 2026)

| Backend | `SCHEMA_VERSION` | Notes |
|---------|------------------|--------|
| SQLite | **50** | Named steps; `search_index_value_reference_target` after `bulk_submissions_updated_at`; fork outbox dead-letter before slot-2. |
| Postgres | **59** | Same naming discipline; no integer replay on feat. |

Workspace **0.2.8**. MSRV **1.91.1**.

---

## Sync 9 — 13 Nov 2026

| Ref | Value |
|-----|--------|
| atrius `main` | `6927bc473` (Helios `a547196c0`, **0.2.8**) |
| Rollback tag | `pre-merge-main-2026-11-13` → `be4c16ab4` |
| Merge commit | (this merge) |
| Distance | 29 on `main` / **15** non-merge |

### Helios themes adopted

- REST **`$meta`**, **`$meta-add`**, **`$meta-delete`** (`#2566`); write path **`prepare_write`** / `drop_empty_elements` (`#2554`); persistence **`merge_update_meta`** on update (`#2562`).
- Search: chain-ending **`_has`** (`#2556`); unknown nested `_has` parameters (`#2563`).
- Bulk submit: `content_to_write` applies empty-element cleanup after merge mode.

### Conflicts resolved (high signal)

| Location | Decision |
|----------|----------|
| sqlite/postgres `storage.rs` | Helios `store_update` + **`maybe_enqueue_*`** on the same transaction |
| S3 `bulk_submit.rs` | `content_to_write` + fork **`ingest_validation_error`** |
| `validation.rs` | **`prepare_write`** + fork storage **`prefetch_resolve`** |

### Verification (recorded)

`cargo build`, `cds-server`, rest lib **1029**, rest **+subscriptions** green, hts green.

---

## Keep-list delta (paste into Word — Nov syncs)

- Batch: **PUT `ifNoneMatch` create-if-absent** alongside Helios GET `#869` / CAS `#2527`; keep **`bundle_patch_format`**; missing DELETE is Helios **204** `#2523` (not fork decline).
- Subscriptions: durable **outbox** + heartbeat + **`fhirPathCriteria`**; **`$events`** = Helios range query **or** outbox cursor when no range params (HIS/BFF).
- Observability: **no `extract_trace_context`**; keep **`inject_trace_context`** + Helios traceparent adoption.
- Bulk submit: **`IngestValidator`** / `ingest_validation_error` on sqlite ingest path.
- Still: `helios-terminology-client`, **`helios-auth` + `redis`**, **`r6/`** not flat `r6.rs`, named schema ledger.

---

## Word doc — suggested section headings to paste under

1. **Sync log** — subsections Sync 1–8 (30 Oct … 12 Nov) with tables above.
2. **Keep-list delta** — November bullets in previous section.
3. **Rollback tags** — through `pre-merge-main-2026-11-12`.
4. **HIS pin reminders** — observability on 9 Nov and 12 Nov waves (after push to feat tip `6d1269fe9`).
5. **Operator notes** — `docs/` `git add -f` on every merge (Helios `#1654`); run rest with **`subscriptions`** feature; 2 Nov batch follow-up `3f5f7cab9`.

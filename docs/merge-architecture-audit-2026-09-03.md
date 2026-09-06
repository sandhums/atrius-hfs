# Architecture audit of the Helios → Atrius merge decisions

Audit date: 3 Sep 2026. Reviewed state: `feat-clinical-reasoning` at `7d47ce1b3`
(the Sep 3 sync merge), against `main` at `f9197baf7` (exact replica of
`upstream/main`, whose tip `fa33d5ddc` is fully contained).

Follow-up: **4 Sep 2026** on the same branch. §3.1 (env/scripts + package overlay)
is in progress in the working tree; see **§6**. **§3.2** (Postgres write TX),
**§3.3** (named `schema_migrations` ledger), **§3.4** (merge playbook keep-list),
and **§3.5** (directory-layout regen + runbook, 5 Sep), **§3.6**
(`Principal::stub` / `#[non_exhaustive]`), and **§3.7** (Redis JTI, outbox
dead-letter / zero-delivery, SQLite claim CAS, HTS per-instance system-id
cache; 5 Sep) are closed.

Follow-up: **6 Sep 2026.** The five write-path / `$validate` holes in §3.4 and
the remaining slice matchers (`exists`, `extension('url')`, reslicing, in-scope
`resolve()`) are closed in-repo (`4e2b8ebc6`, `331f2ddbc`). **Pending work is
§5.1** — do not treat the 3 Sep §5 numbering as the live queue.

Companion to *HELIOS vs Atrius.docx*, which records what each sync decided. This
document asks a different question: **are those decisions still correct, and do
they hold up for durability and extensibility?** Sections are written to be
paste-ready into the docx if wanted.

Method: read the merged code directly rather than trusting the sync records, and
compare against `main` for every shared file. `cargo check --workspace --all-targets` is green (exit 0; only the pre-existing `missing_docs` warnings on
the outbox types), so nothing here is a build failure.

Two findings were live defects rather than design debt: **§3.1**, where IG
validation was disabled in deployments whose configuration asserted `strict`
(config/scripts/resolver work in §6; ABDM env example + staging 422 still open),
and **§3.2**, where the outbox lost events on Postgres (closed: `WriteTx`).
Start there. The rest is durability and extensibility debt, ordered by how much
it compounds. The live remaining queue is **§5.1**.

---



## 1. The divergence surface

Excluding the generated FHIR models, the fork substantively edits **51 shared
upstream files**. Another 96 touched files differ only by rustfmt / let-chain
brace churn. The substantive edits are concentrated in a small set of files,
which is the reason most syncs auto-merge:


| Shared upstream file                                 | Non-whitespace lines diverged |
| ---------------------------------------------------- | ----------------------------- |
| `crates/subscriptions/src/engine/mod.rs`             | 567                           |
| `crates/hts/src/backends/sqlite/value_set.rs`        | 359                           |
| `crates/persistence/src/backends/sqlite/storage.rs`  | 264                           |
| `crates/subscriptions/src/topics/mod.rs`             | 256                           |
| `crates/subscriptions/src/evaluator/filter_match.rs` | 219                           |
| `crates/hts/src/operations/validate_code.rs`         | 219                           |
| `crates/rest/src/handlers/batch.rs`                  | 167                           |
| `crates/persistence/src/backends/sqlite/schema.rs`   | 160                           |
| `crates/persistence/src/backends/postgres/schema.rs` | 119                           |
| `crates/fhir-validator/src/converter/slicing.rs`     | 112                           |
| `crates/rest/src/validation.rs`                      | 96                            |


Everything else Atrius-specific lives in its own crates (`cds-server`,
`fhir-terminology`, `fhir-valueset-gen`) or its own modules (`subscriptions/ outbox.rs`, `heartbeat.rs`, `topics/fhirpath_criteria.rs`, `persistence/*/ subscription_outbox.rs`). **This separation is the strongest structural choice in
the fork** and should be defended deliberately on future syncs: every time a
feature is implemented as a new module rather than an edit to an upstream
function, the permanent merge cost drops to zero.

---



## 2. Decisions that are correct — close these out

These were reviewed and need no revisiting.

- `$reindex`**: adopting upstream's** `OperationsBundle`**.** Correct, and it fixed a
real defect. The fork's `ReindexController` used an `unsafe` TypeId downcast
and was built from the composite *primary only*, so on the Postgres +
Elasticsearch staging topology it rebuilt the Postgres index table nothing
queries and left the Elasticsearch index — the one serving search — stale.
Upstream's `ReindexOperation::with_parts(source, targets, registries)` rebuilds
both targets and re-extracts through the per-tenant registry, so custom
`ChargeItem` SearchParameters are honoured.
- **HTS** `concepts_search_fts`**: taking upstream's.** Correct. Same table, same
purpose, column-compatible with the fork's insert statements, and upstream's
incremental `concepts_fts_built` tracker replaced the fork's full re-tokenise
on every boot (10–25 s with SNOMED + LOINC loaded).
- **Dropping the fork's** `SubscriptionStatusStore` **for upstream's**
`with_status_store`**.** Correct — avoids two write-back paths for one piece of
state.
- **Dropping the pre-ballot** `sof-capabilities-inline` **extension.** Correct; it
has no 3.0.0-ballot counterpart and upstream's negative test is the right pin.
- **JTI: keeping revocation, accepting the replay-cache deletion.** Correct, and
the semantic distinction is the right one — a resource server has no use for a
replay cache, but a logout deny-list is real. The check is **fail-closed**
(Redis error → `AuthError::InternalError` → 401), which is the right default.
Cross-repo migration is clean: `atrius-his` carries no stale `build_jti_cache`,
`DisabledJtiCache`, or `HIS_AUTH_JTI_BACKEND` references.
- `ChannelDispatcherRegistry` **replacing upstream's per-channel fields.**
Correct, and better than what it replaced: `with_dispatcher` admits new channel
types without editing an engine `match`, no capability was lost, and a missing
messaging config degrades to the same `Error` status transition as upstream's
`messaging_channel: None`. **Worth offering upstream** — it would delete one of
the fork's largest recurring conflict sources.
- `topics/fhirpath_criteria.rs`**.** Clean seam (delegated from
`matching_topics`, not an in-place rewrite of upstream matching) and correctly
fail-closed: parse errors, eval errors, non-boolean results, and unparseable
resource bodies all yield "no match" rather than delivering.
- **The single-engine validation cutover (**`f257914a8`**, 1 Aug 2026).** The best
decision in the log — but see §3.1 and §3.4 for the cost of the *record* of it
being wrong.

---



## 3. Findings, worst first



### 3.1 IG validation is silently off in the clinical deployments

**Status (4 Sep 2026):** clinical env examples + setup scripts now set
`HFS_VALIDATION_MODE=enforce` and `HFS_FHIR_PACKAGES=atrius.fhir.r4.india@0.1.0`;
the resolver no longer requires sushi `dependsOn` packages in the cache. See
§6. The 3 Sep snapshot below is kept as the defect that motivated that work.
Still open: `deploy/clinical/.env.abdm.example` still sets the dead
`HFS_PROFILE_*` variables; no startup warn on recognised-but-removed `HFS_*`;
no staging proof that a non-conformant write returns 422.

The most urgent item in this audit, and a direct consequence of the drift in
§3.4. `HFS_PROFILE_MANIFEST` and `HFS_PROFILE_VALIDATION_MODE` are read **nowhere
in the Rust workspace** (`git grep` over `*.rs` returns nothing) — they died with
the 1 Aug cutover. But the deployment configuration still sets them, and never
sets their replacements:


| File                                          | Sets (dead)                          | Sets `HFS_VALIDATION_MODE` / `HFS_FHIR_PACKAGES` |
| --------------------------------------------- | ------------------------------------ | ------------------------------------------------ |
| `deploy/env/hfs-clinical.env` (**live**)      | `HFS_PROFILE_VALIDATION_MODE=strict` | neither                                          |
| `deploy/env/hfs-ndhm-validate.env` (**live**) | `HFS_PROFILE_VALIDATION_MODE=strict` | neither                                          |
| `deploy/env/hfs-clinical.env.example`         | `strict`                             | neither                                          |
| `deploy/clinical/.env.atrius.example`         | `strict`                             | neither                                          |
| `deploy/clinical/.env.abdm.example`           | `warn`                               | neither                                          |


*Table is the 3 Sep snapshot. Clinical rows are superseded by §6.1;*
`.env.abdm.example` *is not.*

Nothing under `deploy/` or `scripts/` **as of 3 Sep** set `HFS_VALIDATION_MODE`,
`HFS_FHIR_PACKAGES`, or `HFS_FHIR_PACKAGE_CACHE` — the only occurrences were
comments and `echo` lines inside `scripts/setup-atrius-profile-registry.sh`.
And `HFS_VALIDATION_MODE` defaults to `off` (`crates/rest/src/config.rs`).

So the effective state of every one of those deployments is:

- `HFS_PROFILE_VALIDATION_MODE=strict` — **ignored**
- `HFS_VALIDATION_MODE` unset — **write-path validation off**
- `HFS_FHIR_PACKAGES` unset — **no Atrius/NDHM IG profiles loaded at all**

They assert strict IG conformance enforcement and are running with validation
entirely disabled and no IG profiles present. This has been the case since 1 Aug
and is invisible: no startup warning, because the server has no reason to
complain about a variable it does not read.

`scripts/setup-atrius-profile-registry.sh` is self-contradictory — line 16 notes
the legacy path was removed, then line 95 still instructs the operator to set
`HFS_PROFILE_VALIDATION_MODE=strict`. `scripts/build-atrius-profile-manifest.sh`
still builds manifests that nothing consumes, and
`scripts/p0-import-terminology.sh:38` still tells operators to set the dead
variable.

**Fix:** migrate the env files to `HFS_VALIDATION_MODE=enforce` +
`HFS_FHIR_PACKAGE_CACHE` / `HFS_FHIR_PACKAGES` per
`crates/fhir-validator/docs/packages.md`, fix the three scripts, and verify on a
staging deploy that an intentionally non-conformant resource is actually
rejected. Consider having `ServerConfig::try_from_env` fail — or at minimum warn
loudly — on recognised-but-removed `HFS_*` variables, so the next removal cannot
fail this quietly.

### 3.2 The Postgres outbox is not transactional (durability defect)

The outbox design rests on the claim that the outbox row is enqueued in the same
transaction as the resource write. **That holds on SQLite and fails on
Postgres**, which is what staging runs.

The guarantee is stated in the code itself, which makes the gap easy to miss on
review — `enqueue_resource_event` documents it unconditionally for all SQL
backends and skips direct dispatch on the strength of it:

```250:264:crates/subscriptions/src/engine/mod.rs
    /// When a durable outbox is attached, SQL backends write the outbox row in
    /// the **same transaction** as the resource. This method only notifies the
    /// outbox worker (no second enqueue). Without an outbox, falls back to
    /// in-process `tokio::spawn` of [`Self::on_resource_event`].
    pub fn enqueue_resource_event(self: &Arc<Self>, event: ResourceEvent) {
        if self.outbox.is_some() {
            // ...
            self.outbox_notify.notify_one();
            return;
        }
```

SQLite wraps all four write arms (`create`, `update`, `delete`, `restore`) in
`begin_immediate_tx(&mut conn, "write")` … `tx.commit()`. The Sep 1 ordering bug
(`drop(conn)` landing before the enqueue) is genuinely fixed in every arm.

Postgres does not. The only `.transaction()` in
`crates/persistence/src/backends/postgres/storage.rs` is in `purge` (line 1089).
The direct `create` path issues three independent autocommit statements on one
pooled client:

```248:304:crates/persistence/src/backends/postgres/storage.rs
        let inserted = execute_cached(
                &client,
                "WITH ins AS (
                     INSERT INTO resources (...)
                     ...
        self.index_resource(&client, tenant_id, resource_type, &id, now, IndexWrite::Fresh, &resource).await?;
        // ...
        super::subscription_outbox::PostgresSubscriptionOutbox::maybe_enqueue_on_client(
```

`update` (456–543) and `restore` (1212–1266) follow the same shape. Any error or
crash after the resource CTE commits but before the outbox insert leaves a
committed resource with **no subscription event, silently and permanently** —
nothing reconciles resources against the outbox.

Bundle writes are unaffected: `PostgresTransaction::new` issues a real `BEGIN`
(145–149) and enqueues on the transaction client, so transaction and batch paths
are atomic. The exposure is single-resource REST CRUD, which is the majority of
traffic.

The Postgres `delete` path is otherwise correct: the CTE writes `'null'::jsonb`
to history but uses `RETURNING data` so `previous_resource` is stamped on the
outbox event without a second round trip.

**Fix:** wrap resource write + index + enqueue in one explicit transaction, or
fold the outbox insert into the resource CTE. Add a test that injects a failure
between the resource write and the enqueue and asserts neither is visible.

**Closed 5 Sep 2026** (`9e69fa873`). Direct REST CRUD (`create` / `update` /
`delete` / `restore`) uses `WriteTx`
(`crates/persistence/src/backends/postgres/write_tx.rs`): `BEGIN` on the pooled
client, resource + index + outbox enqueue, then `COMMIT`. Drop without commit
rolls back. Bundle writes already used `PostgresTransaction`.

### 3.3 Schema renumbering is positional, with no provenance guard

The renumbered ladders are **contiguous and duplicate-free today** — 18 SQLite
steps (`SCHEMA_VERSION = 19`), 37 Postgres steps (`SCHEMA_VERSION = 38`). Both
historical duplicate-function incidents (the `migrate_v16_to_v17` outbox/provider
collision, and the `migrate_v17_to_v18` bytes duplicate) are genuinely resolved,
one definition each. Overlapping Helios DDL is written defensively
(`CREATE TABLE IF NOT EXISTS`, `PRAGMA table_info` guards, `ADD COLUMN IF NOT EXISTS`), so re-running upstream DDL under a shifted number no-ops rather than
failing.

The structural problem is that the version is a bare integer meaning "how many
steps have run". There is no fingerprint, checksum, or provenance column, so the
dispatch loop cannot distinguish a database stamped by upstream numbering from
one stamped by fork numbering. The consequence:

> A database at version ≥ 17 stamped under **upstream** numbering will never run
> `migrate_v16_to_v17`, and therefore **never gets the** `subscription_outbox`
> **table**. No error at startup — just a missing table.

Concretely: an upstream SQLite v18 database upgraded by a fork binary runs only
`migrate_v18_to_v19` (a no-op on already-present byte columns), lands on 19, and
has no outbox. Same for an upstream Postgres v36 → 37.

Cost of the arrangement: Postgres carries **20 shifted upstream migrations**,
SQLite 2, and the fork is permanently +1 versus upstream at the same semantic
schema. If upstream only ever appends at the tip, each future sync is one copied
function plus one dispatch arm — cheap. Another version collision means another
renumbering pass.

Test coverage is the weak point. All 10 schema tests live in the SQLite module;
**Postgres** `schema.rs` **has no unit tests at all**. `test_migration_ladder_replays_ on_a_current_database` does replay from every version on a fork-numbered
database, which is valuable, but **no test simulates the upstream-numbered →
fork-numbered path** — precisely the scenario that silently drops the outbox.

Minor defects (3 Sep snapshot): `migrate_v36_to_v37`'s error strings read
`"Migration v35->v36 failed"`, and several Postgres migration doc comments still
cite upstream's original numbers (e.g. `migrate_v23_to_v24` documented as
"v22→v23"). **Error strings are now `v36->v37` / `v37->v38`.** Leftover comments
remain (§5.1 item 16).

**Fix:** record applied-migration provenance (a `schema_migrations` table of
applied step names, or a `flavour` marker), or at minimum add a startup
reconciliation that creates `subscription_outbox` if absent regardless of
version. Add the heal-path test. Add Postgres schema tests.

**Closed 4 Sep 2026.** Both SQL backends dispatch from named `schema_migrations`
rows. The integer is an operator stamp and a one-time backfill
(`classify_numbering` / `implied_applied_indices`). Outbox missing at tip, or
unrecorded by name with the integer already at tip, still runs the step. Fork
DBs at the tip do not replay the Postgres index ladder.

### 3.4 The merge log describes a validation architecture deleted a month ago

Commit `f257914a8`, **1 Aug 2026**, "refactor(rest): cut over to single Helios
validation engine", removed `ProfileValidationService`, `HFS_PROFILE_MANIFEST`,
`HFS_PROFILE_VALIDATION_MODE`, and the `fhir-validation*` crates. Atrius IG
profiles now load through `HFS_FHIR_PACKAGE_CACHE` / `HFS_FHIR_PACKAGES` into
`helios-fhir-validator`'s `packages` module and are layered into upstream's
`CompositeResolver` alongside the tenant overlay and the embedded core pack
(`crates/rest/src/validation.rs:298–314`). There is one engine.

`ProfileValidationService`, `profile_validation`, and `enforce_profile_on_write`
have **zero occurrences** in the workspace. The cutover is an ancestor of every
`pre-merge-main-*` tag from 11 Aug onward.

The decision was right — it is the one place the fork converged instead of
accumulating a parallel implementation. But the record of it is wrong in two ways
that matter, because the docx is the model future conflict resolutions are
reasoned against:

1. Every sync entry from 11 Aug to 3 Sep lists **"Dual validators: Atrius
  package/profile path plus Helios helios-fhir-validator"** in the keep-list.
   There is no second engine; there is one engine with an extra resolver layer.
2. The carried backlog line **"Atrius profile-manifest enforcement does not run
  on batch or transaction entries — HIS writes heavily through both"** has been
   obsolete since 1 Aug. The unified `ValidationService::check_write` **does** run
   on batch POST/PUT (batch.rs:995–1002, 1096–1102, 1184–1191) and on the
   transaction pre-flight loop (batch.rs:599–616).

The real remaining gaps listed below were **closed 5 Sep 2026** in-repo:

- Bulk-submit ingest calls `IngestValidator` / `check_write` before create/update
  (honours `HFS_VALIDATION_MODE`; tests that omit a validator stay unvalidated).
- Transaction `DELETE` entries fail the bundle at pre-flight if the instance is
  missing; AuditEvent DELETE remains refused by `admit_bundle_mutation`.
- Bundle `PATCH` entries apply (format inferred from the entry resource) and
  run `check_write` on the patched representation in both batch and transaction.
- `$validate` `mode` now changes enforcement: `create` (duplicate id), `update`
  (id required / not found), `delete` (id, existence, AuditEvent), `profile`
  (ignore `meta.profile`).
- Slice `type` / `profile` / `binding` / `exists` / `extension` matchers are
  evaluated in `engine/slicing.rs`. `resolve()` is in-scope only (`contained` /
  Bundle entries, no store). Binding does not expand a ValueSet at mark time.

Stale references to the removed path survive in **13 files** across `deploy/` and
`scripts/`, including two live env files — see §3.1, which is the operational
consequence and the most urgent item in this audit. Separately,
`docs/auth-verification.md:51` still documents the removed
`HFS_AUTH_JTI_BACKEND`.

**Fix:** correct the docx keep-list and backlog, and replace the obsolete line with the five real gaps above.

**Closed 4 Sep 2026** in-repo (the Word keep-list is outside git).
`docs/clinical-reasoning/upstream-merge.md` is the living keep-list: one engine,
`HFS_FHIR_PACKAGES` overlay, `check_write` on batch/transaction POST/PUT/PATCH
and DELETE existence, bulk-submit ingest, and `$validate` modes. Do not restore
`fhir-validation*`. Slice `type`/`profile`/`binding`/`exists`/`extension`
matchers are evaluated; `resolve()` is in-scope only.

### 3.5 The FHIR directory-layout split: durable mechanism, stale content

See §4 for the full tradeoff analysis. The audit finding itself is narrow:

The layout is **not** a one-time hand split — `helios-fhir-gen` on this branch
natively emits `<version>/{mod.rs,primitives/,complex_types/,resources/}` and
removes the legacy flat file (`crates/fhir-gen/src/lib.rs:336–368, 476–517`), with
a test pinning the directory output (3464–3469). Upstream's generator still emits
one file per version (`main:crates/fhir-gen/src/lib.rs:323`).

The problem is that resolving the recurring modify/delete conflict with `git rm`
discards **all** of upstream's body changes to the regenerated models, and the
fork's own regen has not kept pace. `ViewDefinition` is provably stale in all four
versions: still annotated `CanonicalResource` rather than `DomainResource`; missing
`approvalDate`, `lastReviewDate`, `effectivePeriod`, `topic`, `author`, `editor`,
`reviewer`, `endorser`, `relatedArtifact`; `summary_fields` missing
`effective_period`; nested `ViewDefinitionSelect` / `SelectColumn` / `Constant`
still carrying pre-ballot backbone wrapper fields; and the sql-expressions
invariant still in its pre-ballot form
(`crates/fhir/src/r4/resources/view_definition.rs:27–38, 671–683, 939–940`).

**Closed 5 Sep 2026.** Models were regenerated from current specs into
`crates/fhir/src/{r4,r4b,r5,r6}/` (directories, not `r4.rs`). ViewDefinition is
`DomainResource` with `effective_period` in `summary_fields` on all four
versions. `./scripts/diff-fhir-model-signatures.py` matches Helios flat-file
shape (853 / 882 / 1098 / 884 types). Runbook:
[docs/fhir-model-regen.md](fhir-model-regen.md). Option A (offer the generator
upstream) is still open.

Upstream touched the flat model files in 11 commits since January, several
carrying real model-shape changes beyond buildId churn — `4bfb2f597`
(ViewDefinition → DomainResource), `c2ce8a6f5` (boxed `Resource` enum variants),
`4917a4cbe` (summary-field metadata), `dec6039f3` (FHIRPath field-type tables),
`586e77469` (macro-derived type info). Each was silently dropped.

There is no regen runbook. `crates/fhir-gen/README.md:119–125` and
`book/src/development/code-generation.md:29` still describe flat-file output, and
`docs/clinical-reasoning/upstream-merge.md:160–166` still gives conflict guidance
for flat `r4.rs` and references a `r4/terminology/**` path that does not exist.

### 3.6 `Principal.fhir_user` is a recurring, cheaply fixable trap

The fork adds `fhir_user: Option<String>` to upstream's `Principal`
(`crates/auth/src/principal.rs:10–27`). Because the field is additive to a
struct built by literal, upstream tests that construct `Principal { .. }` fail to
**compile** rather than conflict — an invisible trap that has broken 5 of the last
6 syncs (Aug 11 batch test, Aug 27 `public_url_contract.rs` + `bulk_export.rs`,
Sep 1 `bulk_export_http.rs`, Sep 3 `sql_export_http.rs`).

There are ~18 construction sites; 17 are tests, 1 is production
(`provider/jwks_bearer.rs:200`). Upstream's `Principal` offers no seam — no
`Default`, no builder, no `new()`, not `#[non_exhaustive]` — and the fork's test
helper is private to `principal.rs`.

**Fix:** mark `Principal` `#[non_exhaustive]` and add a public constructor
(`Principal::stub(subject, scopes)` or a builder) in `helios-auth`, then migrate
the ~17 test literals. `#[non_exhaustive]` prevents literal construction from
other crates, so future fork fields get a default in exactly one place. Upstream
literals then need a one-time per-file conversion on sync instead of a
field-by-field chase. Worth offering upstream behind a `test-utils` feature.

**Closed 5 Sep 2026.** `Principal` is `#[non_exhaustive]`. Tests outside
`helios-auth` use `Principal::stub(subject, scopes)` plus `with_issuer` /
`with_tenant_id` / `with_fhir_user`. The JWT path in `jwks_bearer.rs` still
uses a same-crate struct literal. On the next Helios sync, convert any new
`Principal { .. }` in incoming tests to `stub` once per file instead of
adding `fhir_user: None`.

### 3.7 Smaller items worth queueing

- **No timeout on the Redis revocation call.** `RedisJtiRevocation::is_revoked`
does `get_multiplexed_async_connection()` + `EXISTS` with no
`tokio::time::timeout` (`crates/auth/src/jti/revocation.rs:53–66`), and it runs
on **every** authenticated request carrying a `jti`, uncached. Combined with
fail-closed semantics, a Redis stall becomes a total authentication outage
rather than a slow one. Add a short timeout and decide explicitly what a
*timeout* (as distinct from a definite error) should do.
- **Tokens with no** `jti` **skip revocation entirely** and are accepted
(`provider/jwks_bearer.rs:151–159`). Correct given the BFF also no-ops in that
case, but worth asserting the IdP always issues `jti`.

**Closed 5 Sep 2026.** Redis uses a shared `ConnectionManager` (not a new
multiplexed connection per request). Each `EXISTS` is bounded by
`HFS_AUTH_JTI_REVOCATION_TIMEOUT_MS` (default 500). Timeout and Redis errors
both fail closed as `AuthError::RevocationUnavailable` → HTTP 503 (distinct
from `TokenRevoked` → 401). When the Redis checker is enabled, a missing or
blank `jti` is `MissingJti` → 401: the token cannot be named on the blocklist.
`NoOpJtiRevocation` still accepts tokens without `jti`. Boot connects to Redis
with a 5s cap. This is still a deny-list, not a replay cache.

- **No dead-letter on the outbox.** After max retries a row is delayed 3600 s with
`"max retries exceeded"` and retried hourly forever — never tombstoned
(`subscriptions/src/outbox.rs:106–158`).
- **An outbox row is marked processed if the evaluation loop completes**, even
when every channel dispatch failed permanently — `on_resource_event` returns
`()`. `DeliveryStats` is then the only signal. Worth an explicit metric or log
for "processed with zero successful deliveries".

**Closed 5 Sep 2026.** Exhausted outbox claims set `dead_at` (`mark_dead`)
instead of a 3600s retry. Claim skips `dead_at IS NOT NULL`; `$events` still
reads only `processed_at IS NOT NULL`, so tombstones are not recovery events.
Named step `subscription_outbox_dead_letter` (SQLite 22 / Postgres 39). When
matches are non-empty and every dispatch fails, the engine still marks the row
processed (to avoid re-incrementing `event_number`) and logs
`Subscription event processed with zero successful deliveries`. HIS is
untouched: its charge-trigger outbox already dead-letters.

- **SQLite's claim query has no** `FOR UPDATE SKIP LOCKED` **equivalent** (Postgres
does, `postgres/subscription_outbox.rs:189–207`); it SELECTs then per-row
UPDATEs, so concurrent nodes can double-claim. Fine for single-node SQLite;
document it as such.

**Closed 5 Sep 2026.** SQLite claim takes a process-local mutex (shared-cache
pools fail concurrent `BEGIN IMMEDIATE` with `SQLITE_LOCKED`, which
`busy_timeout` does not cover), then `BEGIN IMMEDIATE` and a compare-and-swap
`UPDATE` (`processed_at IS NULL AND dead_at IS NULL AND (locked_until IS NULL
OR locked_until < now)`). Workers that share **one database file** cannot
double-claim. Clustered dispatch still needs Postgres: do not point several
HFS nodes at separate SQLite copies of the same logical outbox.

- `HTS SYSTEM_ID_CACHE` **remains a process-wide static** in
`crates/hts/src/backends/sqlite/value_set.rs` (already carried in the backlog).

**Closed 5 Sep 2026.** `cs_system_id_cache` and `cs_language_cache` live on
`SqliteTerminologyBackend` with the other iter3 memos. `invalidate_caches`
clears them (exhaustive destructure). Parallel in-memory backends no longer
leak `(url → system_id)`. Postgres `CLOSURE_*` process-wide caches were not
part of this item.

Delivery semantics are correct and single-pathed otherwise: with an outbox
attached, `enqueue_resource_event` only notifies the worker and returns rather
than also spawning a direct dispatch (`engine/mod.rs:254–269`), so there is no
double-delivery window; and `DeliveryStats` is recorded inside
`dispatch_with_retry`, which the outbox path also goes through, so the operator
page does not under-report.

---



## 4. The FHIR directory-layout decision: tradeoffs

This is the oldest fork decision — commit `97a93c7fe`, **20 Feb 2026**,
"Refactored generator to output resources in separate files" — predating the
merge cadence entirely. It deserves a deliberate re-decision rather than being
carried by inertia, so the inputs are laid out here.

### What the split buys

The ergonomics argument is strong and should not be understated:


|                          | Upstream (flat)       | Fork (directory)                       |
| ------------------------ | --------------------- | -------------------------------------- |
| Files per version        | 1                     | ~210                                   |
| Largest single file (R4) | **190,868 lines**     | 23,440 (`r4/mod.rs`); resources ~1,100 |
| Total across 4 versions  | ~834k lines / 4 files | ~846k lines / 834 files                |


A 190k-line Rust file is genuinely hostile: rust-analyzer indexing, incremental
recompilation granularity, editor navigation, and code review all degrade badly.
Per-resource files also make `git blame` and diffs on a single resource
comprehensible. This is a real engineering benefit, not a preference.

### What the split costs

- **~4 modify/delete conflicts per sync**, mechanical to resolve but requiring a
conscious decision each time.
- **Silent loss of upstream model semantics.** This is the actual cost, and it has
already been paid: keeping the deletion discards upstream's regenerated bodies,
so `ViewDefinition` is pre-ballot and at least four other upstream model-shape
changes never landed (§3.5). Nothing surfaces this — it is not a conflict, not a
test failure, not a warning.
- **A permanently forked generator.** `crates/fhir-gen/src/lib.rs` diverges by 170
substantive lines plus a fork-only `directory_output_helpers.rs`. Any upstream
change to the generator now conflicts. (Encouragingly, upstream has touched
neither `fhir-gen/src/` nor the flat model files since the current merge-base,
so the immediate pressure is low.)
- **Regen is expensive and undocumented** — a large-diff full regen with no
runbook, and the published docs still describe the flat layout.



### What the split does *not* cost

`crates/fhir-terminology` carries its own per-version modules generated by
`fhir-valueset-gen` from `crates/fhir-gen/resources/<VERSION>/valuesets.json`. It
does **not** depend on the `crates/fhir` directory layout. Neither does
`crates/fhir/src/lib.rs`, which declares `pub mod r4;` identically on both
branches — Rust resolves `r4/mod.rs` and `r4.rs` interchangeably. There is no
`r4/terminology/` directory despite what `docs/clinical-reasoning/upstream-merge.md`
implies. **So the layout is not load-bearing for any Atrius crate**, which keeps
all three options genuinely open.

### The three options

**A. Upstream the generator change.** Offer Helios the directory output, ideally
behind a generator flag so they can adopt it without disruption. If accepted, the
divergence disappears entirely, the ergonomics win becomes shared, and regen
tracks upstream automatically. The argument to Helios writes itself: nobody wants
to open a 190k-line file. Risk: they may decline or sit on it, and the fork
carries the delta meanwhile — which is the status quo, so the downside is bounded.
This is the only option that *removes* the problem rather than managing it.

**B. Keep the delta, add a regen discipline.** Accept the layout permanently and
close the correctness gap with process: on any sync where upstream touches
`crates/fhir/src/r*.rs` with more than buildId churn, sync the spec JSON and run
the fork generator, then diff the regenerated tree to confirm upstream's model
changes actually landed. Needs a written runbook and a way to distinguish
substantive upstream regen from timestamp churn (a script comparing struct
signatures rather than raw diffs). Cost: recurring, forever, and dependent on
noticing.

**C. Revert to upstream's flat files.** Eliminates the conflict class, the
generator fork, and the staleness risk in one move, and — since nothing
Atrius-specific depends on the layout — is mechanically feasible: regenerate flat,
delete the directories, revert the generator. Cost: surrender a real ergonomics
win and reintroduce 190k-line files. Only attractive if the merge burden is
judged to dominate, which the evidence does not currently support.

### Recommendation

**A, with B as the interim.** Attempt to upstream it, because it is the only
option that ends the divergence, and the pitch is easy. **B is done (5 Sep
2026):** models regenerated into `crates/fhir/src/r4/` (and r4b/r5/r6);
runbook [fhir-model-regen.md](fhir-model-regen.md); signature check
`scripts/diff-fhir-model-signatures.py` matches Helios. C is not recommended.

---



## 5. Recommended backlog, by value over effort

### 5.1 Pending (6 Sep 2026)

This is the live queue. Closed 3–6 Sep items stay in §5.2 for the record.

**Ops / config (§3.1, §6.1)**

1. Migrate `deploy/clinical/.env.abdm.example` off dead `HFS_PROFILE_MANIFEST` /
   `HFS_PROFILE_VALIDATION_MODE` / `HFS_PROFILE_VALIDATION_ADDONS`. Clinical HFS
   examples already use `HFS_VALIDATION_MODE` + `HFS_FHIR_PACKAGES`.
2. Warn or fail at HFS startup on recognised-but-removed `HFS_PROFILE_*` (and
   similar). Nothing in `helios-rest` currently complains; leftover env still
   looks like validation is on.
3. Staging proof: with `HFS_VALIDATION_MODE=enforce` and the Atrius package
   overlay, a knowingly non-conformant write returns **422**.

**Offer upstream (§2, §4 option A)**

4. `ChannelDispatcherRegistry` and the directory-layout FHIR generator — the
   two changes that would delete the fork's largest recurring conflict sources.

**NDHM / ABDM (§6.2–§6.3) — HIS boundary, not a second HFS engine**

5. NDHM `$validate` at the ABDM export/remap gate. Seed `ndhm.in` on that
   validator only. Do **not** add it to clinical `HFS_FHIR_PACKAGES`. Export
   preflight still hits clinical HFS and strips remapped NDHM `meta.profile`.
6. Align HIS remap canonicals with published NDHM 6.5.0 (`StructureDefinition/Patient`,
   not `ndhmPatient`, and the same for Encounter/Claim).
7. Optional: machine-check the claim “Atrius-valid ⇒ NDHM-valid after remap”
   (`targetProfile` + required-binding supersets). `scripts/ndhm-parity-diff.py`
   does not do this today.

**One-engine validator leftovers (not a second engine)**

8. Binding discriminators do not expand a ValueSet at mark time.
9. Mixed-kind discriminator sets still warn and stay dormant.
10. Store-backed `resolve()` is **out of scope**. In-scope `contained` / Bundle
    `entry.resource` is implemented; unresolved references do not match.
11. Conditional PATCH inside a Bundle is refused (instance-url PATCH is
    implemented).

**IG / QI-Core (AtriusIGDraft, not HFS runtime)**

12. Decide QI-Core `mustSupport` parity for `AtriusInDeviceRequest`
    (`status` / `intent` / `code[x]` / `codeReference` / `codeCodeableConcept`).
    MS is informational in HFS — no validation change.
13. FSH allows `codeOptions` (`0..1`) but does not encode QI-Core `drq-3`
    (coding XOR codeOptions).
14. CI assert: seeded IG materialization `warnings.is_empty()`.
15. Overlay `hl7.fhir.uv.extensions.r4` only if untyped
    `individual-recordedSexOrGender` instances need structural overlay
    (not in the embedded R4 core pack).

**Docs / cosmetics**

16. Postgres schema comments that still cite upstream's original step numbers.
17. Optional: `docs/auth-verification.md` still *names* removed
    `HFS_AUTH_JTI_BACKEND` as part of the #205 history — not live config.

### 5.2 Closed since the 3 Sep audit

Original §5 numbers in parentheses. These are **not** the live queue.

1. Clinical HFS env/scripts migrated to `HFS_VALIDATION_MODE` + `HFS_FHIR_PACKAGES`
   (4 Sep / §6.1). Remaining operator work is §5.1 items 1–3.
3. Postgres write + index + outbox in one `WriteTx` (§3.2, `9e69fa873`).
4. Keep-list corrected in `docs/clinical-reasoning/upstream-merge.md` (§3.4).
5. Bulk-submit ingest through `IngestValidator` / `check_write` (`4e2b8ebc6`).
6. Named `schema_migrations` ledger + heal path (§3.3).
7. `#[non_exhaustive]` + `Principal::stub()` (§3.6, `e782018de`).
8. Directory-layout FHIR models regenerated + regen runbook (§3.5, `7a0323097`).
9. Redis JTI `EXISTS` timeout, fail-closed 503, require `jti` (§3.7).
11. Slice `type` / `profile` / `binding` / `exists` / `extension` matchers, plus
    in-scope `resolve()` (`4e2b8ebc6`, `331f2ddbc`). Leftovers are §5.1 items 8–11.
12. Outbox dead-letter; zero-delivery log; SQLite claim CAS; HTS per-instance
    system-id cache (§3.7).
13. `migrate_v36_to_v37` error strings (comments still leftover — §5.1 item 16).
19. Audit sink `search_index` slot-2 columns, `SCHEMA_VERSION` 38 (§6.6).

Also closed in §6: converter content-reference + choice-branch children (§6.4);
versioned canonical lookup `Patient|4.0.1` (§6.5).

---



## 6. Follow-up work (4–6 Sep 2026)

Work on `feat-clinical-reasoning` after this audit, addressing §3.1, the
package-overlay model, and the converter defects the first clean overlay boot
exposed (§6.4). Atrius IG Draft was **not** changed. **§3.3** and **§3.4** are
closed (named schema ledger; merge playbook). **§3.2** and **§3.5** closed 5 Sep
(Postgres write TX; directory-layout regen + runbook). **§3.6** closed 5 Sep
(`Principal::stub`). **§3.7 Redis JTI** closed 5 Sep (timeout, ConnectionManager,
require `jti`). **§3.7 outbox dead-letter and zero-delivery log** closed 5 Sep.
**§3.7 SQLite claim CAS and HTS `SYSTEM_ID_CACHE`** closed 5 Sep. No remaining
§3.7 items. **6 Sep:** write-path / `$validate` holes and discriminator
completeness (§6.7). Live leftovers are **§5.1**.

### 6.1 Code and operator config

**Env (clinical HFS now uses the post-cutover variables).** Dead
`HFS_PROFILE_VALIDATION_MODE=strict` removed from the files we own for clinical
start. Replacements:

```text
HFS_FHIR_PACKAGE_CACHE=./data/fhir-packages   # or /opt/atrius/data/fhir-packages
HFS_FHIR_PACKAGES=atrius.fhir.r4.india@0.1.0  # from published IG package.json
HFS_VALIDATION_MODE=enforce
```


| File                                                  | Change                                                                                                            |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `deploy/env/hfs-clinical.env` (gitignored live local) | Relative `./data` paths (IDE `.env` does not expand `${ATRIUS_HFS_PATH}`); package cache + `enforce`              |
| `deploy/env/hfs-clinical.env.example`                 | Same variables; production-style absolute cache path in comments                                                  |
| `deploy/clinical/.env.atrius.example`                 | Same                                                                                                              |
| `deploy/clinical/.env.abdm.example`                   | **Not migrated** — still `HFS_PROFILE_MANIFEST` / `HFS_PROFILE_VALIDATION_MODE` / `HFS_PROFILE_VALIDATION_ADDONS` |
| `docs/his/fhir-native-his-plan.md`                    | Example block uses the new trio                                                                                   |
| `.gitignore`                                          | `/data/fhir-packages/`                                                                                            |


`name@version` is `atrius.fhir.r4.india@0.1.0` (IG publisher), not the
placeholder `atrius.in.r4`.

**Scripts no longer tell operators to set** `HFS_PROFILE_VALIDATION_MODE=strict`**.**


| Script                                     | Change                                                                                                                                                                                                                                                |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/setup-atrius-profile-registry.sh` | Fetches/expands published `package.tgz`, seeds `data/fhir-packages/{name}/{version}/`, prints the three env vars. `SKIP_MANIFESTS=1` skips optional audit JSON. Verifies ≥100 top-level SDs and Patient/Encounter files                               |
| `scripts/build-atrius-profile-manifest.sh` | Optional audit inventory only. No HL7 datatype/extension stitch (`manifests/deps/hl7-*` not needed — embedded R4 core pack already has `SimpleQuantity`, `patient-nationality`, …). Core filter is all top-level package `StructureDefinition-*.json` |
| `scripts/p0-import-terminology.sh`         | Echoes the new env trio                                                                                                                                                                                                                               |
| `scripts/load-atrius-ig-package.sh`        | Small alignment with the setup path                                                                                                                                                                                                                   |
| `scripts/README.md`                        | Documents cache seed, listed-package overlay, sushi `dependsOn` not loaded                                                                                                                                                                            |


**Resolver: listed packages only.** `resolve_packages` used to walk every
`package.json` `dependencies` entry and fail offline if any was missing. Boot
then died on `ndhm.in@6.5.0` (and would have died on THO, Extensions Pack, CRMI,
xver, `hl7.fhir.r4.core` next). New contract:

- Fail if a package named in `HFS_FHIR_PACKAGES` is absent from the cache.
- Do **not** walk or overlay `package.json` dependencies.
- Overlay order = list order (earlier wins in `CompositeResolver`).
- Terminology in a package is still HTS, not the schema registry.

Touched: `crates/fhir-validator/src/packages/{resolve.rs,mod.rs}`,
`crates/fhir-validator/tests/packages_tests.rs` (6 tests, passing),
`crates/fhir-validator/docs/packages.md`, `crates/rest/src/{config.rs,validation.rs}`,
`docs/validation-cutover.md`.

Rebuild/restart HFS is required for the resolver change to take effect.

### 6.2 Decisions (no IG Draft edit)

**Do not remove** `ndhm.in` **from** `sushi-config.yaml`**.** That pin is IG Publisher
authoring metadata (NDHM ValueSets, CodeSystems, four extension SDs). Atrius
profiles already parent FHIR R4; zero `atrius-in-*` `baseDefinition`s are NDHM.
HFS does not need `ndhm.in` as a schema parent. Editing only the seeded cache
`package.json` would be overwritten by the next setup run.

**Do not auto-load the NPM closure onto clinical HFS.** `HFS_FHIR_PACKAGES` is
the runtime validation surface. Sushi `dependsOn` is the publish graph.


| Package in published `package.json` | Clinical HFS                                                                                    |
| ----------------------------------- | ----------------------------------------------------------------------------------------------- |
| `atrius.fhir.r4.india@0.1.0`        | Overlay this (listed)                                                                           |
| `hl7.fhir.r4.core@4.0.1`            | Skip — embedded R4 core pack                                                                    |
| `hl7.terminology.r4@7.2.0`          | Skip — CS/VS via HTS                                                                            |
| `ndhm.in@6.5.0`                     | Skip on clinical store. List explicitly only on an ABDM export validator                        |
| Extensions Pack / CRMI / xver       | Skip unless those SDs must be on the validation surface and are not inlined in Atrius snapshots |


**Parent R4, match NDHM constraints, export-remap for ABDM — keep that split.**
`$validate` is profile-identity based. Atrius-valid storage instances are not
automatically NDHM-valid:

- Stored `meta.profile` is Atrius; NDHM rules apply after clone+remap (HIS
`ndhm_export.rs`).
- `only Reference(AtriusIn…)` vs NDHM `targetProfile` is not compared by
`scripts/ndhm-parity-diff.py`. Remapping every Bundle entry is what makes
the graph pass, not FSH parenting.
- Atrius ValueSet supersets at `required` strength can fail NDHM; extensible
is more forgiving. Extra Atrius extensions usually pass if NDHM leaves
`extension` open. Stricter Atrius cardinality helps NDHM.
- Export preflight today hits **clinical HFS** and **strips** remapped NDHM
URLs (`atrius-his` `abdm.rs`). Real `ndhm.in` `$validate` is still deferred
(`ndhm_parity.plan.md` `ndhm-export-validate`).

Inheriting `Parent: $ABDMPatient` would freeze a small document/claims IG as
the operational parent and fight CDS/QI-Core. NDHM 6.5.0 itself parents R4.

### 6.3 Related findings (not fixed)

- **HIS remap canonicals vs published** `ndhm.in` **6.5.0.** Several constants use
`…/ndhmPatient`, `…/ndhmEncounter`, `…/ndhmClaim`, … Published URLs are
`…/StructureDefinition/Patient`, `Encounter`, `Claim`. Document/payer envelope
profiles already match (`OPConsultRecord`, `DocumentBundle`, `ClaimBundle`).
A remapped instance claiming `ndhmPatient` would fail a strict NDHM validator
even with perfect field parity.
- **Embedded R4 core pack** covers the HL7 datatypes/extensions that used to be
stitched into manifests. Rare gap called out earlier: `individual-recordedSexOrGender`
is used in Atrius Patient FSH and is **not** in that embedded pack; add
`hl7.fhir.uv.extensions.r4@5.3.0` to `HFS_FHIR_PACKAGES` only if untyped
instances of that extension need structural overlay.
- **Startup still silent** on `HFS_PROFILE_`* if someone leaves them set
(backlog item 2).
- **§3.7** (Redis JTI, outbox dead-letter / zero-delivery, SQLite claim CAS,
  HTS per-instance system-id cache) — closed 5 Sep. **§3.2**, **§3.3**,
  **§3.4**, **§3.5**, **§3.6** closed separately.



### 6.4 Converter: the ~105 materialization warnings

With `HFS_FHIR_PACKAGES` finally overlaying the IG (§6.1), the first clean boot
logged `inserted=197 convert_errors=0` alongside ~105 `package materialization warning` lines. Both kinds were converter defects, not IG defects; the IG Draft
was again not changed. After the fixes: `inserted=197`**,** `convert_errors=0`**,**
`warnings=0`**.**

**Absolute** `contentReference` **was unparseable (102 of the warnings).** Snapshot
generation rewrites a base local fragment into the absolute form whenever the
element is copied into a derived profile, because a bare `#Composition.section`
would otherwise resolve against the *profile's* canonical, which does not define
the element. So R4 core carries `#Composition.section` while every Atrius
Composition profile carries
`http://hl7.org/fhir/StructureDefinition/Composition#Composition.section`. The
publisher is correct and no FSH edit can affect it — all 102 occurrences in the
package are in `snapshot`, none in any `differential`.

`parse_content_reference` did `strip_prefix('#')`, which returns `None` on the
absolute form. Effect was worse than log noise: `section.elements.section`
converted to `{array: true, short: "Nested Section"}` with **no**
`elementReference` **at all**, so sub-sections had no shape — not Atrius rules,
not even R4 `Composition.section`. Now `rsplit_once('#')`, yielding
`["Composition", "elements", "section"]`.

Note the resulting semantic, which is R4's and not something HFS chose: nested
sub-sections resolve to **base** `Composition.section`. First-level Atrius
section slices do not recurse. Constraints two levels deep must be authored
explicitly in FSH.

Verified for this corpus: all 102 targets are core HL7 base resources
(`Composition` ×64, `Bundle` ×13, `Observation` ×8, `ClaimResponse` ×6, …) and
the fragment root always equals the target's type name, which is what
`resolve_element_reference` keys on.

**Children of a choice branch were stranded (3 of the warnings).** The log line
`slice 'codeCodeableConcept': discriminator(s) [] not translatable to a match`
was misleading twice over. The IG declares the discriminator correctly
(`{type: "type", path: "$this"}` on `DeviceRequest.code[x]`), and no slice
anywhere in the package has an empty parent discriminator. It is also unrelated
to `mustSupport`, which HFS carries for the editor UI and never enforces.

The trigger was the one FSH path that goes *underneath* a choice branch,
`code[x]:codeCodeableConcept.extension:codeOptions`. `apply` treats
`code[x]:codeCodeableConcept` as a choice branch when it is the final segment,
but `descend` treated the same form as slicing, so a path with further segments
created a node named literally `code[x]` holding a slice — and `apply_choice`
never copies `ed.slicing`, hence the empty discriminator. `codeReference` has no
grandchildren, which is exactly why it never warned.

Effect, again beyond log noise: the converted schema contained a spurious
`elements["code[x]"]` whose slice carried the `codeOptions` extension, while the
real `elements["codeCodeableConcept"]` branch had no `extensions` map. Instance
JSON contains `codeCodeableConcept`, never `code[x]`, so the constraint was
silently unenforceable. `descend` now routes `<name>[x]:<branch>` to the branch
element, matching `apply`.

**Touched.** `crates/fhir-validator/src/converter/{tree.rs,mod.rs}`;
`crates/fhir-validator/tests/converter_tests.rs` plus fixtures
`profile-content-reference-absolute.json` and `profile-choice-branch-child.json`
(both assert `warnings.is_empty()` and full deep equality).

**Regression proof.** `cargo test -p helios-fhir-validator` green; the three
pinned conformance baselines match exactly under `--features R4B,R5 -- --ignored`
(R4 unchanged at 2912 resources / 210 with issues); `cargo check --workspace --all-targets` clean. Pre-existing upstream `cargo fmt` drift in
`crates/fhir-validator/src/editor.rs` is untouched and unrelated.

**Fork cost.** Both edits are in `converter/`, which upstream owns, so this
grows the merge surface that §4 argues to keep small — `converter/slicing.rs`
was already +112 lines diverged. Decision was fork-only, no upstream PR. These
are generic FHIR-conformance bugs affecting any IG with recursive backbones or
constrained choice branches, so they remain good upstream candidates if that
call is revisited.

**Not done: QI-Core** `mustSupport` **parity.** `AtriusInDeviceRequest` marks
`status`, `intent`, `code[x]`, `codeReference`, `codeCodeableConcept` as MS
where QI-Core does not. Since MS is informational in HFS, this changes no
validation behaviour and was deliberately kept separate from the fix above —
tracked as a parity decision, not a defect.

### 6.5 Versioned canonical lookup (`Patient|4.0.1`)

First Patient create after overlay+enforce failed with
`could not resolve schema 'http://hl7.org/fhir/StructureDefinition/Patient|4.0.1'`.
IG Publisher writes `baseDefinition` (and `type.profile` / `targetProfile`) as
`url|version`. The converter copies that into `schema.base`. The core pack is
indexed under the unversioned URL and the resource name. `SchemaRegistry::resolve`
was an exact-key lookup, so walking the Atrius Patient profile's base chain
emitted `unknown-schema` and enforce-mode 422'd the write.

`SchemaRegistry::resolve` now retries after stripping `|version` (same rule
terminology already used for ValueSet canonicals). The `packages_tests` example
profile's `baseDefinition` is the versioned form so this cannot regress silently.

No IG Draft change. Rebuild/restart HFS required.

### 6.6 Audit sink `search_index` slot-2 columns (`SCHEMA_VERSION` 38)

Clinical FHIR writes succeeded because `HFS_STORAGE_BACKEND=postgres-elasticsearch`
skips Postgres `search_index`. The dedicated audit database (`HFS_AUDIT_BACKEND=database`)
uses a standalone `PostgresBackend`, so every AuditEvent **does** insert index
rows. That database was already populated at the v18 layout fork
(`search_index_layout = legacy`) and had been migrated to v37 without ever
gaining `value_token_system_2` / `value_token_code_2` / `value_number_2`. Those
columns were added to `CREATE TABLE` for new databases (#279) but never
`ALTER TABLE`'d onto existing ones. The writer always binds them, so the insert
failed with Postgres `42703`; `tokio_postgres::Error`'s `Display` is `"db error"`,
which is what the audit sink logged.

v38 `ADD COLUMN IF NOT EXISTS`s the three nullable slot-2 columns on every
layout. `classify_postgres_error` now walks `source()` so the next catalog
mismatch names the column. Restart HFS against the audit URL to migrate
in place; do not dump `HFS_AUDIT_DATABASE_URL` in logs or tickets.

### 6.7 Write-path `$validate` holes and discriminator completeness (6 Sep 2026)

`4e2b8ebc6` closed the five remaining single-engine write / `$validate` gaps
listed under §3.4: bulk-submit `IngestValidator` / `check_write`, transaction
DELETE existence, bundle PATCH + `check_write`, `$validate` modes, and
type/profile/binding slice matchers.

`331f2ddbc` completed discriminator coverage (`exists`, `extension('url')`,
reslicing) and in-scope `resolve()` (`contained` + Bundle `entry.resource` only
— no store). Binding ValueSet expand at mark time, mixed-kind discriminator
sets, and store-backed `resolve()` stay out of this engine; see §5.1 items 8–11.
Do not restore `fhir-validation*`. `position` is ordered-slice declaration
order, not a FHIR discriminator.
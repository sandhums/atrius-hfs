# Fork change: ICD-safe `$expand` / `$subsumes` / `/import` / DELETE (2026-09-20)

**Status:** Local, uncommitted Atrius changes on `helios-hts`. Keep this file
when merging `upstream/main`. Intended for upstream contribution to
[HeliosSoftware/hfs](https://github.com/HeliosSoftware/hfs) once reviewed.

**Why this exists:** Atrius re-axised dQM ValueSets onto WHO ICD-10
(`http://hl7.org/fhir/sid/icd-10`) and loaded ICD-11 MMS plus the three
ICD-10↔ICD-11 ConceptMaps through `POST /import`. Four HTS defects then showed
up that SNOMED RF2 / ICD-10-CM CLI importers never hit. Every one of them
returned a *wrong-but-plausible* answer, or aborted the process, rather than a
clear error. They will reappear if this patch is dropped on an upstream merge
and the ICD loaders are re-run. Item 5 is the matching HFS `:in` miss on the
same Diabetes ValueSet once `$expand` is healthy.

**Not in this document:** HIS loaders (`claml_to_fhir.py`, `mms_to_fhir.py`,
`mapping_to_conceptmap.py`), the ValueSet re-axis script, or IDSP `A97`
corrections. Those live in `atrius-his` / `AtriusIGDraft`. Items 1–4 are the
HTS Rust surface. Item 5 is the HFS *consumer* of `$expand` (search `:in`);
it lives next to this file because losing it looks like an HTS expand
regression on the same Diabetes ValueSet.

**Related Atrius docs:**

- [`fork-ecl-fts-typeahead-expand.md`](fork-ecl-fts-typeahead-expand.md) — earlier
  Atrius `$expand` fork (ECL + FTS typeahead). Same file, different patch.
- [`expand-paths-architecture.md`](expand-paths-architecture.md) — full `$expand`
  routing map. The `is-a` fast path below is a new branch on that map.
- `atrius-his/scripts/icd/README.md` — load order, `--replace`, `?finalize=true`.
- `atrius-his/scripts/icd/verify-icd-load.py` — 22 post-load invariants. Run
  after any HTS rebuild or upstream merge that touches these files.
- `AtriusIGDraft/docs/dqm-adoption-plan.md` §11.5 — why the ValueSets need this.

---

## What to keep vs adopt on an upstream merge

Direction is **checkout Atrius → merge `upstream/main` into it**. For each item
below: keep ours unless upstream shipped an equivalent that already covers the
same symptom. If they did, drop ours and note it here.

| # | Symptom if this patch is lost | Keep / conflict hotspot |
|---|-------------------------------|-------------------------|
| 1 | Unbounded `$expand` of any `is-a` / `descendent-of` ValueSet whose codes contain `.` or `-` (ICD-10 `E11.3`, LOINC `718-7`) returns **HTTP 400** `Invalid ECL expression: Unexpected character: '.'`. Bounded `count=` still works, so IG Publisher / validators that omit `count` fail and typeahead that passes one does not. | `backends/sqlite/value_set.rs` `apply_compose_filters` — same file as the FTS typeahead fork. Upstream still routes these through the ECL parser. **Keep ours.** Postgres already has its own CTE/`is-a` path and was not changed. |
| 2 | Unbounded hierarchical `$expand` covering a SNOMED mutual parent/child pair (`310387003` ⇄ `707221002`) **aborts the process** (stack overflow). CMS165 `is-a` ValueSets (e.g. pregnancy, 39 ICD-10 blocks) default to tree mode when `$expand` omits `count`/`excludeNested`, which is how the sidecar evaluates measures. | Same file, `build_subtree` / `build_hierarchical_expansion` (iterative heap walk + path-local cycle skip). Postgres `build_subtree` has the same walk. **Keep ours.** A global visited-set from upstream would be the wrong fix — these are poly-hierarchies. |
| 3 | Chunked `POST /import` of a CodeSystem into a *running* server leaves `concept_closure` empty. `$subsumes` returns `not-subsumed` for a genuine parent/child; `is-a` `$expand` still works (it walks `concept_hierarchy`). Split-brain. | `operations/import_bundle.rs` (`?finalize=true`), `import/mod.rs` (`BundleImportBackend::rebuild_missing_closures`), sqlite/postgres `mod.rs` + `schema.rs` (return `usize`). Trait default is `Ok(0)`, so adding the method is merge-safe; the `Query(ImportParams)` extractor on `import_handler` will conflict if upstream also changed the signature. **Keep ours.** |
| 4 | `DELETE /CodeSystem/{id}` of a resource imported via `/import` returns **HTTP 500** `resource not found`. Imported content is permanently undeletable; `/import` only upserts, so retired codes linger forever. | `operations/crud.rs` `delete_resource`. **Keep ours.** FHIR DELETE is idempotent — absent is 204. |
| 5 | HFS `Condition?code:in=…/atrius-vs-diabetes` returns **total=0** even though `$validate-code` for `44054006` is true and GET `$expand?count=10000` lists all 546 codes. CMS951 (and any retrieve on a ValueSet larger than the HTS default page) scores IP empty. Essential Hypertension is 16 codes and still works, which is how this looked like a measure/CQL flake. | `crates/terminology-client` `expand_value_set` (page `$expand`) and `crates/persistence` SQLite token OR chunking. **Keep ours.** HTS paging when `count` is omitted is FHIR-legal; the client must walk `expansion.total`. |

A related behaviour is **not a code change** and will not show up in the diff,
but it is the thing most likely to look like a regression after a
merge-and-reload:

> HTS serves `$expand` / `$validate-code` from the precomputed
> `value_set_expansions` table, not by evaluating `compose` live. Deleting or
> replacing a CodeSystem invalidates those rows and nothing rebuilds them. The
> ValueSet `GET` still shows the `is-a` filters; `$expand` silently loses that
> arm. **Re-import the authored ValueSets after any CodeSystem `--replace`.**
> `verify-icd-load.py` §7 asserts this.

---

## Files touched (merge checklist)

```
crates/hts/src/backends/sqlite/value_set.rs     # 1, 2
crates/hts/src/backends/sqlite/mod.rs           # 3
crates/hts/src/backends/sqlite/schema.rs        # 3 (migrate_concept_closure → usize)
crates/hts/src/backends/postgres/mod.rs         # 3
crates/hts/src/backends/postgres/schema.rs      # 3 (migrate_concept_closure_pg → usize)
crates/hts/src/import/mod.rs                    # 3 (trait method + default)
crates/hts/src/operations/import_bundle.rs      # 3 (Query param + test)
crates/hts/src/operations/crud.rs               # 4
crates/terminology-client/src/lib.rs            # 5 (page $expand for :in)
crates/persistence/src/backends/sqlite/search/query_builder.rs  # 5 (token OR chunks)
crates/rest/tests/terminology_integration.rs    # 5
```

**Not changed:** `backends/postgres/value_set.rs` (already has a non-ECL `is-a`
path), ECL parser itself, RF2 / ICD-10-CM CLI importers.

Signature changes that will fail to compile if only half-merged:

| Symbol | Before | After |
|--------|--------|-------|
| `schema::migrate_concept_closure` | `-> rusqlite::Result<()>` | `-> rusqlite::Result<usize>` |
| `schema::migrate_concept_closure_pg` | `-> Result<(), _>` | `-> Result<usize, _>` |
| `Sqlite/PostgresTerminologyBackend::rebuild_missing_closures` | `-> Result<(), HtsError>` (inherent, unused by `/import`) | `-> Result<usize, HtsError>` + trait impl |
| `import_handler` | `State(state), body: Bytes` | `State(state), Query(params), body: Bytes` |
| `ImportResponse` | no `closures_rebuilt` | `closures_rebuilt: usize` (omitted from JSON when 0) |

---

## 1. Unbounded `is-a` / `descendent-of` no longer goes through ECL

**Where:** `apply_compose_filters` in `backends/sqlite/value_set.rs`, just
before the `ecl::parse_and_evaluate` slow path.

**What was wrong:** the unbounded path (no prior candidate set — i.e. `$expand`
without `count`) compiled every `concept` / `is-a` / `descendent-of` filter
into an ECL expression and handed it to a parser that only accepts numeric
SCTIDs. Dotted or dashed codes (`E11`, `E11.3`, `A00-A09`, `718-7`) returned
HTTP 400. The bounded path already used `concept_closure` and was fine, which
is why this looked like an `$expand` flake that only IG Publisher hit.

**Fix:** when `property` is `concept` and `op` is `is-a` or `descendent-of`,
walk `concept_closure` via a new `query_descendants_full` (mirror of the
existing `query_ancestors_full` used by `generalizes`). ECL remains the path
for actual `constraint` filters.

This **depends on `concept_closure` being populated**. A system imported by
bundle without `?finalize=true` (item 3) will now expand `is-a` to just the
root, which is a quieter failure than the 400 — `verify-icd-load.py` §4 and §8
catch it.

---

## 2. Path-local cycle guard in hierarchical `$expand`

**Where:** `build_hierarchical_expansion` / `build_subtree` in the same file.

**What was wrong:** `concept_hierarchy` is not acyclic. SNOMED RF2 imports
leave mutual parent/child pairs (the one that took the server down was
`310387003` ⇄ `707221002`, diabetic intracapillary glomerulosclerosis /
diabetic glomerulosclerosis). `build_subtree` recursed with no cycle check;
any unbounded `$expand` whose member set covered such a pair overflowed the
worker stack and aborted the process.

**Fix:**

- `build_subtree` takes a `path: &mut HashSet<(system, code)>` of the current
  ancestor chain. An edge back onto `path` is skipped.
- The guard is **path-local, not global**. These are poly-hierarchies: the
  same concept legitimately nests under several parents. A global visited set
  would drop those copies and shrink the tree.
- If the edge set is a cycle with no external entry, every member has a parent
  inside the expansion and there is no root. Fall back to the flat list rather
  than returning empty.

**Do not "fix" this with a global `visited`.** The regression tests exist
specifically to stop that:

- `build_subtree_terminates_on_cyclic_hierarchy`
- `build_subtree_keeps_shared_concept_under_every_parent`

**Count trap when re-verifying:** unbounded hierarchical `$expand` nests a
concept under every parent, so node count ≫ distinct `(system, code)`.
`atrius-vs-diabetes` is 546 distinct concepts across 5,427 nodes. Compare
sets, never lengths. `verify-icd-load.py` §8 does this.

---

## 3. `POST /import?finalize=true` rebuilds missing closures

**Where:** `import_bundle.rs`, `import/mod.rs`, both backends' `schema.rs` /
`mod.rs`.

**What was wrong:** `write_code_system` deletes the system's `concept_closure`
rows on every call and only rebuilds when it finds the system *empty*. That is
deliberate — rebuilding after every SNOMED chunk is hours. The rebuild was
deferred to the startup migration.

That leaves a gap for a **chunked import into an already-running server**:

1. Chunk 1 creates the system with no concepts → no hierarchy, nothing to close.
2. Chunk 2 inserts the first parent/child → system was empty, closure is built.
3. Chunk 3 inserts more children → system is non-empty, closure rows are
   deleted, rebuild is skipped.
4. Server never restarts, so the startup migration never runs.

Result: `concept_hierarchy` is populated (`is-a` `$expand` works) and
`concept_closure` is empty (`$subsumes` returns `not-subsumed`). FHIRPath
`subsumes()` and HIS dual-coding that depends on it fail silently.

**Fix:**

- `BundleImportBackend::rebuild_missing_closures() -> Result<usize>` walks
  every system that has hierarchy edges and no closure rows, same query as the
  startup migration. Default impl returns `Ok(0)`.
- SQLite runs it on a blocking thread and invalidates caches when it did work.
  Postgres delegates to the existing `migrate_concept_closure_pg`.
- Both migrations now return the number of systems rebuilt so the response can
  say whether anything happened.
- `POST /import?finalize=true` calls it after the bundle write. Loaders pass
  this on their **last chunk only**. A call on an up-to-date database is one
  query.
- `ImportResponse.closures_rebuilt` is the count (omitted from JSON when 0).
  A rebuild failure is non-fatal (concepts are stored either way; startup will
  still rebuild) and is appended to `errors`, which flips the status to 207.

Regression test: `finalize_rebuilds_closure_dropped_by_a_chunked_import` —
three chunks, `$subsumes` is `not-subsumed` after chunk 3, `subsumes` after
`?finalize=true`, and the response reports `closures_rebuilt: 1`.

**Loader contract (HIS, not HTS):** both `claml_to_fhir.py` and
`mms_to_fhir.py` pass `?finalize=true` on the last chunk and print
`rebuilt concept closure for N system(s)`. If that line is missing after a
reload, `$subsumes` is dead even though this HTS patch is present.

---

## 4. DELETE of `/import`-only resources is idempotent 204

**Where:** `delete_resource` in `operations/crud.rs`.

**What was wrong:** `/import` writes HTS normalized tables only
(`code_systems`, `concepts`, `concept_hierarchy`, …). It never inserts a row
in `helios-persistence`. `DELETE` insisted on a persistence row, surfaced the
store's not-found as HTTP 500, and never reached normalized-table cleanup.
There was then no way to drop a code that disappeared upstream, because
`/import` only upserts. That is how 172 fabricated ICD-10 codes (`M00.3` and
friends) survived a loader fix that no longer emitted them.

**Fix:**

1. Read the persistence store to recover a canonical URL *if* a row is there.
2. Soft-delete in persistence **only when** `in_store`.
3. Always run normalized-table cleanup (SQLite `hts_delete_cs` / `_vs` / `_cm`,
   or `terminology_importer.delete_normalized`).
4. Evict expand caches either way.
5. Return **204 No Content**, including for an id that exists nowhere.

Tests:

- `cs_delete_removes_hts_normalized_rows` (existing; still the create-via-POST path)
- imported-id DELETE now 204 (new; the `/import` then `DELETE /CodeSystem/imported-cs` case)
- `delete_of_absent_code_system_is_idempotent` (new; `never-existed` → 204)

`claml_to_fhir.py --replace` depends on this. Without it, `--replace` 500s and
the operator is stuck with the old CodeSystem.

---

## 5. HFS `:in` must page `$expand` (not take the first 16 codes)

**Where:** `TerminologyClient::expand_value_set` in
`crates/terminology-client/src/lib.rs` (used by REST search `:in` / `:below` /
`:above`). SQLite follow-up in
`crates/persistence/src/backends/sqlite/search/query_builder.rs`.

**What was wrong:** HFS rewrites `code:in=<ValueSet URL>` to
`code=system\|c1,system\|c2,…` after `POST /ValueSet/$expand` with only a
`url` parameter. Atrius HTS, when `count` is omitted, returns a **16-code
page** and still sets `expansion.total` to the full size (546 for
`atrius-vs-diabetes`). The client treated that page as the whole set.

`44054006` (fixture type 2 diabetes) is not among those 16, so
`Condition?code:in=…/atrius-vs-diabetes` is empty. CMS951 IP is then false
for every patient who should qualify. CMS165 still passed because Essential
Hypertension expands to exactly 16 codes.

GET `$expand?count=10000` and `$validate-code` both already knew the code
was in the set — so this looked like a Keycloak / CQL / fixture bug. The
sidecar MeasureReport was `status=complete` with Encounter and labs in
`evaluatedResource` and **no Condition**.

A second landmine once the full 546 tokens reach SQLite: each `system|code`
is two binds, OR'd as `(X) OR (Y)`. That parse tree blows up around 1000
nodes (#943). 546 × 2 is past that.

**Fix:**

- `expand_value_set` / `expand_subsumption` send `count=10000` and follow
  `offset` until `collected >= expansion.total` (or the page is short / empty).
  Cap at 1_000_000 codes.
- Cache key is `post|{hts-base}|{url}` so two HTS bases in one process do not
  share a truncated page.
- SQLite folds token OR lists into 250-value `IN` subqueries and ORs those
  membership clauses (AND of `NOT IN` for `:not`).

**Do not "fix" this by raising the HTS default page and leaving the client
unpaged.** FHIR `$expand` is allowed to page; the next 2_000-code VS will
miss again.

Tests:

- `helios-terminology-client`: `should_fetch_next_expand_page` /
  `expansion_total`
- `helios-persistence --lib`: `large_token_or_is_chunked_into_separate_in_subqueries`
- `helios-rest --test terminology_integration`: `test_in_modifier_pages_hts_expand`

Live check (HFS `:8082`, HTS `:9091`, tenant `atrius-hospitals`, after the
CMS951 fixture seed):

```text
# must be 1, not 0 — 44054006 is in the VS and on the patient
GET /Condition?patient=cms951-numerator-panel&code:in=https://atrius.in/fhir/r4/atrius-in/ValueSet/atrius-vs-diabetes

python3 scripts/measure-eval-runner.py --measure cms951
```

---

## How to re-verify after a merge

From a running HTS (`http://127.0.0.1:9091`) that has the ICD stack loaded:

```bash
# Unit + the four new regressions (no HTS process needed)
cargo test -p helios-hts --lib \
  finalize_rebuilds_closure_dropped_by_a_chunked_import \
  build_subtree_terminates_on_cyclic_hierarchy \
  build_subtree_keeps_shared_concept_under_every_parent \
  delete_of_absent_code_system_is_idempotent

# Full crate
cargo test -p helios-hts --lib          # 716 passed at time of writing

# Item 5 (HFS :in paging — no running HFS needed)
cargo test -p helios-terminology-client
cargo test -p helios-persistence --lib large_token_or_is_chunked
cargo test -p helios-rest --test terminology_integration

# Live invariants (needs HTS + loaded ICD-10 / ICD-11 / ConceptMaps / ValueSets)
python3 /Users/sandhu/RustroverProjects/atrius-his/scripts/icd/verify-icd-load.py
```

If `verify-icd-load.py` §4 (`$subsumes E11 ⊃ E11.3`) fails after a merge that
kept this patch, the CodeSystem was re-imported without `?finalize=true`. If
§7 (authored ValueSets still have an ICD arm) fails, the CodeSystem was
`--replace`d and the ValueSets were not re-imported. Neither is an HTS code
regression.

Manual smoke, if you do not want the Python harness:

```text
# 1. dotted is-a, no count — must not 400
GET /ValueSet/$expand?url=…/atrius-vs-diabetes

# 2. $subsumes on a bundle-imported system — must be "subsumes", not "not-subsumed"
GET /CodeSystem/$subsumes?system=http://hl7.org/fhir/sid/icd-10&codeA=E11&codeB=E11.3
GET /CodeSystem/$subsumes?system=http://id.who.int/icd/release/11/mms&codeA=5A13&codeB=5A13.2

# 3. DELETE of an imported CodeSystem — must be 204, not 500
DELETE /CodeSystem/icd-10          # then re-import; do not leave this down

# 4. HFS :in must see codes past the first $expand page (needs HFS + seeded fixture)
GET HFS /Condition?patient=cms951-numerator-panel&code:in=…/atrius-vs-diabetes
# total=1, id cms951-numerator-panel-dm — not total=0
```

---

## Upstream contribution notes

These five are independent and can be separate PRs, in this order, if Helios
wants them separately:

1. **Cycle guard** — smallest, SNOMED-only, process-killing. No ICD needed to
   demonstrate. Tests are pure in-memory.
2. **ECL bypass for `is-a` / `descendent-of`** — needs any dotted-code system
   (LOINC is enough if they do not want ICD-10 in the fixture).
3. **`?finalize=true`** — the test is self-contained (synthetic CodeSystem, no
   terminology download). Highest leverage for anyone doing chunked `/import`.
4. **Idempotent DELETE** — FHIR-correct regardless of import; the `/import`-only
   hole is the motivation.
5. **Client-side `$expand` paging for `:in`** — independent of HTS. Any FHIR
   server that pages `$expand` when `count` is omitted will hit this. The
   SQLite OR-chunk is a separate one-file PR if they already page.

Item 3's trait method has a default, so it does not force every backend to
change. Item 1 must stay path-local. Item 5 must stay paged on the client —
do not "fix" it by only raising the HTS default `count`.

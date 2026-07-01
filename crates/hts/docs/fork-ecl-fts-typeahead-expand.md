# Fork change: ECL + text `$expand` — FTS-first typeahead (Atrius)

**Status:** Local enhancement on `helios-hts` (Atrius stack). Intended for upstream
contribution to [HeliosSoftware/hfs](https://github.com/HeliosSoftware/hfs).

**Motivation:** Clinical UI SNOMED typeahead (chief complaint, assessment) calls
`ValueSet/$expand` with an intensional ValueSet (ECL in `compose.filter`) plus a
`filter` text parameter. Before this change, HTS:

1. Materialised the **full ECL expansion** (hundreds of thousands of concepts).
2. Applied a substring filter in Rust.
3. Returned the first N rows sorted by **concept id**.

That produced poor typeahead UX (e.g. `filter=headache` surfaced long FSNs like
“Headache due to external compression of head” before “Headache”). The SNOMED
International demo uses description search with relevance ranking inside an ECL
constraint; this patch approximates that behaviour within HTS SQLite.

**Consumer:** `atrius-bff` → `GET /bff/terminology/snomed/search` → HTS
`POST /ValueSet/$expand` with IG ValueSet
`https://atrius.in/fhir/r4/atrius-in/ValueSet/atrius-reason-for-encounter`.

---

## Summary of behaviour after patch

For `$expand` requests with **both**:

- a compose `filter` containing ECL (`property: constraint`, `op: =`), and
- a text `filter` parameter (≥ 3 characters),

HTS uses an **FTS-first** path on **both** inline compose bodies and **stored**
ValueSet URLs (e.g. `url=https://atrius.in/.../atrius-reason-for-encounter`):

```text
concepts_search_fts MATCH filter
    → rank by bm25 + clinical heuristics
    → ecl::filter_candidates (closure-table membership, no full ECL expand)
    → paginate / return
```

When a stored ValueSet is served from the **expansion cache** (full materialised
list), the substring filter path applies `sort_typeahead_candidates` before pagination.

Synonym designations (`concept_designations`) are indexed so matches on synonyms
still return the concept’s **preferred display** from `concepts.display`.

---

## Files touched (merge checklist)

| File | Change |
|------|--------|
| `src/backends/sqlite/schema.rs` | New virtual table `concepts_search_fts` |
| `src/backends/sqlite/mod.rs` | Clear `concepts_search_fts` on FTS rebuild (startup / post-import) |
| `src/backends/sqlite/value_set.rs` | FTS-first routing for ECL+filter; ranked search; synonym index build |
| `src/ecl/evaluator.rs` | New `filter_candidates()` — batch ECL membership via `concept_closure` |
| `src/ecl/mod.rs` | Export `filter_candidates` |
| `tests/ecl_expand.rs` | Integration tests for ECL + text filter ranking |

**Not changed:** Postgres backend (`backends/postgres/value_set.rs`) — SQLite only.

---

## Schema addition

```sql
CREATE VIRTUAL TABLE concepts_search_fts USING fts5(
  system_id UNINDEXED, code UNINDEXED, term,
  tokenize='trigram case_sensitive 0'
);
```

Populated in `populate_concepts_search_fts_for_system()` (called from
`ensure_concepts_fts` and `prebuild_concepts_fts`):

- One row per concept: `term = concepts.display`
- One row per designation: `term = concept_designations.value`, `rowid = -cd.id`

Requires **`concept_closure`** to be populated (already done on SNOMED RF2 import).

---

## New public API (sqlite)

```rust
// src/ecl/mod.rs
pub fn filter_candidates(
    conn: &Connection,
    system_id: &str,
    ecl: &str,
    candidates: &[String],
) -> Result<HashSet<String>, HtsError>;
```

Evaluates ECL as set logic (AND / OR / MINUS / focus operators) but only returns
codes present in `candidates`, using closure joins instead of full expansion.

---

## Key functions (value_set.rs)

| Function | Role |
|----------|------|
| `expand_inline_filtered` | Routes ECL+filter to FTS-first when `has_ecl_constraint` |
| Stored VS URL path (`expand` spawn_blocking) | Calls `expand_inline_filtered` when `filter` present (cold cache); sorts cached expansions |
| `fts_candidates_ranked_for_system` | FTS query + bm25 + preferred display lookup |
| `sort_typeahead_candidates` / `typeahead_match_score` | Clinical term ranking heuristics |
| `populate_concepts_search_fts_for_system` | Builds synonym-aware search index |
| `apply_compose_filters_to_candidates` | Now handles `("constraint", "=")` via `ecl::filter_candidates` |

---

## Operational notes

- **Restart HTS** after deploy so `prebuild_concepts_fts` rebuilds
  `concepts_search_fts` (~10–25 s for full SNOMED).
- No separate migration beyond existing schema apply ( `CREATE VIRTUAL TABLE IF NOT EXISTS` ).
- BFF may still re-rank / over-fetch results; HTS ranking is authoritative for
  single-ValueSet ECL expands.

---

## Tests

```bash
cargo test -p helios-hts --features sqlite --test ecl_expand
```

New cases:

- `ecl_constraint_with_text_filter_ranks_short_terms_first`
- `ecl_or_with_text_filter_returns_ranked_matches`

---

## Upstream merge notes

1. Update the existing comment block at `expand_inline_filtered` “Routing: FTS-first
   vs property-first” — it previously stated ECL constraints are not batchable;
   this patch adds ECL batch filtering via `filter_candidates`.
2. Consider porting `filter_candidates` to Postgres when ECL expand is implemented there.
3. Ranking heuristics mirror SNOMED Search and Data Entry Guide priorities; bm25
   ordering could be tuned against SNOMED browser fixtures if Helios adds conformance tests.

**Related Atrius docs:** `atrius-clinical-ui/docs/snomed-structured-data-entry-plan.md`,
[`expand-paths-architecture.md`](expand-paths-architecture.md) (full `$expand` routing map).

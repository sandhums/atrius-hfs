# ValueSet `$expand` — path architecture (Helios HTS + Atrius fork)

**Status:** Reference doc for Atrius stack maintainers. Describes how HTS routes
`ValueSet/$expand` requests in the SQLite backend, what Helios shipped upstream,
what the Atrius fork added, and where simplification is (and is not) warranted.

**Related:**

- [`fork-ecl-fts-typeahead-expand.md`](fork-ecl-fts-typeahead-expand.md) — Atrius
  ECL + text filter / typeahead ranking patch
- `atrius-clinical-ui/docs/snomed-structured-data-entry-plan.md` — clinical UI
  consumer (BFF → HTS `$expand` with `filter`)

---

## Three layers

Every `$expand` request passes through three layers before codes are returned:

```text
HTTP handlers (4 endpoints)
    ↓
operations/expand.rs  (parameter parsing, tx-resource shortcut, response cache)
    ↓
backends/sqlite/value_set.rs :: expand()
```

| Layer | Role | Distinct paths |
|-------|------|----------------|
| **HTTP** | `POST/GET` type-level + instance-level | **4** entry points (all call one backend) |
| **Operations** | Parse Parameters, `tx-resource` shadowing, serialized FHIR response cache | **~3** (handler cache hit · inline cache hit · call backend) |
| **SQLite backend** | Actual expansion logic | **~12** ways to produce a response |

Postgres (`backends/postgres/value_set.rs`) has a **separate, simpler** backend
(mostly warm cache vs cold compute). Atrius runs **SQLite only**.

---

## SQLite `expand()` — ~12 response paths

These are the paths that **return** an `ExpandResponse` (or error early).

### A. Async in-memory hot paths (4) — no DB thread

Checked **before** `spawn_blocking`, in order:

| # | Path | When |
|---|------|------|
| 1 | **`implicit_index`** | URL-based implicit VS (`?fhir_vs`), warm in-memory index |
| 2 | **`inline_compose_index`** | Inline compose body, unfiltered, warm |
| 3 | **`property_result_cache`** | Inline + text filter + `property=` compose filters (EX08) |
| 4 | **`plain_fts_cache`** | Inline + text filter + plain full-system includes (EX07) |

All use `page_in_memory()` → **concept-id / import order**, not clinical
ranking (unless the index was built from ranked data).

### B. `spawn_blocking` early exits (~7)

**Inline compose body** (`req.value_set` set):

| # | Path | When |
|---|------|------|
| 5 | **Implicit compose DB cache page** | Warm `implicit_expansion_cache`, bounded, no filter |
| 6 | **BFS single hierarchy** | One `is-a` / `descendent-of` include |
| 7 | **BFS multi-include hierarchy** | OR of simple hierarchy includes |

**Stored ValueSet URL** (`req.url` resolves in DB):

| # | Path | When |
|---|------|------|
| 8 | **`compose_page_fast`** | Extensional VS with embedded codes (VSAC-style) |
| 9 | **`expand_inline_filtered`** | Stored VS + text filter — **Atrius fork** (FTS-first + ECL) |
| 10 | **`compute_expansion_with_versions`** | Full materialisation (cold or version-pinned) |
| 11 | **`fetch_cache`** | Pre-materialised `value_set_expansions` row |

**Implicit VS** (URL not in DB → CodeSystem fallback):

| # | Path | When |
|---|------|------|
| 12 | **BFS cold page** | `bfs_expand_page` while cache populates in background |
| 13 | **Implicit in-memory index** | Warm `implicit_expansion_cache` |
| 14 | **Implicit SQL page** | Fallback if in-memory lock fails |

*(Numbers overlap slightly depending on whether stored-VS sub-branches are
counted separately; the important point is **~12 distinct serve strategies**.)*

### C. Common tail (2)

After building `all_codes` (when no early return):

| # | Path | When |
|---|------|------|
| 15 | **Filter + `sort_typeahead_candidates` + paginate** | Flat list; cached stored VS + filter lands here |
| 16 | **`build_hierarchical_expansion`** | Tree mode (`hierarchical=true`) |

---

## Inside `expand_inline_filtered` — filtered expand strategies

Once on the filtered path, routing per include:

| Strategy | When |
|----------|------|
| **Plain multi-system FTS** | All includes are bare `system` (no ECL) |
| **FTS-first + ECL/hierarchy filter** | ECL `constraint` or batchable filters + filter ≥ 3 chars ← **clinical typeahead** |
| **Property-first** | `property=` filters (HL7 package fixtures) |
| **Explicit concept list / SQL LIKE fallback** | Enumerated codes, short filters |

---

## Stored URL vs inline — why the headache bug happened

Chief-complaint search uses a **stored URL**:

```http
POST /ValueSet/$expand
  url = https://atrius.in/fhir/r4/atrius-in/ValueSet/atrius-reason-for-encounter
  filter = headache
```

That request **never** hit inline paths (1–4, 5–7).

| Before Atrius fix | After Atrius fix |
|-------------------|------------------|
| `compute_expansion` or cached full list → substring filter → **concept-id order** | **`expand_inline_filtered`** (cold) or **`sort_typeahead_candidates`** (warm cache) |

Inline POST with the same compose already used **`expand_inline_filtered`** — the
fork “worked” in isolation but not in the UI because the BFF passes `url=`, not
an inline compose body.

---

## Helios upstream vs Atrius fork

Baseline: upstream Helios commit `870cc75f1` (“full HTS + performance benchmarks”).

### What Helios already had

| Item | Count / detail |
|------|----------------|
| Top-level `$expand` exits (SQLite) | **~12** (same as today) |
| Async in-memory hot paths | **4** (EX03/06/07/08) |
| `expand_inline_filtered` strategies | **~4** (plain FTS, property cache, FTS-first for batchable hierarchy, property-first / full ECL) |
| ECL + text filter | **Full ECL materialisation** — comments explicitly said ECL `constraint` is *not* batchable |
| Stored URL + filter | Full expand → substring filter → concept-id order |
| `expand_inline_filtered` wiring | **Inline compose only**, not stored URLs |

### What Atrius added (local fork, see `fork-ecl-fts-typeahead-expand.md`)

| Change | New top-level path? |
|--------|-------------------|
| `filter_candidates()` — batch ECL membership on candidate set | No |
| `concepts_search_fts` + synonym indexing | No |
| `fts_candidates_ranked_for_system` + `sort_typeahead_candidates` | No |
| **ECL FTS-first** branch inside `expand_inline_filtered` | **+1 strategy** (5th inside filtered expand) |
| Stored URL + `filter` → `expand_inline_filtered` | **Wiring fix** |
| Cached stored expansion + `filter` → `sort_typeahead_candidates` | **Wiring fix** |

**Net:** **0 new top-level paths**, **1 new filtered strategy**, **2 wiring fixes**.

Rough diff vs upstream baseline: **~315 lines** in `value_set.rs` (ranking, ECL
batch filter, stored-URL hook).

---

## Ranking-aware paths

Only these paths produce **clinically sensible typeahead order** (short/common
terms first):

1. **`expand_inline_filtered`** — FTS-first branch (ECL + filter ≥ 3)
2. **Common filter tail** — `sort_typeahead_candidates` after substring filter

All other paths return **concept-id or import order**. That is acceptable for
paginated hierarchy expands and IG conformance tests; it is **not** acceptable
for SNOMED clinical typeahead without an extra ranking step.

---

## Should this be simplified?

### Do not remove Helios performance paths

BFS, expansion caches, and async hot paths exist for good reasons:

- Avoid >30 s full ECL materialisation on large SNOMED hierarchies
- IG conformance / load benchmarks (EX01–EX08)
- Extensional VSAC paging without loading entire ValueSets

Collapsing these into one generic path would regress performance and tests.

### Do simplify the model

| Recommendation | Verdict |
|----------------|---------|
| **One filtered-expand entry for all compose sources** | Yes — inline and stored URL should both call the same helper; only compose resolution differs |
| **Central ranking after every text `filter`** | Yes — prevents “works inline, broken by URL” regressions; in-memory hot paths still need this |
| **Contribute fork upstream to Helios** | Yes — reduces long-term fork surface |
| **BFF workaround: POST inline compose instead of `url=`** | Possible short-term; not a substitute for backend unification |
| **Delete cache/BFS paths** | No |

### Target architecture (conceptual)

```text
resolve compose (inline body | stored URL | implicit VS)
    ↓
if filter present:
    expand_inline_filtered(compose, filter)   ← single canonical filtered path
else if bounded + simple hierarchy:
    BFS fast paths
else if cache warm:
    cache / in-memory index
else:
    compute_expansion_with_versions
    ↓
if filter present on any fallback path:
    sort_typeahead_candidates
    ↓
paginate or build tree
```

---

## Operational notes

- **Restart HTS** after deploying fork changes; FTS index rebuild on startup
  takes ~1–2 minutes for full SNOMED (`concepts_search_fts`).
- Verify typeahead ordering:

  ```bash
  curl -s -X POST 'http://127.0.0.1:9091/ValueSet/$expand' \
    -H 'Content-Type: application/fhir+json' \
    -d '{"resourceType":"Parameters","parameter":[
      {"name":"url","valueUri":"https://atrius.in/fhir/r4/atrius-in/ValueSet/atrius-reason-for-encounter"},
      {"name":"filter","valueString":"headache"},
      {"name":"count","valueInteger":8}
    ]}'
  ```

  First hit should be **Headache** (25064002), not a long FSN.

---

## Key source files

| File | Role |
|------|------|
| `src/operations/expand.rs` | HTTP handlers, handler cache, `tx-resource` shortcut |
| `src/backends/sqlite/value_set.rs` | `expand()`, hot paths, `expand_inline_filtered`, ranking |
| `src/ecl/evaluator.rs` | `filter_candidates()` |
| `src/backends/sqlite/schema.rs` | `concepts_search_fts` virtual table |

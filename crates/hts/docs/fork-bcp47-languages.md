# Fork change: BCP 47 languages without a CodeSystem (2026-09-22)

**Status:** Local Atrius change on `helios-hts`, commit `3ecac00f4` on
`feat-clinical-reasoning`. Keep this file when merging `upstream/main`.
Intended for upstream contribution to
[HeliosSoftware/hfs](https://github.com/HeliosSoftware/hfs) once reviewed.

**Why this exists:** Startup and `$expand` logged

```text
CodeSystem urn:ietf:bcp:47 was not found and has been excluded from the expansion
```

`urn:ietf:bcp:47` is not in the terminology database, and it should not be
imported as a curated CodeSystem. FHIR already published the finite lists.
The open set is a grammar, the same way `urn:ietf:bcp:13` (MIME types) is
handled in `bcp13.rs`. Dropping this patch brings the warning back and makes
`languages` / `written-language` / `Languages` expand to nothing.

**Not this patch:** designation language matching (`language.rs`, RFC 4647
lookup for `de` vs `de-DE`). That is already separate. BCP-13 / `mimetypes`
is [`ig-publisher-compatibility.md`](ig-publisher-compatibility.md) §4.

**Related Atrius docs:**

- [`fork-ecl-fts-typeahead-expand.md`](fork-ecl-fts-typeahead-expand.md) — ECL +
  FTS typeahead. Same `sqlite/value_set.rs`, different sites.
- [`fork-icd-import-expand-subsumes-2026-09-20.md`](fork-icd-import-expand-subsumes-2026-09-20.md)
  — ICD `$expand` / `$subsumes` / `/import`. Same file again.
- [`expand-paths-architecture.md`](expand-paths-architecture.md) — `$expand` routing.

---

## What to keep vs adopt on an upstream merge

Direction is **checkout Atrius → merge `upstream/main` into it**. Keep ours
unless upstream already expands enumerated `urn:ietf:bcp:47` includes from
the ValueSet and validates well-formed tags against `all-languages`.

| # | Symptom if this patch is lost | Keep / conflict hotspot |
|---|-------------------------------|-------------------------|
| 1 | `$expand` of `http://hl7.org/fhir/ValueSet/languages` (and `written-language`, `http://terminology.hl7.org/ValueSet/Languages`) warns and returns **no codes**. The concepts are already on `compose.include.concept`. Hindi (`hi`) and Punjabi (`pa`) disappear from pickers. | Every "CodeSystem was not found" arm in `backends/sqlite/value_set.rs` (`expand_single_include_local`, `expand_inline_filtered`, `expand_inline_plain_fts`, `load_plain_corpus_and_cache`) and the matching arm in `backends/postgres/value_set.rs`. Those arms are also touched by the ECL and ICD forks. **Keep the `bcp47::` calls.** A loaded CodeSystem row must still win: do not emit ValueSet concepts when `resolve_compose_system_id` returns an id. |
| 2 | `$validate-code` of `hi-IN` / `pa-IN` / `en-US` against `urn:ietf:bcp:47` or `http://hl7.org/fhir/ValueSet/all-languages` returns unknown-system / not-in-vs. Patient communication language fails. | `bcp47.rs`, sqlite + postgres `code_system.rs` `NotFound` arm, sqlite + postgres `value_set.rs` validate short-circuit (next to the `bcp13` call), `operations/validate_code.rs` inline ValueSet path. **Keep ours.** |
| 3 | Unfiltered `$expand` of `all-languages` logs the missing-CodeSystem warning and returns an empty expansion, which looks like "no languages exist". | Same expand arms. Must return `HtsError::TooCostly` (`unbounded_expansion_error`), not the warning. |

Do **not** replace this with a CodeSystem at `urn:ietf:bcp:47` built from the
56-code or 485-code lists. That URL is the IETF system. A short list rejects
valid tags (`hi-IN`, `pa-IN`) and makes `all-languages` look complete.
`http://hl7.org/fhir/ValueSet/languages` is already the curated picker.

A CodeSystem row that *is* loaded at `urn:ietf:bcp:47` wins. Grammar validation
runs only when the row is absent. `validate_code_loaded_bcp47_uses_stored_concepts`
locks that.

---

## Behaviour

| Request | Result |
|---------|--------|
| `$expand` `ValueSet/languages` (explicit `concept[]`, no CodeSystem row) | Those codes, with the displays on the ValueSet. No warning. |
| `$expand` `ValueSet/languages` with `filter=pun` | `pa` / Punjabi only. |
| `$validate-code` url=`ValueSet/languages` code=`hi` | `result=true`, display `Hindi`. |
| `$validate-code` url=`ValueSet/languages` code=`hi-IN` | `result=false`. Regional tags are not members of the common list. |
| `$validate-code` system=`urn:ietf:bcp:47` or url=`ValueSet/all-languages` code=`hi-IN` or `pa-IN` | `result=true`. Grammar only: primary subtag of 2–3 letters, then subtags of 1–8 alphanumerics. No IANA registry. |
| `$validate-code` … code=`Hindi` | `result=false`. |
| `$expand` `ValueSet/all-languages` (whole-system include, no `concept[]`) | HTTP 422 `too-costly`. |

The four ValueSets in a bootstrapped `hts.db` that compose this system:

| ValueSet | Shape |
|----------|--------|
| `http://hl7.org/fhir/ValueSet/languages\|4.0.1` | 56 explicit concepts, includes `en-US` |
| `http://hl7.org/fhir/ValueSet/written-language\|4.0.1` | 26 explicit concepts |
| `http://terminology.hl7.org/ValueSet/Languages\|1.0.0` | 485 explicit concepts (`hi`, `pa`, not `hi-IN`) |
| `http://hl7.org/fhir/ValueSet/all-languages\|4.0.1` | `{"include":[{"system":"urn:ietf:bcp:47"}]}` |

---

## Files touched (merge checklist)

```
crates/hts/src/bcp47.rs                              # new module
crates/hts/src/lib.rs                                # mod bcp47
crates/hts/src/backends/sqlite/value_set.rs          # expand + validate
crates/hts/src/backends/sqlite/code_system.rs        # CodeSystem/$validate-code
crates/hts/src/backends/postgres/value_set.rs        # same
crates/hts/src/backends/postgres/code_system.rs      # same
crates/hts/src/operations/validate_code.rs           # inline all-languages
```

`load_plain_corpus_and_cache` returns `None` for an unbounded BCP 47 include
so the caller falls through to `expand_inline_plain_fts`, which returns
`TooCostly`. Do not turn that `None` back into the old warning.

Version-mismatch (`__UNKNOWN_CS_VERSION_EXP__`) still runs first when a
compose pin names a version and some other version of the system is stored.
The BCP 47 branches run only after that check finds no row.

---

## Tests

```
cargo test -p helios-hts --lib bcp47
cargo test -p helios-hts --lib expand_all_languages
cargo test -p helios-hts --lib validate_languages_uses
cargo test -p helios-hts --lib validate_all_languages
```

- `bcp47::tests::*`
- `expand_enumerated_bcp47_without_codesystem`
- `expand_enumerated_bcp47_text_filter`
- `expand_all_languages_is_too_costly`
- `validate_languages_uses_enumerated_codes`
- `validate_all_languages_accepts_well_formed_tag`
- `validate_code_bcp47_grammar_when_codesystem_absent`
- `validate_code_loaded_bcp47_uses_stored_concepts`

After a merge, rebuild `hts` before judging startup logs. The running binary
does not pick this up from the database.

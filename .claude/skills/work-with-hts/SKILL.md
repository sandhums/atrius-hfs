---
name: work-with-hts
description: Work on the HFS Terminology Server. Use for hts runtime configuration, HTS APIs, terminology import, bootstrap sync, CodeSystem, ValueSet, ConceptMap, SNOMED, LOINC, RxNorm, ICD-10-CM, and HFS terminology integration.
---

# HTS

Use this when working on the `hts` binary or terminology server behavior.

## Running

```bash
# Default: SQLite, port 9091 (Atrius)
cargo run --bin hts

# Custom database path and port
HTS_DATABASE_URL=./my-terminology.db HTS_SERVER_PORT=9091 cargo run --bin hts
```

## Environment

| Variable | Default | Description |
|---|---|---|
| `HTS_SERVER_PORT` | `9091` | Server port (Atrius default) |
| `HTS_SERVER_HOST` | `127.0.0.1` | Host to bind |
| `HTS_LOG_LEVEL` | `info` | Log level: error, warn, info, debug, trace |
| `HTS_DATABASE_URL` | `./data/hts.db` | SQLite file path or PostgreSQL connection URL |
| `HTS_STORAGE_BACKEND` | `sqlite` | Storage backend; postgres when built with `--features postgres` |
| `HTS_BOOTSTRAP_DIR` | none | Directory of terminology files imported on startup |
| `HTS_MAX_BODY_SIZE` | `10485760` | Max request body size in bytes, applied after decompression |
| `HTS_ENABLE_CORS` | `true` | Enable CORS |
| `HTS_CORS_ORIGINS` | `*` | Allowed CORS origins |

## Bootstrap Directory Sync

When `HTS_BOOTSTRAP_DIR` points at a directory, HTS synchronizes recognized files into the database on every startup. Each recognized file is hashed with SHA-256 and recorded in a `bootstrap_imports` ledger keyed by file name. A file imports only when the hash is absent or changed.

- Adding a new file imports it on the next restart.
- Replacing a file with a newer terminology release re-imports it. Imports upsert on canonical `(url, version)`, so a new version coexists with the old and a same-version re-import refreshes in place.
- Unchanged files are skipped without reparsing.
- RxNorm RRF directory signatures are derived from immediate children's `(name, size)` instead of full content hashing to avoid rereading multi-gigabyte folders on every boot.

## API Endpoints

| Operation | Method | URL |
|---|---|---|
| health | GET | `/health` |
| capabilities | GET | `/metadata` |
| lookup | POST | `/CodeSystem/$lookup` |
| validate code, CodeSystem | POST | `/CodeSystem/$validate-code` |
| subsumes | POST | `/CodeSystem/$subsumes` |
| expand | POST | `/ValueSet/$expand` |
| validate code, ValueSet | POST | `/ValueSet/$validate-code` |
| translate | POST | `/ConceptMap/$translate` |
| closure | POST | `/ConceptMap/$closure` |
| import bundle | POST | `/import` |
| CRUD | GET/POST/PUT/DELETE | `/CodeSystem/:id`, `/ValueSet/:id`, `/ConceptMap/:id` |

`$validate-code` on a stored ValueSet whose compose is only `concept is-a` / `descendent-of` includes (OR'd) uses `concept_closure` and does not materialise the expansion. Full `$expand` of a large product-class set can still 422 `too-costly` (`HTS_MAX_EXPANSION_SIZE`); paginate with `count`.

## Quick Examples

```bash
# Import a FHIR Bundle containing CodeSystem, ValueSet, or ConceptMap resources
curl -X POST http://localhost:9091/import \
  -H "Content-Type: application/fhir+json" \
  -d @bundle.json

# Lookup a concept
curl -X POST http://localhost:9091/CodeSystem/\$lookup \
  -H "Content-Type: application/fhir+json" \
  -d '{"resourceType":"Parameters","parameter":[{"name":"url","valueUri":"http://example.org/cs"},{"name":"code","valueCode":"ABC"}]}'

# Expand a value set
curl -X POST http://localhost:9091/ValueSet/\$expand \
  -H "Content-Type: application/fhir+json" \
  -d '{"resourceType":"Parameters","parameter":[{"name":"url","valueUri":"http://example.org/vs"}]}'

# Expand a Value Set Composition Language (VCL) expression (implicit ValueSet URL)
curl -G http://localhost:8090/ValueSet/\$expand \
  --data-urlencode 'url=http://fhir.org/VCL?v1=%28http%3A%2F%2Fsnomed.info%2Fsct%29%3C%3C404684003'
```

## VCL (Value Set Composition Language)

Spec: <https://build.fhir.org/ig/FHIR/ig-guidance/vcl.html> (CI build 0.1.0, still moving). Code: `crates/hts/src/vcl/` (parser → AST → `ValueSet` JSON).

- **Transport:** only the implicit URL `http://fhir.org/VCL?v1=<expr>` in the `url` parameter of `ValueSet/$expand`, `ValueSet/$validate-code` and `$batch-validate-code` (batch-level `url`). There is no dedicated parameter. `/ValueSet/{id}/...` routes never see VCL (the stored canonical replaces `url`).
- **Mechanism:** `rewrite_vcl_params` drops `url` (and any caller `valueSet`) and appends a synthetic inline `valueSet` whose `url` is the original VCL URL. Everything after that is the normal inline-compose path, so it works on SQLite and PostgreSQL.
- **Encoding:** the `v1` value is percent-decoded exactly once (hand-written; `+` stays `+`). On GET the query is already decoded once by the server, so a client must encode the whole `url` value; a nested URI containing `%28` is decoded one extra time.
- **Errors:** malformed VCL → 400 `vs-invalid`, `Invalid VCL expression at position N: …` (1-based column in the decoded expression). Valid but unsupported construct → 501 `not-supported`. An expression with no system and no `^(vs)` → 400 (the spec requires a `systemUri`).
- **Grammar:** the published ANTLR grammar. One operator kind per level: `A,B;C` and `A - B - C` are errors, write `(A,B);C`, `(A;B)-(C)`. `-` is a code character, so `A-B` is one code; exclusion needs spaces or parentheses. Limits: 64 KiB expression, 32 nesting levels.
- **Deviations (lenient):** a hierarchy operator without a property (`(sct)<<404684003`, the issue's example) means `concept<<…`; one-element lists `prop^{A}` are accepted.

| VCL | compose | Status |
|---|---|---|
| `(sys)*` | include with only `system` | supported (materialises the whole system) |
| `prop=v`, `prop<<c`, `prop<c`, `prop<!c`, `prop/"re"` | `=`, `is-a`, `descendent-of`, `child-of`, `regex` | supported |
| `prop~<<c` | `is-not-a` | supported (added in this feature) |
| `prop>>c` | `generalizes` | supported (PostgreSQL support added in this feature) |
| `prop^{a,b}`, `prop~^{a,b}` | `in` / `not-in`, comma-separated value | supported (multi-value added in this feature) |
| `;` / `,` / `-` / `(…)` | several `include` / filters in one include / `exclude` / grouping | supported; non-flat groups are hoisted to `contained` `#vcl-N` (max 3 levels) |
| `^(vs)` | `include.valueSet` | supported; stored ValueSets, cycle detection in the backend |
| `!!<`, `?`, `.` (of), `^{filter…}`, `prop^URI` | — | parsed, then 501 |

Known limits: `$validate-code` on a VCL set expands it fully, so large sets (e.g. `(sct)<<404684003`) return 422 `too-costly` at the default `HTS_MAX_EXPANSION_SIZE` (3500); that inline path ignores `tx-resource`, version pins and `activeOnly`; an unloaded system gives 404 rather than `result=false`; include cycles give warning+empty on SQLite but `VsInvalid` on PostgreSQL. VCL is not advertised in `/metadata` (the IG `capterms.json` fixture expects exactly 12 `expansion.parameter` entries) and has no HTS UI entry yet.

## HFS Integration

Set `HFS_TERMINOLOGY_SERVER=http://localhost:9091` on HFS to enable:

- FHIR search `:in` modifier, using ValueSet expansion to filter by code.
- FHIRPath `memberOf()` and `subsumes()` delegation through `FHIRPATH_TERMINOLOGY_SERVER`.

## Bulk Import CLI

Load terminology packages directly from the filesystem:

```bash
# HL7 FHIR NPM package
cargo run --bin hts -- import ./hl7.terminology.r4-6.0.0.tgz

# SNOMED CT RF2 ZIP, requires NRC license
cargo run --bin hts -- import ./SnomedCT_InternationalRF2_*.zip --format snomed-rf2

# LOINC CSV ZIP, requires free Regenstrief registration
cargo run --bin hts -- import ./Loinc_*.zip --format loinc

# ICD-10-CM tabular XML
cargo run --bin hts -- import ./icd10cm_tabular_2025.xml

# RxNorm RRF folder, requires free NLM terms of service
cargo run --bin hts -- import ./RxNorm_full_current/rrf/

# Common flags
cargo run --bin hts -- import ./package.tgz \
  --database-url ./data/hts.db \
  --batch-size 1000 \
  --dry-run \
  --verbose
```

LOINC language translations under `AccessoryFiles/LinguisticVariants/`, such as `frFR28LinguisticVariant.csv`, are imported automatically as FHIR `concept.designation` entries tagged with BCP-47 language values such as `fr-FR` and `de-DE`.

SNOMED CT descriptions in all languages — including per-language Description files from national extensions — are imported as `concept.designation` entries tagged with the RF2 language code. Every language refset marks its preferred synonyms with `preferredForLanguage` designations (a bare language tag plus a dialect tag like `en-US`/`da-DK`/`fr-CA` for published national refsets), and `en-US`→`en-GB` preference picks the display. Select language via the `displayLanguage` parameter or the `Accept-Language` header on `$lookup` / `$expand` / `$validate-code`; matching is BCP-47-aware (`de-DE` finds `de`, `fr` accepts `fr-CA`).

SNOMED **extension** packages (e.g. the NRCeS India Drug Extension `IN1000189`) contain only their own module; their `Is-a` and attribute relationships point at International concepts absent from the package. Never import them standalone — the extension ends up severed from the International hierarchy (no `$subsumes` / `is-a` / ECL reach from International groupers, no ingredient attributes, unversioned `$lookup` misses the codes). Layer them onto the loaded International release instead:

```bash
hts import ./SnomedCT_InternationalRF2_PRODUCTION_20260501T120000Z.zip --format snomed-rf2
hts import ./SnomedCT_IndiaDrugExtensionRF2_PRODUCTION_IN1000189_20260313T120000Z.zip \
  --format snomed-rf2 --extends 20260501
```

`--extends <base-version>` writes the extension's concepts into the base `(url, version)` row, keeps relationships whose destination is in either the package or the base, and rebuilds the base closure + FTS once. It checks the package's module-dependency refset against the base and reports a mismatch plus the count of still-unresolved relationships as non-fatal errors (exit 2); a one-release skew with a handful of dangling destinations is normal. The merged row's title records both parts (`SNOMED CT (20260501 + IN1000189 20260313)`). Clients then use the base version (or none) — pinning the extension's own release date returns `not-found`.

## Format Auto-detection

| Extension or pattern | Detected format |
|---|---|
| `.tgz` or `.tar.gz` | `hl7-npm` |
| `*_tabular*.xml` | `icd10-cm` |
| `.rrf` or directory | `rxnorm` |
| `.zip` containing RF2 files | `snomed-rf2` |
| `.zip` containing `LoincTable.csv` | `loinc` |
| `.zip` containing `RXNCONSO.RRF` | `rxnorm` |

Zip files that cannot be auto-detected require `--format`.

## SQLite caches

Hot-path memos (implicit/inline indexes, `$lookup` / `$validate-code` responses, CodeSystem URL→storage id, CodeSystem language, …) live on the `SqliteTerminologyBackend` instance. Parallel tests with separate in-memory databases must not share those maps. `TerminologyCaches::invalidate_caches` clears them after bundle import and after CRUD writes. Do not restore process-wide `OnceLock` caches for system id or language.

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success; all resources imported |
| `1` | Fatal error; import aborted |
| `2` | Success with non-fatal errors; some records skipped |

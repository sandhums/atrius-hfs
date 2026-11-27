# Local HTS terminology load (Atrius R4)

Reload the sqlite database the Atrius development HTS serves. The server listens on `127.0.0.1:9091` and reads `atrius-hfs/data/hts.db`. The release `hts` binary is built with the sqlite backend only.

Two kinds of load:

| Step | Tool | HTS |
|---|---|---|
| HL7 packages, SNOMED, LOINC, RxNorm | `./target/release/hts import` | Stopped. The command writes `./data/hts.db` in the working directory. |
| NDHM, the Atrius IG, WHO ICD-10, ICD-11, and the ICD-10↔ICD-11 maps | HTTP loaders | Running on port 9091. |

`hts import` is run from the `atrius-hfs` repository root. The WHO loaders are run from the `atrius-his` repository root.

Licensed and WHO distributions used below live in:

```text
/Users/sandhu/Library/Mobile Documents/com~apple~CloudDocs/Downloads/TerminologyData/terminology-data
```

That path is called `$DATA` in the commands.

## Files to load

| File | Loads |
|---|---|
| `$DATA/hl7.fhir.r4.core-4.0.1.tgz` | FHIR R4 core code systems. Use this copy. The smaller `hl7.fhir.r4.core.tgz` next to `terminology-data/`, and the unpacked `package/` directory, are the same package. |
| `crates/hts/terminology-data/hl7.terminology.r4-7.5.0.tgz` | HL7 terminology, R4 package |
| `$DATA/SnomedCT_InternationalRF2_PRODUCTION_20260801T120000Z.zip` | SNOMED CT International, Snapshot `20260801` |
| `$DATA/SnomedCT_IndiaDrugExtensionRF2_PRODUCTION_IN1000189_20260831T120000Z.zip` | NRCeS India Drug Extension `IN1000189`, release `20260831`, layered onto `20260801` |
| `$DATA/Loinc_2.83.zip` | LOINC 2.83 |
| `$DATA/RxNorm_full_04062026/rrf` | RxNorm full release of 4 June 2026. Import the `rrf` directory. The sibling `.zip` is the same archive. |
| `$DATA/ndhm.tgz` | ABDM / NDHM code systems and value sets, via `scripts/p0-import-terminology.sh` once HTS is up |
| Atrius IG `output/` | Atrius code systems and value sets, same script |
| `$DATA/icd102019en.xml` | WHO ICD-10 ClaML, version `2019-covid-expanded` |
| `$DATA/SimpleTabulation-ICD-11-MMS-en/SimpleTabulation-ICD-11-MMS-en.txt` | ICD-11 MMS linearization |
| `$DATA/mapping/10To11MapToOneCategory.txt`, `10To11MapToMultipleCategories.txt`, `11To10MapToOneCategory.txt` | ICD-10 ↔ ICD-11 ConceptMaps |

## Files to leave unloaded

| File | Why it stays out |
|---|---|
| `hl7.terminology-7.5.0.tgz` and `hl7.terminology.r5-7.5.0.tgz` | Both are FHIR 5.0.0. This server and the Atrius IG are R4. Importing them stores the same code-system URLs again. |
| `icd10cm-table-and-index-2026.zip` | US ICD-10-CM. HIS stamps WHO `http://hl7.org/fhir/sid/icd-10` and ICD-11 MMS. ICD-10-CM is provenance for ValueSet authoring, not a system HIS writes. |
| `hl7.fhir.us.core-*.tgz`, `hl7.fhir.uv.ips-*.tgz` | Not dependencies of the Atrius IG. |
| `us.nlm.vsac-*.tgz`, `vsac-supplement.bundle.json`, `us.cdc.phinvads-*.tgz` | US value-set distributions. |
| ICD-9-CM, ICD-11↔ICD-10 `foundation_*` tables, `mapping/*.xlsx` | The category text tables are the maps HIS translates with. The foundation tables are a different WHO map and are not loaded. |
| MeSH, NCI Thesaurus, NDC, NUCC, UCUM, DICOM Part 16 | Not used by this IG's terminology path. |

The IG still declares `hl7.terminology.r4` at **7.2.0** for the publisher and profile validation. The HTS load is the **7.5.0** R4 package above. Those are separate copies.

## 1. HL7, SNOMED, LOINC, RxNorm

Stop HTS so nothing else has `data/hts.db` open. From `atrius-hfs`:

```bash
cd /Users/sandhu/RustroverProjects/atrius-hfs
DATA="/Users/sandhu/Library/Mobile Documents/com~apple~CloudDocs/Downloads/TerminologyData/terminology-data"
HTS=./target/release/hts

"$HTS" import "$DATA/hl7.fhir.r4.core-4.0.1.tgz"
"$HTS" import ./crates/hts/terminology-data/hl7.terminology.r4-7.5.0.tgz

# International Snapshot first. The extension package contains only its own module.
"$HTS" import "$DATA/SnomedCT_InternationalRF2_PRODUCTION_20260801T120000Z.zip" --format snomed-rf2
"$HTS" import "$DATA/SnomedCT_IndiaDrugExtensionRF2_PRODUCTION_IN1000189_20260831T120000Z.zip" \
  --format snomed-rf2 --extends 20260801

"$HTS" import "$DATA/Loinc_2.83.zip" --format loinc
"$HTS" import "$DATA/RxNorm_full_04062026/rrf"
```

`Loinc_2.83.zip` has no `Loinc_2.83/` folder, and `loinc.xml` leaves `<version>` unset. The importer records `2.83` from the file name. That fallback is `loinc_release_version_from_filename` in `src/import/loinc_csv.rs`; keep it when merging `upstream/main` ([ig-publisher-compatibility.md](../ig-publisher-compatibility.md) §3). A binary built before the fallback imports the concepts and writes a blank `CodeSystem.version`, which overwrites a labeled row. Do not run this command with an older `hts`. The current `data/hts.db` row is already version `2.83` on the column and in `resource_json` (185,132 concepts) and does not need to be imported again.

`--extends` is the International version `20260801`, taken from the International filename. It is not the extension date `20260831`. The merged CodeSystem title is `SNOMED CT (20260801 + IN1000189 20260831)`. Clients use version `20260801` or no version. Pinning `20260831` does not find the extension codes.

Importing the India package by itself drops every relationship whose destination is an International concept. `$subsumes`, `is-a`, and ECL from International groupers then never reach an extension concept. A module-dependency mismatch or a small count of unresolved relationships is reported as exit code 2 and is expected when the extension was built against a nearby International release. A large unresolved count means the wrong base was passed to `--extends`.

## 2. NDHM and the Atrius IG

Start HTS on port 9091, then from `atrius-hfs`:

```bash
./scripts/p0-import-terminology.sh --fhir-version r4
```

Prerequisites are in that script: HTS healthy, the Atrius IG already built (`AtriusIGDraft/output`), and the NDHM mirror (default `~/Downloads/ndhm`, or `ATRIUS_NDHM_DIR`). `$DATA/ndhm.tgz` is the same NDHM package when the mirror is not already unpacked.

## 3. WHO ICD-10, ICD-11, and the maps

`hts import` has an ICD-10-CM importer and no WHO ICD-10 or ICD-11 importer. These three commands POST bundles to the running server. From `atrius-his`:

```bash
cd /Users/sandhu/RustroverProjects/atrius-his
DATA="/Users/sandhu/Library/Mobile Documents/com~apple~CloudDocs/Downloads/TerminologyData/terminology-data"

python3 scripts/icd/claml_to_fhir.py \
  --claml "$DATA/icd102019en.xml" \
  --hts-url http://127.0.0.1:9091 \
  --replace

python3 scripts/icd/mms_to_fhir.py \
  --tabulation "$DATA/SimpleTabulation-ICD-11-MMS-en/SimpleTabulation-ICD-11-MMS-en.txt" \
  --hts-url http://127.0.0.1:9091

python3 scripts/icd/mapping_to_conceptmap.py \
  --mapping-dir "$DATA/mapping" \
  --hts-url http://127.0.0.1:9091

python3 scripts/icd/verify-icd-load.py
```

| Resource | Identity |
|---|---|
| WHO ICD-10 CodeSystem | `http://hl7.org/fhir/sid/icd-10` @ `2019-covid-expanded` (16,735 concepts) |
| ICD-11 MMS CodeSystem | `http://id.who.int/icd/release/11/mms` @ `2025-01-24` (35,339 codable concepts) |
| `atrius-in-icd10-to-icd11-mms` | `10To11MapToOneCategory.txt` |
| `atrius-in-icd10-to-icd11-mms-multi` | `10To11MapToMultipleCategories.txt` |
| `atrius-in-icd11-mms-to-icd10` | `11To10MapToOneCategory.txt` |

HIS fills the axis the clinician did not type by calling `$translate` on those ConceptMaps. `$translate` needs the maps. `$lookup`, `$validate-code`, and typeahead also need the two CodeSystems.

Load-order rules that fail silently are written up in [`atrius-his/scripts/icd/README.md`](../../../../../atrius-his/scripts/icd/README.md): chunked imports must finalize concept closure, `--replace` is required when codes may have been retired, and ValueSets have to be imported again after a CodeSystem replace because HTS keeps expansions in `value_set_expansions`. `verify-icd-load.py` checks both axes, the three maps, and the IDSP codes.

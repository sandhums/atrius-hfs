# Single-engine validation cutover

HFS now uses **only** `helios-fhir-validator` for `$validate` and write-path
enforcement. The Atrius `fhir-validation` crate and `HFS_PROFILE_MANIFEST` /
`HFS_PROFILE_VALIDATION_MODE` path have been removed.

## Operator config

| Variable | Role |
|----------|------|
| `HFS_FHIR_PACKAGE_CACHE` | Curated FHIR NPM package cache root |
| `HFS_FHIR_PACKAGES` | Comma-separated `name@version` packages to overlay (listed packages only; `package.json` deps are not walked) |
| `HFS_VALIDATION_MODE` | `off` / `log` / `enforce` on create/update/patch/batch/**transaction**/bulk-submit ingest |
| `HFS_VALIDATION_TERMINOLOGY` | `off` / `embedded` / `remote` / `tiered`. Atrius clinical + NDHM validate use `tiered`: FHIR-core enums from the validator gz pack, everything else (SNOMED, LOINC, NDHM, Atrius) via `HFS_TERMINOLOGY_SERVER`. |

See [crates/fhir-validator/docs/packages.md](../crates/fhir-validator/docs/packages.md).

## Terminology: `tiered`

Atrius clinical HFS and the NDHM export validator set
`HFS_VALIDATION_TERMINOLOGY=tiered` with `HFS_VALIDATION_TERMINOLOGY_FAIL=open`.

- FHIR-core enumerated ValueSets (status, gender, category, …) are answered
  from the validator gz pack (`CoreTerminology::member`) with no HTS round trip.
- ValueSets **not** in that pack — SNOMED, LOINC, RxNorm, UCUM, THO
  (`marital-status`), NDHM, Atrius — go to `HFS_TERMINOLOGY_SERVER`
  (`ValueSet/$validate-code`).
- `open` warns when HTS is unreachable; switch to `closed` in production if
  HTS is a hard dependency.

`embedded` alone is **not** HTS. The R4 pack is ~480 `http://hl7.org/fhir/ValueSet/*`
URLs generated from the spec `valuesets.json` bundle. Unknown URLs used to
return `Ok(true)` and silently accept SNOMED/NDHM bindings.

**Do not merge Atrius or NDHM ValueSets into
`crates/fhir-validator/packs/terminology_r4.json.gz`.** That file is regenerated
from the HL7 spec; mixing custom URLs in would be wiped on the next regen and
would lie about what “FHIR core” means. Keep Atrius/NDHM in HTS
(`scripts/import-ndhm-atrius-terminology.py`, `hts import`, or
`HTS_BOOTSTRAP_DIR`). `HFS_FHIR_PACKAGES` overlays StructureDefinitions only;
CodeSystem/ValueSet files in the package are ignored for binding checks.

A second Atrius validator pack is optional later, only if HTS latency or
offline `$validate` becomes a problem.

Env templates are checked by `python3 scripts/check-env-drift.py` (Atrius CI).

## Retired crates (backup branch)

`crates/fhir-terminology` (generated local ValueSet/CodeSystem tables) and
`crates/fhir-valueset-gen` (`atrius-fhir-valueset-gen`) had **zero dependents**
and duplicated the validator gz packs plus HTS. They are deleted from
`feat-clinical-reasoning`. Full sources remain on

`backup/fhir-terminology-and-valueset-gen` (commit `1d8c0e43e`).

Restore with `git checkout backup/fhir-terminology-and-valueset-gen -- crates/fhir-terminology crates/fhir-valueset-gen` and add those paths back to `default-members`. Do not put them on the default build path.

HTTP to HTS now goes through `helios-terminology-client` (`crates/terminology-client`): REST search `$expand`, validation `$validate-code`, FHIRPath `%terminologies`, and the UI picker share one process-wide 300s TTL cache.

Cassandra and Neo4j cargo features (`cdrs-tokio` / `neo4rs`) were never implemented and have been removed from `helios-persistence`.

## Staging checklist

1. Run `./scripts/setup-atrius-profile-registry.sh` — expands the published IG and
   seeds `data/fhir-packages/{name}/{version}/` (override with `HFS_FHIR_PACKAGE_CACHE`).
   Optional audit JSON under `manifests/` is written unless `SKIP_MANIFESTS=1`.
2. Use the values the script prints, e.g.:

   ```bash
   HFS_FHIR_PACKAGE_CACHE=/path/to/atrius-hfs/data/fhir-packages
   HFS_FHIR_PACKAGES=atrius.fhir.r4.india@0.1.0   # from the IG package.json
   HFS_VALIDATION_MODE=enforce
   ```

3. Smoke `$validate` and HIS write paths (including transaction Bundles).
4. Compare issues against prior dual-engine baselines for Patient / Encounter / Appointment.

`name@version` always comes from the published IG’s `package.json` (IG publisher),
not from HFS. HFS does **not** overlay that file’s `dependencies` unless those
packages are also listed in `HFS_FHIR_PACKAGES`. The
`atrius-r4-profile-manifest*.json` files are **not** read by HFS.

## Staging 422 proof

With `HFS_VALIDATION_MODE=enforce` and the Atrius overlay loaded, a write that
is not schema-valid must return **422**:

```bash
HFS_BASE_URL=http://127.0.0.1:8082 ./scripts/prove-validation-enforce.sh
```

CI covers the same contract in `crates/rest/tests/validation_enforcement_tests.rs`.

## NDHM / ABDM export validator (not clinical HFS)

Do **not** add `ndhm.in` to clinical `HFS_FHIR_PACKAGES`. Seed a second HFS
process with that package and point HIS at it:

```bash
./scripts/setup-ndhm-profile-registry.sh
./scripts/run-hfs-ndhm.sh   # :8083, HFS_FHIR_PACKAGES=ndhm.in@6.5.0
# HIS_NDHM_VALIDATE_URL=http://127.0.0.1:8083
```

Env templates: `deploy/clinical/.env.abdm.example`,
`deploy/env/hfs-ndhm-validate.env.example`.

HFS fails startup if leftover `HFS_PROFILE_MANIFEST`,
`HFS_PROFILE_VALIDATION_MODE`, or `HFS_PROFILE_VALIDATION_ADDONS` are set.
Use `HFS_VALIDATION_MODE` + `HFS_FHIR_PACKAGES` instead. `HFS_PROFILE_CORPUS`
is a search-profiling knob and is still valid.

## Remaining gaps (not a second engine)

`ValidationService::check_write` **does** run on create/update/patch, batch
POST/PUT/PATCH, transaction POST/PUT/PATCH, transaction DELETE existence, and
bulk-submit ingest (when the worker is given a `ValidationService`).

`$validate` `mode` is enforced: `create` / `update` / `delete` / `profile`.

Slice `type` / `profile` / `binding` / `exists` / `extension` discriminators
are evaluated (binding expand at mark time; mixed-kind AND as `type: "all"`).
Batch `[type]?[criteria]` PATCH is resolved. `resolve()` follows in-scope
targets and tenant-store `Type/id` that REST prefetched into the validator
pool (the engine itself does no I/O). Remaining hole (not a second engine):

- Conditional URL criteria in a `transaction` (`[type]?[criteria]`) are still
  declined whole (#859). Batch resolves them.

Sync keep-list: [clinical-reasoning/upstream-merge.md](./clinical-reasoning/upstream-merge.md).

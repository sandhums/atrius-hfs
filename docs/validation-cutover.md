# Single-engine validation cutover

HFS now uses **only** `helios-fhir-validator` for `$validate` and write-path
enforcement. The Atrius `fhir-validation` crate and `HFS_PROFILE_MANIFEST` /
`HFS_PROFILE_VALIDATION_MODE` path have been removed.

## Operator config

| Variable | Role |
|----------|------|
| `HFS_FHIR_PACKAGE_CACHE` | Curated FHIR NPM package cache root |
| `HFS_FHIR_PACKAGES` | Comma-separated `name@version` packages to overlay (listed packages only; `package.json` deps are not walked) |
| `HFS_VALIDATION_MODE` | `off` / `log` / `enforce` on create/update/patch/batch/**transaction** |

See [crates/fhir-validator/docs/packages.md](../crates/fhir-validator/docs/packages.md).

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

## Remaining gaps (not a second engine)

`ValidationService::check_write` **does** run on create/update/patch, batch
POST/PUT, and transaction POST/PUT. These holes are still real:

- **Bulk-submit ingest** writes via `storage.create` / `update` and does not
  call `check_write`.
- **Transaction `DELETE`** entries are skipped by the pre-flight validation
  loop.
- **Bundle `PATCH`** entries return 501 (instance PATCH does validate).
- **`$validate` `mode`** is parsed; only `delete` short-circuits.
- **Slice `type` / `profile` / `binding` discriminators** are converted to
  Match IR but the engine still evaluates pattern matches only.

Sync keep-list: [clinical-reasoning/upstream-merge.md](./clinical-reasoning/upstream-merge.md).

# Single-engine validation cutover

HFS now uses **only** `helios-fhir-validator` for `$validate` and write-path
enforcement. The Atrius `fhir-validation` crate and `HFS_PROFILE_MANIFEST` /
`HFS_PROFILE_VALIDATION_MODE` path have been removed.

## Operator config

| Variable | Role |
|----------|------|
| `HFS_FHIR_PACKAGE_CACHE` | Curated FHIR NPM package cache root |
| `HFS_FHIR_PACKAGES` | Comma-separated `name@version` roots |
| `HFS_VALIDATION_MODE` | `off` / `log` / `enforce` on create/update/patch/batch/**transaction** |

See [crates/fhir-validator/docs/packages.md](../crates/fhir-validator/docs/packages.md).

## Staging checklist

1. Seed the package cache with the Atrius IG (+ deps) via `PackageCache::ensure_from_tgz` or an expanded package tree.
2. Set `HFS_FHIR_PACKAGE_CACHE`, `HFS_FHIR_PACKAGES`, and `HFS_VALIDATION_MODE=enforce`.
3. Smoke `$validate` and HIS write paths (including transaction Bundles).
4. Compare issues against prior dual-engine baselines for Patient / Encounter / Appointment.

Legacy manifest scripts under `scripts/*atrius*` may still expand packages for
cache seeding; they no longer feed a separate validator.

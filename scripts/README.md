# Scripts

Local clinical-reasoning helpers. Prefer these over ad-hoc `cargo run` so every
service uses the same **release binary** + `deploy/env/*.env` wiring.

Hospital foundation, ADT journeys, and FHIR Subscription seeds/smokes live in
**atrius-his** (`../atrius-his/scripts` — see that repo’s `scripts/README.md`).
Do not duplicate patient/encounter/subscription demo seeding here.

## Start the stack (separate terminals)

```bash
./scripts/build-clinical-reasoning.sh   # once after code changes

./scripts/run-hts.sh
./scripts/run-hfs.sh              # clinical :8082
./scripts/run-hfs-ndhm.sh         # NDHM export validator :8083 (optional)
./scripts/run-kr-hfs.sh           # KR :8079
./scripts/run-cql-sidecar.sh      # :8088 (JVMsidecar or SIDECAR_JAR)
./scripts/run-cds-server.sh       # :8095
```

| Script | Binary | Default env |
|--------|--------|-------------|
| `run-hts.sh` | `target/release/hts` | `deploy/env/hts.env` |
| `run-hfs.sh` | `target/release/hfs` | `deploy/env/hfs-clinical.env` |
| `run-hfs-ndhm.sh` | `target/release/hfs` | `deploy/env/hfs-ndhm-validate.env` or `.env.abdm.example` |
| `run-kr-hfs.sh` | `target/release/hfs` | `deploy/env/hfs-kr.env` |
| `run-cds-server.sh` | `target/release/cds-server` | `deploy/env/cds-server.env` |
| `run-cql-sidecar.sh` | jar or Maven `MainKt` | `deploy/env/cql-sidecar.env` |

Override env: `ENV_FILE=/path/to/file ./scripts/run-hfs.sh`.

Production systemd uses the same env templates under `/etc/atrius/` — see
`docs/clinical-reasoning/production-deployment.md`.

## Data / catalog

| Script | Role |
|--------|------|
| `import-cms165-demo.py` | Seed `cms165-demo` chart + sidecar verify |
| `import-synthea-atrius.py` | Bulk Synthea → clinical HFS |
| `purge-kr-ecqm-artifacts.py` | Delete leftover CMS Libraries / synthesized PlanDefinitions from KR |
| `setup-plandefinition-cds-catalog.sh` | **Orchestrator**: import Atrius IG → KR if needed, then regenerate catalog |
| `generate-cds-hooks-manifest.py` | **Writer**: KR PlanDefinitions → `manifests/cds-services-kr.json` |
| `validate-manifest-kr-pins.sh` | Check manifest pins exist on KR |
| `p0-import-terminology.sh` / `import-ndhm-atrius-terminology.py` | HTS terminology loads |

Atrius authoring lives in **AtriusIGDraft** (`translate-cql.sh` +
`import-atrius-kr-libraries.py --clinical-reasoning`). Day-to-day: run the
orchestrator; use the writer alone only when KR already has PlanDefinitions.

## Smoke tests

| Script | Role |
|--------|------|
| `cds-cms165-prefetch-smoke.sh` | CMS165 CDS Hooks with populated prefetch |
| `cds-er-chest-pain-smoke.sh` | ER chest pain pathway |
| `cds-atrius-services-smoke.sh` | All Atrius ECA rules and order sets (critical labs, preventive care, sepsis, DDI, HF admission, imaging appropriateness, renal dosing, surgical safety checklist) with synthetic prefetch and assertions |

## IG / profiles

| Script | Role |
|--------|------|
| `setup-atrius-profile-registry.sh` | **Recommended**: fetch published `package.tgz` → seed `data/fhir-packages/{name}/{version}/` (optional audit manifests) |
| `setup-ndhm-profile-registry.sh` | Seed `ndhm.in@6.5.0` from `~/.fhir/packages` for the ABDM export validator only — **not** clinical HFS |
| `prove-validation-enforce.sh` | POST a non-conformant Patient; expect **422** (`HFS_VALIDATION_MODE=enforce`) |
| `build-atrius-profile-manifest.sh` | Optional audit JSON under `manifests/` (HFS does not read these) |
| `load-atrius-ig-package.sh` | Expand package into `manifests/atrius-ig-package/` |

**Do we need the JSON manifests?** No for HFS runtime. After cutover, HFS loads IG
profiles only from `HFS_FHIR_PACKAGE_CACHE` + `HFS_FHIR_PACKAGES`. The
`atrius-r4-profile-manifest*.json` files are optional audit inventories
(`SKIP_MANIFESTS=1` skips them).

**Do we need `manifests/deps/hl7-*`?** No. Datatypes (`SimpleQuantity`, …) and
standard Patient extensions (`patient-nationality`, …) come from the
`helios-fhir-validator` **embedded R4 core pack**. Do not seed those into the
package cache.

After `./scripts/setup-atrius-profile-registry.sh`, use the values it prints
(e.g. `atrius.fhir.r4.india@0.1.0` from the IG `package.json`). HFS overlays
only packages named in `HFS_FHIR_PACKAGES`; sushi `dependsOn` entries in that
`package.json` (NDHM, THO, Extensions Pack, …) are not loaded unless listed:

```bash
HFS_FHIR_PACKAGE_CACHE=./data/fhir-packages
HFS_FHIR_PACKAGES=atrius.fhir.r4.india@0.1.0
HFS_VALIDATION_MODE=enforce
HFS_TERMINOLOGY_SERVER=http://127.0.0.1:9091
```

Paths in `deploy/env/hfs-clinical.env` are relative to the repo root
(`./scripts/run-hfs.sh` cds there). Override the cache root with
`HFS_FHIR_PACKAGE_CACHE=…` when running setup.

## Shared

- `lib/common.sh` — env sourcing + release-binary checks for `run-*.sh`
- `cds_manifest_common.py` — helpers for CDS catalog Python tools

## FHIR model regen

This fork emits `crates/fhir/src/r4/` (a directory), not Helios's `r4.rs`.
Runbook: `docs/fhir-model-regen.md`.

| Script | Role |
|--------|------|
| `diff-fhir-model-signatures.py` | Compare struct/enum shape to Helios flat files |

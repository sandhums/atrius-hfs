# Scripts

Local clinical-reasoning helpers. Prefer these over ad-hoc `cargo run` so every
service uses the same **release binary** + `deploy/env/*.env` wiring.

## Start the stack (separate terminals)

```bash
./scripts/build-clinical-reasoning.sh   # once after code changes

./scripts/run-hts.sh
./scripts/run-hfs.sh              # clinical :8082
./scripts/run-kr-hfs.sh           # KR :8079
./scripts/run-cr-fhir-bridge.sh   # :8081
./scripts/run-cql-sidecar.sh      # :8088 (JVMsidecar or SIDECAR_JAR)
./scripts/run-cds-server.sh       # :8095
```

| Script | Binary | Default env |
|--------|--------|-------------|
| `run-hts.sh` | `target/release/hts` | `deploy/env/hts.env` |
| `run-hfs.sh` | `target/release/hfs` | `deploy/env/hfs-clinical.env` |
| `run-kr-hfs.sh` | `target/release/hfs` | `deploy/env/hfs-kr.env` |
| `run-cr-fhir-bridge.sh` | `target/release/cr-fhir-bridge` | `deploy/env/cr-fhir-bridge.env` |
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
| `activity-definition-apply-smoke.sh` | ActivityDefinition `$apply` |

## IG / profiles

| Script | Role |
|--------|------|
| `setup-atrius-profile-registry.sh` | **Recommended**: fetch published `package.tgz` → expand → relative manifests → verify |
| `build-atrius-profile-manifest.sh` | Regenerate `manifests/atrius-r4-profile-manifest-core.json` (default: atrius.in) |
| `load-atrius-ig-package.sh` | Expand package into `manifests/atrius-ig-package/` |
| `generate-atrius-mapper-manifest.py` | Runtime Atrius→QI-Core mapper |

After setup, clinical HFS uses:
`HFS_PROFILE_MANIFEST=manifests/atrius-r4-profile-manifest-core.json` +
`HFS_PROFILE_VALIDATION_MODE=strict`.

## Shared

- `lib/common.sh` — env sourcing + release-binary checks for `run-*.sh`
- `cds_manifest_common.py` — helpers for CDS catalog Python tools

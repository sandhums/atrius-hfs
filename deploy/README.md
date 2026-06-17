# Deployment assets

## Local development

| Path | Purpose |
|------|---------|
| `clinical/.env.atrius.example` | Clinical HFS — copy to `.env` in repo root or `deploy/clinical/.env` |
| `kr/.env.kr.example` | KR HFS — copy to `deploy/kr/.env.kr` |

## Production (systemd)

| Path | Purpose |
|------|---------|
| `env/*.env.example` | Per-service env templates → `/etc/atrius/*.env` |
| `systemd/*.service` | Unit files for HTS, HFS (×2), bridge, sidecar, cds-server |
| `systemd/atrius-clinical-reasoning.target` | Start/stop entire stack |
| `systemd/install.sh` | Install binaries, env seeds, enable units |
| `bin/run-cql-sidecar.sh` | JVM sidecar launcher (`SIDECAR_JAR`, `JAVA_OPTS`) |

Full guide: [docs/clinical-reasoning/production-deployment.md](../docs/clinical-reasoning/production-deployment.md)

KR library pinning & cache flush: [docs/clinical-reasoning/kr-library-pinning.md](../docs/clinical-reasoning/kr-library-pinning.md)

```bash
cargo build --release -p helios-hts -p helios-hfs -p cr-fhir-bridge -p cds-server
SIDECAR_JAR_SRC=/path/to/sidecar.jar ./deploy/systemd/install.sh
sudo systemctl enable --now atrius-clinical-reasoning.target
```

# Deployment assets

## Local development

Prefer **`deploy/env/*.env`** + **`./scripts/run-*.sh`** (release binaries). See [scripts/README.md](../scripts/README.md) and [startup-guide.md](../docs/clinical-reasoning/startup-guide.md).

| Path | Purpose |
|------|---------|
| `env/hfs-clinical.env` | Clinical HFS (`./scripts/run-hfs.sh`) |
| `env/hfs-kr.env` | KR HFS (`./scripts/run-kr-hfs.sh`) |
| `env/hts.env` | HTS (`./scripts/run-hts.sh`; auto-seeded from example) |
| `env/cds-server.env` | cds-server (`./scripts/run-cds-server.sh`) |
| `env/cql-sidecar.env` | Sidecar (`./scripts/run-cql-sidecar.sh`) |
| `env/*.env.example` | Templates (also used for production `/etc/atrius`) |
| `clinical/.env.atrius.example` | Legacy sqlite-oriented notes — prefer `env/hfs-clinical.env` |

```bash
./scripts/build-clinical-reasoning.sh
./scripts/run-hts.sh          # terminal 1
./scripts/run-hfs.sh          # terminal 2
./scripts/run-kr-hfs.sh       # terminal 3
./scripts/run-cql-sidecar.sh
./scripts/run-cds-server.sh
```

## Production (systemd)

| Path | Purpose |
|------|---------|
| `env/*.env.example` | Per-service env templates → `/etc/atrius/*.env` |
| `systemd/*.service` | Unit files for HTS, HFS (×2), sidecar, cds-server |
| `systemd/atrius-clinical-reasoning.target` | Start/stop entire stack |
| `systemd/install.sh` | Install binaries, env seeds, enable units |
| `bin/run-cql-sidecar.sh` | JVM sidecar launcher (`SIDECAR_JAR`, `JAVA_OPTS`) |

Full guide: [docs/clinical-reasoning/production-deployment.md](../docs/clinical-reasoning/production-deployment.md)

KR library pinning & cache flush: [docs/clinical-reasoning/kr-library-pinning.md](../docs/clinical-reasoning/kr-library-pinning.md)

```bash
./scripts/build-clinical-reasoning.sh
# or:
# cargo build --release -p helios-hts -p helios-hfs --bin hfs --features postgres,redis,R4 \
#   -p cds-server
SIDECAR_JAR_SRC=/path/to/sidecar.jar ./deploy/systemd/install.sh
sudo systemctl enable --now atrius-clinical-reasoning.target
```

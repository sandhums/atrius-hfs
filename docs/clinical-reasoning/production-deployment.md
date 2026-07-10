# Clinical Reasoning Stack — Production Deployment (systemd)

Run the full CDS + eCQM stack under **systemd** with documented env files instead of manual terminals.

Local development remains in [startup-guide.md](./startup-guide.md).

## Layout

Default install paths (`deploy/systemd/install.sh`):

| Path | Contents |
|------|----------|
| `/opt/atrius/bin/` | `hts`, `hfs`, `cr-fhir-bridge`, `cds-server`, `run-cql-sidecar.sh` |
| `/opt/atrius/lib/` | `cql-sidecar.jar` (from JVMsidecar build) |
| `/opt/atrius/data/` | SQLite DBs, `HFS_DATA_DIR` artifacts |
| `/opt/atrius/manifests/` | Profile manifest, CDS catalog JSON |
| `/etc/atrius/*.env` | Per-service configuration (not in git) |
| `/etc/systemd/system/atrius-*.service` | Unit files |

Override install root: `ATRIUS_HOME=/srv/atrius ./deploy/systemd/install.sh`

## Services and dependency order

```text
atrius-hts.service
atrius-hfs-clinical.service  ──┐
atrius-hfs-kr.service          ──┼──► atrius-cr-fhir-bridge.service
                                 │           │
                                 │           ▼
                                 └──► atrius-cql-sidecar.service
                                             │
                                             ▼
                                    atrius-cds-server.service

atrius-clinical-reasoning.target  →  enables / starts all of the above
```

| Unit | Port (default) | Env file |
|------|----------------|----------|
| `atrius-hts.service` | 9091 | `/etc/atrius/hts.env` |
| `atrius-hfs-clinical.service` | 8082 | `/etc/atrius/hfs-clinical.env` |
| `atrius-hfs-kr.service` | 8079 | `/etc/atrius/hfs-kr.env` |
| `atrius-cr-fhir-bridge.service` | 8081 | `/etc/atrius/cr-fhir-bridge.env` |
| `atrius-cql-sidecar.service` | 8088 | `/etc/atrius/cql-sidecar.env` |
| `atrius-cds-server.service` | 8095 | `/etc/atrius/cds-server.env` |

Env templates (committed): `deploy/env/*.env.example`

## Build and install

### 1. Rust binaries

From the atrius-hfs repository root:

```bash
./scripts/build-clinical-reasoning.sh
# Equivalent:
# cargo build --release -p helios-hts --bin hts
# cargo build --release -p helios-hfs --bin hfs --features postgres,redis,R4
# cargo build --release -p cr-fhir-bridge --bin cr-fhir-bridge
# cargo build --release -p cds-server --bin cds-server
```

### 2. JVM sidecar jar

From the **JVMsidecar** repository:

```bash
mvn -q package -DskipTests
# Copy the runnable jar — path varies by build; common:
#   target/jvm-sidecar-*.jar  or  target/*-exec.jar
```

### 3. Install

```bash
SIDECAR_JAR_SRC=/path/to/JVMsidecar/target/your-sidecar.jar \
  ./deploy/systemd/install.sh
```

Creates user `atrius` (if missing), copies binaries to `/opt/atrius/bin`, seeds `/etc/atrius/*.env` from examples when absent, installs systemd units, runs `daemon-reload`.

### 4. Configure env

Edit each file under `/etc/atrius/`. Critical wiring (must stay aligned):

```bash
# clinical HFS
HFS_TERMINOLOGY_SERVER=http://127.0.0.1:9091

# bridge
CR_FHIR_BRIDGE_UPSTREAM_URL=http://127.0.0.1:8082
CR_FHIR_BRIDGE_KR_URL=http://127.0.0.1:8079

# cds-server + sidecar evaluate requests
CDS_HFS_BASE_URL=http://127.0.0.1:8081   # bridge — NOT 8082
CDS_HTS_BASE_URL=http://127.0.0.1:9091
CDS_LIBRARY_BASE_URL=http://127.0.0.1:8079
CDS_CLINICAL_REASONING_URL=http://127.0.0.1:8088
```

Use **absolute paths** for SQLite files in production (`/opt/atrius/data/*.db`).

Expose cds-server to external CDS clients: set `CDS_SERVER_HOST=0.0.0.0` and put TLS/auth on a reverse proxy.

### 5. Import data (once per environment)

Follow [data-import.md](./data-import.md) using the same DB paths as in env files. **KR libraries:** eCQM NPM import + Atrius IG import — see [data-import.md § KR HFS](./data-import.md#kr-hfs-knowledge-libraries). Minimum before enabling the target:

- HTS terminology (+ SNOMED, ICD, RxNorm as needed)
- Clinical patient chart (or demo)
- KR eCQM libraries
- CDS manifest at `CDS_SERVICES_MANIFEST_PATH`

### 6. Enable stack

```bash
sudo systemctl enable --now atrius-clinical-reasoning.target
```

Start or stop individual services:

```bash
sudo systemctl restart atrius-cr-fhir-bridge
sudo systemctl stop atrius-cds-server
```

## Operations

### Status and logs

```bash
systemctl status atrius-clinical-reasoning.target
systemctl status 'atrius-*'
journalctl -u atrius-cds-server -f
journalctl -u atrius-cql-sidecar --since "1 hour ago"
```

### Health checks

| Service | URL |
|---------|-----|
| HTS | `GET http://127.0.0.1:9091/health` |
| Clinical HFS | `GET http://127.0.0.1:8082/health` |
| KR HFS | `GET http://127.0.0.1:8079/health` |
| Bridge | `GET http://127.0.0.1:8081/health` |
| Sidecar | `GET http://127.0.0.1:8088/health` |
| cds-server | `GET http://127.0.0.1:8095/health` (liveness), `GET /ready` (KR library pins) |
| cds-server discovery | `GET http://127.0.0.1:8095/cds-services` |

Wire `/ready` into orchestration when `CDS_VALIDATE_KR_LIBRARIES=true`.

### KR library version pinning (production)

Enable on **cds-server**:

```bash
CDS_REQUIRE_LIBRARY_VERSION=true
CDS_VALIDATE_KR_LIBRARIES=true
```

| Flag | Effect |
|------|--------|
| `CDS_REQUIRE_LIBRARY_VERSION` | Manifest services with `libraryId`+`expression` must declare `libraryVersion` |
| `CDS_VALIDATE_KR_LIBRARIES` | At startup, probe KR `Library?name=…&version=…` for each unique pin; fail boot if missing |

`GET /ready` returns **200** when the startup probe succeeded, **503** when KR libraries are unavailable.

Regenerate the manifest after KR import so pins match deployed artifacts:

```bash
./scripts/generate-cds-hooks-manifest.py   # sets libraryVersion from KR Library.version
```

**Upgrade runbook (eCQM library change):**

1. Import new ELM to KR — prefer a **new** `Library.version` (e.g. `0.1.1`), not in-place overwrite.
2. Regenerate CDS manifest / redeploy `cds-services-kr.json`.
3. Restart **cds-server** (re-probes KR pins) or rely on startup validation.
4. **Flush sidecar ELM cache** (no full JVM restart required):

```bash
# Local / systemd (set SIDECAR_ADMIN_TOKEN in production)
curl -s -X POST http://127.0.0.1:8088/v1/admin/cache/libraries/clear \
  -H "Authorization: Bearer ${SIDECAR_ADMIN_TOKEN:-}"
```

Clears compiled CQL stacks, cached KR `Library` resources, and ValueSet expansion buckets.

5. Smoke one CDS invoke (e.g. `./scripts/cds-cms165-prefetch-smoke.sh`).

**Scope:** This is **slice 1** of library pinning + cache policy (manifest enforcement, KR probe, manual cache flush). **Slice 1 is complete** when ops flags are enabled and `/ready` + smoke pass. Next work: **[roadmap.md](./roadmap.md)** (slice 2 cache keys → slice 3 PlanDefinition-first → authoring).

### Binary upgrade

```bash
cargo build --release ...
SIDECAR_JAR_SRC=... ./deploy/systemd/install.sh   # overwrites binaries
sudo systemctl restart atrius-clinical-reasoning.target
```

After KR-only data changes, prefer **cache flush** over full stack restart when possible.

### Uninstall

```bash
sudo systemctl disable --now atrius-clinical-reasoning.target
sudo rm /etc/systemd/system/atrius-*.service /etc/systemd/system/atrius-*.target
sudo systemctl daemon-reload
# Remove /opt/atrius and /etc/atrius manually if desired
```

## Security notes

- Units run as unprivileged user `atrius`.
- Env files in `/etc/atrius/` should be mode `0640`, owned `root:atrius`.
- Internal services default to `127.0.0.1`; expose only cds-server (or an API gateway) externally.
- Do not set `CDS_MEASUREMENT_PERIOD_*` in production — pass `measurementPeriod` on each CDS invoke.

## Observability (minimal v1)

Structured invoke logs on **cds-server** (`cds_invoke_metrics`) and per-request + `GET /metrics` counters on the **JVM sidecar**. No Prometheus in this slice — see **[observability.md](./observability.md)**.

## See also

- [roadmap.md](./roadmap.md) — stack status, slice 2–3 plan, authoring phase
- [README.md](./README.md) — architecture and port map
- [observability.md](./observability.md) — invoke logs, sidecar `/metrics`, operational queries
- [kr-library-pinning.md](./kr-library-pinning.md) — version pinning, cache policy, slice 1 detail
- [cds-prefetch.md](./cds-prefetch.md) — CDS client vs backend prefetch
- [startup-guide.md](./startup-guide.md) — manual local stack
- [troubleshooting.md](./troubleshooting.md)

# Clinical Reasoning — Observability

cds-server uses Helios `helios-observability`: Prometheus `GET /metrics`, optional OTLP traces (`OTEL_EXPORTER_OTLP_ENDPOINT`), and JSON logs (`LOG_FORMAT=json`). Invoke-level structured logs (`cds_invoke_metrics`) remain for per-service latency.

JVM sidecar uses Prometheus text at `GET /metrics` (JSON at `/metrics.json`). Set `SIDECAR_ENV=staging|production` and `SIDECAR_ADMIN_TOKEN` in non-dev.

## Design

| Layer | Mechanism | Aggregation |
|-------|-----------|-------------|
| **cds-server** | Prometheus `GET /metrics` + optional OTLP | Prometheus / Tempo (see `atrius-his/deploy/observability`) |
| **cds-server** | `tracing` target `cds_invoke_metrics` | `journalctl -u atrius-cds-server \| grep cds_invoke_metrics` |
| **JVM sidecar** | SLF4J INFO on evaluate/apply completion | Same pattern in `atrius-cql-sidecar` logs |
| **JVM sidecar** | Prometheus `GET /metrics` (+ JSON `/metrics.json`) | Prometheus scrape (`host.docker.internal:8088`) |

## cds-server invoke logs

Each completed CDS Hooks invoke (demo, sidecar `$apply`, sidecar `evaluate/expression`) emits one line:

```text
target=cds_invoke_metrics
service_id=<cds service id>
eval_path=demo | apply | evaluate
library_id=<manifest library id>
library_version=<optional>
duration_ms=<u64>
outcome=ok | error
http_status=<optional u16>
```

Example (journald):

```bash
journalctl -u atrius-cds-server -f | grep cds_invoke_metrics
```

Fields are stable for log-based metrics (count errors, p95 duration by `service_id`, etc.).

Implementation: `crates/cds-server/src/invoke_metrics.rs`, wired from `crates/cds-server/src/services/mod.rs`.

## Environment

| Variable | Effect |
|----------|--------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint (e.g. `http://127.0.0.1:4317`) |
| `OTEL_SERVICE_NAME` | Override (default `cds-server`) |
| `LOG_FORMAT=json` | JSON structured logs |
| `HELIOS_OBS_MODE` | `default` / `full` / `no-span` / `off` |

## JVM sidecar per-request logs

**Evaluate** (`POST /v1/evaluate/expression`):

```text
sidecar evaluate completed libraryId=... libraryVersion=... expression=... durationMs=...
  libraryStackCacheHit=true|false|null krLibraryFetches=<n> error=false|true
```

**Apply** (`POST /v1/plandefinition/apply`):

```text
sidecar apply completed planDefinitionId=... durationMs=... error=false|true
```

`krLibraryFetches` is the number of KR `Library` HTTP loads during that request (uncached fetches only).

## JVM sidecar `GET /metrics`

Default response is **Prometheus text** (scrapable). JSON remains at `/metrics.json` or with `Accept: application/json` / `?format=json`.

```bash
curl -s http://127.0.0.1:8088/metrics | head
curl -s http://127.0.0.1:8088/metrics.json | jq .
```

Prometheus series (label `service="cql-sidecar"`):

| Metric | Type | Meaning |
|--------|------|---------|
| `sidecar_evaluate_total` / `sidecar_evaluate_errors_total` | counter | Expression evaluations |
| `sidecar_evaluate_duration_ms_sum` / `sidecar_evaluate_avg_duration_ms` | counter / gauge | Evaluate latency |
| `sidecar_apply_total` / `sidecar_apply_*` | counter / gauge | PlanDefinition `$apply` |
| `sidecar_library_stack_cache_hits_total` / `_misses_total` | counter | Prepared CQL stack cache |
| `sidecar_kr_library_fetches_total` | counter | KR `Library` HTTP fetches |

**Admin auth:** set `SIDECAR_ENV=staging` (or `production`) and `SIDECAR_ADMIN_TOKEN` — process refuses to start without the token. Unset / `development` keeps local admin open when the token is unset.

Use **cache hit ratio** ≈ `hits / (hits + misses)` after warm-up to validate KR pinning and cache flush policy. See [kr-library-pinning.md](./kr-library-pinning.md).

## Operational queries

```bash
# CDS invoke error rate (last hour, rough)
journalctl -u atrius-cds-server --since "1 hour ago" | grep cds_invoke_metrics | grep -c 'outcome=error'

# Sidecar KR fetch pressure (JSON convenience)
curl -s http://127.0.0.1:8088/metrics.json | jq '.krLibraryFetches'

# Library stack cache effectiveness
curl -s http://127.0.0.1:8088/metrics.json | jq '{hits:.libraryStackCacheHits, misses:.libraryStackCacheMisses}'
```

## See also

- [production-deployment.md](./production-deployment.md) — systemd units and ports
- [kr-library-pinning.md](./kr-library-pinning.md) — version pinning and cache flush
- `atrius-his/deploy/observability/` — Prometheus scrape includes `cql-sidecar:8088`

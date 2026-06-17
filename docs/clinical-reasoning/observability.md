# Clinical Reasoning — Minimal Observability (v1)

Production hardening slice: **structured logs + simple counters** — no Prometheus or OpenTelemetry in this version.

## Design

| Layer | Mechanism | Aggregation |
|-------|-----------|-------------|
| **cds-server** | `tracing` target `cds_invoke_metrics` | `journalctl -u atrius-cds-server \| grep cds_invoke_metrics` |
| **JVM sidecar** | SLF4J INFO on evaluate/apply completion | Same pattern in `atrius-cql-sidecar` logs |
| **JVM sidecar** | `GET /metrics` JSON snapshot | Ad-hoc curl, load-balancer health scripts |

Full Prometheus / Grafana dashboards are on the [hardening roadmap](#roadmap).

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

Fields are stable for log-based metrics (count errors, p95 duration by `service_id`, etc.) without pulling in a metrics stack.

Implementation: `crates/cds-server/src/invoke_metrics.rs`, wired from `crates/cds-server/src/services/mod.rs`.

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

Process-wide cumulative counters (resets on restart):

```bash
curl -s http://127.0.0.1:8088/metrics | jq .
```

```json
{
  "evaluateTotal": 42,
  "evaluateErrors": 1,
  "evaluateAvgDurationMs": 312.5,
  "applyTotal": 3,
  "applyAvgDurationMs": 1200.0,
  "libraryStackCacheHits": 30,
  "libraryStackCacheMisses": 12,
  "krLibraryFetches": 8
}
```

| Field | Meaning |
|-------|---------|
| `evaluateTotal` / `evaluateErrors` | Expression evaluations |
| `evaluateAvgDurationMs` | Mean wall time per evaluate |
| `applyTotal` / `applyAvgDurationMs` | PlanDefinition `$apply` calls |
| `libraryStackCacheHits` / `libraryStackCacheMisses` | Prepared CQL stack cache (FHIR-backed loads only) |
| `krLibraryFetches` | Total KR `Library` resource fetches (process lifetime) |

Use **cache hit ratio** ≈ `hits / (hits + misses)` after warm-up to validate KR pinning and cache flush policy. See [kr-library-pinning.md](./kr-library-pinning.md).

## Operational queries

```bash
# CDS invoke error rate (last hour, rough)
journalctl -u atrius-cds-server --since "1 hour ago" | grep cds_invoke_metrics | grep -c 'outcome=error'

# Sidecar KR fetch pressure
curl -s http://127.0.0.1:8088/metrics | jq '.krLibraryFetches'

# Library stack cache effectiveness
curl -s http://127.0.0.1:8088/metrics | jq '{hits:.libraryStackCacheHits, misses:.libraryStackCacheMisses}'
```

## Roadmap (not in v1)

- Prometheus `/metrics` text format or OTLP export
- Per-tenant URL routing metrics
- Dashboards (Grafana) for invoke latency, KR fetch rate, cache hit ratio
- Alerting on `evaluateErrors` / CDS `outcome=error` thresholds

## See also

- [production-deployment.md](./production-deployment.md) — systemd units and ports
- [kr-library-pinning.md](./kr-library-pinning.md) — version pinning and cache flush

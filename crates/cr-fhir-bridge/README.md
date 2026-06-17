# cr-fhir-bridge

Thin **HTTP reverse proxy** between the JVM clinical reasoning sidecar and **clinical HFS**.

**Architecture & startup:** [docs/clinical-reasoning/README.md](../../docs/clinical-reasoning/README.md) · [startup guide](../../docs/clinical-reasoning/startup-guide.md)

The sidecar evaluates QI-Core ELM and reads patient data via `hfsBaseUrl`. Point that URL at this bridge instead of raw clinical HFS so responses are projected through [`atrius-runtime-mapper`](../atrius-runtime-mapper) before the sidecar sees them.

**Knowledge Repository (KR)** and **HTS** are unchanged — configure `libraryBaseUrl` and `htsBaseUrl` on the sidecar separately.

## Architecture

```text
sidecar hfsBaseUrl → cr-fhir-bridge
                         ├─ /Library*     → KR HFS (CQL includes; pass-through)
                         └─ everything else → clinical HFS (Atrius→QI-Core map)
```

The JVM sidecar resolves **primary** libraries from `libraryBaseUrl` (KR) but CQL **`include`** dependencies from `hfsBaseUrl`. Point the sidecar at this bridge and set **`CR_FHIR_BRIDGE_KR_URL`** so `/Library` reads reach KR while Patient/Condition/etc. still come from clinical HFS.

## Run

```bash
# Clinical HFS on 8082; KR on 8079; bridge on 8081
CR_FHIR_BRIDGE_UPSTREAM_URL=http://127.0.0.1:8082 \
CR_FHIR_BRIDGE_KR_URL=http://127.0.0.1:8079 \
  cargo run --bin cr-fhir-bridge

# Sidecar / cds-server: aim hfsBaseUrl at the bridge (not raw clinical HFS)
CDS_HFS_BASE_URL=http://127.0.0.1:8081
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CR_FHIR_BRIDGE_HOST` | `127.0.0.1` | Listen host |
| `CR_FHIR_BRIDGE_PORT` | `8081` | Listen port |
| `CR_FHIR_BRIDGE_UPSTREAM_URL` | `http://127.0.0.1:8080` | Clinical HFS base URL |
| `CR_FHIR_BRIDGE_KR_URL` | (none) | KR HFS base URL — proxy `/Library` reads here for CQL includes |
| `ATRIUS_MAPPER_MANIFEST` | (none) | Path to mapper manifest JSON; uses built-in v0.1 when unset |
| `CR_FHIR_BRIDGE_LOG_LEVEL` | `info` | Log level |
| `CR_FHIR_BRIDGE_ENABLE_CORS` | `true` | Enable CORS |
| `CR_FHIR_BRIDGE_REQUEST_TIMEOUT` | `30` | Upstream request timeout (seconds) |
| `CR_FHIR_BRIDGE_MAX_BODY_SIZE` | `10485760` | Max request/response body (bytes) |
| `CR_FHIR_BRIDGE_SIDECAR_URL` | `http://127.0.0.1:8088` | JVM sidecar for `$apply` (empty disables apply routes) |
| `CR_FHIR_BRIDGE_HTS_URL` | `http://127.0.0.1:8090` | HTS base forwarded to sidecar on `$apply` |
| `CR_FHIR_BRIDGE_PUBLIC_URL` | `http://{host}:{port}` | Bridge URL used as sidecar `hfsBaseUrl` during `$apply` |

## Behaviour

- Proxies all FHIR REST paths and methods to upstream clinical HFS.
- When `CR_FHIR_BRIDGE_KR_URL` is set, **`GET/POST /Library`** (and sub-paths) go to KR unchanged (no Atrius projection).
- **`POST /PlanDefinition/$apply`**, **`POST /PlanDefinition/{id}/$apply`**, **`POST /ActivityDefinition/$apply`**, and **`POST /ActivityDefinition/{id}/$apply`** accept FHIR **Parameters** and delegate to the JVM sidecar (CQF). Responses are **Parameters** with a **`return`** resource. Requires **`CR_FHIR_BRIDGE_SIDECAR_URL`**.
- **`GET /metadata`** advertises `$apply` when the sidecar is configured.
- Forwards tenant and auth headers (`X-Tenant-ID`, `Authorization`, `Accept`, `Prefer`, conditional headers).
- On successful `application/fhir+json` (or `application/json`) responses, applies runtime projection to `Bundle` and single-resource bodies.
- Writes and non-JSON responses pass through unchanged.

### `$apply` example

```bash
curl -sS -X POST http://127.0.0.1:8081/PlanDefinition/cms165/$apply \
  -H 'Content-Type: application/fhir+json' \
  -d '{
    "resourceType": "Parameters",
    "parameter": [
      { "name": "subject", "valueString": "Patient/demo" },
      { "name": "encounter", "valueString": "Encounter/er-1" },
      { "name": "practitioner", "valueString": "Practitioner/doc-1" }
    ]
  }'
```

## Status

v0.1 — Condition projection via shared mapper crate. Encounter/Observation rules follow in mapper v0.2+.

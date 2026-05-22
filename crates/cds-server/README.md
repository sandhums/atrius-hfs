# cds-server

HTTP CDS Hooks discovery + invocation server. Loads **many CDS service ids** from a **Knowledge Repository FHIR `Binary`** (JSON payload, base64 in `Binary.data`) or from **`CDS_SERVICES_MANIFEST_PATH`**. Each entry maps to **`patient-view`** plus JVM **`libraryId` / `expression`** for **[`atrius-clinical-reasoning`](../../atrius-clinical-reasoning)**.

See **[cds-services.manifest.example.json](cds-services.manifest.example.json)** for a multi-service template.

## KR manifest (`Binary`)

1. Author JSON:

```json
{
  "services": [
    {
      "id": "hello-world-name",
      "hook": "patient-view",
      "title": "Patient name",
      "description": "Greeter",
      "prefetch": {
        "patient": "Patient/{{context.patientId}}"
      },
      "libraryId": "HelloWorld",
      "expression": "PatientName",
      "resolveFromFhir": true,
      "cdsHooksVersion": "1.0"
    }
  ]
}
```

2. Store as FHIR R4 `Binary` with `contentType: application/json` and `data` = base64(JSON).

3. Run `cds-server` with **`CDS_LIBRARY_BASE_URL`** = KR base and **`CDS_KR_SERVICES_BINARY_ID`** = that `Binary.id`.

When **`CDS_CLINICAL_REASONING_URL`** is set, you must supply the catalog via **`CDS_SERVICES_MANIFEST_PATH`** or **`CDS_KR_SERVICES_BINARY_ID`** (with **`CDS_LIBRARY_BASE_URL`**).

Without sidecar URL (or empty), the server stays in **demo** mode but still uses the manifest for discovery; invokes return demo cards.

## Run (examples)

KR-driven:

```bash
cargo run -p cds-server -- \
  --clinical-reasoning-url http://127.0.0.1:8088 \
  --library-base-url http://127.0.0.1:8079 \
  --kr-services-binary-id cds-services-catalog \
  --hfs-base-url http://localhost:8082 \
  --hts-base-url http://localhost:8090
```

Local JSON file:

```bash
cargo run -p cds-server -- \
  --clinical-reasoning-url http://127.0.0.1:8088 \
  --services-manifest-path ./cds-services.manifest.json \
  --hfs-base-url http://localhost:8082 \
  --hts-base-url http://localhost:8090 \
  --library-base-url http://127.0.0.1:8079
```

Endpoints:

- `GET /health`
- `GET /cds-services` — discovery (all manifest ids)
- `POST /cds-services/{id}` — hook payload (`application/json`)
- `POST /cds-services/{id}/feedback`

See **`cargo run -p cds-server -- --help`** for env names (`CDS_*`).

**Clinical data 404s with Spring-style JSON:** If evaluation errors mention HTTP 404 with a body like `"timestamp"`, `"path":"/Condition"`, `"error":"Not Found"`, that shape is almost never from Helios HFS (FHIR uses `OperationOutcome` or search `Bundle`s). It usually means the **JVM FHIR client inside the sidecar** hit a **non-FHIR** HTTP stack—often **`localhost` vs `127.0.0.1`**, a **path prefix** mismatch, or the sidecar running in **Docker** where `localhost` is not the host. **`CDS_HFS_BASE_URL`** must be the exact clinical base reachable **from the sidecar process**, matching what you verify with Postman (same scheme/host/port/prefix). Example ports (yours may differ): HFS **8082**, KR **8079**, JVM sidecar **8088**, cds-server **8095**.

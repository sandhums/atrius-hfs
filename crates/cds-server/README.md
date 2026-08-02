# cds-server

HTTP CDS Hooks discovery + invocation server. Loads **many CDS service ids** from a **Knowledge Repository FHIR `Binary`** (JSON payload, base64 in `Binary.data`) or from **`CDS_SERVICES_MANIFEST_PATH`**. Each entry maps to a [CDS Hooks Library](https://cds-hooks.hl7.org/hooks/) hook plus JVM **`libraryId` / `expression`** or **`planDefinitionId`** via the in-crate [`clinical_reasoning`](src/clinical_reasoning/) sidecar client.

**Architecture & startup:** [docs/clinical-reasoning/README.md](../../docs/clinical-reasoning/README.md) · [startup guide](../../docs/clinical-reasoning/startup-guide.md)

**Generate catalog from KR PlanDefinitions:** [scripts/generate-cds-hooks-manifest.py](../../scripts/generate-cds-hooks-manifest.py) → `manifests/cds-services-kr.json` (or `./scripts/setup-plandefinition-cds-catalog.sh`)

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
        "patient": "Patient/{{context.patientId}}",
        "conditions": "Condition?patient={{context.patientId}}",
        "encounters": "Encounter?patient={{context.patientId}}",
        "observations": "Observation?patient={{context.patientId}}",
        "procedures": "Procedure?patient={{context.patientId}}",
        "medicationRequests": "MedicationRequest?patient={{context.patientId}}",
        "immunizations": "Immunization?patient={{context.patientId}}",
        "diagnosticReports": "DiagnosticReport?patient={{context.patientId}}",
        "serviceRequests": "ServiceRequest?patient={{context.patientId}}",
        "allergies": "AllergyIntolerance?patient={{context.patientId}}",
        "coverage": "Coverage?beneficiary=Patient/{{context.patientId}}"
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

### Measurement Period (eCQM reporting interval)

CMS and other eCQM CQL libraries take a **`Measurement Period`** parameter: the **reporting window** for the measure run (e.g. CY2026). CQL then counts patients whose encounters, BP readings, etc. fall **within** that interval.

Set on **each CDS Hooks invoke** in hook `context` (primary):

```json
POST /cds-services/atriuscms165controllinghighbp
{
  "hook": "patient-view",
  "hookInstance": "...",
  "context": {
    "patientId": "cms165-demo",
    "userId": "Practitioner/example",
    "measurementPeriod": {
      "low": "2026-01-01",
      "high": "2026-12-31"
    }
  },
  "prefetch": { ... }
}
```

Optional fallbacks: CDS `extension["https://atrius.dev/cds-measurement-period"]`, or
`CDS_MEASUREMENT_PERIOD_LOW` / `CDS_MEASUREMENT_PERIOD_HIGH` on cds-server for local dev.

### Prefetch (CDS client responsibility)

`GET /cds-services` returns **prefetch templates** per service (FHIR URLs with `{{context.patientId}}`).
The **CDS client** resolves those against its FHIR server and sends **populated** resources on
`POST /cds-services/{id}`. cds-server forwards `prefetch` to the sidecar; it does not fetch clinical data.

**Backend scope:** discovery templates from manifest, pass-through on invoke (`services/mod.rs`),
and sidecar consumption (JVM repo). Template resolution is not implemented in cds-server.
Full breakdown: [docs/clinical-reasoning/cds-prefetch.md](../../docs/clinical-reasoning/cds-prefetch.md).

- Context-only invoke (`prefetch: {}`): sidecar uses REST fallback via `CDS_HFS_BASE_URL` (clinical HFS).
- Client-simulated invoke: see [startup guide — CDS client responsibilities](../../docs/clinical-reasoning/startup-guide.md#cds-client-responsibilities-prefetch--invoke) and Postman collection [docs/clinical-reasoning/postman/](../../docs/clinical-reasoning/postman/).

### SMART `fhirAuthorization` (production FHIR access)

Per [CDS Hooks FHIR resource access](https://cds-hooks.hl7.org/), the CDS client may pass
`fhirServer` + `fhirAuthorization` so the CDS service can read clinical FHIR beyond prefetch
using a **short-lived OAuth 2.0 bearer token** scoped to the CDS service and current user.

cds-server:

1. Validates `fhirAuthorization` (`Bearer`, non-empty `access_token`, `scope`, `subject`).
2. Requires `fhirServer` when `fhirAuthorization` is present.
3. Overrides sidecar **`hfsBaseUrl`** with `fhirServer` for that invoke (HTS / KR bases unchanged).
4. Forwards credentials to the JVM sidecar as `fhirAuthorization` (clinical REST only).

Local dev without SMART: omit both fields; configured `CDS_HFS_BASE_URL` (clinical HFS) is used.

| Env | Role |
|-----|------|
| `CDS_REQUIRE_FHIR_AUTHORIZATION=true` | Reject invoke without `fhirAuthorization` |
| `CDS_FHIR_SERVER_ALLOWLIST` | Comma-separated allowed `fhirServer` hosts |
| `CDS_FEEDBACK_FHIR_BASE_URL` | Enable GuidanceResponse / Flag writes to clinical HFS |
| `CDS_FEEDBACK_OAUTH_TOKEN_URL` / `CLIENT_ID` / `CLIENT_SECRET` | Client-credentials for those writes (preferred) |
| `CDS_FEEDBACK_OAUTH_TLS_INSECURE=true` | Local Keycloak self-signed only |
| `CDS_FEEDBACK_FHIR_BEARER_TOKEN` | Static bearer override (expires; smoke only) |

Example invoke body:

```json
{
  "hook": "patient-view",
  "hookInstance": "...",
  "fhirServer": "https://ehr.example.com/fhir",
  "fhirAuthorization": {
    "access_token": "opaque-token-from-ehr",
    "token_type": "Bearer",
    "expires_in": 300,
    "scope": "user/Patient.read user/Observation.read",
    "subject": "atriuscms165controllinghighbp"
  },
  "context": {
    "patientId": "cms165-demo",
    "userId": "Practitioner/example",
    "measurementPeriod": { "low": "2026-01-01", "high": "2026-12-31" }
  },
  "prefetch": {}
}
```

cds-server does **not** validate the token with Keycloak/Okta — the **EHR authorization server**
issues it; the **clinical FHIR server** enforces it on sidecar REST calls. Token validation at
cds-server ingress (optional JWT introspection) is a separate hardening step.

### KR library version pinning

Manifest `libraryVersion` pins which CQL ELM the sidecar loads from KR. Production flags:

| Env | Role |
|-----|------|
| `CDS_REQUIRE_LIBRARY_VERSION=true` | Reject manifests / invokes without explicit version |
| `CDS_VALIDATE_KR_LIBRARIES=true` | Startup probe + `GET /ready` for library pins and manifest `planDefinitionId` |

After KR re-import, flush sidecar caches: `POST {sidecar}/v1/admin/cache/libraries/clear`
(optional `SIDECAR_ADMIN_TOKEN`).

Full scope (first slice vs roadmap): [kr-library-pinning.md](../../docs/clinical-reasoning/kr-library-pinning.md).
Runbook: [production-deployment.md](../../docs/clinical-reasoning/production-deployment.md).

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
CDS_MEASUREMENT_PERIOD_LOW=2026-01-01 CDS_MEASUREMENT_PERIOD_HIGH=2026-12-31 \
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

**CQL `include` libraries:** The sidecar loads the **primary and all `include` libraries from `libraryBaseUrl` (KR)**. Set **`CDS_LIBRARY_BASE_URL`** to KR; clinical **`CDS_HFS_BASE_URL`** Re-import KR libraries from AtriusIGDraft (`import-atrius-kr-libraries.py`) so `Library.version` matches ELM (avoids `libraryVersion does not match ELM identifier version`).

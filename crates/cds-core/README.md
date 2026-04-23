# cds-core

**Evaluation logic** and [`CdsHooksService`](https://docs.rs/helios-cds-hooks) implementations. **Outbound** EHR reads use `reqwest` in [`fhir_fetch`](src/fhir_fetch.rs) (`GET Patient/{id}` when `fhirServer` and `fhirAuthorization` are both set). CDS Hooks **inbound** HTTP is in [`cds-server`](../cds-server).

- [`patient_view_greeting`](src/evaluate.rs) — uses prefetch keys and, when the EHR authorizes, [`try_patient_display_name`](src/fhir_fetch.rs) to show a name from the live Patient resource.
- [`get_patient_json`](src/fhir_fetch.rs) / [`FhirFetchError`](src/fhir_fetch.rs) — reusable FHIR read helpers; add POST `$evaluate` next to the same module pattern.
- [`PatientGreeterService`](src/patient_greeter.rs) — example `patient-view` service, then build [`Card`](https://docs.rs/helios-cds-hooks)s.

```text
EHR -> cds-server (HTTP in) -> ServiceWrapper<YourService> -> cds-core (evaluate, FHIR GET out) -> CdsResponse
```

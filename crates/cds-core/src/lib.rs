//! Clinical **evaluation** and [`CdsHooksService`](helios_cds_hooks::CdsHooksService) implementations.
//!
//! Routing and TLS termination live in [`cds-server`](../cds-server). This crate runs rules,
//! optional **EHR FHIR reads** ([`fhir_fetch`](fhir_fetch)), and (later) [FHIR Clinical Reasoning](https://www.hl7.org/fhir/clinicalreasoning-module.html)
//!-style evaluation, then map to [`CdsResponse`](helios_cds_hooks::CdsResponse).

mod evaluate;
mod fhir_fetch;
mod patient_greeter;

pub use evaluate::patient_view_greeting;
pub use fhir_fetch::{
    FhirFetchError, get_patient_json, patient_display_name, try_patient_display_name,
};
pub use patient_greeter::PatientGreeterService;

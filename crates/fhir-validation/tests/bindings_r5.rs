//! R5 ValueSet binding integration tests.
//!
//! Covers binding targets used by the validator: primitive `code`, `string`, `uri`, `Coding`,
//! `CodeableConcept`, and `CodeableReference` (R5).
//!
//! - **Local terminology**: [`LocalTerminologyService`] (safe for CI without a terminology server).
//! - **Remote terminology**: several `#[tokio::test]` cases call [`RemoteTerminologyService`] against
//!   [HTS](https://github.com/) (Helios Terminology Server; default `http://localhost:9091`).
//!   Set `FHIR_TERMINOLOGY_BASE_URL` to override. HTS needs **FHIR core** ValueSets/CodeSystems for
//!   the HL7 bindings exercised here (not SNOMED, not R4 ABDM). **ABDM/NDHM** validation is R4-only:
//!   see **`r4_suite`** (`ndhm_patient_hts`, profile manifests under `manifests/`).
//!
//! Example JSON primarily lives under `crates/fhir/tests/data/json/R5/`; a few focused fixtures
//! under `tests/fixtures/r5/` cover types not present in that corpus (e.g. `StructureDefinition`
//! language, `Slot.serviceType`).
//!
//! Related integration crates: **`invariants_r5`**, **`profiles_r5`** (R5 `StructureDefinition` / `meta.profile`),
//! **`examples_r5`**, **`framework`**, **`r4_suite`** (R4 ABDM profiles + HTS).

#![cfg(feature = "R5")]
pub mod common;
// Integration-test submodules live under `tests/` by default; keep sources in `tests/bindings_r5/`.
#[path = "bindings_r5/harness.rs"]
mod harness;
#[path = "bindings_r5/remote_terminology.rs"]
mod remote_terminology;
#[path = "bindings_r5/resource_examples.rs"]
mod resource_examples;
#[path = "bindings_r5/resource_fixtures.rs"]
mod resource_fixtures;
#[path = "bindings_r5/smoke_curated.rs"]
mod smoke_curated;
#[path = "bindings_r5/unit_binding_handlers.rs"]
mod unit_binding_handlers;

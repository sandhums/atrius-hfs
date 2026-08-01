//! Generated local FHIR terminology tables.
//!
//! This crate holds the output of `fhir-valueset-gen`: per-version modules of
//! strongly typed CodeSystem enums, ValueSet membership helpers, and a
//! canonical-URL index used for local (offline) binding
//! validation. Codes that cannot be decided locally (licensed, intensional, or
//! very large terminologies such as SNOMED CT or LOINC) return
//! [`TerminologyValidationError::RemoteValidationRequired`], which the
//! validation layer escalates to a remote terminology service.
//!
//! The data is generated from `crates/fhir-gen/resources/<VERSION>/valuesets.json`.
//! To regenerate a version:
//!
//! ```bash
//! cargo run -p atrius-fhir-valueset-gen -- R4
//! ```
//!
//! Version modules are feature-gated the same way as `helios-fhir` (`R4` is
//! the default).

mod error;
pub use error::TerminologyValidationError;

#[cfg(feature = "R4")]
pub mod r4;
#[cfg(feature = "R4B")]
pub mod r4b;
#[cfg(feature = "R5")]
pub mod r5;
#[cfg(feature = "R6")]
pub mod r6;

//! FHIR validation framework.
//!
//! This crate provides structural validation, invariant evaluation, and
//! terminology-aware binding validation for generated FHIR model types.
//!
//! # Layout
//!
//! Core framework types live at the crate root.
//! Version-specific validation lives under `r4`, `r4b`, `r5`, and `r6`.
//!
//! # Sync vs async validation
//!
//! The crate supports both synchronous and asynchronous validation flows.
//!
//! ## Synchronous validation
//!
//! The synchronous path is intended for local validation and backward-compatible
//! call sites. It performs:
//!
//! - structural checks
//! - invariant evaluation
//! - local terminology and binding validation
//!
//! When synchronous terminology is needed, the sync path uses
//! [`TerminologyServiceSync`].
//!
//! ## Asynchronous validation
//!
//! The asynchronous path is the preferred production path when remote
//! terminology may be required. It performs the same validation work as the
//! synchronous path, but can call remote terminology services without blocking
//! on ad hoc Tokio runtimes.
//!
//! When asynchronous terminology is needed, the async path uses
//! [`TerminologyService`].
//!
//! In general:
//!
//! - use sync validation for local-only validation, tests, and compatibility
//!   paths
//! - use async validation for server-side or production flows that may require
//!   remote terminology lookups
//!
//! # Terminology behavior
//!
//! Binding validation is local-first.
//!
//! Generated local terminology helpers are consulted first. If a binding can be
//! decided locally, the validator emits a normal binding result immediately.
//! Remote terminology is only consulted when the local helper reports
//! `RemoteValidationRequired`.
//!
//! This keeps validation fast and deterministic when terminology rules are fully
//! known locally, while still allowing remote FHIR terminology servers to be
//! used for more complex ValueSet semantics.
//!
//! ## Remote terminology outcomes
//!
//! Successful remote `ValueSet/$validate-code` responses are parsed into
//! [`TerminologyMembershipOutcome`], which preserves:
//!
//! - membership result
//! - optional server message
//! - optional terminology metadata such as code, system, and version
//!
//! Non-2xx remote responses are represented as structured remote terminology
//! errors and surfaced as terminology validation issues with readable
//! diagnostics.
//!
//! # Severity model
//!
//! Validation issues generally follow these rules:
//!
//! - required binding misses produce errors
//! - extensible binding misses produce warnings
//! - invariant failures produce invariant issues
//! - malformed or unavailable terminology responses produce terminology issues
//!
//! # Versioned modules
//!
//! Version-specific modules expose the generated traits, dispatchers, and helper
//! functions for each supported FHIR release.

pub mod binding;
pub mod core;
pub mod evaluators;
pub mod terminology;
pub use core::*;
pub use evaluators::*;
pub use terminology::*;

#[cfg(feature = "R4")]
pub mod r4;

#[cfg(feature = "R4B")]
pub mod r4b;

#[cfg(feature = "R5")]
pub mod r5;

#[cfg(feature = "R6")]
pub mod r6;

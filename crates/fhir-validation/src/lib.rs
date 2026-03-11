//! FHIR validation framework.
//!
//! Core framework types live at the crate root.
//! Version-specific validation lives under `r4`, `r4b`, `r5`, `r6`.

pub mod core;
pub mod terminology;
pub mod invariants;
pub mod invariant_evaluator;
pub use invariants::*;
pub use core::*;
pub use invariant_evaluator::*;
pub use terminology::*;

#[cfg(feature = "R4")]
pub mod r4;

#[cfg(feature = "R4B")]
pub mod r4b;

#[cfg(feature = "R5")]
pub mod r5;

#[cfg(feature = "R6")]
pub mod r6;



//! Axum [`Router`](axum::Router) for [CDS Hooks](https://cds-hooks.hl7.org/) — discovery,
//! service invocation, and feedback.
//!
//! This crate wraps [`CdsHooksService`](helios_cds_hooks::CdsHooksService) implementations in
//! an object-safe [`CdsServiceDispatch`] so many services can be registered by id and served
//! from one HTTP stack.
//!
//! For **FHIR Clinical Reasoning** (`PlanDefinition` / `$apply`, `GuidanceResponse`, etc.),
//! integrate that in your domain layer and map results to [`CdsResponse`](helios_cds_hooks::CdsResponse);
//! this crate only handles the CDS Hooks HTTP surface.

mod dispatch;
mod router;

pub use dispatch::{CdsServiceDispatch, CdsServiceRegistry, ServiceWrapper};
pub use router::cds_hooks_router;

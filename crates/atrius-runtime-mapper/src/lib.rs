//! Atrius → QI-Core runtime projection for CQL evaluation.
//!
//! Clinical data is stored on **clinical HFS** with Atrius profiles. The JVM sidecar
//! evaluates unchanged QI-Core ELM and expects QI-Core-shaped FHIR JSON at retrieve
//! time. This crate is the compatibility layer between those two worlds.
//!
//! **Knowledge Repository (KR)** — a separate HFS instance for eCQM libraries and
//! measures — is never passed through this mapper.

mod bundle;
mod condition;
mod error;
mod inventory;
pub mod manifest;
mod profile;

pub use bundle::{BundleProjectionStats, project_bundle, project_resource};
pub use condition::{
    ConditionBranch, condition_category_codes, evaluation_profile_url, is_atrius_condition,
    project_condition, select_condition_branch,
};
pub use error::{MapperError, MapperResult};
pub use manifest::{
    ATRIUS_PROFILE_BASE, MapperManifest, ProfileMapping, QICORE_CONDITION_ENCOUNTER_DIAGNOSIS,
    QICORE_CONDITION_PROBLEMS_HEALTH_CONCERNS,
};
pub use profile::{has_profile, profiles_in_meta, project_profile_swap, set_evaluation_profile};

use serde_json::Value;

/// Runtime mapper configured with an Atrius IG profile manifest.
#[derive(Debug, Clone)]
pub struct RuntimeMapper {
    manifest: MapperManifest,
}

impl RuntimeMapper {
    #[must_use]
    pub fn new(manifest: MapperManifest) -> Self {
        Self { manifest }
    }

    #[must_use]
    pub fn manifest(&self) -> &MapperManifest {
        &self.manifest
    }

    /// Project a FHIR `Bundle` (typically clinical prefetch or search results).
    pub fn project_bundle(&self, bundle: Value) -> MapperResult<(Value, BundleProjectionStats)> {
        project_bundle(&self.manifest, bundle)
    }

    /// Project a single FHIR resource value.
    pub fn project_resource(&self, resource: &mut Value) -> MapperResult<bool> {
        project_resource(&self.manifest, resource)
    }
}

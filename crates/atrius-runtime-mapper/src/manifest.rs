use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{MapperError, MapperResult};

/// Canonical Atrius StructureDefinition base from the Atrius IG.
pub const ATRIUS_PROFILE_BASE: &str = "https://atrius.in/fhir/r4/atrius-in/StructureDefinition/";

/// Default QI-Core evaluation targets for Condition branches (QI-Core STU6).
pub const QICORE_CONDITION_ENCOUNTER_DIAGNOSIS: &str =
    "http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-condition-encounter-diagnosis";
pub const QICORE_CONDITION_PROBLEMS_HEALTH_CONCERNS: &str =
    "http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-condition-problems-health-concerns";

/// One Atrius storage profile → QI-Core evaluation profile pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileMapping {
    pub atrius_profile: String,
    pub evaluation_profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
}

/// Mapper manifest consumed at runtime (generated from the Atrius IG build).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MapperManifest {
    #[serde(default)]
    pub profile_mappings: Vec<ProfileMapping>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition_encounter_diagnosis_evaluation_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition_problems_health_concerns_evaluation_profile: Option<String>,
}

impl MapperManifest {
    /// Load manifest JSON from disk.
    pub fn from_json_file(path: impl AsRef<Path>) -> MapperResult<Self> {
        let text = std::fs::read_to_string(path)?;
        Self::from_json_str(&text)
    }

    pub fn from_json_str(text: &str) -> MapperResult<Self> {
        let manifest: Self = serde_json::from_str(text)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> MapperResult<()> {
        for mapping in &self.profile_mappings {
            if mapping.atrius_profile.trim().is_empty() {
                return Err(MapperError::InvalidManifest(
                    "profile_mappings entry missing atrius_profile".into(),
                ));
            }
            if mapping.evaluation_profile.trim().is_empty() {
                return Err(MapperError::InvalidManifest(
                    "profile_mappings entry missing evaluation_profile".into(),
                ));
            }
        }
        Ok(())
    }

    /// Built-in manifest for early integration (Condition + common actor anchors).
    #[must_use]
    pub fn default_v0_1() -> Self {
        Self {
            profile_mappings: vec![
                profile_pair("atrius-in-patient", "qicore-patient", "Patient"),
                profile_pair("atrius-in-encounter", "qicore-encounter", "Encounter"),
                profile_pair(
                    "atrius-in-condition-encounter-diagnosis",
                    "qicore-condition-encounter-diagnosis",
                    "Condition",
                ),
                profile_pair(
                    "atrius-in-condition-problems-health-concerns",
                    "qicore-condition-problems-health-concerns",
                    "Condition",
                ),
            ],
            condition_encounter_diagnosis_evaluation_profile: Some(
                QICORE_CONDITION_ENCOUNTER_DIAGNOSIS.into(),
            ),
            condition_problems_health_concerns_evaluation_profile: Some(
                QICORE_CONDITION_PROBLEMS_HEALTH_CONCERNS.into(),
            ),
        }
    }

    /// Full v0.1 inventory from `runtime-mapper.md` (~60 profile pairs).
    #[must_use]
    pub fn full_inventory() -> Self {
        crate::inventory::full_inventory_manifest()
    }

    #[must_use]
    pub fn evaluation_profile_index(&self) -> HashMap<String, String> {
        self.profile_mappings
            .iter()
            .map(|m| (m.atrius_profile.clone(), m.evaluation_profile.clone()))
            .collect()
    }

    /// Default QI-Core profile when storage has no `meta.profile`, but the FHIR type
    /// maps to exactly one entry in the manifest (e.g. Patient → QICorePatient).
    #[must_use]
    pub fn default_evaluation_profile_for_resource_type(
        &self,
        resource_type: &str,
    ) -> Option<&str> {
        let matches: Vec<_> = self
            .profile_mappings
            .iter()
            .filter(|m| m.resource_type.as_deref() == Some(resource_type))
            .collect();
        if matches.len() == 1 {
            Some(matches[0].evaluation_profile.as_str())
        } else {
            None
        }
    }

    #[must_use]
    pub fn condition_encounter_diagnosis_profile(&self) -> &str {
        self.condition_encounter_diagnosis_evaluation_profile
            .as_deref()
            .unwrap_or(QICORE_CONDITION_ENCOUNTER_DIAGNOSIS)
    }

    #[must_use]
    pub fn condition_problems_health_concerns_profile(&self) -> &str {
        self.condition_problems_health_concerns_evaluation_profile
            .as_deref()
            .unwrap_or(QICORE_CONDITION_PROBLEMS_HEALTH_CONCERNS)
    }
}

fn profile_pair(name: &str, eval_name: &str, resource_type: &str) -> ProfileMapping {
    ProfileMapping {
        atrius_profile: format!("{ATRIUS_PROFILE_BASE}{name}"),
        evaluation_profile: format!(
            "http://hl7.org/fhir/us/qicore/StructureDefinition/{eval_name}"
        ),
        resource_type: Some(resource_type.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_manifest_validates() {
        let m = MapperManifest::default_v0_1();
        m.validate().unwrap();
        assert!(
            m.evaluation_profile_index()
                .contains_key(&format!("{ATRIUS_PROFILE_BASE}atrius-in-patient"))
        );
    }

    #[test]
    fn roundtrip_json() {
        let m = MapperManifest::default_v0_1();
        let json = serde_json::to_string_pretty(&m).unwrap();
        let parsed = MapperManifest::from_json_str(&json).unwrap();
        assert_eq!(m, parsed);
    }
}

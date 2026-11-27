//! Every Atrius IG URL cds-server writes, defined once.
//!
//! [`IG_CANONICALS`] must each resolve to a resource in the Atrius IG package
//! (`atrius.fhir.india.r4`); the test below checks them against the package copy in
//! `data/fhir-packages`, which atrius-his `scripts/sync-ig.sh` keeps current.

pub const ATRIUS_IN_GROUP_PROFILE: &str =
    "https://atrius.in/fhir/StructureDefinition/atrius-in-group";
pub const ATRIUS_IN_FLAG_PROFILE: &str =
    "https://atrius.in/fhir/StructureDefinition/atrius-in-flag";
pub const CDS_FEEDBACK_EXTENSION: &str =
    "https://atrius.in/fhir/StructureDefinition/atrius-in-cds-feedback";
pub const CDS_SUBSCRIPTION_SOURCE_EXTENSION: &str =
    "https://atrius.in/fhir/StructureDefinition/atrius-in-cds-subscription-source";
pub const CDS_FLAG_SYSTEM: &str = "https://atrius.in/fhir/CodeSystem/atrius-in-cds-flag";
pub const CDS_OVERRIDE_REASON_SYSTEM: &str =
    "https://atrius.in/fhir/CodeSystem/atrius-in-cds-override-reason";
pub const CDS_HOOKS_CARD_IDENTIFIER_SYSTEM: &str =
    "https://atrius.in/fhir/identifier/cds-hooks-card";
pub const CQL_COHORT_IDENTIFIER_SYSTEM: &str = "https://atrius.in/fhir/identifier/cql-cohort";

/// CDS Hooks response `extension` keys (not FHIR artifacts; documented on the IG
/// `cds-clinical-reasoning` page). clinical-ui reads both.
pub const CARE_PLAN_EXTENSION_KEY: &str = "https://atrius.in/fhir/cds-hooks/extension/care-plan";
pub const APPLY_TIMING_EXTENSION_KEY: &str =
    "https://atrius.in/fhir/cds-hooks/extension/cds-apply-timing";

pub const IG_CANONICALS: &[&str] = &[
    ATRIUS_IN_GROUP_PROFILE,
    ATRIUS_IN_FLAG_PROFILE,
    CDS_FEEDBACK_EXTENSION,
    CDS_SUBSCRIPTION_SOURCE_EXTENSION,
    CDS_FLAG_SYSTEM,
    CDS_OVERRIDE_REASON_SYSTEM,
    CDS_HOOKS_CARD_IDENTIFIER_SYSTEM,
    CQL_COHORT_IDENTIFIER_SYSTEM,
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::PathBuf;

    const BASE: &str = "https://atrius.in/fhir";

    #[test]
    fn every_constant_is_under_the_ig_base() {
        for url in IG_CANONICALS
            .iter()
            .chain([&CARE_PLAN_EXTENSION_KEY, &APPLY_TIMING_EXTENSION_KEY])
        {
            assert!(url.starts_with(BASE), "{url} is not under {BASE}");
        }
    }

    #[test]
    fn ig_canonicals_exist_in_the_ig_package() {
        let dir = std::env::var_os("ATRIUS_IG_PACKAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../data/fhir-packages/atrius.fhir.india.r4/0.1.0")
            });
        let Ok(entries) = std::fs::read_dir(&dir) else {
            eprintln!(
                "skipped: no Atrius IG package at {} (run atrius-his scripts/sync-ig.sh)",
                dir.display()
            );
            return;
        };
        let mut known = HashSet::new();
        for entry in entries.flatten() {
            let Ok(text) = std::fs::read_to_string(entry.path()) else {
                continue;
            };
            let Ok(res) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            if let Some(url) = res["url"].as_str() {
                known.insert(url.to_string());
            }
            for id in res["uniqueId"].as_array().into_iter().flatten() {
                if id["type"] == "uri"
                    && let Some(v) = id["value"].as_str()
                {
                    known.insert(v.to_string());
                }
            }
        }
        let missing: Vec<_> = IG_CANONICALS
            .iter()
            .filter(|u| !known.contains(**u))
            .collect();
        assert!(
            missing.is_empty(),
            "not in the IG package at {}: {missing:?}",
            dir.display()
        );
    }
}

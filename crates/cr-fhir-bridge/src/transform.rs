//! Apply runtime Atrius→QI-Core projection to upstream FHIR JSON bodies.
//!
//! Called from [`crate::proxy::proxy_fhir`] only for **clinical** upstream responses (not KR
//! `/Library` pass-through). Successful `application/fhir+json` / `application/json` bodies are
//! parsed and passed to [`atrius_runtime_mapper::RuntimeMapper`]:
//!
//! - **`Bundle`** — each entry resource may be projected (search sets, history, transaction results).
//! - **Single resource** — projected when a mapper rule matches `meta.profile` / resource type.
//!
//! Non-success status codes, non-JSON content types, and projection errors **pass through** the
//! upstream body unchanged (with a warning log) so clinical operations never fail solely because
//! mapping rules are incomplete.

use atrius_runtime_mapper::{MapperError, RuntimeMapper};
use serde_json::Value;

/// Counts from projecting a response body.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformStats {
    pub bundles: u32,
    pub resources: u32,
    pub projected: u32,
    pub skipped: u32,
}

/// Project a parsed FHIR JSON value when it is a `Bundle` or single resource.
pub fn transform_fhir_value(
    mapper: &RuntimeMapper,
    value: &mut Value,
) -> Result<TransformStats, MapperError> {
    let Some(resource_type) = value.get("resourceType").and_then(|v| v.as_str()) else {
        return Ok(TransformStats::default());
    };

    match resource_type {
        "Bundle" => {
            let (projected, bundle_stats) = mapper.project_bundle(value.take())?;
            *value = projected;
            Ok(TransformStats {
                bundles: 1,
                projected: bundle_stats.projected,
                skipped: bundle_stats.skipped,
                ..Default::default()
            })
        }
        _ => {
            if mapper.project_resource(value)? {
                Ok(TransformStats {
                    resources: 1,
                    projected: 1,
                    ..Default::default()
                })
            } else {
                Ok(TransformStats::default())
            }
        }
    }
}

/// Whether a response content-type looks like FHIR JSON worth parsing.
pub fn is_fhir_json_content_type(content_type: &str) -> bool {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    ct.eq_ignore_ascii_case("application/fhir+json")
        || ct.eq_ignore_ascii_case("application/json")
        || ct.eq_ignore_ascii_case("application/fhir+json;charset=utf-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use atrius_runtime_mapper::{
        MapperManifest, QICORE_CONDITION_ENCOUNTER_DIAGNOSIS, RuntimeMapper,
    };
    use serde_json::json;

    #[test]
    fn transforms_search_bundle() {
        let mapper = RuntimeMapper::new(MapperManifest::default_v0_1());
        let mut body = json!({
            "resourceType": "Bundle",
            "type": "searchset",
            "entry": [{
                "resource": {
                    "resourceType": "Condition",
                    "meta": { "profile": ["https://atrius.in/fhir/r4/atrius-core/StructureDefinition/atrius-condition-encounter-diagnosis"] },
                    "category": [{ "coding": [{ "system": "http://terminology.hl7.org/CodeSystem/condition-category", "code": "encounter-diagnosis" }] }],
                    "code": { "coding": [{ "system": "http://hl7.org/fhir/sid/icd-10", "code": "I10" }] }
                }
            }]
        });

        let stats = transform_fhir_value(&mapper, &mut body).unwrap();
        assert_eq!(stats.projected, 1);
        assert_eq!(
            body["entry"][0]["resource"]["meta"]["profile"][0],
            QICORE_CONDITION_ENCOUNTER_DIAGNOSIS
        );
    }

    #[test]
    fn leaves_operation_outcome_unchanged() {
        let mapper = RuntimeMapper::new(MapperManifest::default_v0_1());
        let mut body = json!({
            "resourceType": "OperationOutcome",
            "issue": [{ "severity": "error", "code": "not-found" }]
        });
        let stats = transform_fhir_value(&mapper, &mut body).unwrap();
        assert_eq!(stats, TransformStats::default());
    }
}

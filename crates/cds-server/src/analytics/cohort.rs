//! CQL expression → patient membership → FHIR [`Group`](https://hl7.org/fhir/R4/group.html).

use chrono::Utc;
use serde_json::{Value, json};

use crate::clinical_reasoning::NormalizedSidecarResult;

pub const ATRIUS_IN_GROUP_PROFILE: &str =
    "https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-group";
pub const COHORT_IDENTIFIER_SYSTEM: &str = "https://atrius.in/fhir/r4/identifier/cql-cohort";
pub const MAX_COHORT_PATIENTS: usize = 200;
pub const DEFAULT_COHORT_CONCURRENCY: usize = 8;

/// Whether a sidecar expression result counts the patient in the cohort.
pub fn result_is_member(result: &Value) -> bool {
    match crate::clinical_reasoning::normalize_sidecar_result(result) {
        NormalizedSidecarResult::Bool(b) => b,
        NormalizedSidecarResult::Null => false,
        NormalizedSidecarResult::Number(n) => n.as_f64().is_some_and(|x| x != 0.0),
        NormalizedSidecarResult::String(s) => {
            let t = s.trim().to_ascii_lowercase();
            t == "true" || t == "yes" || t == "1"
        }
        NormalizedSidecarResult::Object(map) => map
            .get("valueBoolean")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        NormalizedSidecarResult::Array(items) => items.iter().any(result_is_member),
        NormalizedSidecarResult::FhirResource(v) => {
            v.get("resourceType").and_then(Value::as_str) == Some("Patient")
        }
    }
}

pub fn normalize_patient_id(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let id = trimmed
        .strip_prefix("Patient/")
        .or_else(|| trimmed.strip_prefix("patient/"))
        .unwrap_or(trimmed)
        .trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

pub fn sanitize_fhir_id_fragment(s: &str) -> String {
    let filtered: String = s
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '.')
        .collect();
    let trimmed = filtered.trim_matches('-').trim_matches('.');
    if trimmed.is_empty() {
        "cohort".into()
    } else {
        trimmed.chars().take(40).collect()
    }
}

pub fn generated_group_id(library_id: &str) -> String {
    format!(
        "cql-{}-{}",
        sanitize_fhir_id_fragment(library_id),
        Utc::now().timestamp_millis()
    )
}

pub fn group_resource(
    id: &str,
    name: &str,
    library_id: &str,
    library_version: Option<&str>,
    expression: &str,
    member_ids: &[String],
) -> Value {
    let version = library_version.unwrap_or("");
    let criterion = if version.is_empty() {
        format!("{library_id}#{expression}")
    } else {
        format!("{library_id}|{version}#{expression}")
    };
    let members: Vec<Value> = member_ids
        .iter()
        .map(|pid| json!({ "entity": { "reference": format!("Patient/{pid}") } }))
        .collect();
    json!({
        "resourceType": "Group",
        "id": id,
        "meta": {
            "profile": [ATRIUS_IN_GROUP_PROFILE]
        },
        "identifier": [{
            "system": COHORT_IDENTIFIER_SYSTEM,
            "value": criterion
        }],
        "type": "person",
        "actual": true,
        "name": name,
        "quantity": members.len(),
        "characteristic": [{
            "code": { "text": "CQL expression" },
            "valueString": criterion,
            "exclude": false
        }],
        "member": members
    })
}

pub fn export_hints(hfs_base: &str, group_id: &str) -> Value {
    let base = hfs_base.trim_end_matches('/');
    json!({
        "group": format!("Group/{group_id}"),
        "sqlRun": format!("{base}/$sql-run"),
        "sqlExport": format!("{base}/$sql-export"),
        "bulkExport": format!("{base}/Group/{group_id}/$export"),
        "body": {
            "group": format!("Group/{group_id}"),
            "view": "https://atrius.in/fhir/r4/atrius-in/ViewDefinition/atrius-in-patient"
        },
        "note": "Pass group as Group/{id} on HFS $sql-run / $sql-export (or GET Group/{id}/$export). Review the Group before kicking off an export."
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn boolean_true_is_member() {
        assert!(result_is_member(&json!(true)));
        assert!(result_is_member(&json!({"valueBoolean": true})));
        assert!(!result_is_member(&json!(false)));
        assert!(!result_is_member(&json!(null)));
    }

    #[test]
    fn patient_id_strips_reference() {
        assert_eq!(normalize_patient_id("Patient/p1").as_deref(), Some("p1"));
        assert_eq!(normalize_patient_id("  p1  ").as_deref(), Some("p1"));
        assert!(normalize_patient_id("  ").is_none());
    }

    #[test]
    fn group_json_has_profile_and_members() {
        let g = group_resource(
            "cql-demo-1",
            "CMS165 IP",
            "AtriusCMS165ControllingHighBP",
            Some("0.1.0"),
            "Initial Population",
            &["p1".into(), "p2".into()],
        );
        assert_eq!(g["resourceType"], "Group");
        assert_eq!(g["quantity"], 2);
        assert_eq!(g["member"][0]["entity"]["reference"], "Patient/p1");
        assert_eq!(g["meta"]["profile"][0], ATRIUS_IN_GROUP_PROFILE);
        assert_eq!(
            g["characteristic"][0]["valueString"],
            "AtriusCMS165ControllingHighBP|0.1.0#Initial Population"
        );
    }
}

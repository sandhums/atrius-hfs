//! Normalize JVM sidecar [`super::dto::EvaluateExpressionResponse::result`] JSON for Rust callers.
//!
//! CQ Framework may return FHIR resources as a **JSON string** whose contents are a FHIR JSON object
//! (“double-encoded”). [`normalize_sidecar_result`] detects that pattern via a top-level `resourceType` field.
//!
//! Nested strings inside arrays or objects are **not** scanned when classifying the top-level `result`
//! — only the top-level `result` value drives [`NormalizedSidecarResult`].
//! Use [`unwrap_nested_fhir_json_strings`] when displaying or forwarding aggregate results (e.g. CDS cards).

use serde_json::{Map, Number, Value};

/// Structured view of sidecar `result` after decoding FHIR double-encoding.
#[derive(Debug, Clone, PartialEq)]
pub enum NormalizedSidecarResult {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Value>),
    Object(Map<String, Value>),
    /// FHIR resource parsed from an engine JSON **string** payload (see module docs).
    FhirResource(Value),
}

/// Classify `result` from [`super::dto::EvaluateExpressionResponse`].
#[must_use]
pub fn normalize_sidecar_result(result: &Value) -> NormalizedSidecarResult {
    match result {
        Value::Null => NormalizedSidecarResult::Null,
        Value::Bool(b) => NormalizedSidecarResult::Bool(*b),
        Value::Number(n) => NormalizedSidecarResult::Number(n.clone()),
        Value::String(s) => normalize_string_result(s),
        Value::Array(a) => NormalizedSidecarResult::Array(a.clone()),
        Value::Object(o) => NormalizedSidecarResult::Object(o.clone()),
    }
}

fn normalize_string_result(s: &str) -> NormalizedSidecarResult {
    let Ok(parsed) = serde_json::from_str::<Value>(s) else {
        return NormalizedSidecarResult::String(s.to_string());
    };
    if let Value::Object(ref map) = parsed
        && map
            .get("resourceType")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
            .is_some()
    {
        return NormalizedSidecarResult::FhirResource(parsed);
    }
    NormalizedSidecarResult::String(s.to_string())
}

/// Walk a [`Value`] tree and replace string leaves that contain serialized FHIR JSON (object with non-empty `resourceType`) with the parsed object.
///
/// CQFramework often returns CQL lists of resources as an array of JSON **strings**; without this step,
/// `serde_json::to_string_pretty` shows each row as an escaped string instead of nested JSON objects.
#[must_use]
pub fn unwrap_nested_fhir_json_strings(v: Value) -> Value {
    match v {
        Value::String(s) => try_parse_fhir_json_object(&s).unwrap_or(Value::String(s)),
        Value::Array(a) => {
            Value::Array(a.into_iter().map(unwrap_nested_fhir_json_strings).collect())
        }
        Value::Object(m) => Value::Object(
            m.into_iter()
                .map(|(k, v)| (k, unwrap_nested_fhir_json_strings(v)))
                .collect(),
        ),
        other => other,
    }
}

fn try_parse_fhir_json_object(s: &str) -> Option<Value> {
    let parsed: Value = serde_json::from_str(s).ok()?;
    parsed
        .get("resourceType")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())?;
    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn double_encoded_fhir_patient() {
        let inner = json!({"resourceType": "Patient", "id": "p1"});
        let result = Value::String(inner.to_string());
        match normalize_sidecar_result(&result) {
            NormalizedSidecarResult::FhirResource(v) => assert_eq!(v, inner),
            other => panic!("expected FhirResource, got {other:?}"),
        }
    }

    #[test]
    fn plain_string_unparsed_stays_string() {
        let result = Value::String("hello".into());
        match normalize_sidecar_result(&result) {
            NormalizedSidecarResult::String(s) => assert_eq!(s, "hello"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn json_object_without_resource_type_is_not_fhir_variant() {
        let result = json!({"foo": 1});
        match normalize_sidecar_result(&result) {
            NormalizedSidecarResult::Object(m) => assert_eq!(m["foo"], json!(1)),
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn array_of_stringified_fhir_resources_unwraps_for_display() {
        let c = json!({"resourceType": "Condition", "id": "c1"});
        let arr = Value::Array(vec![
            Value::String(c.to_string()),
            Value::String(json!({"resourceType": "Patient", "id": "p1"}).to_string()),
        ]);
        let out = unwrap_nested_fhir_json_strings(arr);
        let Value::Array(items) = out else {
            panic!("expected array");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], c);
        assert_eq!(items[1]["resourceType"], json!("Patient"));
    }
}

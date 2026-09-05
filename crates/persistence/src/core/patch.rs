//! Shared patch application for instance PATCH, conditional PATCH, and Bundle
//! PATCH entries.
//!
//! Bundle entries have no Content-Type. Format is inferred from the entry
//! resource: a `Parameters` body is FHIRPath Patch, a JSON array is RFC 6902
//! JSON Patch, and any other JSON object is RFC 7386 merge-patch.

use serde_json::Value;

use crate::core::storage::PatchFormat;
use crate::error::{StorageError, StorageResult, ValidationError};

/// Infers a [`PatchFormat`] from a Bundle entry's `resource`.
pub fn patch_format_from_bundle_resource(resource: &Value) -> StorageResult<PatchFormat> {
    if resource.get("resourceType").and_then(Value::as_str) == Some("Parameters") {
        return Ok(PatchFormat::FhirPathPatch(resource.clone()));
    }
    if resource.is_array() {
        return Ok(PatchFormat::JsonPatch(resource.clone()));
    }
    if resource.is_object() {
        return Ok(PatchFormat::MergePatch(resource.clone()));
    }
    Err(StorageError::Validation(ValidationError::InvalidResource {
        message: "Bundle PATCH resource must be a Parameters resource, a JSON Patch array, or a merge-patch object".to_string(),
        details: vec![],
    }))
}

/// Applies `patch` to `resource` and returns the patched document.
pub fn apply_resource_patch(resource: &Value, patch: &PatchFormat) -> StorageResult<Value> {
    match patch {
        PatchFormat::JsonPatch(operations) => apply_json_patch(resource, operations),
        PatchFormat::MergePatch(merge_doc) => Ok(apply_merge_patch(resource, merge_doc)),
        PatchFormat::FhirPathPatch(params) => apply_fhirpath_patch(resource, params),
    }
}

/// Applies a Bundle PATCH entry's resource to the current representation,
/// refusing a `resourceType` change.
pub fn patched_from_bundle_entry(current: &Value, patch_resource: &Value) -> StorageResult<Value> {
    let format = patch_format_from_bundle_resource(patch_resource)?;
    let patched = apply_resource_patch(current, &format)?;
    if let (Some(was), Some(now)) = (
        current.get("resourceType").and_then(Value::as_str),
        patched.get("resourceType").and_then(Value::as_str),
    ) && was != now
    {
        return Err(StorageError::Validation(ValidationError::InvalidResource {
            message: "Cannot change resourceType via patch".to_string(),
            details: vec![],
        }));
    }
    Ok(patched)
}

fn apply_json_patch(resource: &Value, patch_doc: &Value) -> StorageResult<Value> {
    let patch: json_patch::Patch = serde_json::from_value(patch_doc.clone()).map_err(|e| {
        StorageError::Validation(ValidationError::InvalidResource {
            message: format!("Invalid JSON Patch document: {e}"),
            details: vec![],
        })
    })?;
    let mut patched = resource.clone();
    json_patch::patch(&mut patched, &patch).map_err(|e| {
        StorageError::Validation(ValidationError::InvalidResource {
            message: format!("Failed to apply JSON Patch: {e}"),
            details: vec![],
        })
    })?;
    Ok(patched)
}

fn apply_merge_patch(resource: &Value, merge_doc: &Value) -> Value {
    let mut patched = resource.clone();
    json_patch::merge(&mut patched, merge_doc);
    patched
}

/// FHIRPath Patch via a Parameters resource. Handles common `replace` / `add`
/// / `delete` operations on simple `Resource.field` paths; insert/move and
/// full FHIRPath evaluation are not implemented.
fn apply_fhirpath_patch(resource: &Value, patch_params: &Value) -> StorageResult<Value> {
    let parameter = patch_params.get("parameter").and_then(Value::as_array);
    let Some(parameter) = parameter else {
        return Err(StorageError::Validation(ValidationError::InvalidResource {
            message: "FHIRPath Patch must have a 'parameter' array".to_string(),
            details: vec![],
        }));
    };

    let mut patched = resource.clone();
    for operation in parameter {
        let Some(parts) = operation.get("part").and_then(Value::as_array) else {
            continue;
        };

        let mut op_type = None;
        let mut op_path = None;
        let mut op_name = None;
        let mut op_value = None;

        for part in parts {
            match part.get("name").and_then(Value::as_str) {
                Some("type") => {
                    op_type = part
                        .get("valueCode")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                Some("path") => {
                    op_path = part
                        .get("valueString")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                Some("name") => {
                    op_name = part
                        .get("valueString")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                Some("value") => {
                    op_value = extract_part_value(part);
                }
                _ => {}
            }
        }

        match op_type.as_deref() {
            Some("replace") => {
                if let (Some(path), Some(value)) = (&op_path, &op_value) {
                    fhirpath_replace(&mut patched, path, value);
                }
            }
            Some("add") => {
                if let (Some(path), Some(name), Some(value)) = (&op_path, &op_name, &op_value) {
                    fhirpath_add(&mut patched, path, name, value);
                }
            }
            Some("delete") => {
                if let Some(path) = &op_path {
                    fhirpath_delete(&mut patched, path);
                }
            }
            _ => {}
        }
    }
    Ok(patched)
}

/// Extracts the `value[x]` payload from a FHIRPath Patch `Parameters.part`
/// whose `name` is `"value"`.
fn extract_part_value(part: &Value) -> Option<Value> {
    part.as_object()?.iter().find_map(|(k, v)| {
        let suffix = k.strip_prefix("value")?;
        suffix
            .chars()
            .next()?
            .is_ascii_uppercase()
            .then(|| v.clone())
    })
}

fn fhirpath_replace(resource: &mut Value, path: &str, value: &Value) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 2
        && let Some(obj) = resource.as_object_mut()
    {
        obj.insert(parts[1].to_string(), value.clone());
    }
}

fn fhirpath_add(resource: &mut Value, path: &str, name: &str, value: &Value) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1
        && parts[0]
            == resource
                .get("resourceType")
                .and_then(Value::as_str)
                .unwrap_or("")
        && let Some(obj) = resource.as_object_mut()
    {
        obj.insert(name.to_string(), value.clone());
    }
}

fn fhirpath_delete(resource: &mut Value, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 2
        && let Some(obj) = resource.as_object_mut()
    {
        obj.remove(parts[1]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_parameters_body_is_fhirpath_patch() {
        let doc = json!({ "resourceType": "Parameters", "parameter": [] });
        assert!(matches!(
            patch_format_from_bundle_resource(&doc).unwrap(),
            PatchFormat::FhirPathPatch(_)
        ));
    }

    #[test]
    fn a_json_array_is_json_patch() {
        let doc = json!([{ "op": "replace", "path": "/active", "value": false }]);
        assert!(matches!(
            patch_format_from_bundle_resource(&doc).unwrap(),
            PatchFormat::JsonPatch(_)
        ));
    }

    #[test]
    fn an_object_is_merge_patch() {
        let doc = json!({ "resourceType": "Patient", "name": [{ "family": "Patched" }] });
        assert!(matches!(
            patch_format_from_bundle_resource(&doc).unwrap(),
            PatchFormat::MergePatch(_)
        ));
    }

    #[test]
    fn merge_patch_replaces_family() {
        let current = json!({
            "resourceType": "Patient",
            "id": "p1",
            "name": [{ "family": "Nguyen" }]
        });
        let patch = json!({
            "resourceType": "Patient",
            "name": [{ "family": "Patched" }]
        });
        let patched = patched_from_bundle_entry(&current, &patch).unwrap();
        assert_eq!(patched["name"][0]["family"], "Patched");
        assert_eq!(patched["id"], "p1");
    }

    #[test]
    fn json_patch_replaces_a_pointer() {
        let current = json!({ "resourceType": "Patient", "active": true });
        let patch = PatchFormat::JsonPatch(json!([
            { "op": "replace", "path": "/active", "value": false }
        ]));
        let patched = apply_resource_patch(&current, &patch).unwrap();
        assert_eq!(patched["active"], false);
    }

    #[test]
    fn resource_type_change_is_refused() {
        let current = json!({ "resourceType": "Patient", "id": "p1" });
        let patch = json!({ "resourceType": "Observation" });
        let err = patched_from_bundle_entry(&current, &patch).unwrap_err();
        assert!(err.to_string().contains("resourceType"));
    }
}

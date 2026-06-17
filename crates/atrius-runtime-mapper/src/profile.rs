use serde_json::{Value, json};

use crate::error::MapperResult;
use crate::manifest::MapperManifest;

/// Swap `meta.profile` when a declared Atrius profile matches the manifest index,
/// or when the resource type has a single default evaluation profile.
pub fn project_profile_swap(manifest: &MapperManifest, resource: &mut Value) -> MapperResult<bool> {
    let index = manifest.evaluation_profile_index();
    let declared = profiles_in_meta(resource);
    for profile in &declared {
        if let Some(evaluation) = index.get(profile.as_str()) {
            set_evaluation_profile(resource, evaluation);
            return Ok(true);
        }
        // tolerate versioned/canonical suffix variants
        if let Some((_, evaluation)) = index
            .iter()
            .find(|(atrius, _)| profile.starts_with(atrius.as_str()))
        {
            set_evaluation_profile(resource, evaluation);
            return Ok(true);
        }
    }

    if declared.is_empty()
        && let Some(resource_type) = resource.get("resourceType").and_then(|v| v.as_str())
        && let Some(evaluation) =
            manifest.default_evaluation_profile_for_resource_type(resource_type)
    {
        set_evaluation_profile(resource, evaluation);
        return Ok(true);
    }

    Ok(false)
}

/// Return declared `meta.profile` URLs on a resource.
pub fn profiles_in_meta(resource: &Value) -> Vec<String> {
    resource
        .get("meta")
        .and_then(|m| m.get("profile"))
        .and_then(|p| p.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

pub fn has_profile(resource: &Value, profile_url: &str) -> bool {
    profiles_in_meta(resource).iter().any(|p| p == profile_url)
}

pub fn has_profile_suffix(resource: &Value, suffix: &str) -> bool {
    profiles_in_meta(resource)
        .iter()
        .any(|p| p.ends_with(suffix) || p.ends_with(&format!("/{suffix}")))
}

/// Replace `meta.profile` with a single evaluation profile URL.
pub fn set_evaluation_profile(resource: &mut Value, evaluation_profile: &str) {
    let obj = resource.as_object_mut().expect("resource must be object");
    if !obj.get("meta").is_some_and(|m| m.is_object()) {
        obj.insert("meta".into(), json!({}));
    }
    let meta = obj.get_mut("meta").expect("meta object");
    if let Some(meta_obj) = meta.as_object_mut() {
        meta_obj.insert(
            "profile".into(),
            Value::Array(vec![Value::String(evaluation_profile.into())]),
        );
    }
}

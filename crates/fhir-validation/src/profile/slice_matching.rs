//! Slice instance matching for profile rules that carry `slice_name`.
//!
//! Used by cardinality, fixed/pattern, and type constraint validators when a rule's
//! `ElementDefinition.path` is still the sliced base path (or a child under it) rather
//! than a distinct unsliced path.

use crate::profile::cardinality::relative_profile_path;
use crate::profile::helpers::get_values_at_relative_path;
use crate::profile::types::{
    ExtractedElementRule, ExtractedProfile, ExtractedValueConstraint,
};
use serde_json::Value;

/// FHIR element path for user-facing messages — includes slice discriminator when present.
pub fn profile_element_display_path(rule: &ExtractedElementRule) -> String {
    match rule.slice_name.as_deref() {
        Some(slice) => format!("{}:{}", rule.path, slice),
        None => rule.path.clone(),
    }
}

/// Repeating-element base path for rules sharing the same `slice_name`.
pub fn slice_repeating_base_path<'a>(
    profile: &'a ExtractedProfile,
    rule: &ExtractedElementRule,
) -> Option<&'a str> {
    let slice_name = rule.slice_name.as_deref()?;
    profile
        .element_rules
        .iter()
        .filter(|candidate| candidate.slice_name.as_deref() == Some(slice_name))
        .map(|candidate| candidate.path.as_str())
        .min_by_key(|path| path.len())
}

fn slice_root_rule<'a>(
    profile: &'a ExtractedProfile,
    rule: &ExtractedElementRule,
) -> Option<&'a ExtractedElementRule> {
    let slice_name = rule.slice_name.as_deref()?;
    let base_path = slice_repeating_base_path(profile, rule)?;
    profile.element_rules.iter().find(|candidate| {
        candidate.path == base_path && candidate.slice_name.as_deref() == Some(slice_name)
    })
}

fn slice_url_fixed_value<'a>(
    profile: &'a ExtractedProfile,
    slice_root: &ExtractedElementRule,
) -> Option<&'a Value> {
    let slice_name = slice_root.slice_name.as_deref()?;
    let url_path = format!("{}.url", slice_root.path);
    profile.element_rules.iter().find_map(|rule| {
        if rule.path != url_path || rule.slice_name.as_deref() != Some(slice_name) {
            return None;
        }
        match &rule.value_constraint {
            Some(ExtractedValueConstraint::Fixed(value)) => Some(value),
            _ => None,
        }
    })
}

/// Return `true` when `instance` is a repeated item belonging to `rule`'s slice.
pub fn matches_slice_instance(
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    instance: &Value,
) -> bool {
    matches_slice_instance_at_index(profile, rule, instance, 0)
}

/// Like [`matches_slice_instance`], but supplies the zero-based index of the
/// repeated item for `position` discriminators.
pub fn matches_slice_instance_at_index(
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
    instance: &Value,
    item_index: usize,
) -> bool {
    let Some(slice_root) = slice_root_rule(profile, rule) else {
        return false;
    };

    if let Some(fixed_url) = slice_url_fixed_value(profile, slice_root) {
        return instance
            .get("url")
            .is_some_and(|actual_url| actual_url == fixed_url);
    }

    crate::profile::slicing::instance_matches_named_slice(
        profile,
        slice_root,
        instance,
        item_index,
    )
}

/// Count repeated items at the slice base path that belong to `rule`'s slice.
pub fn count_slice_instances(
    root: &Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
) -> usize {
    let Some(base_path) = slice_repeating_base_path(profile, rule) else {
        return 0;
    };
    let Some(relative_base) = relative_profile_path(resource_type, base_path) else {
        return 0;
    };

    get_values_at_relative_path(root, relative_base)
        .into_iter()
        .enumerate()
        .filter(|(item_index, instance)| {
            matches_slice_instance_at_index(profile, rule, instance, *item_index)
        })
        .count()
}

/// Resolve JSON values for a sliced rule path, scoped to matching slice instances only.
pub fn get_slice_scoped_values<'a>(
    root: &'a Value,
    resource_type: &str,
    profile: &ExtractedProfile,
    rule: &ExtractedElementRule,
) -> Vec<&'a Value> {
    let Some(base_path) = slice_repeating_base_path(profile, rule) else {
        return Vec::new();
    };
    let Some(relative_base) = relative_profile_path(resource_type, base_path) else {
        return Vec::new();
    };

    let child_suffix = if rule.path == base_path {
        None
    } else {
        rule.path
            .strip_prefix(base_path)
            .and_then(|rest| rest.strip_prefix('.'))
    };

    get_values_at_relative_path(root, relative_base)
        .into_iter()
        .enumerate()
        .filter(|(item_index, instance)| {
            matches_slice_instance_at_index(profile, rule, instance, *item_index)
        })
        .flat_map(|(_, instance)| match child_suffix {
            None => vec![instance],
            Some(suffix) => get_values_at_relative_path(instance, suffix),
        })
        .collect()
}

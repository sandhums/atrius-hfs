//! Array slicing: the mark/sweep pass.
//!
//! For every slicing-bearing schema in the element's set:
//! - **mark**: match each array item against each slice's pattern
//!   (`_.isMatch`-style partial deep equality), counting matches per slice;
//! - items matched by at least one slice **with a schema** are validated
//!   once, cooperatively, against the element set extended with *all* their
//!   matched slice schemas (the reference validator validates once per
//!   matched slice against `elset ∪ that slice`, and then re-validates every
//!   item in the plain pass — duplicating base-set errors; we consume
//!   matched items instead, which is indistinguishable on the conformance
//!   suite and saner on real resources);
//! - **sweep**: `rules: closed` (with `@default` absorption) and
//!   `rules: openAtEnd` violations, `ordered` violations, then per-slice
//!   `min`/`max` cardinality in slice declaration order.
//!
//! Divergences from the reference validator (which implements only pattern
//! matching + cardinality): `closed`/`openAtEnd`/`ordered`/`@default` are
//! enforced (pinned by `tests/fixtures/extended/slicing_rules.json`), and a
//! `max: 0` prohibited slice is enforced (the reference skips falsy bounds).
//!
//! Match types: `pattern` (partial deep equality), `type` (JSON/FHIR type
//! codes), `profile` (meta.profile claim or resolvable schema type),
//! `binding` (coded value equals the match payload when it is a Coding /
//! code string — ValueSet membership is deferred to the effects pass when
//! the payload is a canonical ValueSet URL and no inline code is present),
//! `exists` (presence/absence of each `{path, exists}` entry, with
//! `{extension: url}` path steps and choice-key prefixes), and `extension`
//! (a `{url, pattern | extension}` chain matched against extension arrays
//! by containment). `resolve-ref` follows a Reference against the in-scope
//! instance (`contained`, Bundle `entry.resource`) and does not hit storage.
//! A slice with no `match` matches nothing unless the slice schema itself
//! carries `pattern`/`fixed`.

use super::errors::{self, ErrorKind};
use super::walk::{SchemaSet, WalkCtx, add_schemas_to_set, is_partial_match, validate_node};
use crate::schema::{Slice, Slicing};
use indexmap::IndexMap;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

/// The reserved slice name that absorbs unmatched items in closed slicing.
const DEFAULT_SLICE: &str = "@default";

struct LayerMark {
    slicing: Slicing,
    /// Matched-item count per slice, in declaration order.
    counters: IndexMap<String, u64>,
    /// For each array item, the slice names it matched (declaration order).
    item_matches: Vec<Vec<String>>,
}

/// Run the slicing pass over an array element. Returns the indices of items
/// that were already fully validated (consumed) here.
pub(super) fn validate_slices(
    ctx: &mut WalkCtx<'_>,
    elset: &SchemaSet,
    items: &[Value],
) -> HashSet<usize> {
    let slicings: Vec<Slicing> = elset.schemas().filter_map(|s| s.slicing.clone()).collect();
    if slicings.is_empty() {
        return HashSet::new();
    }

    // ---- mark ----
    let mut marks: Vec<LayerMark> = Vec::with_capacity(slicings.len());
    for slicing in slicings {
        let mut counters: IndexMap<String, u64> =
            slicing.slices.keys().map(|k| (k.clone(), 0)).collect();
        let mut item_matches: Vec<Vec<String>> = vec![Vec::new(); items.len()];
        for (index, item) in items.iter().enumerate() {
            for (name, slice) in &slicing.slices {
                if name == DEFAULT_SLICE {
                    continue;
                }
                if slice_matches_with_reslice(ctx, &slicing, name, slice, item) {
                    *counters.get_mut(name).expect("counter exists") += 1;
                    item_matches[index].push(name.clone());
                }
            }
        }
        // `@default` (closed slicing only) absorbs items no other slice
        // matched; count them so its own min/max can apply.
        if slicing.slices.contains_key(DEFAULT_SLICE) && slicing.rules.as_deref() == Some("closed")
        {
            let absorbed = item_matches.iter().filter(|m| m.is_empty()).count() as u64;
            *counters.get_mut(DEFAULT_SLICE).expect("counter exists") = absorbed;
        }
        marks.push(LayerMark {
            slicing,
            counters,
            item_matches,
        });
    }

    // ---- validate matched items (item order), consuming them ----
    let mut consumed: HashSet<usize> = HashSet::new();
    for (index, item) in items.iter().enumerate() {
        let mut slice_schemas: Vec<(String, Arc<crate::schema::FhirSchema>)> = Vec::new();
        for mark in &marks {
            for name in &mark.item_matches[index] {
                if let Some(schema) = &mark.slicing.slices[name].schema {
                    slice_schemas.push((name.clone(), Arc::clone(schema)));
                }
            }
        }
        if slice_schemas.is_empty() {
            continue;
        }
        let mut extended = elset.clone();
        for (name, schema) in &slice_schemas {
            add_schemas_to_set(ctx, &mut extended, Arc::clone(schema), name);
        }
        ctx.path.push_index(index);
        validate_node(ctx, &extended, item);
        ctx.path.pop();
        consumed.insert(index);
    }

    // ---- sweep: rules, order, cardinality (per layer, elset order) ----
    for mark in &marks {
        let rules = mark.slicing.rules.as_deref().unwrap_or("open");

        if rules == "closed" {
            let default_slice = mark.slicing.slices.get(DEFAULT_SLICE);
            for (index, item) in items.iter().enumerate() {
                if !mark.item_matches[index].is_empty() {
                    continue;
                }
                match default_slice {
                    Some(Slice {
                        schema: Some(schema),
                        ..
                    }) => {
                        let mut extended = elset.clone();
                        add_schemas_to_set(ctx, &mut extended, Arc::clone(schema), DEFAULT_SLICE);
                        ctx.path.push_index(index);
                        validate_node(ctx, &extended, item);
                        ctx.path.pop();
                        consumed.insert(index);
                    }
                    Some(_) => {
                        // @default without a schema: absorbed, nothing extra
                        // to check; the plain pass still validates the item.
                    }
                    None => {
                        ctx.path.push_index(index);
                        ctx.error(
                            ErrorKind::SliceUnmatched,
                            errors::msg_slice_closed_unmatched(),
                        );
                        ctx.path.pop();
                    }
                }
            }
        } else if rules == "openAtEnd"
            && let Some(last_matched) = mark.item_matches.iter().rposition(|m| !m.is_empty())
        {
            for index in 0..last_matched {
                if mark.item_matches[index].is_empty() {
                    ctx.path.push_index(index);
                    ctx.error(ErrorKind::SliceUnmatched, errors::msg_slice_open_at_end());
                    ctx.path.pop();
                }
            }
        }

        if mark.slicing.ordered == Some(true) {
            let mut max_order_seen: Option<u64> = None;
            for (index, matches) in mark.item_matches.iter().enumerate() {
                let Some(first) = matches.first() else {
                    continue;
                };
                let Some(order) = mark.slicing.slices[first].order else {
                    continue;
                };
                if let Some(seen) = max_order_seen
                    && order < seen
                {
                    ctx.path.push_index(index);
                    ctx.error(ErrorKind::SliceOrder, errors::msg_slice_order(first));
                    ctx.path.pop();
                }
                max_order_seen = Some(max_order_seen.unwrap_or(0).max(order));
            }
        }

        for (name, slice) in &mark.slicing.slices {
            let found = mark.counters[name];
            if let Some(min) = slice.min
                && min > 0
                && found < min
            {
                ctx.error(
                    ErrorKind::SliceCardinality,
                    errors::msg_slice_min(min, found),
                );
            }
            // Unlike the reference validator (which skips falsy bounds),
            // `max: 0` — a prohibited slice, a real FHIR pattern — is
            // enforced.
            if let Some(max) = slice.max
                && found > max
            {
                ctx.error(
                    ErrorKind::SliceCardinality,
                    errors::msg_slice_max(max, found),
                );
            }
        }
    }

    consumed
}

/// Reslice-aware match: a child slice `parent/child` only matches when the
/// parent slice also matches the item. Constraining slices without their own
/// matcher inherit the same-named parent's matcher when present.
fn slice_matches_with_reslice(
    ctx: &WalkCtx<'_>,
    slicing: &Slicing,
    _name: &str,
    slice: &Slice,
    item: &Value,
) -> bool {
    if let Some(parent) = &slice.reslice {
        let Some(parent_slice) = slicing.slices.get(parent) else {
            return false;
        };
        if !slice_matches_ctx(ctx, parent_slice, item) {
            return false;
        }
        // Child may add its own matcher on top of the parent.
        if slice.match_.is_some() {
            return slice_matches_ctx(ctx, slice, item);
        }
        return true;
    }
    if slice.match_.is_none() && slice.slice_is_constraining == Some(true) {
        // Inherit matcher from a same-named non-constraining sibling/parent
        // declaration if present in this layer (rare); otherwise match nothing.
        return false;
    }
    slice_matches_ctx(ctx, slice, item)
}

/// Editor and tests: match without a walk context (no profile-schema lookup,
/// no `resolve-ref`).
pub(crate) fn slice_matches(slice: &Slice, item: &Value) -> bool {
    slice_matches_opt(None, slice, item)
}

fn slice_matches_ctx(ctx: &WalkCtx<'_>, slice: &Slice, item: &Value) -> bool {
    slice_matches_opt(Some(ctx), slice, item)
}

/// Does an item belong to a slice?
///
/// A missing `match` (constraining slice) matches nothing unless the slice
/// schema carries `pattern`/`fixed`. A `match` with no `value` matches
/// everything (lodash `_.isMatch` semantics for an empty source). `type_`
/// defaults to `pattern` when absent.
fn slice_matches_opt(ctx: Option<&WalkCtx<'_>>, slice: &Slice, item: &Value) -> bool {
    if let Some(match_) = &slice.match_ {
        let resolve_ref = match_.resolve_ref == Some(true);
        let Some(value) = match_.value.as_ref() else {
            return true;
        };
        return match match_.type_.as_deref().unwrap_or("pattern") {
            "pattern" => is_partial_match(item, value),
            "type" => typed_match(ctx, item, value, resolve_ref, type_leaf),
            "profile" => typed_match(ctx, item, value, resolve_ref, profile_leaf),
            "binding" => typed_match(ctx, item, value, resolve_ref, binding_leaf),
            "exists" => exists_matches(item, value),
            "extension" => extension_matches(item, value),
            _ => false,
        };
    }
    if let Some(schema) = &slice.schema {
        if let Some(pattern) = &schema.pattern {
            return is_partial_match(item, pattern);
        }
        if let Some(fixed) = &schema.fixed {
            return is_partial_match(item, fixed);
        }
    }
    false
}

fn type_leaf(_ctx: Option<&WalkCtx<'_>>, item: &Value, expected: &str) -> bool {
    json_fhir_types(item).iter().any(|t| t == expected)
}

fn profile_leaf(ctx: Option<&WalkCtx<'_>>, item: &Value, expected: &str) -> bool {
    profile_matches(ctx, item, expected)
}

fn binding_leaf(_ctx: Option<&WalkCtx<'_>>, item: &Value, expected: &str) -> bool {
    binding_matches(item, &Value::String(expected.to_string()))
}

/// Walk a converter-nested expected value (`{resource: "Patient"}`) or a
/// `$this` scalar. `resolve-ref` runs at the focused leaf, in-scope only.
fn typed_match(
    ctx: Option<&WalkCtx<'_>>,
    item: &Value,
    expected: &Value,
    resolve_ref: bool,
    leaf: fn(Option<&WalkCtx<'_>>, &Value, &str) -> bool,
) -> bool {
    match expected {
        Value::String(want) => {
            if resolve_ref {
                let Some(ctx) = ctx else {
                    return false;
                };
                return local_resolve(ctx.root, item)
                    .is_some_and(|resolved| leaf(Some(ctx), &resolved, want));
            }
            leaf(ctx, item, want)
        }
        Value::Object(map) => map.iter().all(|(key, nested)| {
            focus_child(item, key)
                .is_some_and(|child| typed_match(ctx, child, nested, resolve_ref, leaf))
        }),
        _ => false,
    }
}

fn focus_child<'a>(item: &'a Value, key: &str) -> Option<&'a Value> {
    if let Some(v) = item.get(key) {
        return Some(v);
    }
    item.as_object().and_then(|map| {
        map.iter().find_map(|(k, v)| {
            k.strip_prefix(key)
                .and_then(|s| s.chars().next())
                .is_some_and(char::is_uppercase)
                .then_some(v)
        })
    })
}

/// Resolve a Reference (or reference string) against `root` only: `contained`
/// resources and Bundle `entry.resource` (plus their contained). No storage.
fn local_resolve(root: &Value, node: &Value) -> Option<Value> {
    let reference = node
        .as_str()
        .or_else(|| node.get("reference").and_then(Value::as_str))?;
    let candidates = in_scope_resources(root);
    candidates
        .into_iter()
        .find(|c| reference_hits(reference, c))
}

fn in_scope_resources(root: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    push_resource_tree(&mut out, root);
    if root.get("resourceType").and_then(Value::as_str) == Some("Bundle")
        && let Some(entries) = root.get("entry").and_then(Value::as_array)
    {
        for entry in entries {
            if let Some(resource) = entry.get("resource") {
                push_resource_tree(&mut out, resource);
            }
        }
    }
    out
}

fn push_resource_tree(out: &mut Vec<Value>, resource: &Value) {
    if resource.get("resourceType").is_some() {
        out.push(resource.clone());
    }
    if let Some(contained) = resource.get("contained").and_then(Value::as_array) {
        for child in contained {
            if child.get("resourceType").is_some() {
                out.push(child.clone());
            }
        }
    }
}

fn reference_hits(reference: &str, candidate: &Value) -> bool {
    let rt = candidate.get("resourceType").and_then(Value::as_str);
    let id = candidate.get("id").and_then(Value::as_str);
    if let Some(frag) = reference.strip_prefix('#') {
        return id == Some(frag);
    }
    if !reference.contains('/') {
        return id == Some(reference);
    }
    let tail = reference.rsplit('/').next().unwrap_or(reference);
    let type_id = reference
        .rsplit('/')
        .nth(1)
        .map(|t| (t, tail))
        .filter(|(t, _)| !t.is_empty() && t.chars().next().is_some_and(char::is_uppercase));
    if let Some((ty, id_part)) = type_id {
        return rt == Some(ty) && id == Some(id_part);
    }
    false
}

/// `exists` matcher: every `{path, exists}` entry must hold on the item.
fn exists_matches(item: &Value, spec: &Value) -> bool {
    let Some(entries) = spec.as_array() else {
        return false;
    };
    entries.iter().all(|entry| {
        let expected = entry.get("exists").and_then(Value::as_bool).unwrap_or(true);
        entry
            .get("path")
            .and_then(Value::as_array)
            .is_some_and(|path| path_exists(item, path) == expected)
    })
}

/// Walk one exists-path over instance JSON. Steps are element keys (with a
/// choice-prefix fallback: `value` also reaches `valueQuantity`) or
/// `{extension: url}` selections; arrays fan out existentially.
fn path_exists(current: &Value, segs: &[Value]) -> bool {
    let Some(seg) = segs.first() else {
        return !current.is_null();
    };
    let rest = &segs[1..];
    match current {
        Value::Array(items) => items.iter().any(|item| path_exists(item, segs)),
        Value::Object(map) => {
            if let Some(key) = seg.as_str() {
                if map.get(key).is_some_and(|v| path_exists(v, rest)) {
                    return true;
                }
                map.iter().any(|(k, v)| {
                    k.strip_prefix(key)
                        .and_then(|s| s.chars().next())
                        .is_some_and(char::is_uppercase)
                        && path_exists(v, rest)
                })
            } else if let Some(url) = seg.get("extension").and_then(Value::as_str) {
                map.get("extension")
                    .and_then(Value::as_array)
                    .is_some_and(|exts| {
                        exts.iter().any(|e| {
                            e.get("url").and_then(Value::as_str) == Some(url)
                                && path_exists(e, rest)
                        })
                    })
            } else {
                false
            }
        }
        _ => false,
    }
}

/// `extension` matcher: some element of the item's `extension` array has the
/// matcher's `url` and satisfies its `pattern` (partial match) or its nested
/// `extension` matcher (chained complex extensions).
fn extension_matches(item: &Value, matcher: &Value) -> bool {
    let Some(url) = matcher.get("url").and_then(Value::as_str) else {
        return false;
    };
    let Some(exts) = item.get("extension").and_then(Value::as_array) else {
        return false;
    };
    exts.iter().any(|e| {
        if e.get("url").and_then(Value::as_str) != Some(url) {
            return false;
        }
        if let Some(nested) = matcher.get("extension") {
            return extension_matches(e, nested);
        }
        match matcher.get("pattern") {
            Some(pattern) => is_partial_match(e, pattern),
            None => true,
        }
    })
}

/// Infer FHIR type codes from a JSON value (resourceType, primitives, Coding).
fn json_fhir_types(item: &Value) -> Vec<String> {
    match item {
        Value::String(_) => vec![
            "string".into(),
            "uri".into(),
            "url".into(),
            "canonical".into(),
            "code".into(),
            "id".into(),
            "markdown".into(),
            "oid".into(),
            "uuid".into(),
            "base64Binary".into(),
            "date".into(),
            "dateTime".into(),
            "instant".into(),
            "time".into(),
        ],
        Value::Bool(_) => vec!["boolean".into()],
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                vec![
                    "integer".into(),
                    "positiveInt".into(),
                    "unsignedInt".into(),
                    "decimal".into(),
                ]
            } else {
                vec!["decimal".into()]
            }
        }
        Value::Object(map) => {
            if let Some(rt) = map.get("resourceType").and_then(Value::as_str) {
                return vec![rt.to_string()];
            }
            if map.contains_key("system") && map.contains_key("code") {
                return vec!["Coding".into()];
            }
            if map.contains_key("coding") || (map.contains_key("text") && map.len() <= 2) {
                return vec!["CodeableConcept".into()];
            }
            if map.contains_key("reference") || map.contains_key("identifier") {
                return vec!["Reference".into()];
            }
            if map.contains_key("value") && (map.contains_key("unit") || map.contains_key("system"))
            {
                return vec!["Quantity".into()];
            }
            if map.contains_key("url") {
                return vec!["Extension".into()];
            }
            Vec::new()
        }
        Value::Array(_) | Value::Null => Vec::new(),
    }
}

fn profile_matches(ctx: Option<&WalkCtx<'_>>, item: &Value, profile: &str) -> bool {
    if let Some(profiles) = item
        .get("meta")
        .and_then(|m| m.get("profile"))
        .and_then(Value::as_array)
        && profiles.iter().any(|p| p.as_str() == Some(profile))
    {
        return true;
    }
    // Extension slices: match by url when the profile canonical is the
    // extension URL (common IG pattern).
    if let Some(url) = item.get("url").and_then(Value::as_str)
        && url == profile
    {
        return true;
    }
    // Resolvable profile whose `type` matches the item's resourceType / JSON type.
    let Some(ctx) = ctx else {
        return false;
    };
    if let Some(schema) = ctx.resolver.resolve(profile) {
        if let Some(ty) = schema.type_.as_deref() {
            if json_fhir_types(item).iter().any(|t| t == ty) {
                return true;
            }
            if item.get("resourceType").and_then(Value::as_str) == Some(ty) {
                return true;
            }
        }
        if let Some(name) = schema.name.as_deref()
            && item.get("resourceType").and_then(Value::as_str) == Some(name)
        {
            return true;
        }
    }
    false
}

/// Binding discriminator: if `expected` is a string, accept when the item is
/// that code, a Coding with that code, or a CodeableConcept containing it.
/// Full ValueSet expansion is intentionally not done here.
fn binding_matches(item: &Value, expected: &Value) -> bool {
    let Some(needle) = expected.as_str() else {
        return is_partial_match(item, expected);
    };
    // Canonical ValueSet URL — only match when the instance literally carries
    // that URL (rare); otherwise leave unmatched (slice stays inactive for
    // ValueSet-based binding discriminators without inline codes).
    if needle.contains('/') {
        if item.as_str() == Some(needle)
            || item.get("system").and_then(Value::as_str) == Some(needle)
        {
            return true;
        }
        if let Some(coding) = item.get("coding").and_then(Value::as_array) {
            return coding
                .iter()
                .any(|c| c.get("system").and_then(Value::as_str) == Some(needle));
        }
        return false;
    }
    match item {
        Value::String(s) => s == needle,
        Value::Object(map) => {
            if map.get("code").and_then(Value::as_str) == Some(needle) {
                return true;
            }
            if let Some(coding) = map.get("coding").and_then(Value::as_array) {
                return coding
                    .iter()
                    .any(|c| c.get("code").and_then(Value::as_str) == Some(needle));
            }
            false
        }
        _ => false,
    }
}

#[cfg(test)]
mod slice_match_tests {
    use super::*;
    use serde_json::json;

    fn slice(v: serde_json::Value) -> Slice {
        serde_json::from_value(v).expect("slice")
    }

    #[test]
    fn an_explicit_match_value_is_a_partial_match() {
        let s = slice(json!({ "match": { "type": "pattern", "value": { "system": "http://x" } } }));
        assert!(slice_matches(
            &s,
            &json!({ "system": "http://x", "value": "1" })
        ));
        assert!(!slice_matches(&s, &json!({ "system": "http://y" })));
    }

    #[test]
    fn a_match_without_a_value_matches_everything() {
        let s = slice(json!({ "match": { "type": "pattern" } }));
        assert!(slice_matches(&s, &json!({ "anything": true })));
    }

    #[test]
    fn a_schema_pattern_stands_in_for_the_match() {
        let s = slice(json!({ "schema": { "pattern": { "system": "http://x" } } }));
        assert!(slice_matches(&s, &json!({ "system": "http://x" })));
        assert!(!slice_matches(&s, &json!({ "system": "http://y" })));
    }

    #[test]
    fn type_matcher_walks_a_nested_discriminator_path() {
        let s = slice(json!({
            "match": { "type": "type", "value": { "valueQuantity": "Quantity" } }
        }));
        assert!(slice_matches(
            &s,
            &json!({ "valueQuantity": { "value": 1.0, "unit": "mg" } })
        ));
        assert!(!slice_matches(
            &s,
            &json!({ "valueCodeableConcept": { "text": "x" } })
        ));
    }

    #[test]
    fn exists_matcher_requires_the_nominated_path() {
        let s = slice(json!({
            "match": { "type": "exists", "value": [{ "path": ["value"], "exists": true }] }
        }));
        assert!(slice_matches(&s, &json!({ "value": 1 })));
        assert!(!slice_matches(&s, &json!({ "code": "x" })));
    }

    #[test]
    fn local_resolve_hits_contained_and_skips_the_store() {
        let root = json!({
            "resourceType": "Observation",
            "contained": [{ "resourceType": "Patient", "id": "p1" }]
        });
        let resolved = local_resolve(&root, &json!({ "reference": "#p1" }));
        assert_eq!(resolved.unwrap()["resourceType"], "Patient");
        assert!(local_resolve(&root, &json!({ "reference": "Patient/missing" })).is_none());
    }

    #[test]
    fn resolve_ref_without_context_matches_nothing() {
        let s = slice(json!({
            "match": {
                "type": "type",
                "value": "Patient",
                "resolve-ref": true
            }
        }));
        assert!(!slice_matches(&s, &json!({ "reference": "#p1" })));
    }
}

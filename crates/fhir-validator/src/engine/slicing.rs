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
//! `pattern`, `type`, `profile`, and `binding` matchers are evaluated.
//! `resolve-ref` is not: a slice whose only matcher is `resolve-ref` matches
//! nothing. A slice with no `match` at all (a constraining slice) also
//! matches nothing. Binding discriminators that name a ValueSet canonical
//! cannot expand that set at mark time; they match when a coding's `system`
//! equals the canonical or a collected `code` equals a non-URL value.

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
                if slice_matches(slice, item) {
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

/// Does an item belong to a slice?
///
/// A missing `match` (constraining slice) matches nothing. A `match` with no
/// `value` matches everything (lodash `_.isMatch` for an empty source).
/// `pattern` is partial deep equality. `type` / `profile` / `binding` walk a
/// nested value the converter built from the discriminator path. `resolve-ref`
/// matches nothing.
pub(crate) fn slice_matches(slice: &Slice, item: &Value) -> bool {
    if let Some(match_) = &slice.match_ {
        let kind = match_.type_.as_deref().unwrap_or("pattern");
        if kind == "resolve-ref" {
            return false;
        }
        return match match_.value.as_ref() {
            Some(expected) => match kind {
                "type" => typed_value_matches(item, expected, fhir_type_matches),
                "profile" => typed_value_matches(item, expected, profile_matches),
                "binding" => typed_value_matches(item, expected, binding_matches),
                _ => is_partial_match(item, expected),
            },
            None => true,
        };
    }
    // The converter carries a pattern/value discriminator as the slice
    // schema's pattern (or fixed) keyword rather than an explicit match.
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

/// Walk a converter-nested expected value (`{code: "CodeableConcept"}`) or a
/// `$this` scalar, applying `leaf` at each focused node.
fn typed_value_matches(data: &Value, expected: &Value, leaf: fn(&Value, &str) -> bool) -> bool {
    match expected {
        Value::String(want) => leaf(data, want),
        Value::Object(map) => map.iter().all(|(key, nested)| {
            data.get(key)
                .is_some_and(|child| typed_value_matches(child, nested, leaf))
        }),
        _ => false,
    }
}

fn fhir_type_matches(data: &Value, expected: &str) -> bool {
    if let Some(rt) = data.get("resourceType").and_then(Value::as_str) {
        return rt == expected;
    }
    if let Some(t) = data.get("type") {
        let type_code = t.as_str().or_else(|| t.get("code").and_then(Value::as_str));
        if type_code == Some(expected) {
            return true;
        }
    }
    match expected {
        "boolean" => data.is_boolean(),
        "integer" | "unsignedInt" | "positiveInt" => data.as_i64().is_some(),
        "decimal" => data.is_number(),
        "string" | "code" | "id" | "uri" | "url" | "canonical" | "oid" | "uuid" | "markdown"
        | "base64Binary" | "date" | "dateTime" | "instant" | "time" | "xhtml" => data.is_string(),
        "Extension" => data.get("url").is_some(),
        "Reference" => {
            data.get("reference").is_some()
                || data.get("identifier").is_some()
                || data.get("display").is_some()
        }
        "Quantity" | "Age" | "Distance" | "Duration" | "Count" | "Money" => {
            data.get("value").is_some() || data.get("unit").is_some() || data.get("code").is_some()
        }
        "CodeableConcept" => data.get("coding").is_some() || data.get("text").is_some(),
        "Coding" => data.get("system").is_some() || data.get("code").is_some(),
        "Identifier" => data.get("system").is_some() || data.get("value").is_some(),
        "Period" => data.get("start").is_some() || data.get("end").is_some(),
        "Range" => data.get("low").is_some() || data.get("high").is_some(),
        "Ratio" => data.get("numerator").is_some() || data.get("denominator").is_some(),
        "HumanName" => {
            data.get("family").is_some()
                || data.get("given").is_some()
                || data.get("text").is_some()
        }
        "Address" => {
            data.get("line").is_some() || data.get("city").is_some() || data.get("use").is_some()
        }
        "Attachment" => {
            data.get("contentType").is_some()
                || data.get("data").is_some()
                || data.get("url").is_some()
        }
        "Annotation" => data.get("text").is_some(),
        "ContactPoint" => data.get("system").is_some() || data.get("value").is_some(),
        "Timing" => data.get("repeat").is_some() || data.get("event").is_some(),
        _ => false,
    }
}

fn profile_matches(data: &Value, canonical: &str) -> bool {
    if data.get("url").and_then(Value::as_str) == Some(canonical) {
        return true;
    }
    if let Some(profiles) = data.pointer("/meta/profile").and_then(Value::as_array) {
        if profiles.iter().any(|p| p.as_str() == Some(canonical)) {
            return true;
        }
    }
    false
}

/// Binding discriminator without a terminology expand: a coding `system` that
/// equals the ValueSet/system canonical, or a primitive/code equal to a
/// non-URL expected value.
fn binding_matches(data: &Value, expected: &str) -> bool {
    if !expected.contains("://") {
        return collected_codes(data).iter().any(|c| c == expected);
    }
    if data.get("system").and_then(Value::as_str) == Some(expected) {
        return true;
    }
    if let Some(codings) = data.get("coding").and_then(Value::as_array) {
        if codings
            .iter()
            .any(|c| c.get("system").and_then(Value::as_str) == Some(expected))
        {
            return true;
        }
    }
    false
}

fn collected_codes(data: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(s) = data.as_str() {
        out.push(s.to_string());
    }
    if let Some(s) = data.get("code").and_then(Value::as_str) {
        out.push(s.to_string());
    }
    if let Some(codings) = data.get("coding").and_then(Value::as_array) {
        for c in codings {
            if let Some(code) = c.get("code").and_then(Value::as_str) {
                out.push(code.to_string());
            }
        }
    }
    out
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
    fn a_schema_fixed_stands_in_for_the_match() {
        let s = slice(json!({ "schema": { "fixed": { "system": "http://x" } } }));
        assert!(slice_matches(&s, &json!({ "system": "http://x" })));
    }

    #[test]
    fn no_discriminator_at_all_matches_nothing() {
        let s = slice(json!({ "min": 1 }));
        assert!(!slice_matches(&s, &json!({ "system": "http://x" })));
    }

    #[test]
    fn a_schema_with_neither_pattern_nor_fixed_matches_nothing() {
        let s = slice(json!({ "schema": { "type": "Identifier" } }));
        assert!(!slice_matches(&s, &json!({ "system": "http://x" })));
    }

    #[test]
    fn type_matcher_uses_resource_type() {
        let s = slice(json!({ "match": { "type": "type", "value": "Patient" } }));
        assert!(slice_matches(&s, &json!({ "resourceType": "Patient" })));
        assert!(!slice_matches(
            &s,
            &json!({ "resourceType": "Observation" })
        ));
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
    fn profile_matcher_uses_extension_url() {
        let s = slice(json!({
            "match": { "type": "profile", "value": "http://example.org/ext" }
        }));
        assert!(slice_matches(
            &s,
            &json!({ "url": "http://example.org/ext" })
        ));
        assert!(!slice_matches(
            &s,
            &json!({ "url": "http://example.org/other" })
        ));
    }

    #[test]
    fn binding_matcher_uses_coding_system() {
        let s = slice(json!({
            "match": { "type": "binding", "value": "http://loinc.org" }
        }));
        assert!(slice_matches(
            &s,
            &json!({ "coding": [{ "system": "http://loinc.org", "code": "123" }] })
        ));
        assert!(!slice_matches(
            &s,
            &json!({ "coding": [{ "system": "http://snomed.info/sct", "code": "123" }] })
        ));
    }

    #[test]
    fn resolve_ref_matches_nothing() {
        let s = slice(json!({ "match": { "type": "resolve-ref", "value": "Patient" } }));
        assert!(!slice_matches(&s, &json!({ "reference": "Patient/1" })));
    }
}

//! Token parameter handler for Elasticsearch.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::search::{IMPLICIT_TOKEN_SYSTEM, implicit_system_candidates};
use crate::types::{SearchModifier, SearchParameter};

/// Builds one clause for every token value on `param`.
///
/// `:in` expansions (CMS122 / CMS165 Advanced Illness is 1,882 codes) used to
/// become one `nested` bool per `system|code`. Elasticsearch then rejects the
/// search with `too_many_nested_clauses` (`maxClauseCount` 2048). Collapse
/// same-shape values into a `terms` query per system so the clause count stays
/// in the single digits.
///
/// Returns `None` for `:text` / `:of-type` so the caller keeps the per-value
/// path.
pub fn build_multi_value_clause(param: &SearchParameter) -> Option<Value> {
    if !can_collapse_token_values(param) {
        return None;
    }
    if param.values.len() <= 1 {
        return param
            .values
            .first()
            .and_then(|value| build_clause(param, &value.value));
    }

    let mut code_only: BTreeSet<&str> = BTreeSet::new();
    let mut no_system: BTreeSet<&str> = BTreeSet::new();
    let mut system_only: BTreeSet<&str> = BTreeSet::new();
    let mut system_codes: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();

    for value in &param.values {
        match value.value.split_once('|') {
            None if !value.value.is_empty() => {
                code_only.insert(value.value.as_str());
            }
            Some(("", code)) if !code.is_empty() => {
                no_system.insert(code);
            }
            Some((system, "")) if !system.is_empty() => {
                system_only.insert(system);
            }
            Some((system, code)) if !system.is_empty() && !code.is_empty() => {
                system_codes.entry(system).or_default().insert(code);
            }
            _ => {}
        }
    }

    let mut clauses: Vec<Value> = Vec::new();
    if !code_only.is_empty() {
        clauses.push(nested_token_codes(
            param.name.as_str(),
            None,
            &code_only,
            false,
        ));
    }
    if !no_system.is_empty() {
        clauses.push(nested_token_codes(
            param.name.as_str(),
            None,
            &no_system,
            true,
        ));
    }
    if !system_only.is_empty() {
        clauses.push(nested_system_only(param.name.as_str(), &system_only));
    }
    for (system, codes) in &system_codes {
        clauses.push(nested_token_codes(
            param.name.as_str(),
            Some(*system),
            codes,
            false,
        ));
    }

    match clauses.len() {
        0 => None,
        1 => clauses.pop(),
        _ => Some(json!({
            "bool": {
                "should": clauses,
                "minimum_should_match": 1
            }
        })),
    }
}

fn can_collapse_token_values(param: &SearchParameter) -> bool {
    !matches!(
        param.modifier,
        Some(SearchModifier::Text)
            | Some(SearchModifier::TextAdvanced)
            | Some(SearchModifier::CodeText)
            | Some(SearchModifier::OfType)
    )
}

fn nested_token_codes(
    name: &str,
    system: Option<&str>,
    codes: &BTreeSet<&str>,
    require_absent_or_implicit_system: bool,
) -> Value {
    let mut must_conditions = vec![json!({ "term": { "search_params.token.name": name } })];
    if let Some(system) = system {
        must_conditions.push(json!({
            "terms": { "search_params.token.system": implicit_system_candidates(system) }
        }));
    }
    if require_absent_or_implicit_system {
        must_conditions.push(json!({
            "bool": {
                "should": [
                    { "bool": { "must_not": [
                        { "exists": { "field": "search_params.token.system" } }
                    ] } },
                    { "term": { "search_params.token.system": IMPLICIT_TOKEN_SYSTEM } }
                ],
                "minimum_should_match": 1
            }
        }));
    }
    let code_list: Vec<&str> = codes.iter().copied().collect();
    if code_list.len() == 1 {
        must_conditions.push(json!({ "term": { "search_params.token.code": code_list[0] } }));
    } else {
        must_conditions.push(json!({ "terms": { "search_params.token.code": code_list } }));
    }
    json!({
        "nested": {
            "path": "search_params.token",
            "query": {
                "bool": { "must": must_conditions }
            }
        }
    })
}

fn nested_system_only(name: &str, systems: &BTreeSet<&str>) -> Value {
    let system_list: Vec<&str> = systems.iter().copied().collect();
    let system_clause = if system_list.len() == 1 {
        json!({ "term": { "search_params.token.system": system_list[0] } })
    } else {
        json!({ "terms": { "search_params.token.system": system_list } })
    };
    json!({
        "nested": {
            "path": "search_params.token",
            "query": {
                "bool": {
                    "must": [
                        { "term": { "search_params.token.name": name } },
                        system_clause
                    ]
                }
            }
        }
    })
}

/// Builds an ES query clause for a token search parameter.
pub fn build_clause(param: &SearchParameter, value: &str) -> Option<Value> {
    let name = &param.name;

    // Handle modifiers first
    match param.modifier {
        // `:not` builds the POSITIVE clause here; the query builder negates once,
        // around the OR of all values (#473). Negating per value made
        // `:not=a,b` mean "NOT a OR NOT b", so any resource carrying both values
        // satisfied one half of the OR and leaked back into the result set.
        Some(SearchModifier::Text) => {
            return build_text_clause(name, value);
        }
        Some(SearchModifier::TextAdvanced) => {
            return build_text_advanced_clause(name, value);
        }
        Some(SearchModifier::CodeText) => {
            return build_code_text_clause(name, value);
        }
        Some(SearchModifier::OfType) => {
            return build_of_type_clause(name, value);
        }
        _ => {}
    }

    build_token_condition(name, value, param.modifier.as_ref())
}

/// Builds the core token matching condition.
fn build_token_condition(
    name: &str,
    value: &str,
    _modifier: Option<&SearchModifier>,
) -> Option<Value> {
    let mut must_conditions = vec![json!({ "term": { "search_params.token.name": name } })];

    if let Some((system, code)) = value.split_once('|') {
        if system.is_empty() && !code.is_empty() {
            // |code - code with no system. A `code` element has no system
            // property either; its entry carries the marker (#1379).
            must_conditions.push(json!({ "term": { "search_params.token.code": code } }));
            must_conditions.push(json!({
                "bool": {
                    "should": [
                        { "bool": { "must_not": [
                            { "exists": { "field": "search_params.token.system" } }
                        ] } },
                        { "term": { "search_params.token.system": IMPLICIT_TOKEN_SYSTEM } }
                    ],
                    "minimum_should_match": 1
                }
            }));
        } else if !system.is_empty() && code.is_empty() {
            // system| - any code in system
            must_conditions.push(json!({ "term": { "search_params.token.system": system } }));
        } else {
            // system|code - both must match; or a `code` element, whose
            // system is implicit and not verifiable here (#1379).
            must_conditions.push(json!({
                "terms": { "search_params.token.system": implicit_system_candidates(system) }
            }));
            must_conditions.push(json!({ "term": { "search_params.token.code": code } }));
        }
    } else {
        // code only - match code in any system
        must_conditions.push(json!({ "term": { "search_params.token.code": value } }));
    }

    Some(json!({
        "nested": {
            "path": "search_params.token",
            "query": {
                "bool": {
                    "must": must_conditions
                }
            }
        }
    }))
}

/// Builds a :text modifier clause (search on display).
fn build_text_clause(name: &str, value: &str) -> Option<Value> {
    Some(json!({
        "nested": {
            "path": "search_params.token",
            "query": {
                "bool": {
                    "must": [
                        { "term": { "search_params.token.name": name } },
                        {
                            "match": {
                                "search_params.token.display": {
                                    "query": value,
                                    "operator": "and"
                                }
                            }
                        }
                    ]
                }
            }
        }
    }))
}

/// Builds a :text-advanced modifier clause (ES query string on display).
fn build_text_advanced_clause(name: &str, value: &str) -> Option<Value> {
    Some(json!({
        "nested": {
            "path": "search_params.token",
            "query": {
                "bool": {
                    "must": [
                        { "term": { "search_params.token.name": name } },
                        {
                            "query_string": {
                                "default_field": "search_params.token.display",
                                "query": value
                            }
                        }
                    ]
                }
            }
        }
    }))
}

/// Builds a :code-text modifier clause (case-insensitive starts-with on display).
fn build_code_text_clause(name: &str, value: &str) -> Option<Value> {
    Some(json!({
        "nested": {
            "path": "search_params.token",
            "query": {
                "bool": {
                    "must": [
                        { "term": { "search_params.token.name": name } },
                        { "match_phrase_prefix": { "search_params.token.display": value } }
                    ]
                }
            }
        }
    }))
}

/// Builds a :of-type modifier clause for Identifier types.
/// Format: type-system|type-code|value
fn build_of_type_clause(name: &str, value: &str) -> Option<Value> {
    let parts: Vec<&str> = value.splitn(3, '|').collect();
    if parts.len() < 3 {
        return None;
    }

    let type_system = parts[0];
    let type_code = parts[1];
    let identifier_value = parts[2];

    let mut must_conditions = vec![
        json!({ "term": { "search_params.token.name": name } }),
        json!({ "term": { "search_params.token.code": identifier_value } }),
    ];

    if !type_system.is_empty() {
        must_conditions
            .push(json!({ "term": { "search_params.token.identifier_type_system": type_system } }));
    }
    if !type_code.is_empty() {
        must_conditions
            .push(json!({ "term": { "search_params.token.identifier_type_code": type_code } }));
    }

    Some(json!({
        "nested": {
            "path": "search_params.token",
            "query": {
                "bool": {
                    "must": must_conditions
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SearchParamType, SearchValue};

    fn make_param(name: &str, modifier: Option<SearchModifier>) -> SearchParameter {
        SearchParameter {
            name: name.to_string(),
            param_type: SearchParamType::Token,
            modifier,
            values: vec![SearchValue::eq("test")],
            chain: vec![],
            components: vec![],
        }
    }

    #[test]
    fn test_code_only() {
        let param = make_param("code", None);
        let clause = build_clause(&param, "8867-4").unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("8867-4"));
        assert!(s.contains("search_params.token.code"));
    }

    #[test]
    fn test_system_code() {
        let param = make_param("code", None);
        let clause = build_clause(&param, "http://loinc.org|8867-4").unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("http://loinc.org"));
        assert!(s.contains("8867-4"));
    }

    /// #1379: `system|code` accepts the named system or the marker of a `code`
    /// element; `|code` counts the marker as "no system"; `system|` does not
    /// mention it.
    #[test]
    fn test_implicit_system_marker() {
        let param = make_param("gender", None);
        let must = |value: &str| {
            build_clause(&param, value).unwrap()["nested"]["query"]["bool"]["must"].clone()
        };

        assert_eq!(
            must("http://hl7.org/fhir/administrative-gender|female")[1],
            json!({ "terms": { "search_params.token.system": [
                "http://hl7.org/fhir/administrative-gender",
                IMPLICIT_TOKEN_SYSTEM
            ] } })
        );
        assert_eq!(
            must("|female")[2]["bool"]["should"][1],
            json!({ "term": { "search_params.token.system": IMPLICIT_TOKEN_SYSTEM } })
        );
        assert!(
            !must("http://hl7.org/fhir/administrative-gender|")
                .to_string()
                .contains(IMPLICIT_TOKEN_SYSTEM)
        );
    }

    #[test]
    fn test_not_modifier_builds_positive_clause() {
        // The query builder applies the negation once, around all values (#473);
        // the per-value clause must stay positive.
        let param = make_param("gender", Some(SearchModifier::Not));
        let clause = build_clause(&param, "male").unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(!s.contains("must_not"));
        assert!(s.contains("search_params.token.code"));
        assert!(s.contains("male"));
    }

    #[test]
    fn test_text_modifier() {
        let param = make_param("code", Some(SearchModifier::Text));
        let clause = build_clause(&param, "headache").unwrap();
        let s = serde_json::to_string(&clause).unwrap();
        assert!(s.contains("display"));
        assert!(s.contains("headache"));
    }

    #[test]
    fn large_system_code_list_collapses_to_one_nested_terms() {
        let mut param = make_param("code", None);
        param.values = (0..1882)
            .map(|i| SearchValue::eq(format!("http://hl7.org/fhir/sid/icd-10|E{i}")))
            .collect();
        let clause = build_multi_value_clause(&param).unwrap();
        assert!(
            clause["nested"].is_object(),
            "one nested query, not a should-OR"
        );
        let must = clause["nested"]["query"]["bool"]["must"]
            .as_array()
            .expect("must");
        let codes = must
            .iter()
            .find_map(|c| c.pointer("/terms/search_params.token.code"))
            .and_then(|v| v.as_array())
            .expect("terms on code");
        assert_eq!(codes.len(), 1882);
        let encoded = serde_json::to_string(&clause).unwrap();
        let nested_count = encoded.matches("\"nested\"").count();
        assert_eq!(nested_count, 1);
    }

    #[test]
    fn two_systems_become_a_should_of_two_nested_terms() {
        let mut param = make_param("code", None);
        param.values = vec![
            SearchValue::eq("http://snomed.info/sct|44054006"),
            SearchValue::eq("http://hl7.org/fhir/sid/icd-10|E11"),
            SearchValue::eq("http://hl7.org/fhir/sid/icd-10|E10"),
        ];
        let clause = build_multi_value_clause(&param).unwrap();
        let should = clause["bool"]["should"].as_array().expect("should");
        assert_eq!(should.len(), 2);
    }
}

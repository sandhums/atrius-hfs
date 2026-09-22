//! BCP 47 (RFC 5646) language-tag helpers.
//!
//! `urn:ietf:bcp:47` is an open code system. FHIR already publishes the finite
//! lists (`ValueSet/languages`, `ValueSet/written-language`, and
//! `terminology.hl7.org/ValueSet/Languages`) as explicit `compose.include.concept`
//! entries. `ValueSet/all-languages` includes the whole system and cannot be
//! materialised. HTS expands the enumerated lists from the ValueSet itself when
//! no CodeSystem row is loaded, and accepts any well-formed tag on
//! `$validate-code` against the system or `all-languages`. A loaded CodeSystem
//! row wins over this grammar.

use crate::error::HtsError;
use crate::types::{
    ExpansionContains, ExpansionContainsDesignation, ValidateCodeRequest, ValidateCodeResponse,
    ValidationIssue,
};

pub const BCP47_SYSTEM: &str = "urn:ietf:bcp:47";
pub const ALL_LANGUAGES_VS_URL: &str = "http://hl7.org/fhir/ValueSet/all-languages";

pub fn is_all_languages_valueset_url(url: &str) -> bool {
    url.split_once('|').map(|(u, _)| u).unwrap_or(url) == ALL_LANGUAGES_VS_URL
}

/// True when `tag` is a syntactically plausible BCP 47 tag.
///
/// The check is intentionally registry-free: primary subtag of 2–3 ASCII
/// letters, then subtags of 1–8 ASCII alphanumerics. That accepts `hi`, `pa`,
/// `en-US`, `hi-IN`, and `pa-IN`, and rejects empty strings, whitespace, and
/// display names such as `Hindi`.
pub fn is_well_formed_bcp47_tag(tag: &str) -> bool {
    if tag.is_empty() || tag.starts_with('-') || tag.ends_with('-') || tag.contains("--") {
        return false;
    }
    let mut parts = tag.split('-');
    let Some(primary) = parts.next() else {
        return false;
    };
    if !(2..=3).contains(&primary.len()) || !primary.chars().all(|c| c.is_ascii_alphabetic()) {
        return false;
    }
    parts.all(|sub| (1..=8).contains(&sub.len()) && sub.chars().all(|c| c.is_ascii_alphanumeric()))
}

/// Whole-system include of `urn:ietf:bcp:47` (no enumerated concept list).
pub fn is_unbounded_bcp47_include(system_url: &str, inc: &serde_json::Value) -> bool {
    system_url == BCP47_SYSTEM && !include_has_concepts(inc)
}

pub fn unbounded_expansion_error() -> HtsError {
    HtsError::TooCostly(
        "ValueSet expansion of urn:ietf:bcp:47 is too large to enumerate. \
         Validate a specific language tag with $validate-code, or expand \
         http://hl7.org/fhir/ValueSet/languages for the common subset."
            .into(),
    )
}

/// Expansion entries for an enumerated `urn:ietf:bcp:47` include, using the
/// code and display carried on the ValueSet. `None` when this include is not
/// that shape.
pub fn enumerated_bcp47_expansion(
    system_url: &str,
    inc: &serde_json::Value,
) -> Option<Vec<ExpansionContains>> {
    if system_url != BCP47_SYSTEM {
        return None;
    }
    let concepts = inc.get("concept").and_then(|c| c.as_array())?;
    if concepts.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(concepts.len());
    for concept in concepts {
        let Some(code) = concept.get("code").and_then(|c| c.as_str()) else {
            continue;
        };
        if code.is_empty() {
            continue;
        }
        out.push(ExpansionContains {
            system: BCP47_SYSTEM.to_owned(),
            version: None,
            code: code.to_owned(),
            display: concept
                .get("display")
                .and_then(|d| d.as_str())
                .map(str::to_string),
            is_abstract: None,
            inactive: None,
            designations: designations_from_concept(concept),
            properties: vec![],
            extensions: vec![],
            contains: vec![],
        });
    }
    Some(out)
}

/// True when an inline ValueSet is the open language set (canonical
/// `all-languages`, or a compose that includes only the whole BCP 47 system).
pub fn inline_unbounded_language_url(vs: &serde_json::Value) -> Option<String> {
    if let Some(base) = vs.get("url").and_then(|u| u.as_str())
        && is_all_languages_valueset_url(base)
    {
        return Some(qualified_vs_url(
            base,
            vs.get("version").and_then(|v| v.as_str()),
        ));
    }
    let includes = vs
        .get("compose")
        .and_then(|c| c.get("include"))
        .and_then(|i| i.as_array())?;
    if includes.is_empty()
        || !includes
            .iter()
            .all(|inc| compose_include_is_unbounded_bcp47(inc))
    {
        return None;
    }
    let base = vs
        .get("url")
        .and_then(|u| u.as_str())
        .unwrap_or(ALL_LANGUAGES_VS_URL);
    Some(qualified_vs_url(
        base,
        vs.get("version").and_then(|v| v.as_str()),
    ))
}

/// `$validate-code` against `ValueSet/all-languages`.
///
/// Returns `Some` for every request whose URL is that value set, including
/// failures, so the caller does not try to expand the open set.
pub fn validate_all_languages_code(
    url: &str,
    req: &ValidateCodeRequest,
) -> Option<ValidateCodeResponse> {
    if !is_all_languages_valueset_url(url) {
        return None;
    }
    let vs_qualified = if let Some(v) = req.value_set_version.as_deref().filter(|s| !s.is_empty()) {
        format!("{ALL_LANGUAGES_VS_URL}|{v}")
    } else if let Some((_, v)) = url.split_once('|') {
        format!("{ALL_LANGUAGES_VS_URL}|{v}")
    } else {
        ALL_LANGUAGES_VS_URL.to_string()
    };
    Some(validate_unbounded_language_code(&vs_qualified, req))
}

pub fn validate_unbounded_language_code(
    vs_label: &str,
    req: &ValidateCodeRequest,
) -> ValidateCodeResponse {
    let wrong_system = req
        .system
        .as_deref()
        .is_some_and(|sys| !sys.is_empty() && sys != BCP47_SYSTEM);
    if !wrong_system && is_well_formed_bcp47_tag(&req.code) {
        return ValidateCodeResponse {
            result: true,
            message: None,
            display: None,
            system: Some(BCP47_SYSTEM.into()),
            cs_version: None,
            inactive: None,
            issues: vec![],
            caused_by_unknown_system: None,
            concept_status: None,
            normalized_code: None,
        };
    }
    let not_in_vs_text = format!(
        "The provided code '#{}' was not found in the value set '{vs_label}'",
        req.code
    );
    ValidateCodeResponse {
        result: false,
        message: Some(not_in_vs_text.clone()),
        display: None,
        system: None,
        cs_version: None,
        inactive: None,
        issues: vec![ValidationIssue {
            severity: "error".into(),
            fhir_code: "code-invalid".into(),
            tx_code: "not-in-vs".into(),
            text: not_in_vs_text,
            expression: Some("code".into()),
            location: Some("code".into()),
            message_id: Some("None_of_the_provided_codes_are_in_the_value_set_one".into()),
        }],
        caused_by_unknown_system: None,
        concept_status: None,
        normalized_code: None,
    }
}

/// `CodeSystem/$validate-code` when `urn:ietf:bcp:47` is not loaded.
pub fn validate_system_code(code: &str) -> ValidateCodeResponse {
    if is_well_formed_bcp47_tag(code) {
        return ValidateCodeResponse {
            result: true,
            message: None,
            display: None,
            system: Some(BCP47_SYSTEM.into()),
            cs_version: None,
            inactive: None,
            issues: vec![],
            caused_by_unknown_system: None,
            concept_status: None,
            normalized_code: None,
        };
    }
    let text = format!("Unknown code '{code}' in the CodeSystem '{BCP47_SYSTEM}'");
    ValidateCodeResponse {
        result: false,
        message: Some(text.clone()),
        display: None,
        system: Some(BCP47_SYSTEM.into()),
        cs_version: None,
        inactive: None,
        issues: vec![ValidationIssue {
            severity: "error".into(),
            fhir_code: "code-invalid".into(),
            tx_code: "invalid-code".into(),
            text,
            expression: Some("Coding.code".into()),
            location: None,
            message_id: Some("Unknown_Code_in_Version".into()),
        }],
        caused_by_unknown_system: None,
        concept_status: None,
        normalized_code: None,
    }
}

pub fn matches_text_filter(item: &ExpansionContains, filter_lower: &str) -> bool {
    item.code.to_lowercase().contains(filter_lower)
        || item
            .display
            .as_deref()
            .is_some_and(|d| d.to_lowercase().contains(filter_lower))
}

fn include_has_concepts(inc: &serde_json::Value) -> bool {
    inc.get("concept")
        .and_then(|c| c.as_array())
        .is_some_and(|a| !a.is_empty())
}

fn compose_include_is_unbounded_bcp47(inc: &serde_json::Value) -> bool {
    inc.get("system").and_then(|s| s.as_str()) == Some(BCP47_SYSTEM) && !include_has_concepts(inc)
}

fn qualified_vs_url(base: &str, version: Option<&str>) -> String {
    let bare = base.split_once('|').map(|(u, _)| u).unwrap_or(base);
    match version.map(str::trim).filter(|v| !v.is_empty()) {
        Some(v) if !base.contains('|') => format!("{bare}|{v}"),
        _ => base.to_string(),
    }
}

fn designations_from_concept(concept: &serde_json::Value) -> Vec<ExpansionContainsDesignation> {
    concept
        .get("designation")
        .and_then(|d| d.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|d| {
                    let value = d.get("value").and_then(|v| v.as_str())?.to_string();
                    let use_ = d.get("use");
                    Some(ExpansionContainsDesignation {
                        language: d
                            .get("language")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        use_system: use_
                            .and_then(|u| u.get("system"))
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        use_code: use_
                            .and_then(|u| u.get("code"))
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        value,
                        extensions: vec![],
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_primary_and_regional_tags() {
        for tag in ["en", "hi", "pa", "en-US", "hi-IN", "pa-IN", "zh-Hans-CN"] {
            assert!(is_well_formed_bcp47_tag(tag), "{tag}");
        }
    }

    #[test]
    fn rejects_display_names_and_broken_tags() {
        for tag in ["", "Hindi", "e", "en-", "-en", "en--US", "not a tag"] {
            assert!(!is_well_formed_bcp47_tag(tag), "{tag}");
        }
    }

    #[test]
    fn enumerated_include_keeps_display() {
        let inc = serde_json::json!({
            "system": BCP47_SYSTEM,
            "concept": [
                {"code": "hi", "display": "Hindi"},
                {"code": "pa", "display": "Punjabi"}
            ]
        });
        let codes = enumerated_bcp47_expansion(BCP47_SYSTEM, &inc).unwrap();
        assert_eq!(codes.len(), 2);
        assert_eq!(codes[0].code, "hi");
        assert_eq!(codes[0].display.as_deref(), Some("Hindi"));
        assert!(is_unbounded_bcp47_include(
            BCP47_SYSTEM,
            &serde_json::json!({"system": BCP47_SYSTEM})
        ));
    }

    #[test]
    fn all_languages_validate_accepts_regional_tag() {
        let req = ValidateCodeRequest {
            url: Some(format!("{ALL_LANGUAGES_VS_URL}|4.0.1")),
            code: "hi-IN".into(),
            ..Default::default()
        };
        let resp = validate_all_languages_code(ALL_LANGUAGES_VS_URL, &req).unwrap();
        assert!(resp.result);
        assert_eq!(resp.system.as_deref(), Some(BCP47_SYSTEM));
    }
}

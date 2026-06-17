//! Map FHIR **`RequestGroup`** (from PlanDefinition `$apply`) to CDS Hooks [`Card`]s.

use std::collections::HashMap;

use helios_cds_hooks::{Card, Indicator, Source};
use serde_json::{Value, json};

const SOURCE_LABEL: &str = "Atrius clinical reasoning";
const REQUEST_GROUP_EXTENSION_URL: &str = "https://atrius.dev/cds-clinical-reasoning-request-group";
const CQF_MESSAGES_URL: &str = "http://hl7.org/fhir/StructureDefinition/cqf-messages";

fn source() -> Source {
    Source {
        label: SOURCE_LABEL.into(),
        url: None,
        icon: None,
        topic: None,
    }
}

fn truncate_summary(s: &str) -> String {
    crate::services::cards::truncate_summary(s)
}

fn request_group_extension(service_id: &str, request_group: &Value) -> HashMap<String, Value> {
    HashMap::from([(
        REQUEST_GROUP_EXTENSION_URL.to_string(),
        json!({
            "serviceId": service_id,
            "requestGroup": request_group,
        }),
    )])
}

fn first_action_title(actions: &Value) -> Option<String> {
    actions.as_array().and_then(|arr| {
        arr.iter().find_map(|action| {
            action
                .get("title")
                .and_then(|t| t.as_str())
                .map(str::to_string)
                .or_else(|| {
                    action
                        .get("description")
                        .and_then(|d| d.as_str())
                        .map(str::to_string)
                })
                .or_else(|| first_action_title(action.get("action").unwrap_or(&Value::Null)))
        })
    })
}

fn indicator_for_request_group(status: &str, request_group: &Value) -> Indicator {
    if apply_outcome_has_severity(request_group, &["error", "fatal"]) {
        return Indicator::Critical;
    }
    if apply_outcome_has_severity(request_group, &["warning"]) {
        return Indicator::Warning;
    }
    // Clean `$apply`: active/on-hold proposals are informational, not error state.
    let _ = status;
    Indicator::Info
}

/// Resolve CQF `$apply` messages from `cqf-messages` extension → contained OperationOutcome.
fn apply_outcome_issues(request_group: &Value) -> Option<&Vec<Value>> {
    let reference = request_group
        .get("extension")
        .and_then(|ext| ext.as_array())
        .and_then(|exts| {
            exts.iter().find_map(|e| {
                if e.get("url").and_then(|u| u.as_str()) != Some(CQF_MESSAGES_URL) {
                    return None;
                }
                e.get("valueReference")
                    .and_then(|r| r.get("reference"))
                    .and_then(|r| r.as_str())
            })
        })?;

    let id = reference.strip_prefix('#')?;
    request_group
        .get("contained")
        .and_then(|c| c.as_array())
        .and_then(|contained| {
            contained.iter().find_map(|resource| {
                if resource.get("id").and_then(|i| i.as_str()) == Some(id) {
                    resource.get("issue").and_then(|i| i.as_array())
                } else {
                    None
                }
            })
        })
}

fn apply_outcome_has_severity(request_group: &Value, severities: &[&str]) -> bool {
    apply_outcome_issues(request_group).is_some_and(|issues| {
        issues.iter().any(|issue| {
            issue
                .get("severity")
                .and_then(|s| s.as_str())
                .is_some_and(|s| severities.contains(&s))
        })
    })
}

/// Build CDS cards from a `$apply` RequestGroup payload.
pub fn cards_from_request_group(
    service_id: &str,
    service_title: &str,
    request_group: &Value,
) -> Vec<Card> {
    let status = request_group
        .get("status")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");

    let actions_val = request_group.get("action").cloned().unwrap_or(Value::Null);
    let nested_title = first_action_title(&actions_val);

    let title = request_group
        .get("title")
        .and_then(|t| t.as_str())
        .or_else(|| request_group.get("description").and_then(|d| d.as_str()))
        .or(nested_title.as_deref())
        .unwrap_or(service_title);

    let action_count = actions_val.as_array().map_or(0, |a| a.len());

    let summary = if action_count > 0 {
        format!("{title} — {status} ({action_count} action(s))")
    } else {
        format!("{title} — {status}")
    };

    let detail = serde_json::to_string_pretty(request_group)
        .ok()
        .map(|json| format!("```json\n{json}\n```"));

    vec![Card {
        uuid: None,
        summary: truncate_summary(&summary),
        detail,
        indicator: indicator_for_request_group(status, request_group),
        source: source(),
        suggestions: None,
        selection_behavior: None,
        override_reasons: None,
        links: None,
        extension: Some(request_group_extension(service_id, request_group)),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_uses_title_and_status() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "status": "draft",
            "title": "CMS165 Initial Population",
            "action": []
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert!(cards[0].summary.contains("CMS165"));
        assert!(cards[0].summary.contains("draft"));
    }

    #[test]
    fn indicator_info_for_clean_active_apply() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "status": "active",
            "action": [{ "id": "a1", "title": "Do thing" }]
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert_eq!(cards[0].indicator, Indicator::Info);
    }

    #[test]
    fn indicator_critical_when_apply_outcome_has_errors() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "status": "active",
            "extension": [{
                "url": CQF_MESSAGES_URL,
                "valueReference": { "reference": "#apply-outcome" }
            }],
            "contained": [{
                "resourceType": "OperationOutcome",
                "id": "apply-outcome",
                "issue": [{
                    "severity": "error",
                    "diagnostics": "Could not resolve expression"
                }]
            }],
            "action": []
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert_eq!(cards[0].indicator, Indicator::Critical);
    }

    #[test]
    fn indicator_warning_when_apply_outcome_has_warnings_only() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "status": "active",
            "extension": [{
                "url": CQF_MESSAGES_URL,
                "valueReference": { "reference": "#apply-outcome" }
            }],
            "contained": [{
                "resourceType": "OperationOutcome",
                "id": "apply-outcome",
                "issue": [{
                    "severity": "warning",
                    "diagnostics": "Optional action skipped"
                }]
            }],
            "action": []
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert_eq!(cards[0].indicator, Indicator::Warning);
    }
}

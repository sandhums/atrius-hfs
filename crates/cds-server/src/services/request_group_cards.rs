//! Map FHIR **`RequestGroup`** (from PlanDefinition `$apply`) to CDS Hooks [`Card`]s
//! with spec-proper suggestions/actions.
//!
//! # Mapping
//!
//! - Every action group whose direct children resolve to contained request resources
//!   (draft `ServiceRequest` / `MedicationRequest` / `Task` …) becomes one [`Card`].
//! - Each resource-backed child action becomes a [`Suggestion`] with a single
//!   `create` [`Action`] carrying the resolved resource.
//! - `Card.uuid` / `Suggestion.uuid` are the RequestGroup action ids, so clients and
//!   feedback correlate deterministically with PlanDefinition action ids.
//! - FHIR `action.selectionBehavior` maps onto the card `selectionBehavior`
//!   (`at-most-one`/`exactly-one` → `at-most-one`, everything else → `any`).
//! - FHIR `action.priority` escalates the card indicator (`stat` → critical,
//!   `urgent`/`asap` → warning); `$apply` OperationOutcome severities escalate too.
//! - `action.documentation` (RelatedArtifact) maps to absolute card [`Link`]s.
//! - A RequestGroup with no resource-backed actions yields a single informational card.

use helios_cds_hooks::{
    Action, ActionType, Card, Coding, Indicator, Link, LinkType, SelectionBehavior, Source,
    Suggestion,
};
use serde_json::Value;

const SOURCE_LABEL: &str = "Atrius clinical reasoning";
const CQF_MESSAGES_URL: &str = "http://hl7.org/fhir/StructureDefinition/cqf-messages";
/// Override reason codes offered on warning/critical cards (CDS Hooks feedback).
const OVERRIDE_REASON_SYSTEM: &str = "https://atrius.in/fhir/CodeSystem/cds-override-reason";

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

fn default_override_reasons() -> Vec<Coding> {
    [
        ("patient-preference", "Patient preference"),
        ("clinical-judgment", "Clinical judgment — not applicable"),
        ("will-address-later", "Will address later"),
        ("inaccurate-data", "Alert based on inaccurate data"),
    ]
    .into_iter()
    .map(|(code, display)| Coding {
        code: code.into(),
        system: OVERRIDE_REASON_SYSTEM.into(),
        display: Some(display.into()),
    })
    .collect()
}

fn indicator_rank(i: Indicator) -> u8 {
    match i {
        Indicator::Info => 0,
        Indicator::Warning => 1,
        Indicator::Critical => 2,
    }
}

fn max_indicator(a: Indicator, b: Indicator) -> Indicator {
    if indicator_rank(b) > indicator_rank(a) {
        b
    } else {
        a
    }
}

/// FHIR `request-priority` → card indicator escalation.
fn indicator_for_priority(priority: Option<&str>) -> Indicator {
    match priority {
        Some("stat") => Indicator::Critical,
        Some("urgent") | Some("asap") => Indicator::Warning,
        _ => Indicator::Info,
    }
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

fn base_indicator(request_group: &Value) -> Indicator {
    if apply_outcome_has_severity(request_group, &["error", "fatal"]) {
        return Indicator::Critical;
    }
    if apply_outcome_has_severity(request_group, &["warning"]) {
        return Indicator::Warning;
    }
    Indicator::Info
}

fn str_field<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(|s| s.as_str()).filter(|s| !s.is_empty())
}

fn resolve_contained<'a>(request_group: &'a Value, reference: &str) -> Option<&'a Value> {
    let id = reference.strip_prefix('#')?;
    request_group
        .get("contained")
        .and_then(|c| c.as_array())?
        .iter()
        .find(|r| r.get("id").and_then(|i| i.as_str()) == Some(id))
}

/// Resource referenced by an action, resolved from `contained` (excluding OperationOutcome).
fn action_resource<'a>(request_group: &'a Value, action: &Value) -> Option<&'a Value> {
    let reference = action
        .get("resource")
        .and_then(|r| r.get("reference"))
        .and_then(|r| r.as_str())?;
    let resource = resolve_contained(request_group, reference)?;
    if resource.get("resourceType").and_then(|t| t.as_str()) == Some("OperationOutcome") {
        return None;
    }
    Some(resource)
}

fn child_actions(action: &Value) -> &[Value] {
    action
        .get("action")
        .and_then(|a| a.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn primary_coding_display(resource: &Value) -> Option<&str> {
    let coding = resource
        .get("code")
        .or_else(|| resource.get("medicationCodeableConcept"))?
        .get("coding")?
        .as_array()?
        .first()?;
    coding.get("display").and_then(|d| d.as_str())
}

fn suggestion_label(action: &Value, resource: &Value) -> String {
    str_field(action, "title")
        .or_else(|| str_field(action, "description"))
        .map(str::to_string)
        .or_else(|| primary_coding_display(resource).map(str::to_string))
        .or_else(|| {
            resource
                .get("resourceType")
                .and_then(|t| t.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "Proposed order".to_string())
}

/// Map FHIR `action-selection-behavior` codes onto CDS Hooks card selection behavior.
fn selection_behavior_for(action_or_group: &Value) -> SelectionBehavior {
    match str_field(action_or_group, "selectionBehavior") {
        Some("at-most-one") | Some("exactly-one") => SelectionBehavior::AtMostOne,
        _ => SelectionBehavior::Any,
    }
}

/// `action.documentation` RelatedArtifacts with URLs → absolute card links.
fn links_from_documentation(action: &Value, links: &mut Vec<Link>) {
    let Some(docs) = action.get("documentation").and_then(|d| d.as_array()) else {
        return;
    };
    for doc in docs {
        let Some(url) = str_field(doc, "url") else {
            continue;
        };
        if links.iter().any(|l| l.url == url) {
            continue;
        }
        let label = str_field(doc, "display")
            .or_else(|| str_field(doc, "label"))
            .unwrap_or(url);
        links.push(Link {
            label: label.to_string(),
            url: url.to_string(),
            link_type: LinkType::Absolute,
            app_context: None,
            autolaunchable: None,
        });
    }
}

fn suggestion_from_leaf(action: &Value, resource: &Value) -> Suggestion {
    Suggestion {
        label: suggestion_label(action, resource),
        uuid: str_field(action, "id").map(str::to_string),
        is_recommended: None,
        actions: Some(vec![Action {
            action_type: ActionType::Create,
            description: str_field(action, "description")
                .or_else(|| str_field(action, "title"))
                .map(str::to_string)
                .or(Some(suggestion_label(action, resource))),
            resource: Some(resource.clone()),
            resource_id: None,
        }]),
        action_selection_behavior: None,
    }
}

/// Metadata describing the parent whose direct children are turned into suggestions.
struct GroupMeta<'a> {
    id: Option<&'a str>,
    title: Option<&'a str>,
    description: Option<&'a str>,
    selection_behavior: SelectionBehavior,
    priority: Option<&'a str>,
    /// Raw group action, for `documentation` → card links.
    action: Option<&'a Value>,
}

fn card_from_group(
    request_group: &Value,
    meta: &GroupMeta<'_>,
    leaves: &[&Value],
    fallback_title: &str,
    base: Indicator,
) -> Card {
    let mut indicator = max_indicator(base, indicator_for_priority(meta.priority));
    let mut links: Vec<Link> = Vec::new();
    let mut suggestions: Vec<Suggestion> = Vec::new();

    if let Some(group_action) = meta.action {
        links_from_documentation(group_action, &mut links);
    }

    for leaf in leaves {
        let Some(resource) = action_resource(request_group, leaf) else {
            continue;
        };
        indicator = max_indicator(
            indicator,
            indicator_for_priority(str_field(leaf, "priority")),
        );
        links_from_documentation(leaf, &mut links);
        suggestions.push(suggestion_from_leaf(leaf, resource));
    }

    let summary = meta
        .title
        .or(meta.description)
        .unwrap_or(fallback_title)
        .to_string();
    let detail = meta
        .description
        .filter(|d| Some(*d) != meta.title.or(Some(&summary)))
        .map(str::to_string);

    let override_reasons = if indicator == Indicator::Info {
        None
    } else {
        Some(default_override_reasons())
    };

    Card {
        uuid: meta.id.map(str::to_string),
        summary: truncate_summary(&summary),
        detail,
        indicator,
        source: source(),
        suggestions: Some(suggestions),
        selection_behavior: Some(meta.selection_behavior),
        override_reasons,
        links: if links.is_empty() { None } else { Some(links) },
        extension: None,
    }
}

/// Walk sibling actions under one parent: resource-backed children become suggestions on
/// a card for the parent; group children recurse into their own cards.
#[allow(clippy::too_many_arguments)]
fn collect_cards(
    request_group: &Value,
    parent: &GroupMeta<'_>,
    children: &[Value],
    fallback_title: &str,
    base: Indicator,
    out: &mut Vec<Card>,
) {
    let leaves: Vec<&Value> = children
        .iter()
        .filter(|a| action_resource(request_group, a).is_some())
        .collect();

    if !leaves.is_empty() {
        let card = card_from_group(request_group, parent, &leaves, fallback_title, base);
        if card
            .suggestions
            .as_ref()
            .is_some_and(|suggestions| !suggestions.is_empty())
        {
            out.push(card);
        }
    }

    for child in children {
        let nested = child_actions(child);
        if nested.is_empty() {
            continue;
        }
        let meta = GroupMeta {
            id: str_field(child, "id"),
            title: str_field(child, "title"),
            description: str_field(child, "description"),
            selection_behavior: selection_behavior_for(child),
            priority: str_field(child, "priority"),
            action: Some(child),
        };
        collect_cards(request_group, &meta, nested, fallback_title, base, out);
    }
}

fn fallback_card(request_group: &Value, service_title: &str, base: Indicator) -> Card {
    let status = request_group
        .get("status")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");
    let action_count = request_group
        .get("action")
        .and_then(|a| a.as_array())
        .map_or(0, Vec::len);

    let title = str_field(request_group, "title")
        .or_else(|| str_field(request_group, "description"))
        .unwrap_or(service_title);

    let summary = if action_count > 0 {
        format!("{title} — {status} ({action_count} action(s))")
    } else {
        format!("{title} — {status}")
    };

    let detail = apply_outcome_issues(request_group).map(|issues| {
        issues
            .iter()
            .filter_map(|i| {
                let severity = str_field(i, "severity").unwrap_or("information");
                str_field(i, "diagnostics").map(|d| format!("- **{severity}**: {d}"))
            })
            .collect::<Vec<_>>()
            .join("\n")
    });

    Card {
        uuid: str_field(request_group, "id").map(str::to_string),
        summary: truncate_summary(&summary),
        detail: detail.filter(|d| !d.is_empty()),
        indicator: base,
        source: source(),
        suggestions: None,
        selection_behavior: None,
        override_reasons: None,
        links: None,
        extension: None,
    }
}

/// Build CDS cards from a `$apply` RequestGroup payload.
pub fn cards_from_request_group(
    service_id: &str,
    service_title: &str,
    request_group: &Value,
) -> Vec<Card> {
    let base = base_indicator(request_group);
    let root_children = request_group
        .get("action")
        .and_then(|a| a.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    let root_meta = GroupMeta {
        id: str_field(request_group, "id").or(Some(service_id)),
        title: str_field(request_group, "title"),
        description: str_field(request_group, "description"),
        selection_behavior: selection_behavior_for(request_group),
        priority: str_field(request_group, "priority"),
        action: None,
    };

    let mut cards = Vec::new();
    collect_cards(
        request_group,
        &root_meta,
        root_children,
        service_title,
        base,
        &mut cards,
    );

    if cards.is_empty() {
        // Fully pruned $apply (no surviving actions, nothing to report) means the
        // service is not applicable to this patient — return no cards rather than
        // a noise "active" info card on every invoke.
        if root_children.is_empty() && apply_outcome_issues(request_group).is_none() {
            return Vec::new();
        }
        return vec![fallback_card(request_group, service_title, base)];
    }
    cards
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn acs_style_request_group() -> Value {
        json!({
            "resourceType": "RequestGroup",
            "id": "rg-1",
            "status": "active",
            "title": "ER Chest Pain Pathway",
            "contained": [
                {
                    "resourceType": "ServiceRequest",
                    "id": "sr-ecg",
                    "status": "draft",
                    "intent": "order",
                    "code": { "coding": [{ "system": "http://snomed.info/sct", "code": "268400002", "display": "12 lead ECG" }] }
                },
                {
                    "resourceType": "MedicationRequest",
                    "id": "mr-aspirin",
                    "status": "draft",
                    "intent": "order",
                    "medicationCodeableConcept": { "coding": [{ "display": "Aspirin 325 mg" }] }
                }
            ],
            "action": [
                {
                    "id": "chest-pain-classification",
                    "title": "Chest pain classification",
                    "selectionBehavior": "exactly-one",
                    "action": [
                        {
                            "id": "branch-acs",
                            "title": "Acute coronary syndrome",
                            "action": [
                                {
                                    "id": "acs-ecg",
                                    "title": "12-lead ECG",
                                    "resource": { "reference": "#sr-ecg" }
                                },
                                {
                                    "id": "acs-aspirin",
                                    "title": "Aspirin loading dose",
                                    "priority": "stat",
                                    "resource": { "reference": "#mr-aspirin" }
                                }
                            ]
                        }
                    ]
                }
            ]
        })
    }

    #[test]
    fn maps_group_actions_to_card_suggestions() {
        let rg = acs_style_request_group();
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert_eq!(cards.len(), 1);

        let card = &cards[0];
        assert_eq!(card.uuid.as_deref(), Some("branch-acs"));
        assert!(card.summary.contains("Acute coronary syndrome"));
        assert_eq!(card.selection_behavior, Some(SelectionBehavior::Any));

        let suggestions = card.suggestions.as_ref().expect("suggestions");
        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].uuid.as_deref(), Some("acs-ecg"));
        assert_eq!(suggestions[0].label, "12-lead ECG");

        let actions = suggestions[0].actions.as_ref().expect("actions");
        assert_eq!(actions[0].action_type, ActionType::Create);
        assert_eq!(
            actions[0].resource.as_ref().unwrap()["resourceType"],
            "ServiceRequest"
        );
    }

    #[test]
    fn stat_priority_escalates_indicator_and_adds_override_reasons() {
        let rg = acs_style_request_group();
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert_eq!(cards[0].indicator, Indicator::Critical);
        assert!(cards[0].override_reasons.as_ref().is_some_and(|r| !r.is_empty()));
    }

    #[test]
    fn exactly_one_selection_maps_to_at_most_one() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "id": "rg-1",
            "status": "active",
            "contained": [
                { "resourceType": "Task", "id": "t1", "status": "draft" },
                { "resourceType": "Task", "id": "t2", "status": "draft" }
            ],
            "action": [{
                "id": "pick-one",
                "title": "Pick one",
                "selectionBehavior": "exactly-one",
                "action": [
                    { "id": "a", "title": "A", "resource": { "reference": "#t1" } },
                    { "id": "b", "title": "B", "resource": { "reference": "#t2" } }
                ]
            }]
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].selection_behavior, Some(SelectionBehavior::AtMostOne));
    }

    #[test]
    fn documentation_maps_to_absolute_links() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "id": "rg-1",
            "status": "active",
            "contained": [{ "resourceType": "ServiceRequest", "id": "sr1", "status": "draft" }],
            "action": [{
                "id": "grp",
                "title": "Group",
                "action": [{
                    "id": "leaf",
                    "title": "Order",
                    "documentation": [{
                        "type": "documentation",
                        "display": "AHA guideline",
                        "url": "https://example.org/aha"
                    }],
                    "resource": { "reference": "#sr1" }
                }]
            }]
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        let links = cards[0].links.as_ref().expect("links");
        assert_eq!(links[0].url, "https://example.org/aha");
        assert_eq!(links[0].label, "AHA guideline");
        assert_eq!(links[0].link_type, LinkType::Absolute);
    }

    #[test]
    fn group_documentation_maps_to_card_links() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "id": "rg-1",
            "status": "active",
            "contained": [{ "resourceType": "ServiceRequest", "id": "sr1", "status": "draft" }],
            "action": [{
                "id": "grp",
                "title": "Group",
                "documentation": [{
                    "type": "documentation",
                    "display": "ACR criteria",
                    "url": "https://example.org/acr"
                }],
                "action": [{
                    "id": "leaf",
                    "title": "Order",
                    "resource": { "reference": "#sr1" }
                }]
            }]
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        let links = cards[0].links.as_ref().expect("links");
        assert_eq!(links[0].url, "https://example.org/acr");
        assert_eq!(links[0].label, "ACR criteria");
    }

    #[test]
    fn no_cards_when_apply_fully_pruned_without_issues() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "id": "rg-1",
            "status": "draft",
            "title": "CMS165 Initial Population",
            "action": []
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert!(cards.is_empty());
    }

    #[test]
    fn fallback_card_when_no_resource_actions() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "id": "rg-1",
            "status": "draft",
            "title": "CMS165 Initial Population",
            "action": [{ "id": "a1", "title": "Guidance only" }]
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert_eq!(cards.len(), 1);
        assert!(cards[0].summary.contains("CMS165"));
        assert!(cards[0].summary.contains("draft"));
        assert!(cards[0].suggestions.is_none());
        assert!(cards[0].extension.is_none());
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
        assert!(cards[0].detail.as_ref().is_some_and(|d| d.contains("Could not resolve")));
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

    #[test]
    fn operation_outcome_reference_is_not_a_suggestion() {
        let rg = json!({
            "resourceType": "RequestGroup",
            "status": "active",
            "contained": [{
                "resourceType": "OperationOutcome",
                "id": "oo",
                "issue": []
            }],
            "action": [{
                "id": "grp",
                "action": [{ "id": "leaf", "resource": { "reference": "#oo" } }]
            }]
        });
        let cards = cards_from_request_group("svc", "Fallback", &rg);
        assert!(cards[0].suggestions.is_none());
    }
}

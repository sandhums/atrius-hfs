//! Event evaluator.
//!
//! Matches resource write events against active subscriptions by evaluating
//! topic triggers and subscription filter criteria.

pub mod filter_match;

use std::sync::Arc;

use crate::event::ResourceEvent;
use crate::manager::{ActiveSubscription, SubscriptionManager};
use crate::topics::{InMemoryTopicRegistry, TopicMatch};

/// Evaluates which subscriptions should receive notifications for a resource event.
pub struct EventEvaluator {
    topic_registry: Arc<InMemoryTopicRegistry>,
    subscription_manager: Arc<SubscriptionManager>,
}

/// A subscription that matched an event, along with topic information.
#[derive(Debug)]
pub struct EvaluationMatch {
    pub subscription: ActiveSubscription,
    pub topic_match: TopicMatch,
}

impl EventEvaluator {
    pub fn new(
        topic_registry: Arc<InMemoryTopicRegistry>,
        subscription_manager: Arc<SubscriptionManager>,
    ) -> Self {
        Self {
            topic_registry,
            subscription_manager,
        }
    }

    /// Evaluates a resource event and returns all matching subscriptions.
    ///
    /// The flow is:
    /// 1. Find all topics whose triggers match (type + interaction + optional FHIRPath).
    /// 2. For each matching topic, find all active subscriptions.
    /// 3. For each subscription, evaluate filter criteria against the resource.
    /// 4. Return subscriptions that pass all filters.
    pub fn evaluate(&self, event: &ResourceEvent) -> Vec<EvaluationMatch> {
        let topic_matches = self.topic_registry.matching_topics(
            event.tenant_id.as_str(),
            &event.resource_type,
            event.event_type,
            event.resource.as_ref(),
            event.previous_resource.as_ref(),
            event.fhir_version,
        );

        let mut results = Vec::new();

        for topic_match in topic_matches {
            let subscriptions = self
                .subscription_manager
                .active_subscriptions_for_topic(event.tenant_id.as_ref(), &topic_match.topic_url);

            for sub in subscriptions {
                // Evaluate filter criteria against the resource.
                let resource_matches = match &event.resource {
                    Some(resource) => {
                        filter_match::matches_filters(resource, &event.resource_type, &sub.filters)
                    }
                    None => {
                        // Delete events with no resource content: match if no filters.
                        sub.filters.is_empty()
                    }
                };

                if resource_matches {
                    results.push(EvaluationMatch {
                        subscription: sub,
                        topic_match: topic_match.clone(),
                    });
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ResourceEventType;
    use crate::manager::SubscriptionStatusCode;
    use crate::manager::filters::SubscriptionFilter;
    use crate::topics::{FilterDefinition, ResourceTrigger, TopicDefinition};
    use chrono::Utc;
    use helios_fhir::FhirVersion;
    use helios_persistence::tenant::TenantId;
    use serde_json::json;

    fn sample_topic() -> TopicDefinition {
        TopicDefinition {
            canonical_url: "http://example.org/topic/encounter-start".to_string(),
            title: Some("Encounter Start".to_string()),
            resource_triggers: vec![ResourceTrigger {
                resource_type: "Encounter".to_string(),
                interactions: vec![ResourceEventType::Create],
                fhirpath_criteria: None,
            }],
            can_filter_by: vec![FilterDefinition {
                resource_type: Some("Encounter".to_string()),
                filter_parameter: "patient".to_string(),
                comparators: vec!["eq".to_string()],
                modifiers: vec![],
            }],
            notification_shape: vec![],
        }
    }

    fn setup() -> (Arc<InMemoryTopicRegistry>, Arc<SubscriptionManager>) {
        let registry = Arc::new(InMemoryTopicRegistry::new());
        registry.add_topic("t1", sample_topic());

        let manager = Arc::new(SubscriptionManager::new(
            Arc::clone(&registry),
            vec!["rest-hook".to_string()],
        ));

        (registry, manager)
    }

    fn register_active_subscription(
        manager: &SubscriptionManager,
        tenant: &str,
        id: &str,
        _filters: Vec<SubscriptionFilter>,
    ) {
        let resource = crate::manager::tests::default_subscription_json();
        manager
            .register(tenant, id, &resource, FhirVersion::default())
            .unwrap();
        manager
            .update_status(tenant, id, SubscriptionStatusCode::Active)
            .unwrap();
    }

    fn make_event(resource_type: &str, event_type: ResourceEventType) -> ResourceEvent {
        ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: resource_type.to_string(),
            resource_id: "123".to_string(),
            version_id: "1".to_string(),
            event_type,
            resource: Some(json!({
                "resourceType": resource_type,
                "id": "123",
                "status": "in-progress",
                "subject": { "reference": "Patient/456" }
            })),
            previous_resource: None,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn test_match_by_resource_type() {
        let (registry, manager) = setup();
        register_active_subscription(&manager, "t1", "sub-1", vec![]);

        let evaluator = EventEvaluator::new(registry, manager);
        let event = make_event("Encounter", ResourceEventType::Create);

        let matches = evaluator.evaluate(&event);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subscription.id, "sub-1");
        assert_eq!(
            matches[0].topic_match.topic_url,
            "http://example.org/topic/encounter-start"
        );
    }

    #[test]
    fn test_no_match_wrong_resource_type() {
        let (registry, manager) = setup();
        register_active_subscription(&manager, "t1", "sub-1", vec![]);

        let evaluator = EventEvaluator::new(registry, manager);
        let event = make_event("Patient", ResourceEventType::Create);

        let matches = evaluator.evaluate(&event);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_no_match_wrong_interaction() {
        let (registry, manager) = setup();
        register_active_subscription(&manager, "t1", "sub-1", vec![]);

        let evaluator = EventEvaluator::new(registry, manager);
        // Topic only triggers on Create, not Update.
        let event = make_event("Encounter", ResourceEventType::Update);

        let matches = evaluator.evaluate(&event);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_no_match_inactive_subscription() {
        let (registry, manager) = setup();
        // Register but don't activate.
        let resource = crate::manager::tests::default_subscription_json();
        manager
            .register("t1", "sub-1", &resource, FhirVersion::default())
            .unwrap();
        // Status is "requested", not "active".

        let evaluator = EventEvaluator::new(registry, manager);
        let event = make_event("Encounter", ResourceEventType::Create);

        let matches = evaluator.evaluate(&event);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_multiple_subscriptions_same_topic() {
        let (registry, manager) = setup();
        register_active_subscription(&manager, "t1", "sub-1", vec![]);
        register_active_subscription(&manager, "t1", "sub-2", vec![]);

        let evaluator = EventEvaluator::new(registry, manager);
        let event = make_event("Encounter", ResourceEventType::Create);

        let matches = evaluator.evaluate(&event);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_tenant_isolation_in_evaluator() {
        let (registry, manager) = setup();
        registry.add_topic("tenant-a", sample_topic());
        registry.add_topic("tenant-b", sample_topic());
        register_active_subscription(&manager, "tenant-a", "sub-1", vec![]);
        register_active_subscription(&manager, "tenant-b", "sub-2", vec![]);

        let evaluator = EventEvaluator::new(registry, manager);

        // Event from tenant-a should only match tenant-a subscriptions.
        let mut event = make_event("Encounter", ResourceEventType::Create);
        event.tenant_id = TenantId::new("tenant-a");

        let matches = evaluator.evaluate(&event);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].subscription.id, "sub-1");
    }

    #[test]
    fn test_topic_registry_tenant_isolation_in_evaluator() {
        let (registry, manager) = setup();
        // Topic exists only for t1; tenant-b must not see it for matching.
        register_active_subscription(&manager, "t1", "sub-1", vec![]);

        let evaluator = EventEvaluator::new(registry, manager);
        let mut event = make_event("Encounter", ResourceEventType::Create);
        event.tenant_id = TenantId::new("tenant-b");

        assert!(evaluator.evaluate(&event).is_empty());
    }

    #[cfg(feature = "R4")]
    #[test]
    fn fhirpath_criteria_gates_subscription_match_with_previous() {
        let (registry, manager) = setup();
        registry.add_topic(
            "t1",
            TopicDefinition {
                canonical_url: "http://example.org/topic/encounter-start".to_string(),
                title: None,
                resource_triggers: vec![ResourceTrigger {
                    resource_type: "Encounter".to_string(),
                    interactions: vec![ResourceEventType::Update],
                    fhirpath_criteria: Some(
                        "%previous.status != 'finished' and status = 'finished'".into(),
                    ),
                }],
                can_filter_by: vec![],
                notification_shape: vec![],
            },
        );
        register_active_subscription(&manager, "t1", "sub-1", vec![]);
        let evaluator = EventEvaluator::new(registry, manager);

        let finished = json!({
            "resourceType": "Encounter",
            "id": "123",
            "status": "finished",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "subject": { "reference": "Patient/456" }
        });
        let in_progress = json!({
            "resourceType": "Encounter",
            "id": "123",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "subject": { "reference": "Patient/456" }
        });

        let mut match_event = make_event("Encounter", ResourceEventType::Update);
        match_event.resource = Some(finished.clone());
        match_event.previous_resource = Some(in_progress);
        assert_eq!(evaluator.evaluate(&match_event).len(), 1);

        let mut noop = make_event("Encounter", ResourceEventType::Update);
        noop.resource = Some(finished.clone());
        noop.previous_resource = Some(finished);
        assert!(evaluator.evaluate(&noop).is_empty());
    }

    #[cfg(feature = "R4")]
    #[test]
    fn fhirpath_and_patient_filter_both_required() {
        let (registry, manager) = setup();
        registry.add_topic(
            "t1",
            TopicDefinition {
                canonical_url: "http://example.org/topic/encounter-start".to_string(),
                title: None,
                resource_triggers: vec![ResourceTrigger {
                    resource_type: "Encounter".to_string(),
                    interactions: vec![ResourceEventType::Create],
                    fhirpath_criteria: Some("status = 'in-progress'".into()),
                }],
                can_filter_by: vec![FilterDefinition {
                    resource_type: Some("Encounter".to_string()),
                    filter_parameter: "patient".to_string(),
                    comparators: vec!["eq".to_string()],
                    modifiers: vec![],
                }],
                notification_shape: vec![],
            },
        );

        let resource = json!({
            "resourceType": "Subscription",
            "id": "sub-1",
            "status": "requested",
            "criteria": "http://example.org/topic/encounter-start",
            "channel": {
                "type": "rest-hook",
                "endpoint": "https://example.com/webhook",
                "payload": "application/fhir+json"
            },
            "extension": [
                {
                    "url": "http://hl7.org/fhir/uv/subscriptions-backport/StructureDefinition/backport-topic-canonical",
                    "valueCanonical": "http://example.org/topic/encounter-start"
                },
                {
                    "url": "http://hl7.org/fhir/uv/subscriptions-backport/StructureDefinition/backport-filter-criteria",
                    "valueString": "Encounter?patient=Patient/456"
                }
            ]
        });
        manager
            .register("t1", "sub-1", &resource, FhirVersion::R4)
            .unwrap();
        manager
            .update_status("t1", "sub-1", SubscriptionStatusCode::Active)
            .unwrap();

        let evaluator = EventEvaluator::new(registry, manager);

        let mut ok = make_event("Encounter", ResourceEventType::Create);
        ok.resource = Some(json!({
            "resourceType": "Encounter",
            "id": "123",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "subject": { "reference": "Patient/456" }
        }));
        assert_eq!(evaluator.evaluate(&ok).len(), 1);

        let mut wrong_patient = ok.clone();
        wrong_patient.resource = Some(json!({
            "resourceType": "Encounter",
            "id": "123",
            "status": "in-progress",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "subject": { "reference": "Patient/999" }
        }));
        assert!(evaluator.evaluate(&wrong_patient).is_empty());

        let mut wrong_status = ok;
        wrong_status.resource = Some(json!({
            "resourceType": "Encounter",
            "id": "123",
            "status": "planned",
            "class": { "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP" },
            "subject": { "reference": "Patient/456" }
        }));
        assert!(evaluator.evaluate(&wrong_status).is_empty());
    }

    #[test]
    fn test_delete_event_no_resource() {
        let (registry, manager) = setup();

        // Add a topic that triggers on delete.
        registry.add_topic(
            "t1",
            TopicDefinition {
                canonical_url: "http://example.org/topic/encounter-delete".to_string(),
                title: None,
                resource_triggers: vec![ResourceTrigger {
                    resource_type: "Encounter".to_string(),
                    interactions: vec![ResourceEventType::Delete],
                    fhirpath_criteria: None,
                }],
                can_filter_by: vec![],
                notification_shape: vec![],
            },
        );

        register_active_subscription(&manager, "t1", "sub-1", vec![]);

        let evaluator = EventEvaluator::new(registry, manager);

        let event = ResourceEvent {
            tenant_id: TenantId::new("t1"),
            fhir_version: FhirVersion::default(),
            resource_type: "Encounter".to_string(),
            resource_id: "123".to_string(),
            version_id: "2".to_string(),
            event_type: ResourceEventType::Delete,
            resource: None,
            previous_resource: None,
            timestamp: Utc::now(),
        };

        // encounter-start only triggers on Create, not Delete, so no match.
        let matches = evaluator.evaluate(&event);
        assert!(matches.is_empty());
    }
}

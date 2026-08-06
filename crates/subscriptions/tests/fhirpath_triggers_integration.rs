//! Hard integration tests: R4 backport topics with exact Atrius IG fhirPathCriteria
//! through SubscriptionEngine → rest-hook (match vs non-match).

#![cfg(feature = "R4")]

use chrono::Utc;
use helios_fhir::FhirVersion;
use helios_persistence::tenant::TenantId;
use helios_subscriptions::manager::SubscriptionStatusCode;
use helios_subscriptions::{
    ResourceEvent, ResourceEventType, SubscriptionConfig, SubscriptionEngine,
};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request};

const TENANT: &str = "tenant-fp";
const TOPIC_ADMIT: &str = "https://atrius.in/fhir/SubscriptionTopic/encounter-admit";
const TOPIC_CRITICAL: &str = "https://atrius.in/fhir/SubscriptionTopic/lab-result-critical";

fn enc_class() -> Value {
    json!({
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "code": "IMP"
    })
}

fn encounter(id: &str, status: &str, location: Option<&str>) -> Value {
    let mut e = json!({
        "resourceType": "Encounter",
        "id": id,
        "status": status,
        "class": enc_class(),
        "subject": { "reference": "Patient/p1" }
    });
    if let Some(loc) = location {
        e["location"] = json!([{ "location": { "reference": loc } }]);
    }
    e
}

fn observation_lab(id: &str, status: &str, interpretation: Option<&str>) -> Value {
    let mut o = json!({
        "resourceType": "Observation",
        "id": id,
        "status": status,
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "laboratory"
            }]
        }],
        "code": { "coding": [{ "system": "http://loinc.org", "code": "2823-3" }] },
        "subject": { "reference": "Patient/p1" }
    });
    if let Some(code) = interpretation {
        o["interpretation"] = json!([{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                "code": code
            }]
        }]);
    }
    o
}

fn basic_topic(id: &str, url: &str, resource_sd: &str, interactions: &[&str], fhirpath: &str) -> Value {
    let mut trigger_ext = vec![
        json!({ "url": "resource", "valueUri": resource_sd }),
    ];
    for i in interactions {
        trigger_ext.push(json!({ "url": "supportedInteraction", "valueCode": i }));
    }
    trigger_ext.push(json!({ "url": "fhirPathCriteria", "valueString": fhirpath }));

    json!({
        "resourceType": "Basic",
        "id": id,
        "code": {
            "coding": [{
                "system": "http://hl7.org/fhir/fhir-types",
                "code": "SubscriptionTopic"
            }]
        },
        "extension": [
            {
                "url": "http://hl7.org/fhir/5.0/StructureDefinition/extension-SubscriptionTopic.url",
                "valueUri": url
            },
            {
                "url": "http://hl7.org/fhir/4.3/StructureDefinition/extension-SubscriptionTopic.resourceTrigger",
                "extension": trigger_ext
            }
        ]
    })
}

fn admit_topic() -> Value {
    basic_topic(
        "topic-encounter-admit",
        TOPIC_ADMIT,
        "http://hl7.org/fhir/StructureDefinition/Encounter",
        &["create", "update"],
        "(%previous.empty() or %previous.status != 'in-progress') and status = 'in-progress'",
    )
}

fn critical_lab_topic() -> Value {
    basic_topic(
        "topic-lab-result-critical",
        TOPIC_CRITICAL,
        "http://hl7.org/fhir/StructureDefinition/Observation",
        &["create", "update"],
        "category.coding.where(system = 'http://terminology.hl7.org/CodeSystem/observation-category' and code = 'laboratory').exists() and status = 'final' and interpretation.coding.where(code = 'H' or code = 'HH' or code = 'L' or code = 'LL' or code = 'AA').exists()",
    )
}

fn subscription(id: &str, topic: &str, endpoint: &str) -> Value {
    json!({
        "resourceType": "Subscription",
        "id": id,
        "status": "requested",
        "criteria": topic,
        "channel": {
            "type": "rest-hook",
            "endpoint": endpoint,
            "payload": "application/fhir+json"
        },
        "extension": [
            {
                "url": "http://hl7.org/fhir/uv/subscriptions-backport/StructureDefinition/backport-topic-canonical",
                "valueCanonical": topic
            },
            {
                "url": "http://hl7.org/fhir/uv/subscriptions-backport/StructureDefinition/backport-payload-content",
                "valueCode": "id-only"
            }
        ]
    })
}

fn event(
    resource_type: &str,
    id: &str,
    event_type: ResourceEventType,
    resource: Option<Value>,
    previous: Option<Value>,
) -> ResourceEvent {
    ResourceEvent {
        tenant_id: TenantId::new(TENANT),
        fhir_version: FhirVersion::R4,
        resource_type: resource_type.into(),
        resource_id: id.into(),
        version_id: "1".into(),
        event_type,
        resource,
        previous_resource: previous,
        timestamp: Utc::now(),
    }
}

fn notification_type(bundle: &Value) -> Option<&str> {
    let status = bundle.get("entry")?.get(0)?.get("resource")?;
    match status.get("resourceType")?.as_str()? {
        "Parameters" => status.get("parameter")?.as_array()?.iter().find_map(|p| {
            if p.get("name")?.as_str()? == "type" {
                p.get("valueCode")?.as_str()
            } else {
                None
            }
        }),
        "SubscriptionStatus" => status.get("type")?.as_str(),
        _ => None,
    }
}

fn event_notification_count(requests: &[Request]) -> usize {
    requests
        .iter()
        .filter(|r| {
            let Ok(body) = serde_json::from_slice::<Value>(&r.body) else {
                return false;
            };
            notification_type(&body) == Some("event-notification")
        })
        .count()
}

async fn activate_engine(
    server: &MockServer,
    topic: Value,
    sub_id: &str,
    topic_url: &str,
) -> SubscriptionEngine {
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .mount(server)
        .await;

    let engine = SubscriptionEngine::new(
        SubscriptionConfig {
            max_retries: 1,
            ..Default::default()
        },
        "http://localhost:8080".into(),
    );

    let topic_id = topic["id"].as_str().unwrap().to_string();
    engine
        .on_resource_event(event(
            "Basic",
            &topic_id,
            ResourceEventType::Create,
            Some(topic),
            None,
        ))
        .await;

    let endpoint = format!("{}/hook", server.uri());
    engine
        .on_resource_event(event(
            "Subscription",
            sub_id,
            ResourceEventType::Create,
            Some(subscription(sub_id, topic_url, &endpoint)),
            None,
        ))
        .await;

    let reg = engine
        .manager()
        .get_subscription(TENANT, sub_id)
        .expect("subscription registered");
    assert_eq!(reg.status, SubscriptionStatusCode::Active);
    engine
}

#[tokio::test]
async fn admit_topic_notifies_on_create_and_planned_to_in_progress_only() {
    let server = MockServer::start().await;
    let engine = activate_engine(&server, admit_topic(), "sub-admit", TOPIC_ADMIT).await;

    // Match: create in-progress
    engine
        .on_resource_event(event(
            "Encounter",
            "e1",
            ResourceEventType::Create,
            Some(encounter("e1", "in-progress", None)),
            None,
        ))
        .await;

    // Match: planned → in-progress
    engine
        .on_resource_event(event(
            "Encounter",
            "e2",
            ResourceEventType::Update,
            Some(encounter("e2", "in-progress", None)),
            Some(encounter("e2", "planned", None)),
        ))
        .await;

    // No match: already in-progress update (noop)
    engine
        .on_resource_event(event(
            "Encounter",
            "e3",
            ResourceEventType::Update,
            Some(encounter("e3", "in-progress", None)),
            Some(encounter("e3", "in-progress", None)),
        ))
        .await;

    // No match: finished create
    engine
        .on_resource_event(event(
            "Encounter",
            "e4",
            ResourceEventType::Create,
            Some(encounter("e4", "finished", None)),
            None,
        ))
        .await;

    let requests = server.received_requests().await.unwrap();
    // 1 handshake + 2 event notifications
    assert_eq!(
        event_notification_count(&requests),
        2,
        "expected only create-admit and planned→in-progress; got types: {:?}",
        requests
            .iter()
            .filter_map(|r| {
                let b: Value = serde_json::from_slice(&r.body).ok()?;
                notification_type(&b).map(|s| s.to_string())
            })
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn critical_lab_topic_notifies_only_for_final_with_critical_interpretation() {
    let server = MockServer::start().await;
    let engine =
        activate_engine(&server, critical_lab_topic(), "sub-crit", TOPIC_CRITICAL).await;

    for code in ["H", "HH", "L", "LL", "AA"] {
        engine
            .on_resource_event(event(
                "Observation",
                &format!("o-{code}"),
                ResourceEventType::Create,
                Some(observation_lab(&format!("o-{code}"), "final", Some(code))),
                None,
            ))
            .await;
    }

    // Non-matches
    engine
        .on_resource_event(event(
            "Observation",
            "o-prelim",
            ResourceEventType::Create,
            Some(observation_lab("o-prelim", "preliminary", Some("HH"))),
            None,
        ))
        .await;
    engine
        .on_resource_event(event(
            "Observation",
            "o-normal",
            ResourceEventType::Create,
            Some(observation_lab("o-normal", "final", None)),
            None,
        ))
        .await;
    engine
        .on_resource_event(event(
            "Observation",
            "o-vital",
            ResourceEventType::Create,
            Some(json!({
                "resourceType": "Observation",
                "id": "o-vital",
                "status": "final",
                "category": [{
                    "coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "vital-signs"
                    }]
                }],
                "code": { "coding": [{ "system": "http://loinc.org", "code": "8867-4" }] },
                "interpretation": [{ "coding": [{ "code": "HH" }] }]
            })),
            None,
        ))
        .await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        event_notification_count(&requests),
        5,
        "expected one event per critical code H/HH/L/LL/AA"
    );
}

#[tokio::test]
async fn transfer_criteria_via_registry_and_evaluator_path() {
    // Engine + Basic topic with transfer expression; assert notify only on location change.
    let topic = basic_topic(
        "topic-encounter-transfer",
        "https://atrius.in/fhir/SubscriptionTopic/encounter-transfer",
        "http://hl7.org/fhir/StructureDefinition/Encounter",
        &["update"],
        "%previous.location.exists() and location.exists() and %previous.location != location",
    );

    let server = MockServer::start().await;
    let engine = activate_engine(
        &server,
        topic,
        "sub-xfer",
        "https://atrius.in/fhir/SubscriptionTopic/encounter-transfer",
    )
    .await;

    // Match
    engine
        .on_resource_event(event(
            "Encounter",
            "e-xfer",
            ResourceEventType::Update,
            Some(encounter("e-xfer", "in-progress", Some("Location/ot-1"))),
            Some(encounter("e-xfer", "in-progress", Some("Location/ward-a"))),
        ))
        .await;

    // No match: same location
    engine
        .on_resource_event(event(
            "Encounter",
            "e-same",
            ResourceEventType::Update,
            Some(encounter("e-same", "in-progress", Some("Location/ward-a"))),
            Some(encounter("e-same", "in-progress", Some("Location/ward-a"))),
        ))
        .await;

    // No match: create (interaction update-only)
    engine
        .on_resource_event(event(
            "Encounter",
            "e-create",
            ResourceEventType::Create,
            Some(encounter("e-create", "in-progress", Some("Location/ot-1"))),
            None,
        ))
        .await;

    // No match: previous had no location
    engine
        .on_resource_event(event(
            "Encounter",
            "e-noloc",
            ResourceEventType::Update,
            Some(encounter("e-noloc", "in-progress", Some("Location/ot-1"))),
            Some(encounter("e-noloc", "in-progress", None)),
        ))
        .await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(event_notification_count(&requests), 1);
}

#[tokio::test]
async fn delete_with_fhirpath_criteria_never_matches() {
    let topic = basic_topic(
        "topic-enc-del",
        "https://atrius.in/fhir/SubscriptionTopic/encounter-admit",
        "http://hl7.org/fhir/StructureDefinition/Encounter",
        &["create", "update", "delete"],
        "status = 'in-progress'",
    );

    let server = MockServer::start().await;
    let engine = activate_engine(&server, topic, "sub-del", TOPIC_ADMIT).await;

    engine
        .on_resource_event(event(
            "Encounter",
            "e-del",
            ResourceEventType::Delete,
            None,
            Some(encounter("e-del", "in-progress", None)),
        ))
        .await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        event_notification_count(&requests),
        0,
        "delete cannot evaluate fhirPathCriteria without current resource"
    );
}

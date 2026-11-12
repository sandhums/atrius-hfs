//! `GET [base]/Subscription/{id}/$events` when a durable outbox is attached.
//!
//! Without `eventsSinceNumber` / `eventsUntilNumber` the operation pages the
//! outbox's processed rows with `after` / `count` and returns the stored
//! resources, which is what HIS gap recovery and the BFF read. With either
//! range parameter it is the Subscriptions IG range query (#2547), covered in
//! `subscription_events.rs`.
#![cfg(all(feature = "subscriptions", feature = "R4"))]

use std::path::PathBuf;
use std::sync::Arc;

use axum_test::TestServer;
use chrono::Utc;
use helios_fhir::FhirVersion;
use helios_persistence::backends::sqlite::{SqliteBackend, SqliteBackendConfig};
use helios_persistence::core::{
    InMemorySubscriptionOutbox, OutboxEventType, SubscriptionOutboxEntry, SubscriptionOutboxStore,
};
use helios_persistence::tenant::TenantId;
use helios_rest::{AppState, ServerConfig};
use helios_subscriptions::{
    ResourceEvent, ResourceEventType, SubscriptionConfig, SubscriptionEngine,
};
use serde_json::{Value, json};

const TENANT: &str = "default";
const BASE_URL: &str = "https://public.example/fhir";
const TOPIC_URL: &str = "http://example.org/topics/encounter-start";

fn event(resource_type: &str, id: &str, resource: Value) -> ResourceEvent {
    ResourceEvent {
        tenant_id: TenantId::new(TENANT),
        fhir_version: FhirVersion::R4,
        resource_type: resource_type.to_string(),
        resource_id: id.to_string(),
        version_id: "1".to_string(),
        event_type: ResourceEventType::Create,
        resource: Some(resource),
        previous_resource: None,
        timestamp: Utc::now(),
    }
}

/// Enqueues `n` Encounter creates and marks them processed; returns their row ids.
async fn processed_rows(outbox: &InMemorySubscriptionOutbox, n: usize) -> Vec<i64> {
    let mut ids = Vec::new();
    for i in 1..=n {
        let id = format!("enc-{i}");
        let entry = SubscriptionOutboxEntry::new(
            TenantId::new(TENANT),
            FhirVersion::R4,
            "Encounter",
            &id,
            "1",
            OutboxEventType::Create,
            Some(json!({"resourceType": "Encounter", "id": id, "status": "in-progress"})),
            None,
        );
        ids.push(outbox.enqueue(&entry).await.unwrap());
    }
    for row in outbox
        .claim("test", n as u32, std::time::Duration::from_secs(30))
        .await
        .unwrap()
    {
        outbox.mark_processed(row.id.unwrap()).await.unwrap();
    }
    ids
}

async fn server(rows: usize) -> (TestServer, Vec<i64>) {
    let outbox = Arc::new(InMemorySubscriptionOutbox::new());
    let ids = processed_rows(&outbox, rows).await;

    let engine = Arc::new(
        SubscriptionEngine::new(SubscriptionConfig::default(), BASE_URL.to_string())
            .with_outbox(outbox),
    );
    engine
        .on_resource_event(event(
            "Basic",
            "topic-1",
            json!({
                "resourceType": "Basic", "id": "topic-1",
                "code": {"coding": [{"system": "http://hl7.org/fhir/fhir-types", "code": "SubscriptionTopic"}]},
                "extension": [{
                    "url": "http://hl7.org/fhir/5.0/StructureDefinition/extension-SubscriptionTopic.url",
                    "valueUri": TOPIC_URL
                }, {
                    "url": "http://hl7.org/fhir/4.3/StructureDefinition/extension-SubscriptionTopic.resourceTrigger",
                    "extension": [
                        {"url": "resource", "valueUri": "http://hl7.org/fhir/StructureDefinition/Encounter"},
                        {"url": "supportedInteraction", "valueCode": "create"}
                    ]
                }]
            }),
        ))
        .await;
    engine
        .on_resource_event(event(
            "Subscription",
            "manual-sub",
            json!({
                "resourceType": "Subscription", "id": "manual-sub", "status": "requested",
                "criteria": TOPIC_URL,
                "channel": {"type": "rest-hook", "endpoint": "http://127.0.0.1:9/hook", "payload": "application/fhir+json"}
            }),
        ))
        .await;

    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("data"));
    let backend = Arc::new(
        SqliteBackend::with_config(
            ":memory:",
            SqliteBackendConfig {
                data_dir,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    backend.init_schema().unwrap();
    let state = AppState::new(
        backend,
        ServerConfig {
            base_url: BASE_URL.to_string(),
            default_tenant: TENANT.to_string(),
            ..ServerConfig::for_testing()
        },
    )
    .with_subscription_engine(engine);
    let app = helios_rest::routing::fhir_routes::create_routes(state);
    (TestServer::new(app).unwrap(), ids)
}

fn event_entries(bundle: &Value) -> &[Value] {
    &bundle["entry"].as_array().unwrap()[1..]
}

#[tokio::test]
async fn outbox_events_page_with_count_and_carry_the_resource() {
    let (server, _ids) = server(3).await;

    let response = server.get("/Subscription/manual-sub/$events?count=2").await;
    response.assert_status_ok();
    let bundle: Value = response.json();

    assert_eq!(bundle["type"], "history");
    assert_eq!(
        bundle["entry"][0]["resource"]["resourceType"],
        "SubscriptionStatus"
    );
    let events = event_entries(&bundle);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["request"]["method"], "POST");
    assert_eq!(events[0]["request"]["url"], "Encounter/enc-1");
    assert_eq!(events[0]["resource"]["resourceType"], "Encounter");
    assert_eq!(events[1]["request"]["url"], "Encounter/enc-2");
}

#[tokio::test]
async fn outbox_events_resume_after_the_cursor() {
    let (server, ids) = server(3).await;

    let bundle: Value = server
        .get(&format!(
            "/Subscription/manual-sub/$events?after={}",
            ids[1]
        ))
        .await
        .json();

    let urls: Vec<&str> = event_entries(&bundle)
        .iter()
        .map(|e| e["request"]["url"].as_str().unwrap())
        .collect();
    assert_eq!(urls, ["Encounter/enc-3"]);
}

#[tokio::test]
async fn a_range_parameter_still_selects_the_ig_query() {
    let (server, _ids) = server(3).await;

    let bundle: Value = server
        .get("/Subscription/manual-sub/$events?eventsSinceNumber=1")
        .await
        .json();

    assert_eq!(bundle["entry"][0]["resource"]["resourceType"], "Parameters");
}

//! Status write-back: handshake / delivery transitions update stored Subscription.status.

use helios_fhir::FhirVersion;
use helios_persistence::backends::sqlite::SqliteBackend;
use helios_persistence::core::ResourceStorage;
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_subscriptions::manager::SubscriptionStatusCode;
use helios_subscriptions::{
    RehydrationConfig, ResourceEvent, ResourceEventType, ResourceStorageStatusStore,
    SubscriptionConfig, SubscriptionEngine,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TENANT_ID: &str = "tenant-a";
const TOPIC_URL: &str = "http://example.org/topic/encounter-start";

fn current_fhir_version() -> FhirVersion {
    FhirVersion::default()
}

fn uses_backport_ig() -> bool {
    current_fhir_version() == FhirVersion::R4
}

fn topic_resource() -> Value {
    if uses_backport_ig() {
        json!({
            "resourceType": "Basic",
            "id": "topic-1",
            "code": {
                "coding": [{
                    "system": "http://hl7.org/fhir/fhir-types",
                    "code": "SubscriptionTopic"
                }]
            },
            "extension": [{
                "url": "http://hl7.org/fhir/5.0/StructureDefinition/extension-SubscriptionTopic.url",
                "valueUri": TOPIC_URL
            }, {
                "url": "http://hl7.org/fhir/4.3/StructureDefinition/extension-SubscriptionTopic.resourceTrigger",
                "extension": [{
                    "url": "resource",
                    "valueUri": "http://hl7.org/fhir/StructureDefinition/Encounter"
                }, {
                    "url": "supportedInteraction",
                    "valueCode": "create"
                }]
            }]
        })
    } else {
        json!({
            "resourceType": "SubscriptionTopic",
            "id": "topic-1",
            "url": TOPIC_URL,
            "status": "active",
            "resourceTrigger": [{
                "resource": "Encounter",
                "supportedInteraction": ["create"]
            }]
        })
    }
}

fn topic_resource_type() -> &'static str {
    if uses_backport_ig() {
        "Basic"
    } else {
        "SubscriptionTopic"
    }
}

fn subscription_resource(id: &str, status: &str, endpoint: &str) -> Value {
    if uses_backport_ig() {
        json!({
            "resourceType": "Subscription",
            "id": id,
            "status": status,
            "criteria": TOPIC_URL,
            "channel": {
                "type": "rest-hook",
                "endpoint": endpoint,
                "payload": "application/fhir+json"
            }
        })
    } else {
        json!({
            "resourceType": "Subscription",
            "id": id,
            "status": status,
            "topic": TOPIC_URL,
            "channelType": { "code": "rest-hook" },
            "endpoint": endpoint,
            "contentType": "application/fhir+json"
        })
    }
}

fn tenant_context(tenant_id: &str) -> TenantContext {
    TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access())
}

fn storage() -> SqliteBackend {
    let backend = SqliteBackend::in_memory().expect("in-memory sqlite backend");
    backend.init_schema().expect("init schema");
    backend
}

async fn seed(backend: &SqliteBackend, tenant_id: &str, resource_type: &str, resource: Value) {
    backend
        .create(
            &tenant_context(tenant_id),
            resource_type,
            resource,
            current_fhir_version(),
        )
        .await
        .expect("seed resource");
}

async fn stored_status(backend: &SqliteBackend, id: &str) -> String {
    let stored = backend
        .read(&tenant_context(TENANT_ID), "Subscription", id)
        .await
        .expect("read")
        .expect("subscription present");
    stored
        .content()
        .get("status")
        .and_then(|v| v.as_str())
        .expect("status field")
        .to_string()
}

fn engine_with_store(backend: Arc<SqliteBackend>) -> SubscriptionEngine {
    SubscriptionEngine::new(
        SubscriptionConfig {
            max_retries: 0,
            retry_initial_delay: Duration::from_millis(1),
            retry_max_delay: Duration::from_millis(1),
            handshake_max_attempts: 1,
            handshake_retry_initial_delay: Duration::from_millis(1),
            handshake_retry_max_delay: Duration::from_millis(1),
            error_threshold: 1,
            off_threshold: 2,
            ..SubscriptionConfig::default()
        },
        "http://localhost:8080".to_string(),
    )
    .with_status_store(Arc::new(ResourceStorageStatusStore::new(backend)))
}

#[tokio::test]
async fn handshake_persists_active_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let backend = Arc::new(storage());
    seed(&backend, TENANT_ID, topic_resource_type(), topic_resource()).await;
    let endpoint = format!("{}/hook", server.uri());
    seed(
        &backend,
        TENANT_ID,
        "Subscription",
        subscription_resource("sub-1", "requested", &endpoint),
    )
    .await;

    assert_eq!(stored_status(&backend, "sub-1").await, "requested");

    let engine = engine_with_store(Arc::clone(&backend));
    engine
        .rehydrate(
            backend.as_ref(),
            TENANT_ID,
            current_fhir_version(),
            &RehydrationConfig::default(),
        )
        .await;

    let sub = engine
        .manager()
        .get_subscription(TENANT_ID, "sub-1")
        .expect("registered");
    assert_eq!(sub.status, SubscriptionStatusCode::Active);
    assert_eq!(stored_status(&backend, "sub-1").await, "active");
}

#[tokio::test]
async fn persisted_active_rehydrates_without_handshake() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let backend = Arc::new(storage());
    seed(&backend, TENANT_ID, topic_resource_type(), topic_resource()).await;
    let endpoint = format!("{}/hook", server.uri());
    seed(
        &backend,
        TENANT_ID,
        "Subscription",
        subscription_resource("sub-active", "active", &endpoint),
    )
    .await;

    let engine = engine_with_store(Arc::clone(&backend));
    let report = engine
        .rehydrate(
            backend.as_ref(),
            TENANT_ID,
            current_fhir_version(),
            &RehydrationConfig::default(),
        )
        .await;

    assert_eq!(report.handshakes_started, 0);
    let sub = engine
        .manager()
        .get_subscription(TENANT_ID, "sub-active")
        .expect("registered");
    assert_eq!(sub.status, SubscriptionStatusCode::Active);

    let requests = server.received_requests().await.expect("request log");
    assert!(
        requests.is_empty(),
        "stored active must not re-handshake on rehydrate"
    );
}

#[tokio::test]
async fn delivery_failure_persists_error_status() {
    let backend = Arc::new(storage());
    seed(&backend, TENANT_ID, topic_resource_type(), topic_resource()).await;
    seed(
        &backend,
        TENANT_ID,
        "Subscription",
        subscription_resource("sub-fail", "active", "http://127.0.0.1:1/hook"),
    )
    .await;

    // error_threshold=1, off_threshold high so the first failure lands on error.
    let engine = SubscriptionEngine::new(
        SubscriptionConfig {
            max_retries: 0,
            retry_initial_delay: Duration::from_millis(1),
            retry_max_delay: Duration::from_millis(1),
            error_threshold: 1,
            off_threshold: 10,
            ..SubscriptionConfig::default()
        },
        "http://localhost:8080".to_string(),
    )
    .with_status_store(Arc::new(ResourceStorageStatusStore::new(Arc::clone(
        &backend,
    ))));

    engine
        .rehydrate(
            backend.as_ref(),
            TENANT_ID,
            current_fhir_version(),
            &RehydrationConfig {
                handshake_requested: false,
                ..RehydrationConfig::default()
            },
        )
        .await;

    engine
        .on_resource_event(ResourceEvent {
            tenant_id: TenantId::new(TENANT_ID),
            fhir_version: current_fhir_version(),
            resource_type: "Encounter".to_string(),
            resource_id: "enc-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(json!({
                "resourceType": "Encounter",
                "id": "enc-1",
                "status": "in-progress"
            })),
            previous_resource: None,
            timestamp: chrono::Utc::now(),
        })
        .await;

    assert_eq!(
        engine
            .manager()
            .get_subscription(TENANT_ID, "sub-fail")
            .expect("still registered")
            .status,
        SubscriptionStatusCode::Error
    );
    assert_eq!(stored_status(&backend, "sub-fail").await, "error");
}

#[tokio::test]
async fn delivery_failures_persist_off_status() {
    let backend = Arc::new(storage());
    seed(&backend, TENANT_ID, topic_resource_type(), topic_resource()).await;
    seed(
        &backend,
        TENANT_ID,
        "Subscription",
        subscription_resource("sub-off", "active", "http://127.0.0.1:1/hook"),
    )
    .await;

    // off_threshold=1 → first failure turns the subscription off.
    let engine = SubscriptionEngine::new(
        SubscriptionConfig {
            max_retries: 0,
            retry_initial_delay: Duration::from_millis(1),
            retry_max_delay: Duration::from_millis(1),
            error_threshold: 1,
            off_threshold: 1,
            ..SubscriptionConfig::default()
        },
        "http://localhost:8080".to_string(),
    )
    .with_status_store(Arc::new(ResourceStorageStatusStore::new(Arc::clone(
        &backend,
    ))));

    engine
        .rehydrate(
            backend.as_ref(),
            TENANT_ID,
            current_fhir_version(),
            &RehydrationConfig {
                handshake_requested: false,
                ..RehydrationConfig::default()
            },
        )
        .await;

    engine
        .on_resource_event(ResourceEvent {
            tenant_id: TenantId::new(TENANT_ID),
            fhir_version: current_fhir_version(),
            resource_type: "Encounter".to_string(),
            resource_id: "enc-1".to_string(),
            version_id: "1".to_string(),
            event_type: ResourceEventType::Create,
            resource: Some(json!({
                "resourceType": "Encounter",
                "id": "enc-1",
                "status": "in-progress"
            })),
            previous_resource: None,
            timestamp: chrono::Utc::now(),
        })
        .await;

    assert_eq!(
        engine
            .manager()
            .get_subscription(TENANT_ID, "sub-off")
            .expect("registered")
            .status,
        SubscriptionStatusCode::Off
    );
    assert_eq!(stored_status(&backend, "sub-off").await, "off");
}

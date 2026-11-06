//! Backend-agnostic `Bundle.entry.request.ifNoneMatch` on transaction PUT.
//!
//! `If-None-Match: *` is create-if-absent: the entry creates when the resource
//! is missing and fails the whole transaction (rolling sibling writes back)
//! when it already exists. Shared by SQLite and PostgreSQL the same way
//! [`super::if_match_suite`] is.

#![allow(dead_code)]

use serde_json::json;

use helios_fhir::FhirVersion;
use helios_persistence::core::{BundleEntry, BundleMethod, BundleProvider};
use helios_persistence::tenant::TenantContext;

fn put_entry(id: &str, family: &str, if_none_match: Option<&str>) -> BundleEntry {
    BundleEntry {
        method: BundleMethod::Put,
        url: format!("Patient/{id}"),
        resource: Some(json!({
            "resourceType": "Patient",
            "id": id,
            "name": [{"family": family}]
        })),
        if_match: None,
        if_none_match: if_none_match.map(String::from),
        if_none_exist: None,
        full_url: None,
        criteria: None,
    }
}

async fn seed_patient<B: BundleProvider>(backend: &B, tenant: &TenantContext, id: &str) {
    let _ = backend
        .create_or_update(
            tenant,
            "Patient",
            id,
            json!({"resourceType": "Patient", "id": id, "name": [{"family": "Original"}]}),
            FhirVersion::default(),
        )
        .await
        .unwrap();
}

async fn stored_family<B: BundleProvider>(backend: &B, tenant: &TenantContext, id: &str) -> String {
    backend
        .read(tenant, "Patient", id)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("Patient/{id} should still exist"))
        .content()["name"][0]["family"]
        .as_str()
        .unwrap()
        .to_string()
}

/// `If-None-Match: *` on an absent id is a create, not an ignored header.
pub async fn star_if_none_match_creates_when_absent<B: BundleProvider>(
    backend: &B,
    tenant: &TenantContext,
) {
    let result = backend
        .process_transaction(
            tenant,
            vec![put_entry("fresh", "Created", Some("*"))],
            FhirVersion::default(),
        )
        .await
        .unwrap();

    assert_eq!(result.entries[0].status, 201);
    assert_eq!(stored_family(backend, tenant, "fresh").await, "Created");
}

/// A PUT with `*` against a resource that already exists must 412 and roll
/// back every sibling in the same transaction.
pub async fn star_if_none_match_on_existing_rolls_the_transaction_back<B: BundleProvider>(
    backend: &B,
    tenant: &TenantContext,
) {
    seed_patient(backend, tenant, "existing").await;

    let result = backend
        .process_transaction(
            tenant,
            vec![
                put_entry("sibling", "Sibling", None),
                put_entry("existing", "Overwritten", Some("*")),
            ],
            FhirVersion::default(),
        )
        .await;

    assert!(
        result.is_err(),
        "ifNoneMatch * against an existing resource must fail the transaction"
    );
    assert_eq!(stored_family(backend, tenant, "existing").await, "Original");
    assert!(
        !backend.exists(tenant, "Patient", "sibling").await.unwrap(),
        "the sibling PUT must roll back with the failed entry"
    );
}

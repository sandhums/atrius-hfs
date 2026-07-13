//! CDS service implementations and registry.
//!
//! # Evaluation backends
//!
//! [`CdsEvalBackend`] selects how a manifest-defined service invokes clinical reasoning:
//!
//! - **`Demo`** — static demo card when `CDS_CLINICAL_REASONING_URL` is unset.
//! - **`Sidecar`** — when manifest declares `planDefinitionId`, calls
//!   [`ClinicalReasoningClient::apply_plan_definition`]; otherwise legacy
//!   [`ClinicalReasoningClient::evaluate_expression`].
//!
//! # Hook support
//!
//! All hooks from the [CDS Hooks Library](https://cds-hooks.hl7.org/hooks/) are supported
//! via typed context parsing ([`crate::hook_context`]) and sidecar evaluate / `$apply`.
//!
//! # FHIR endpoint wiring
//!
//! The sidecar receives [`FhirServiceEndpoints`] built in [`crate::config::Args::shared_sidecar`].
//! **`hfs_base_url`** must reach `cr-fhir-bridge` so clinical retrieves are QI-Core projected and
//! `/Library` includes resolve against KR.

mod cards;
mod request_group_cards;

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use atrius_clinical_reasoning::{
    ApplyPlanDefinitionRequestBuilder, ClinicalReasoningClient, EvaluateExpressionRequestBuilder,
    FhirServiceEndpoints,
};
use helios_cds_hooks::{CdsHooksError, CdsRequest, CdsResponse, CdsService, FeedbackRequest};

use crate::apply_context::{apply_hook_context, apply_hook_context_to_builder};
use crate::config::EvalTargets;
use crate::cr_error::map_clinical_reasoning_err;
use crate::feedback_store::FeedbackStore;
use crate::fhir_authorization::{FhirAccessPolicy, resolve_clinical_fhir_access};
use crate::hook_context::parse_invoke_hook_context;
use crate::invoke_metrics::log_invoke_completed;
use crate::kr_manifest::ManifestService;
use crate::library_version::{LibraryVersionPolicy, ensure_invoke_library_version};
use crate::measurement_period::resolve_measurement_period_parameters;
use cards::{cards_from_normalized, demo_card_for_patient};
use request_group_cards::cards_from_request_group;
use tracing::debug;

/// CDS response extension key carrying the `$apply` CarePlan for client-side persistence.
pub const CARE_PLAN_EXTENSION_KEY: &str = "https://atrius.in/fhir/extension/care-plan";

/// Shared evaluation backend for all manifest-defined services.
#[derive(Debug, Clone)]
pub enum CdsEvalBackend {
    Demo,
    Sidecar {
        client: Arc<ClinicalReasoningClient>,
        endpoints: Arc<FhirServiceEndpoints>,
        /// eCQM reporting interval for this cds-server instance (see `CDS_MEASUREMENT_PERIOD_*`).
        measurement_period: Option<crate::measurement_period::MeasurementPeriod>,
        /// SMART `fhirAuthorization` policy (require token, `fhirServer` allowlist).
        fhir_access_policy: FhirAccessPolicy,
        /// KR `libraryVersion` pinning policy.
        library_version_policy: LibraryVersionPolicy,
    },
}

/// One CDS Hooks service: discovery metadata + sidecar targets (PlanDefinition `$apply` and/or legacy CQL).
#[derive(Debug)]
pub struct SidecarEvalService {
    definition: CdsService,
    backend: CdsEvalBackend,
    targets: EvalTargets,
    plan_definition_id: Option<String>,
    plan_definition_url: Option<String>,
    feedback_store: Option<Arc<FeedbackStore>>,
}

impl SidecarEvalService {
    pub fn from_manifest_entry(
        entry: &ManifestService,
        backend: CdsEvalBackend,
        feedback_store: Option<Arc<FeedbackStore>>,
    ) -> Self {
        let definition = CdsService {
            hook: entry.hook.clone(),
            title: entry.title.clone(),
            description: entry.description.clone(),
            id: entry.id.clone(),
            prefetch: entry.prefetch.clone(),
            usage_requirements: entry.usage_requirements.clone(),
            version: entry.cds_hooks_version.clone(),
            extension: None,
        };
        Self {
            definition,
            backend,
            targets: entry.eval_targets(),
            plan_definition_id: entry.plan_definition_id.clone(),
            plan_definition_url: entry.plan_definition_url.clone(),
            feedback_store,
        }
    }
}

#[async_trait]
pub trait CdsInvocation: Send + Sync {
    fn definition(&self) -> CdsService;
    async fn invoke(&self, request: &CdsRequest) -> Result<CdsResponse, CdsHooksError>;
    async fn feedback(&self, _feedback: &FeedbackRequest) -> Result<(), CdsHooksError> {
        Ok(())
    }
}

#[async_trait]
impl CdsInvocation for SidecarEvalService {
    fn definition(&self) -> CdsService {
        self.definition.clone()
    }

    async fn invoke(&self, request: &CdsRequest) -> Result<CdsResponse, CdsHooksError> {
        self.invoke_clinical(request).await
    }

    async fn feedback(&self, feedback: &FeedbackRequest) -> Result<(), CdsHooksError> {
        if let Some(ref store) = self.feedback_store {
            store
                .record(
                    &self.definition.id,
                    self.plan_definition_url.as_deref(),
                    feedback,
                )
                .await;
        } else {
            debug!(
                service_id = %self.definition.id,
                entries = feedback.feedback.len(),
                "cds feedback received (persistence disabled — set CDS_FEEDBACK_FHIR_BASE_URL)"
            );
        }
        Ok(())
    }
}

impl SidecarEvalService {
    async fn invoke_clinical(&self, request: &CdsRequest) -> Result<CdsResponse, CdsHooksError> {
        let hook_ctx = parse_invoke_hook_context(request)?;

        let started = Instant::now();

        match &self.backend {
            CdsEvalBackend::Demo => {
                let response = CdsResponse::with_cards(demo_card_for_patient(
                    &hook_ctx.patient_id,
                    &request.hook_instance,
                ));
                log_invoke_completed(
                    &self.definition.id,
                    "demo",
                    &self.targets.library_id,
                    self.targets.library_version.as_deref(),
                    started,
                    "ok",
                    None,
                );
                Ok(response)
            }
            CdsEvalBackend::Sidecar {
                client,
                endpoints,
                measurement_period,
                fhir_access_policy,
                library_version_policy,
            } => {
                ensure_invoke_library_version(
                    &self.targets.library_id,
                    &self.targets.library_version,
                    library_version_policy,
                )?;

                let clinical_access = resolve_clinical_fhir_access(
                    request,
                    &endpoints.hfs_base_url,
                    fhir_access_policy,
                    Some(hook_ctx.patient_id.as_str()),
                )?;

                let use_apply = self
                    .plan_definition_id
                    .as_ref()
                    .is_some_and(|s| !s.trim().is_empty())
                    || self
                        .plan_definition_url
                        .as_ref()
                        .is_some_and(|s| !s.trim().is_empty());

                if use_apply {
                    let mut b = ApplyPlanDefinitionRequestBuilder::new(
                        endpoints.as_ref().clone(),
                        hook_ctx.patient_id.clone(),
                    )
                    .clinical_base_url(clinical_access.hfs_base_url.clone());

                    if let Some(ref id) = self.plan_definition_id {
                        b = b.plan_definition_id(id.clone());
                    }
                    if let Some(ref url) = self.plan_definition_url {
                        b = b.plan_definition_url(url.clone());
                    }
                    if let Some(prefetch) = merge_draft_orders_into_prefetch(
                        &request.context,
                        prefetch_map_for_sidecar(&request.prefetch),
                    ) {
                        b = b.prefetch(prefetch);
                    }
                    if let Some(auth) = clinical_access.fhir_authorization.clone() {
                        b = b.fhir_authorization(auth);
                    }
                    if let Some(params) = resolve_measurement_period_parameters(
                        &hook_ctx.measurement_period,
                        &request.extension,
                        measurement_period,
                    ) {
                        b = b.parameters(params);
                    }

                    let apply_ctx = apply_hook_context(request);
                    b = apply_hook_context_to_builder(b, &apply_ctx);

                    let req = b.build().map_err(|e| {
                        CdsHooksError::InternalError(format!("apply request build: {e}"))
                    })?;

                    debug!(
                        service_id = %self.definition.id,
                        plan_definition_id = ?req.plan_definition_id,
                        plan_definition_url = ?req.plan_definition_url,
                        patient_id = %req.patient_id,
                        "cds-server invoking PlanDefinition/$apply via sidecar"
                    );

                    let apply_started = std::time::Instant::now();
                    let resp = client.apply_plan_definition(req).await;
                    let apply_ms = apply_started.elapsed().as_millis() as u64;

                    match resp {
                        Ok(resp) => {
                            log_invoke_completed(
                                &self.definition.id,
                                "apply",
                                &self.targets.library_id,
                                self.targets.library_version.as_deref(),
                                started,
                                "ok",
                                None,
                            );
                            let title = self
                                .definition
                                .title
                                .as_deref()
                                .unwrap_or(&self.definition.id);
                            let cards = cards_from_request_group(
                                &self.definition.id,
                                title,
                                resp.request_group_value(),
                            );
                            let mut response = CdsResponse::with_cards(cards);
                            let mut ext = std::collections::HashMap::new();
                            // Ship the $apply CarePlan alongside the cards so clients can
                            // persist it on order-set acceptance (basedOn linkage).
                            if !response.cards.is_empty()
                                && resp
                                    .care_plan_value()
                                    .get("resourceType")
                                    .and_then(|v| v.as_str())
                                    == Some("CarePlan")
                            {
                                ext.insert(
                                    CARE_PLAN_EXTENSION_KEY.to_string(),
                                    resp.care_plan_value().clone(),
                                );
                            }
                            ext.insert(
                                "https://atrius.in/fhir/extension/cds-apply-timing".to_string(),
                                serde_json::json!({
                                    "applyMs": apply_ms,
                                    "serviceId": self.definition.id,
                                    "evalPath": "apply",
                                    "totalMs": started.elapsed().as_millis() as u64,
                                }),
                            );
                            response.extension = Some(ext);
                            Ok(response)
                        }
                        Err(e) => {
                            let err = map_clinical_reasoning_err(e);
                            log_invoke_completed(
                                &self.definition.id,
                                "apply",
                                &self.targets.library_id,
                                self.targets.library_version.as_deref(),
                                started,
                                "error",
                                Some(err.status_code()),
                            );
                            Err(err)
                        }
                    }
                } else {
                    let mut b = EvaluateExpressionRequestBuilder::new(
                        endpoints.as_ref().clone(),
                        self.targets.library_id.clone(),
                        self.targets.expression.clone(),
                    )
                    .resolve_library_artifacts_from_fhir(self.targets.resolve_from_fhir)
                    .patient_id(hook_ctx.patient_id.clone())
                    .clinical_base_url(clinical_access.hfs_base_url.clone());

                    if let Some(ref v) = self.targets.library_version {
                        b = b.library_version(v.clone());
                    }
                    if let Some(prefetch) = merge_draft_orders_into_prefetch(
                        &request.context,
                        prefetch_map_for_sidecar(&request.prefetch),
                    ) {
                        b = b.prefetch(prefetch);
                    }
                    if let Some(auth) = clinical_access.fhir_authorization.clone() {
                        b = b.fhir_authorization(auth);
                    }
                    if let Some(params) = resolve_measurement_period_parameters(
                        &hook_ctx.measurement_period,
                        &request.extension,
                        measurement_period,
                    ) {
                        b = b.parameters(params);
                    }

                    let req = b.build().map_err(|e| {
                        CdsHooksError::InternalError(format!("evaluate request build: {e}"))
                    })?;

                    debug!(
                        service_id = %self.definition.id,
                        hfs_base_url = %endpoints.hfs_base_url,
                        hts_base_url = %endpoints.hts_base_url,
                        library_base_url = ?endpoints.library_base_url.as_deref(),
                        patient_id = ?req.patient_id,
                        "cds-server invoking clinical reasoning sidecar (legacy evaluate/expression)"
                    );

                    let resp = client.evaluate_expression(req).await;

                    match resp {
                        Ok(resp) => {
                            log_invoke_completed(
                                &self.definition.id,
                                "expression",
                                &self.targets.library_id,
                                self.targets.library_version.as_deref(),
                                started,
                                "ok",
                                None,
                            );
                            let cards = cards_from_normalized(
                                &self.targets.expression,
                                resp.normalized_result(),
                            );
                            Ok(CdsResponse::with_cards(cards))
                        }
                        Err(e) => {
                            let err = map_clinical_reasoning_err(e);
                            log_invoke_completed(
                                &self.definition.id,
                                "expression",
                                &self.targets.library_id,
                                self.targets.library_version.as_deref(),
                                started,
                                "error",
                                Some(err.status_code()),
                            );
                            Err(err)
                        }
                    }
                }
            }
        }
    }
}

fn prefetch_map_for_sidecar(
    prefetch: &Option<std::collections::HashMap<String, Option<serde_json::Value>>>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let map: serde_json::Map<String, serde_json::Value> = prefetch
        .as_ref()?
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
        .collect();
    if map.is_empty() { None } else { Some(map) }
}

/// Prefetch key under which draft-order context resources are forwarded to the sidecar.
const DRAFT_ORDERS_PREFETCH_KEY: &str = "draftOrders";

/// Merge CDS Hooks `context.draftOrders` (Bundle of unsaved orders on `order-select` /
/// `order-sign` / `order-dispatch`) into the sidecar prefetch map so CQL retrieves see them.
///
/// The sidecar drops prefetch resources without a logical `id` during dedupe, and draft
/// orders are typically idless — assign deterministic transient ids (`draft-order-{n}`)
/// before forwarding.
fn merge_draft_orders_into_prefetch(
    context: &serde_json::Value,
    prefetch: Option<serde_json::Map<String, serde_json::Value>>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let Some(bundle) = context.get("draftOrders").filter(|b| b.is_object()) else {
        return prefetch;
    };

    let mut bundle = bundle.clone();
    if let Some(entries) = bundle
        .get_mut("entry")
        .and_then(|e| e.as_array_mut())
        .filter(|e| !e.is_empty())
    {
        for (n, entry) in entries.iter_mut().enumerate() {
            let Some(resource) = entry.get_mut("resource").and_then(|r| r.as_object_mut()) else {
                continue;
            };
            let missing_id = resource
                .get("id")
                .and_then(|i| i.as_str())
                .is_none_or(|s| s.trim().is_empty());
            if missing_id {
                resource.insert(
                    "id".to_string(),
                    serde_json::Value::String(format!("draft-order-{}", n + 1)),
                );
            }
        }
    } else {
        return prefetch;
    }

    let mut map = prefetch.unwrap_or_default();
    map.insert(DRAFT_ORDERS_PREFETCH_KEY.to_string(), bundle);
    Some(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prefetch_map_for_sidecar_skips_null_entries() {
        let mut raw = std::collections::HashMap::new();
        raw.insert("patient".into(), Some(json!({"resourceType": "Patient"})));
        raw.insert("empty".into(), None);
        let map = prefetch_map_for_sidecar(&Some(raw)).expect("map");
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("patient"));
    }

    #[test]
    fn prefetch_map_for_sidecar_none_when_missing_or_empty() {
        assert!(prefetch_map_for_sidecar(&None).is_none());
        assert!(prefetch_map_for_sidecar(&Some(std::collections::HashMap::new())).is_none());
    }

    #[test]
    fn draft_orders_merged_with_transient_ids() {
        let context = json!({
            "patientId": "p1",
            "draftOrders": {
                "resourceType": "Bundle",
                "entry": [
                    { "resource": { "resourceType": "MedicationRequest", "status": "draft" } },
                    { "resource": { "resourceType": "ServiceRequest", "id": "existing", "status": "draft" } }
                ]
            }
        });
        let merged = merge_draft_orders_into_prefetch(&context, None).expect("map");
        let bundle = merged.get(DRAFT_ORDERS_PREFETCH_KEY).expect("bundle");
        let entries = bundle["entry"].as_array().unwrap();
        assert_eq!(entries[0]["resource"]["id"], "draft-order-1");
        assert_eq!(entries[1]["resource"]["id"], "existing");
    }

    #[test]
    fn draft_orders_preserves_existing_prefetch_entries() {
        let context = json!({
            "draftOrders": {
                "resourceType": "Bundle",
                "entry": [{ "resource": { "resourceType": "ServiceRequest" } }]
            }
        });
        let mut existing = serde_json::Map::new();
        existing.insert("patient".into(), json!({"resourceType": "Patient", "id": "p1"}));
        let merged = merge_draft_orders_into_prefetch(&context, Some(existing)).expect("map");
        assert!(merged.contains_key("patient"));
        assert!(merged.contains_key(DRAFT_ORDERS_PREFETCH_KEY));
    }

    #[test]
    fn no_draft_orders_returns_prefetch_unchanged() {
        let context = json!({ "patientId": "p1" });
        assert!(merge_draft_orders_into_prefetch(&context, None).is_none());

        let empty_bundle = json!({ "draftOrders": { "resourceType": "Bundle" } });
        assert!(merge_draft_orders_into_prefetch(&empty_bundle, None).is_none());
    }
}

/// Build a registry from a KR manifest and shared backend.
pub fn registry_from_manifest(
    manifest: &crate::kr_manifest::CdsServicesManifestFile,
    backend: CdsEvalBackend,
    feedback_store: Option<Arc<FeedbackStore>>,
) -> ServiceRegistry {
    let services: Vec<Arc<dyn CdsInvocation>> = manifest
        .services
        .iter()
        .map(|entry| {
            Arc::new(SidecarEvalService::from_manifest_entry(
                entry,
                backend.clone(),
                feedback_store.clone(),
            )) as Arc<dyn CdsInvocation>
        })
        .collect();
    ServiceRegistry::new(services)
}

/// Registry of invocable CDS services (by `{id}` path segment).
#[derive(Clone)]
pub struct ServiceRegistry {
    inner: Arc<Vec<Arc<dyn CdsInvocation>>>,
}

impl ServiceRegistry {
    pub fn new(services: Vec<Arc<dyn CdsInvocation>>) -> Self {
        Self {
            inner: Arc::new(services),
        }
    }

    pub fn discovery_services(&self) -> Vec<CdsService> {
        self.inner.iter().map(|s| s.definition()).collect()
    }

    pub fn by_id(&self, id: &str) -> Option<Arc<dyn CdsInvocation>> {
        self.inner.iter().find(|s| s.definition().id == id).cloned()
    }
}

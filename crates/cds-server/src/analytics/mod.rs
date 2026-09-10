//! Measure evaluate, CQL cohort materialization, and catalog-grounded view suggestion.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tracing::warn;

use crate::clinical_reasoning::{
    ClinicalReasoningClient, ClinicalReasoningError, EvaluateExpressionRequest,
    EvaluateMeasureRequest, FhirServiceEndpoints,
};
use crate::measurement_period::MeasurementPeriod;

use self::catalog::{CatalogSuggestion, MAX_NL_TEXT_CHARS, ViewCatalog};
use self::cohort::{
    MAX_COHORT_PATIENTS, export_hints, generated_group_id, group_resource, normalize_patient_id,
    result_is_member,
};
use self::persist::{FhirPersister, persist_result_json};

pub mod catalog;
pub mod cohort;
pub mod handlers;
pub mod persist;

/// Shared analytics façade (catalog always present; sidecar engine optional in demo mode).
#[derive(Clone)]
pub struct AnalyticsState {
    pub catalog: Arc<ViewCatalog>,
    pub engine: Option<Arc<AnalyticsEngine>>,
}

impl Default for AnalyticsState {
    fn default() -> Self {
        Self {
            catalog: Arc::new(ViewCatalog::embedded()),
            engine: None,
        }
    }
}

pub struct AnalyticsEngine {
    pub client: Arc<ClinicalReasoningClient>,
    pub endpoints: Arc<FhirServiceEndpoints>,
    pub persist: Option<Arc<FhirPersister>>,
    pub measurement_period: Option<MeasurementPeriod>,
    pub cohort_concurrency: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeasureEvaluateBody {
    #[serde(default)]
    pub measure_id: Option<String>,
    #[serde(default)]
    pub measure_url: Option<String>,
    pub patient_id: String,
    #[serde(default)]
    pub period_start: Option<String>,
    #[serde(default)]
    pub period_end: Option<String>,
    #[serde(default)]
    pub report_type: Option<String>,
    #[serde(default = "default_true")]
    pub persist: bool,
    #[serde(default)]
    pub parameters: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CohortBody {
    pub library_id: String,
    #[serde(default)]
    pub library_version: Option<String>,
    pub expression: String,
    pub patient_ids: Vec<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub period_start: Option<String>,
    #[serde(default)]
    pub period_end: Option<String>,
    #[serde(default = "default_true")]
    pub persist: bool,
    #[serde(default)]
    pub parameters: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct NlViewsBody {
    pub text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NlViewsResponse {
    pub supported: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<CatalogSuggestion>,
    pub explanation: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

fn default_true() -> bool {
    true
}

impl AnalyticsState {
    pub fn suggest_views(&self, text: &str) -> Result<NlViewsResponse, (u16, String)> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err((400, "text must not be empty".into()));
        }
        if trimmed.chars().count() > MAX_NL_TEXT_CHARS {
            return Err((400, format!("text exceeds {MAX_NL_TEXT_CHARS} characters")));
        }
        let suggestions = self.catalog.suggest(trimmed, 5);
        if suggestions.is_empty() {
            return Ok(NlViewsResponse {
                supported: false,
                suggestions: Vec::new(),
                explanation: String::new(),
                caveats: Vec::new(),
                reason: "no catalog ViewDefinition or SQLQuery matched the request".into(),
            });
        }
        Ok(NlViewsResponse {
            supported: true,
            explanation: "Ranked bronze ViewDefinitions / SQLQuery Libraries from the Atrius analytics catalog. Review a canonical, then run it on clinical HFS.".into(),
            caveats: vec![
                "This endpoint never executes FHIR search, $sql-run, or $sql-export.".into(),
                "Suggestions are token overlap against a checked-in catalog, not an LLM.".into(),
            ],
            suggestions,
            reason: String::new(),
        })
    }

    pub async fn evaluate_measure(&self, body: MeasureEvaluateBody) -> Result<Value, (u16, Value)> {
        let engine = self.engine_or_unavailable()?;
        let measure_id = body
            .measure_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let measure_url = body
            .measure_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if measure_id.is_none() && measure_url.is_none() {
            return Err((400, json!({"error": "measureId or measureUrl is required"})));
        }
        let patient_id = normalize_patient_id(&body.patient_id)
            .ok_or_else(|| (400, json!({"error": "patientId must not be blank"})))?;

        let (period_start, period_end) = resolve_period(
            &body.period_start,
            &body.period_end,
            &engine.measurement_period,
        );
        let parameters = merge_parameters(
            body.parameters,
            period_parameters(&engine, &period_start, &period_end),
        );

        let request = EvaluateMeasureRequest {
            measure_id: measure_id.map(str::to_string),
            measure_url: measure_url.map(str::to_string),
            patient_id: patient_id.clone(),
            period_start: period_start.clone(),
            period_end: period_end.clone(),
            report_type: body
                .report_type
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "subject".into()),
            hfs_base_url: engine.endpoints.hfs_base_url.clone(),
            hts_base_url: engine.endpoints.hts_base_url.clone(),
            library_base_url: engine.endpoints.library_base_url.clone(),
            use_server_data: true,
            prefetch: None,
            parameters,
            fhir_authorization: None,
        };

        let evaluated = engine
            .client
            .evaluate_measure(request)
            .await
            .map_err(map_sidecar_err)?;

        let mut report = evaluated.measure_report;
        if report.get("resourceType").and_then(Value::as_str) != Some("MeasureReport") {
            return Err((
                502,
                json!({
                    "error": "sidecar did not return a MeasureReport",
                    "measureReport": report
                }),
            ));
        }

        let mut persisted = Value::Null;
        if body.persist {
            let Some(store) = engine.persist.as_ref() else {
                return Err((
                    503,
                    json!({
                        "error": "persist requested but clinical FHIR write base is not configured",
                        "measureReport": report
                    }),
                ));
            };
            let existing_id = report.get("id").and_then(Value::as_str);
            match store.write("MeasureReport", existing_id, &report).await {
                Ok(result) => {
                    if report.get("id").is_none() {
                        report
                            .as_object_mut()
                            .map(|m| m.insert("id".into(), Value::String(result.id.clone())));
                    }
                    persisted = persist_result_json(&result);
                }
                Err(e) => {
                    warn!(error = %e, "MeasureReport persist failed");
                    return Err((
                        502,
                        json!({
                            "error": e.to_string(),
                            "measureReport": report
                        }),
                    ));
                }
            }
        }

        Ok(json!({
            "measureId": evaluated.measure_id,
            "patientId": patient_id,
            "measureReport": report,
            "persisted": persisted
        }))
    }

    pub async fn materialize_cohort(&self, body: CohortBody) -> Result<Value, (u16, Value)> {
        let engine = self.engine_or_unavailable()?;
        let library_id = body.library_id.trim();
        let expression = body.expression.trim();
        if library_id.is_empty() || expression.is_empty() {
            return Err((
                400,
                json!({"error": "libraryId and expression are required"}),
            ));
        }
        if body.patient_ids.is_empty() {
            return Err((
                400,
                json!({"error": "patientIds must list candidate Patient ids (Patient-context CQL is evaluated per subject)"}),
            ));
        }
        if body.patient_ids.len() > MAX_COHORT_PATIENTS {
            return Err((
                400,
                json!({
                    "error": format!("patientIds exceeds {MAX_COHORT_PATIENTS}")
                }),
            ));
        }

        let mut candidates = Vec::new();
        for raw in &body.patient_ids {
            let Some(id) = normalize_patient_id(raw) else {
                return Err((400, json!({"error": format!("invalid patientId: {raw}")})));
            };
            candidates.push(id);
        }

        let (period_start, period_end) = resolve_period(
            &body.period_start,
            &body.period_end,
            &engine.measurement_period,
        );
        let parameters = merge_parameters(
            body.parameters,
            period_parameters(&engine, &period_start, &period_end),
        );

        let concurrency = engine.cohort_concurrency.max(1);
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut joins = Vec::with_capacity(candidates.len());

        for patient_id in candidates.clone() {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| (500, json!({"error": format!("cohort semaphore: {e}")})))?;
            let client = engine.client.clone();
            let endpoints = engine.endpoints.clone();
            let library_id = library_id.to_string();
            let library_version = body.library_version.clone();
            let expression = expression.to_string();
            let parameters = parameters.clone();
            joins.push(tokio::spawn(async move {
                let _permit = permit;
                let request = EvaluateExpressionRequest {
                    elm: None,
                    elm_format: Default::default(),
                    library_id,
                    library_version,
                    expression: expression.clone(),
                    hfs_base_url: endpoints.hfs_base_url.clone(),
                    hts_base_url: endpoints.hts_base_url.clone(),
                    library_base_url: endpoints.library_base_url.clone(),
                    resolve_library_artifacts_from_fhir: true,
                    included_libraries: vec![],
                    patient_id: Some(patient_id.clone()),
                    parameters,
                    evaluation_date_time: None,
                    prefetch: None,
                    fhir_authorization: None,
                };
                match client.evaluate_expression(request).await {
                    Ok(resp) => Ok((patient_id, result_is_member(&resp.result))),
                    Err(e) => Err((patient_id, e)),
                }
            }));
        }

        let mut members = Vec::new();
        let mut excluded = Vec::new();
        let mut failures = Vec::new();
        for join in joins {
            match join.await {
                Ok(Ok((pid, true))) => members.push(pid),
                Ok(Ok((pid, false))) => excluded.push(pid),
                Ok(Err((pid, e))) => failures.push(json!({
                    "patientId": pid,
                    "error": e.to_string()
                })),
                Err(e) => failures.push(json!({"error": format!("task join: {e}")})),
            }
        }

        if !failures.is_empty() && members.is_empty() && excluded.is_empty() {
            return Err((
                502,
                json!({
                    "error": "every candidate evaluation failed",
                    "failures": failures
                }),
            ));
        }

        let name = body
            .name
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("{library_id}#{expression}"));
        let group_id = generated_group_id(library_id);
        let group = group_resource(
            &group_id,
            &name,
            library_id,
            body.library_version.as_deref(),
            expression,
            &members,
        );

        let mut persisted = Value::Null;
        let mut resolved_id = group_id.clone();
        if body.persist {
            let Some(store) = engine.persist.as_ref() else {
                return Err((
                    503,
                    json!({
                        "error": "persist requested but clinical FHIR write base is not configured",
                        "group": group
                    }),
                ));
            };
            match store.write("Group", Some(&group_id), &group).await {
                Ok(result) => {
                    resolved_id = result.id.clone();
                    persisted = persist_result_json(&result);
                }
                Err(e) => {
                    warn!(error = %e, "Group persist failed");
                    return Err((
                        502,
                        json!({
                            "error": e.to_string(),
                            "group": group
                        }),
                    ));
                }
            }
        }

        let hfs = engine
            .persist
            .as_ref()
            .map(|p| p.base_url().to_string())
            .unwrap_or_else(|| engine.endpoints.hfs_base_url.clone());

        Ok(json!({
            "group": group,
            "memberCount": members.len(),
            "evaluated": candidates.len(),
            "excluded": excluded,
            "failures": failures,
            "persisted": persisted,
            "exportHints": export_hints(&hfs, &resolved_id)
        }))
    }

    fn engine_or_unavailable(&self) -> Result<&AnalyticsEngine, (u16, Value)> {
        self.engine.as_deref().ok_or_else(|| {
            (
                503,
                json!({"error": "analytics engine unavailable (CDS_CLINICAL_REASONING_URL is unset — demo mode)"}),
            )
        })
    }
}

fn resolve_period(
    start: &Option<String>,
    end: &Option<String>,
    fallback: &Option<MeasurementPeriod>,
) -> (Option<String>, Option<String>) {
    let start = start.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let end = end.as_deref().map(str::trim).filter(|s| !s.is_empty());
    if start.is_some() || end.is_some() {
        return (start.map(str::to_string), end.map(str::to_string));
    }
    match fallback {
        Some(p) => (
            Some(p.low.format("%Y-%m-%d").to_string()),
            Some(p.high.format("%Y-%m-%d").to_string()),
        ),
        None => (None, None),
    }
}

fn period_parameters(
    engine: &AnalyticsEngine,
    start: &Option<String>,
    end: &Option<String>,
) -> Option<Value> {
    match (start.as_deref(), end.as_deref()) {
        (Some(low), Some(high)) => {
            MeasurementPeriod::parse_bounds(low, high).map(|p| p.to_cql_parameters())
        }
        _ => engine
            .measurement_period
            .as_ref()
            .map(MeasurementPeriod::to_cql_parameters),
    }
}

fn merge_parameters(caller: Option<Value>, period: Option<Value>) -> Option<Value> {
    match (caller, period) {
        (None, period) => period,
        (Some(Value::Object(mut map)), Some(Value::Object(period))) => {
            for (k, v) in period {
                map.entry(k).or_insert(v);
            }
            Some(Value::Object(map))
        }
        (Some(other), _) => Some(other),
    }
}

fn map_sidecar_err(e: ClinicalReasoningError) -> (u16, Value) {
    let status = match e.sidecar_http_status() {
        Some(404 | 410) => 412,
        Some(400 | 422) => 400,
        Some(s) if (500..600).contains(&s) => 502,
        Some(_) => 502,
        None => 502,
    };
    (
        status,
        json!({
            "error": e.to_string()
        }),
    )
}

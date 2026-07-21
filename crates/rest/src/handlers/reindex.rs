//! `$reindex` operation — rebuild search indexes after SearchParameter changes.

use std::any::TypeId;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use helios_persistence::search::{
    ReindexOperation, ReindexProgress, ReindexRequest, ReindexableStorage, SearchParameterExtractor,
};
use helios_persistence::tenant::TenantContext;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::info;

use crate::error::RestError;
use crate::extractors::TenantExtractor;
use crate::state::AppState;

/// Trait object for reindex jobs (keeps AppState free of concrete storage type).
#[async_trait]
pub trait ReindexController: Send + Sync {
    async fn start(
        &self,
        tenant: TenantContext,
        request: ReindexRequest,
    ) -> Result<String, String>;
    async fn progress(&self, job_id: &str) -> Option<ReindexProgress>;
    async fn cancel(&self, job_id: &str) -> Result<(), String>;
}

/// Wrap [`helios_persistence::search::ReindexOperation`].
pub struct PersistenceReindexController<S>
where
    S: ReindexableStorage + 'static,
{
    inner: Arc<ReindexOperation<S>>,
}

impl<S> PersistenceReindexController<S>
where
    S: ReindexableStorage + 'static,
{
    pub fn new(inner: Arc<ReindexOperation<S>>) -> Self {
        Self { inner }
    }

    /// Build a trait-object controller from a reindexable backend + extractor.
    pub fn boxed(
        storage: Arc<S>,
        extractor: Arc<SearchParameterExtractor>,
    ) -> Arc<dyn ReindexController> {
        Arc::new(Self::new(Arc::new(ReindexOperation::new(storage, extractor))))
    }
}

/// Auto-detect SQLite/Postgres backends and build a `$reindex` controller.
///
/// Composite storage is not detected here — callers pass an explicit controller
/// built from the primary backend (see `helios-hfs` startup).
pub fn try_auto_reindex_controller<S: 'static>(
    storage: &Arc<S>,
) -> Option<Arc<dyn ReindexController>> {
    #[cfg(feature = "sqlite")]
    {
        use helios_persistence::backends::sqlite::SqliteBackend;
        if TypeId::of::<S>() == TypeId::of::<SqliteBackend>() {
            // Safety: TypeId equality guarantees S == SqliteBackend.
            let storage =
                unsafe { &*(storage as *const Arc<S> as *const Arc<SqliteBackend>) };
            return Some(PersistenceReindexController::boxed(
                Arc::clone(storage),
                Arc::clone(storage.search_extractor()),
            ));
        }
    }
    #[cfg(feature = "postgres")]
    {
        use helios_persistence::backends::postgres::PostgresBackend;
        if TypeId::of::<S>() == TypeId::of::<PostgresBackend>() {
            // Safety: TypeId equality guarantees S == PostgresBackend.
            let storage =
                unsafe { &*(storage as *const Arc<S> as *const Arc<PostgresBackend>) };
            return Some(PersistenceReindexController::boxed(
                Arc::clone(storage),
                Arc::clone(storage.search_extractor()),
            ));
        }
    }
    let _ = storage;
    None
}

#[async_trait]
impl<S> ReindexController for PersistenceReindexController<S>
where
    S: ReindexableStorage + 'static,
{
    async fn start(
        &self,
        tenant: TenantContext,
        request: ReindexRequest,
    ) -> Result<String, String> {
        self.inner
            .start(tenant, request)
            .await
            .map_err(|e| e.to_string())
    }

    async fn progress(&self, job_id: &str) -> Option<ReindexProgress> {
        self.inner.get_progress(job_id).await
    }

    async fn cancel(&self, job_id: &str) -> Result<(), String> {
        self.inner.cancel(job_id).await.map_err(|e| e.to_string())
    }
}

#[derive(Debug, Deserialize)]
pub struct ReindexBody {
    #[serde(default)]
    pub resource_types: Vec<String>,
    #[serde(default)]
    pub clear_existing: bool,
    pub batch_size: Option<u32>,
}

/// `POST /$reindex` — start a reindex job. Returns 202 + Parameters with job-id.
pub async fn reindex_kickoff_handler<S>(
    State(state): State<AppState<S>>,
    tenant: TenantExtractor,
    body: Option<Json<ReindexBody>>,
) -> Result<Response, RestError>
where
    S: helios_persistence::core::ResourceStorage + Send + Sync + 'static,
{
    let Some(controller) = state.reindex_controller() else {
        return Err(RestError::NotImplemented {
            feature: "$reindex is not configured for this backend".into(),
        });
    };

    let body = body.map(|j| j.0).unwrap_or(ReindexBody {
        resource_types: Vec::new(),
        clear_existing: false,
        batch_size: None,
    });

    let mut request = if body.resource_types.is_empty() {
        ReindexRequest::all()
    } else {
        ReindexRequest::for_types(body.resource_types.clone())
    };
    if body.clear_existing {
        request = request.clear_existing();
    }
    if let Some(bs) = body.batch_size {
        request = request.with_batch_size(bs);
    }

    let job_id = controller
        .start(tenant.context().clone(), request)
        .await
        .map_err(|e| RestError::InternalError { message: e })?;

    info!(%job_id, "started $reindex job");

    let params = json!({
        "resourceType": "Parameters",
        "parameter": [
            { "name": "job-id", "valueString": job_id },
            { "name": "status", "valueCode": "accepted" }
        ]
    });

    Ok((StatusCode::ACCEPTED, Json(params)).into_response())
}

/// `GET /$reindex-status/{job_id}`
pub async fn reindex_status_handler<S>(
    State(state): State<AppState<S>>,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, RestError>
where
    S: helios_persistence::core::ResourceStorage + Send + Sync + 'static,
{
    let Some(controller) = state.reindex_controller() else {
        return Err(RestError::NotImplemented {
            feature: "$reindex is not configured for this backend".into(),
        });
    };
    let Some(progress) = controller.progress(&job_id).await else {
        return Err(RestError::NotFound {
            resource_type: "ReindexJob".into(),
            id: job_id,
        });
    };
    Ok(Json(progress.to_parameters()))
}

/// `DELETE /$reindex-status/{job_id}`
pub async fn reindex_cancel_handler<S>(
    State(state): State<AppState<S>>,
    Path(job_id): Path<String>,
) -> Result<StatusCode, RestError>
where
    S: helios_persistence::core::ResourceStorage + Send + Sync + 'static,
{
    let Some(controller) = state.reindex_controller() else {
        return Err(RestError::NotImplemented {
            feature: "$reindex is not configured for this backend".into(),
        });
    };
    controller
        .cancel(&job_id)
        .await
        .map_err(|e| RestError::BadRequest { message: e })?;
    Ok(StatusCode::ACCEPTED)
}

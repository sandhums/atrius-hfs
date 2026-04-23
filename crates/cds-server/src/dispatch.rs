//! Object-safe dispatch for [`CdsHooksService`](helios_cds_hooks::CdsHooksService) impls.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use helios_cds_hooks::{
    CdsHooksError, CdsHooksService, CdsRequest, CdsResponse, CdsService, DiscoveryResponse,
    FeedbackRequest,
};

/// Type-erased CDS service for HTTP routing (one entry per discovery `id`).
#[async_trait]
pub trait CdsServiceDispatch: Send + Sync {
    fn definition(&self) -> CdsService;

    async fn handle(&self, request: CdsRequest) -> Result<CdsResponse, CdsHooksError>;

    async fn on_feedback(&self, feedback: &FeedbackRequest) -> Result<(), CdsHooksError>;
}

/// Wraps any [`CdsHooksService`] so it can be stored in [`CdsServiceRegistry`].
pub struct ServiceWrapper<S: CdsHooksService> {
    inner: Arc<S>,
}

impl<S: CdsHooksService> ServiceWrapper<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<S: CdsHooksService> CdsServiceDispatch for ServiceWrapper<S> {
    fn definition(&self) -> CdsService {
        self.inner.definition()
    }

    async fn handle(&self, request: CdsRequest) -> Result<CdsResponse, CdsHooksError> {
        let ctx = self.inner.extract_context(&request)?;
        self.inner.call(&request, &ctx).await
    }

    async fn on_feedback(&self, feedback: &FeedbackRequest) -> Result<(), CdsHooksError> {
        self.inner.on_feedback(feedback).await
    }
}

/// Map of service id → service; used for discovery and dispatch.
pub struct CdsServiceRegistry {
    by_id: HashMap<String, Arc<dyn CdsServiceDispatch>>,
}

/// Duplicate service ids when building a [`CdsServiceRegistry`].
#[derive(Debug, thiserror::Error)]
#[error("duplicate cds service id: {0}")]
pub struct DuplicateServiceIdError(pub String);

impl CdsServiceRegistry {
    /// Fails with [`DuplicateServiceIdError`] if two services share the same `definition().id`.
    pub fn try_from_services(
        services: impl IntoIterator<Item = Arc<dyn CdsServiceDispatch>>,
    ) -> Result<Self, DuplicateServiceIdError> {
        let mut by_id = HashMap::new();
        for s in services {
            let id = s.definition().id.clone();
            if by_id.insert(id.clone(), s).is_some() {
                return Err(DuplicateServiceIdError(id));
            }
        }
        Ok(Self { by_id })
    }

    /// Discovery response for `GET /cds-services`.
    pub fn discovery(&self) -> DiscoveryResponse {
        let mut services: Vec<CdsService> = self.by_id.values().map(|s| s.definition()).collect();
        services.sort_by(|a, b| a.id.cmp(&b.id));
        DiscoveryResponse { services }
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn CdsServiceDispatch>> {
        self.by_id.get(id).map(Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helios_cds_hooks::CdsRequest;
    use helios_cds_hooks::hooks::PatientViewContext;

    struct S;

    #[async_trait::async_trait]
    impl CdsHooksService for S {
        type Context = PatientViewContext;

        fn definition(&self) -> CdsService {
            CdsService {
                hook: "patient-view".to_string(),
                title: None,
                description: "d".to_string(),
                id: "a".to_string(),
                prefetch: None,
                usage_requirements: None,
                version: None,
                extension: None,
            }
        }

        async fn call(
            &self,
            _req: &CdsRequest,
            _ctx: &PatientViewContext,
        ) -> Result<CdsResponse, CdsHooksError> {
            Ok(CdsResponse::empty())
        }
    }

    #[test]
    fn duplicate_id_rejected() {
        let a: Arc<dyn CdsServiceDispatch> = Arc::new(ServiceWrapper::new(Arc::new(S)));
        let b: Arc<dyn CdsServiceDispatch> = Arc::new(ServiceWrapper::new(Arc::new(S)));
        let err = CdsServiceRegistry::try_from_services([a, b]).err().unwrap();
        assert_eq!(err.0, "a");
    }
}

//! Resource validation service: the REST layer's bridge to
//! `helios-fhir-validator`.
//!
//! Owns the per-version core validators (embedded schema packs), the
//! FHIRPath constraint evaluator, the optional terminology provider,
//! per-tenant profile registries fed from stored StructureDefinitions, and
//! the mapping from validator issues onto FHIR `OperationOutcome` issues.
//!
//! The `$validate` operation uses [`ValidationService::validate_resource`]
//! unconditionally; the write path (create/update/batch/transaction/bulk-submit)
//! goes through [`ValidationService::check_write`], which is gated by
//! `HFS_VALIDATION_MODE` (`off` | `log` | `enforce`). Relative `Type/id`
//! references are prefetched from the tenant store before the synchronous
//! validator runs ([`ValidationService::with_storage_resolve`]); the engine
//! itself never performs I/O.

use crate::config::ValidationConfig;
use crate::error::RestError;
use crate::responses::operation_outcome::{
    Issue, IssueSeverity, IssueType, OperationOutcomeBuilder,
};
use async_trait::async_trait;
use dashmap::DashMap;
use helios_fhir::FhirVersion;
use helios_fhir_validator::fhirpath_effects::FhirPathConstraintEvaluator;
use helios_fhir_validator::{
    CodedValue, CompositeResolver, EffectHandlers, ErrorKind, PackageCache, PackageRef,
    SchemaRegistry, SchemaResolver, Severity, TerminologyError, TerminologyProvider,
    UnknownProfilePolicy, ValidationError, ValidationOptions, Validator, dotted_to_fhirpath,
    materialize_package_layers,
};
use helios_persistence::core::ResourceStorage;
use helios_persistence::sof::reference_resolver::{
    StorageBackedResolver, StorageReferenceResolver, collect_missing_references,
};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use serde_json::{Value, json};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Per-tenant, per-version profile registries fed from stored
/// StructureDefinitions.
type TenantProfileMap = DashMap<(String, FhirVersion), Arc<RwLock<SchemaRegistry>>>;

/// Write-path behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationMode {
    /// Skip write-path validation entirely.
    #[default]
    Off,
    /// Validate, log issues, and proceed.
    Log,
    /// Reject invalid resources with `422 Unprocessable Entity`.
    Enforce,
}

/// The validation service. Cheap to construct (core registries load lazily,
/// once per process); hold one in `AppState` for the AST cache to pay off.
pub struct ValidationService {
    mode: ValidationMode,
    constraint_evaluator: Option<FhirPathConstraintEvaluator>,
    terminology: Option<Arc<dyn TerminologyProvider>>,
    /// Constraint ids never evaluated (default: `dom-6`, the narrative
    /// warning that fires on almost every machine-generated resource).
    suppress_constraints: Vec<String>,
    /// Escalate terminology-service failures to errors.
    terminology_fail_closed: bool,
    /// Validate against `meta.profile` claims.
    use_meta_profiles: bool,
    unknown_profile: UnknownProfilePolicy,
    /// Per-tenant profile overlays, fed from stored StructureDefinition
    /// writes. `None` disables the feature.
    tenant_profiles: Option<TenantProfileMap>,
    /// Server-wide IG/NPM package registry layers (`HFS_FHIR_PACKAGES`
    /// list order; earlier wins). Empty when the env var is unset.
    package_layers: Vec<Arc<SchemaRegistry>>,
    /// Optional tenant-store prefetch for FHIRPath / slice `resolve()`.
    /// When set, relative `Type/id` references are batch-read before the
    /// synchronous validator runs. The engine itself never performs I/O.
    reference_resolver: Option<Arc<dyn StorageReferenceResolver>>,
}

impl Default for ValidationService {
    fn default() -> Self {
        Self {
            mode: ValidationMode::Off,
            constraint_evaluator: Some(FhirPathConstraintEvaluator::new()),
            terminology: None,
            suppress_constraints: vec!["dom-6".to_string()],
            terminology_fail_closed: false,
            use_meta_profiles: true,
            unknown_profile: UnknownProfilePolicy::Warn,
            tenant_profiles: Some(DashMap::new()),
            package_layers: Vec::new(),
            reference_resolver: None,
        }
    }
}

impl ValidationService {
    /// A service with the default posture: write path off, constraints on
    /// (dom-6 suppressed), meta.profile honored, unresolvable profiles
    /// warned, stored profiles on, no terminology provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a service from `HFS_VALIDATION_*` / `HFS_FHIR_*` configuration.
    /// `terminology_server` is `HFS_TERMINOLOGY_SERVER` (required for
    /// `terminology = remote`; the config validator enforces the pairing).
    ///
    /// Fails when `HFS_FHIR_PACKAGES` is set and a listed package is missing
    /// from the cache or materialization fails — never boots with a silently
    /// empty overlay. `package.json` dependencies are not required.
    pub fn from_config(
        config: &ValidationConfig,
        terminology_server: Option<&str>,
        version: helios_fhir::FhirVersion,
    ) -> Result<Self, String> {
        let mode = match config.mode.as_str() {
            "log" => ValidationMode::Log,
            "enforce" => ValidationMode::Enforce,
            _ => ValidationMode::Off,
        };
        let unknown_profile = match config.unknown_profile.as_str() {
            "error" => UnknownProfilePolicy::Error,
            "ignore" => UnknownProfilePolicy::Ignore,
            _ => UnknownProfilePolicy::Warn,
        };
        let terminology: Option<Arc<dyn TerminologyProvider>> =
            match (config.terminology.as_str(), terminology_server) {
                ("remote", Some(base)) => Some(Arc::new(RemoteTerminologyProvider::new(
                    base.to_string(),
                    Duration::from_millis(config.terminology_timeout_ms),
                ))),
                // Offline required-binding checks against the FHIR core value
                // sets embedded in helios-fhir-validator (no server needed).
                ("embedded", _) => {
                    let provider: Arc<dyn TerminologyProvider> =
                        helios_fhir_validator::core_terminology(version);
                    Some(provider)
                }
                _ => None,
            };

        let package_layers = load_package_layers(config)?;

        Ok(Self {
            mode,
            constraint_evaluator: config.constraints.then(FhirPathConstraintEvaluator::new),
            terminology,
            suppress_constraints: config.suppress_constraints.clone(),
            terminology_fail_closed: config.terminology_fail == "closed",
            use_meta_profiles: config.meta_profiles,
            unknown_profile,
            tenant_profiles: config.stored_profiles.then(DashMap::new),
            package_layers,
            reference_resolver: None,
        })
    }

    /// Prefetch relative `Type/id` references from `storage` into the
    /// validator's `resolve()` pool. No-op when the caller never attaches this.
    pub fn with_storage_resolve(mut self, storage: Arc<dyn ResourceStorage>) -> Self {
        self.reference_resolver = Some(Arc::new(StorageBackedResolver::new(
            storage,
            StorageBackedResolver::DEFAULT_MAX_FANOUT,
        )));
        self
    }

    /// Replace the terminology provider (bindings stay unchecked without one).
    pub fn with_terminology(mut self, provider: Arc<dyn TerminologyProvider>) -> Self {
        self.terminology = Some(provider);
        self
    }

    /// The configured write-path mode.
    pub fn mode(&self) -> ValidationMode {
        self.mode
    }

    /// Validate a resource against the core pack for `version` (overlaid
    /// with package layers and the tenant's stored profiles) plus any extra
    /// profile canonicals. Structural issues first, then constraint issues,
    /// then binding issues.
    pub async fn validate_resource(
        &self,
        version: FhirVersion,
        resource: &Value,
        profiles: Vec<String>,
        tenant: Option<&str>,
    ) -> Vec<ValidationError> {
        self.validate_resource_with(version, resource, profiles, tenant, self.use_meta_profiles)
            .await
    }

    /// Like [`Self::validate_resource`], with an explicit `meta.profile` switch
    /// (`$validate` `mode=profile` ignores claimed profiles).
    pub async fn validate_resource_with(
        &self,
        version: FhirVersion,
        resource: &Value,
        profiles: Vec<String>,
        tenant: Option<&str>,
        use_meta_profiles: bool,
    ) -> Vec<ValidationError> {
        let resolution_resources = self.prefetch_resolve(version, resource, tenant).await;
        let resolver = self.resolver_for(version, tenant);
        let validator = Validator::new(resolver);
        let opts = ValidationOptions {
            profiles,
            use_meta_profiles,
            unknown_profile: self.unknown_profile,
            slice_binding_version: Some(version),
            resolution_resources,
            ..Default::default()
        };
        let handlers = EffectHandlers {
            constraints: self
                .constraint_evaluator
                .as_ref()
                .map(|e| e as &dyn helios_fhir_validator::ConstraintEvaluator),
            terminology: self.terminology.as_deref(),
            suppress_constraints: &self.suppress_constraints,
            terminology_fail_closed: self.terminology_fail_closed,
            check_extensible_bindings: false,
        };
        validator
            .validate(resource, version, &opts, &handlers)
            .await
    }

    async fn prefetch_resolve(
        &self,
        version: FhirVersion,
        resource: &Value,
        tenant: Option<&str>,
    ) -> Vec<Value> {
        let (Some(resolver), Some(tenant_id)) = (&self.reference_resolver, tenant) else {
            return Vec::new();
        };
        let refs = collect_missing_references(std::slice::from_ref(resource));
        if refs.is_empty() {
            return Vec::new();
        }
        let ctx = TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access());
        resolver.resolve(&ctx, version, &refs).await
    }

    /// Write-path gate. `Ok(())` = proceed with the write; `Err` = reject
    /// (enforce mode with error-severity issues → `422` carrying the full
    /// OperationOutcome).
    pub async fn check_write(
        &self,
        tenant: &str,
        version: FhirVersion,
        resource_type: &str,
        resource: &Value,
    ) -> Result<(), RestError> {
        if self.mode == ValidationMode::Off {
            return Ok(());
        }
        let issues = self
            .validate_resource(version, resource, Vec::new(), Some(tenant))
            .await;
        if issues.is_empty() {
            return Ok(());
        }
        match self.mode {
            ValidationMode::Off => Ok(()),
            ValidationMode::Log => {
                for issue in &issues {
                    warn!(
                        tenant = %tenant,
                        resource_type = %resource_type,
                        path = %issue.path,
                        kind = ?issue.kind,
                        "validation (log mode): {}",
                        issue.message
                    );
                }
                Ok(())
            }
            ValidationMode::Enforce => {
                if has_errors(&issues) {
                    debug!(
                        tenant = %tenant,
                        resource_type = %resource_type,
                        issues = issues.len(),
                        "rejecting write: validation failed"
                    );
                    Err(RestError::ValidationFailed {
                        outcome: validation_outcome(&issues),
                    })
                } else {
                    // Warnings only: proceed, but leave a trace.
                    for issue in &issues {
                        warn!(
                            tenant = %tenant,
                            resource_type = %resource_type,
                            path = %issue.path,
                            "validation warning on write: {}",
                            issue.message
                        );
                    }
                    Ok(())
                }
            }
        }
    }

    /// Fold a stored StructureDefinition into the tenant's profile registry
    /// (called after successful StructureDefinition writes). Conversion
    /// failures are logged, never fatal — the write itself already
    /// succeeded.
    pub fn upsert_stored_profile(&self, tenant: &str, version: FhirVersion, sd: &Value) {
        let Some(registries) = &self.tenant_profiles else {
            return;
        };
        match helios_fhir_validator::converter::convert(sd) {
            Ok(conversion) => {
                for w in &conversion.warnings {
                    warn!(tenant = %tenant, "profile conversion warning: {w}");
                }
                let registry = registries
                    .entry((tenant.to_string(), version))
                    .or_insert_with(|| Arc::new(RwLock::new(SchemaRegistry::new())))
                    .clone();
                let inserted = registry
                    .write()
                    .expect("tenant profile registry lock")
                    .insert(conversion.schema);
                if inserted {
                    debug!(tenant = %tenant, url = ?sd.get("url"), "tenant profile registered");
                } else {
                    warn!(tenant = %tenant, "stored StructureDefinition has neither url nor name; not registered");
                }
            }
            Err(e) => {
                warn!(tenant = %tenant, "stored StructureDefinition failed to convert: {e}");
            }
        }
    }

    fn tenant_overlay(
        &self,
        tenant: Option<&str>,
        version: FhirVersion,
    ) -> Option<Arc<dyn SchemaResolver>> {
        let registries = self.tenant_profiles.as_ref()?;
        let tenant = tenant?;
        let registry = registries.get(&(tenant.to_string(), version))?.clone();
        Some(Arc::new(LockedRegistryResolver(registry)))
    }

    /// `CompositeResolver` layers: tenant overlay, package layers (dependents
    /// before deps), then the embedded core pack.
    fn resolver_for(&self, version: FhirVersion, tenant: Option<&str>) -> Arc<dyn SchemaResolver> {
        let core = helios_fhir_validator::packs::core_registry(version);
        let mut layers: Vec<Arc<dyn SchemaResolver>> = Vec::new();
        if let Some(overlay) = self.tenant_overlay(tenant, version) {
            layers.push(overlay);
        }
        for pkg in &self.package_layers {
            layers.push(Arc::clone(pkg) as Arc<dyn SchemaResolver>);
        }
        if layers.is_empty() {
            return core;
        }
        layers.push(core);
        Arc::new(CompositeResolver::new(layers))
    }
}

fn load_package_layers(config: &ValidationConfig) -> Result<Vec<Arc<SchemaRegistry>>, String> {
    if config.packages.is_empty() {
        return Ok(Vec::new());
    }
    let cache_root = config.package_cache.as_deref().ok_or_else(|| {
        "HFS_FHIR_PACKAGES is set but HFS_FHIR_PACKAGE_CACHE is unset".to_string()
    })?;
    let cache = PackageCache::new(cache_root);
    let mut roots = Vec::with_capacity(config.packages.len());
    for raw in &config.packages {
        roots.push(
            PackageRef::parse(raw).map_err(|e| format!("HFS_FHIR_PACKAGES entry '{raw}': {e}"))?,
        );
    }
    let layers = materialize_package_layers(&cache, &roots)
        .map_err(|e| format!("FHIR package materialization failed: {e}"))?;

    let mut out = Vec::with_capacity(layers.len());
    for (id, registry, report) in layers {
        info!(
            package = %id,
            inserted = report.inserted,
            skipped_abstract = report.skipped_abstract,
            convert_errors = report.convert_errors.len(),
            code_systems = report.code_systems_seen,
            value_sets = report.value_sets_seen,
            "loaded FHIR package schema layer"
        );
        for w in &report.warnings {
            warn!(package = %id, "package materialization warning: {w}");
        }
        for e in &report.convert_errors {
            warn!(package = %id, "package StructureDefinition convert error: {e}");
        }
        out.push(registry);
    }
    Ok(out)
}

/// Resolver adapter over a shared, mutable registry.
struct LockedRegistryResolver(Arc<RwLock<SchemaRegistry>>);

impl SchemaResolver for LockedRegistryResolver {
    fn resolve(&self, reference: &str) -> Option<Arc<helios_fhir_validator::FhirSchema>> {
        self.0
            .read()
            .expect("tenant profile registry lock")
            .resolve(reference)
    }
}

// ---------------------------------------------------------------------
// Remote terminology provider
// ---------------------------------------------------------------------

/// `TerminologyProvider` backed by a FHIR terminology server's
/// `ValueSet/$validate-code`, with a small in-memory TTL cache (neither the
/// fhirpath nor the search terminology clients cache).
pub struct RemoteTerminologyProvider {
    base_url: String,
    client: reqwest::Client,
    /// `(valueSet, coded-token)` → validity, cached for [`CACHE_TTL`].
    cache: DashMap<String, (bool, Instant)>,
}

/// How long `$validate-code` verdicts are cached.
const CACHE_TTL: Duration = Duration::from_secs(300);

impl RemoteTerminologyProvider {
    /// `base_url` is the terminology server root (e.g. `http://hts:8090`).
    pub fn new(base_url: String, timeout: Duration) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("reqwest client builds"),
            cache: DashMap::new(),
        }
    }

    fn payload(value_set: &str, coded: &CodedValue) -> Value {
        let mut parameter = vec![json!({ "name": "url", "valueUri": value_set })];
        match coded {
            CodedValue::Code(code) => {
                parameter.push(json!({ "name": "inferSystem", "valueBoolean": true }));
                parameter.push(json!({ "name": "code", "valueCode": code }));
            }
            CodedValue::Coding(coding) => {
                parameter.push(json!({ "name": "coding", "valueCoding": coding }));
            }
            CodedValue::CodeableConcept(concept) => {
                parameter
                    .push(json!({ "name": "codeableConcept", "valueCodeableConcept": concept }));
            }
        }
        json!({ "resourceType": "Parameters", "parameter": parameter })
    }
}

#[async_trait]
impl TerminologyProvider for RemoteTerminologyProvider {
    async fn validate_code(
        &self,
        value_set: &str,
        coded: &CodedValue,
    ) -> Result<bool, TerminologyError> {
        let key = format!("{value_set}|{coded:?}");
        if let Some(entry) = self.cache.get(&key) {
            let (verdict, at) = *entry;
            if at.elapsed() < CACHE_TTL {
                return Ok(verdict);
            }
        }

        let url = format!("{}/ValueSet/$validate-code", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&Self::payload(value_set, coded))
            .send()
            .await
            .map_err(|e| TerminologyError(format!("request to {url} failed: {e}")))?;
        if !response.status().is_success() {
            return Err(TerminologyError(format!(
                "{url} returned {}",
                response.status()
            )));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| TerminologyError(format!("invalid $validate-code response: {e}")))?;
        let verdict = body
            .get("parameter")
            .and_then(Value::as_array)
            .and_then(|params| {
                params
                    .iter()
                    .find(|p| p.get("name").and_then(Value::as_str) == Some("result"))
            })
            .and_then(|p| p.get("valueBoolean"))
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                TerminologyError("no boolean 'result' parameter in response".to_string())
            })?;

        self.cache.insert(key, (verdict, Instant::now()));
        Ok(verdict)
    }
}

// ---------------------------------------------------------------------
// OperationOutcome mapping
// ---------------------------------------------------------------------

/// Map one validator issue onto an OperationOutcome issue.
pub fn to_outcome_issue(error: &ValidationError) -> Issue {
    let code = match error.kind {
        ErrorKind::Required => IssueType::Required,
        ErrorKind::FixedValue
        | ErrorKind::PatternValue
        | ErrorKind::PrimitiveValue
        | ErrorKind::MaxLength
        | ErrorKind::MinValue
        | ErrorKind::MaxValue
        | ErrorKind::ReferenceTarget => IssueType::Value,
        ErrorKind::FhirpathConstraint | ErrorKind::Questionnaire => IssueType::Invariant,
        // A known extension outside its declared context (#615): invalid
        // content by placement, not a structural malformation.
        ErrorKind::ExtensionContext => IssueType::Invalid,
        ErrorKind::TerminologyBinding => IssueType::CodeInvalid,
        ErrorKind::UnknownSchema | ErrorKind::UnknownProfile => IssueType::NotSupported,
        // Everything structural: unknown-element, shape, cardinality,
        // slicing, choices, wrong container type.
        ErrorKind::UnknownElement
        | ErrorKind::NotArray
        | ErrorKind::NotSingular
        | ErrorKind::Type
        | ErrorKind::Excluded
        | ErrorKind::Min
        | ErrorKind::Max
        | ErrorKind::SliceCardinality
        | ErrorKind::SliceUnmatched
        | ErrorKind::SliceOrder
        | ErrorKind::Choice
        | ErrorKind::ChoiceExcluded => IssueType::Structure,
    };
    let severity = match error.severity {
        Severity::Error => IssueSeverity::Error,
        Severity::Warning => IssueSeverity::Warning,
    };
    Issue::new(severity, code, error.message.clone())
        .with_expression(dotted_to_fhirpath(&error.path))
}

/// Build the `$validate` OperationOutcome: the mapped issues, or the
/// canonical all-clear information issue when there are none.
pub fn validation_outcome(errors: &[ValidationError]) -> Value {
    validation_outcome_from_parts(errors, Vec::new())
}

/// Structural validator issues plus extra OperationOutcome issues (mode
/// checks). All-clear is emitted only when both are empty.
pub fn validation_outcome_from_parts(errors: &[ValidationError], extra: Vec<Issue>) -> Value {
    let mut builder = OperationOutcomeBuilder::new();
    if errors.is_empty() && extra.is_empty() {
        builder = builder.information(IssueType::Informational, "Validation successful");
    } else {
        for error in errors {
            builder = builder.add_issue(to_outcome_issue(error));
        }
        for issue in extra {
            builder = builder.add_issue(issue);
        }
    }
    builder.build()
}

/// Whether any issue is error severity (drives `$validate` reporting and
/// enforce-mode rejection).
pub fn has_errors(errors: &[ValidationError]) -> bool {
    errors.iter().any(|e| e.severity == Severity::Error)
}

#[async_trait]
impl helios_persistence::core::IngestValidator for ValidationService {
    async fn check(
        &self,
        tenant_id: &str,
        fhir_version: FhirVersion,
        resource_type: &str,
        resource: &Value,
    ) -> Result<(), Value> {
        match self
            .check_write(tenant_id, fhir_version, resource_type, resource)
            .await
        {
            Ok(()) => Ok(()),
            Err(RestError::ValidationFailed { outcome }) => Err(outcome),
            Err(e) => Err(json!({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "processing",
                    "diagnostics": e.to_string()
                }]
            })),
        }
    }
}

//! ABDM / NDHM profile validation for the FHIR REST API.
//!
//! NDHM `StructureDefinition` resources ship a merged **snapshot** (HL7 R4 base + national
//! constraints). Validation runs against that snapshot via [`ProfileRegistry`] — no separate
//! HL7 `StructureDefinition/{Type}` entry is required.
//!
//! - **`recurse_on_base_definition`** is **off** for HFS: a second pass on `baseDefinition`
//!   would duplicate snapshot content.
//! - **`HFS_PROFILE_VALIDATION_ADDONS`** (optional): strict unknown JSON keys and cardinality
//!   against a **standalone** HL7 base SD in the registry. Default **off** for full-snapshot
//!   IGs; enable only if you also load HL7 core profiles into the manifest.
//! - **`HFS_TERMINOLOGY_SERVER`**: when set (Helios Terminology Server / HTS), extensible and
//!   required profile bindings call `ValueSet/$validate-code` during `$validate` and write paths.

use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "R4B")]
use fhir_validation::R4BFhirPathEvaluator;
#[cfg(feature = "R4")]
use fhir_validation::R4FhirPathEvaluator;
#[cfg(feature = "R5")]
use fhir_validation::R5FhirPathEvaluator;
#[cfg(feature = "R6")]
use fhir_validation::R6FhirPathEvaluator;
use fhir_validation::issue_to_op_outcome::validation_issues_to_operation_outcome;
use fhir_validation::profile::profile_registry::ProfileRegistry;
use fhir_validation::terminology::service::{RemoteTerminologyService, TerminologyService};
use fhir_validation::{
    FhirPathEvaluator, Severity, ValidationConfig, ValidationIssue, Validator,
    load_profile_registry_from_manifest_file,
};
use helios_fhir::FhirResource;
use helios_fhir::FhirVersion;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::config::{ProfileValidationMode, ServerConfig};
use crate::error::RestError;

/// Shared profile validation state attached to [`crate::state::AppState`].
pub struct ProfileValidationService {
    pub registry: Arc<ProfileRegistry>,
    validator: Validator,
    pub mode: ProfileValidationMode,
    terminology: Option<Arc<RemoteTerminologyService>>,
    validation_addons: bool,
}

impl ProfileValidationService {
    /// Build from server config when `HFS_PROFILE_MANIFEST` is set.
    ///
    /// Returns `None` if the manifest path is unset. Write enforcement still respects
    /// [`ProfileValidationMode::Off`]; [`ProfileValidationMode::Warn`] / [`ProfileValidationMode::Strict`]
    /// apply on create/update paths. `$validate` is available whenever this service is loaded.
    pub fn try_from_config(config: &ServerConfig) -> Result<Option<Arc<Self>>, String> {
        let Some(ref manifest) = config.profile_manifest else {
            if config.profile_validation_mode != ProfileValidationMode::Off {
                warn!(
                    mode = ?config.profile_validation_mode,
                    "HFS_PROFILE_VALIDATION_MODE is active but HFS_PROFILE_MANIFEST is unset"
                );
            }
            return Ok(None);
        };

        let registry = load_profile_registry_from_manifest_file(Path::new(manifest))
            .map_err(|e| format!("load profile manifest {}: {e}", manifest.display()))?;

        if registry.is_empty() {
            return Err(format!(
                "profile manifest {} loaded zero StructureDefinitions",
                manifest.display()
            ));
        }

        let terminology_url = config.terminology_server.clone();
        info!(
            manifest = %manifest.display(),
            profile_count = registry.len(),
            mode = ?config.profile_validation_mode,
            terminology_server = terminology_url.as_deref().unwrap_or("(none)"),
            "Loaded NDHM/ABDM profile manifest"
        );

        let mut validation_config = ValidationConfig::default();
        validation_config.enable_base_definition_url_lookup = false;
        // NDHM/ABDM profiles include merged snapshots; base rules are already extracted.
        validation_config.recurse_on_base_definition = false;
        validation_config.strict_extensible_bindings = false;

        let terminology = terminology_url.as_ref().map(|url| {
            Arc::new(RemoteTerminologyService::new(
                url.clone(),
                config.default_fhir_version,
            ))
        });

        Ok(Some(Arc::new(Self {
            registry: Arc::new(registry),
            validator: Validator::new(validation_config),
            mode: config.profile_validation_mode,
            terminology,
            validation_addons: config.profile_validation_addons,
        })))
    }

    pub fn profile_count(&self) -> usize {
        self.registry.len()
    }

    pub fn has_terminology_server(&self) -> bool {
        self.terminology.is_some()
    }

    pub fn parse_resource(
        &self,
        json: &Value,
        fhir_version: FhirVersion,
    ) -> Result<FhirResource, RestError> {
        parse_fhir_resource_json(json, fhir_version)
    }

    /// Validation of declared `meta.profile` URLs (manifest / IG), with optional HTS.
    ///
    /// Runs async terminology lookups on the current Tokio runtime. On the multi-threaded runtime
    /// (production Axum), uses `block_in_place` so handlers stay `Send`. On current-thread runtimes
    /// (some tests), uses `Handle::block_on` directly.
    pub fn validate_resource_sync(&self, resource: &FhirResource) -> Vec<ValidationIssue> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| {
                    handle.block_on(self.validate_resource_async(resource))
                })
            }
            Ok(handle) => {
                // Current-thread runtimes (some unit tests): cannot block_on from an async task.
                // Production servers use the multi-threaded runtime branch above.
                if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                    panic!(
                        "profile validation with remote terminology requires a multi-threaded Tokio runtime; use #[tokio::test(flavor = \"multi_thread\")] in tests"
                    );
                }
                handle.block_on(self.validate_resource_async(resource))
            }
            Err(_) => tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime for profile validation")
                .block_on(self.validate_resource_async(resource)),
        }
    }

    async fn validate_resource_async(&self, resource: &FhirResource) -> Vec<ValidationIssue> {
        let evaluator = evaluator_for_resource(resource);
        let terminology = self
            .terminology
            .as_deref()
            .map(|t| t as &dyn TerminologyService);

        if self.validation_addons {
            self.validator
                .validate_manifest_profiles_with_addons_async(
                    resource,
                    terminology,
                    evaluator.as_ref(),
                    self.registry.as_ref(),
                )
                .await
        } else {
            self.validator
                .validate_manifest_profiles_async(
                    resource,
                    terminology,
                    evaluator.as_ref(),
                    self.registry.as_ref(),
                )
                .await
        }
    }

    /// Write-path policy: `warn` logs and persists; `strict` rejects on error-level issues.
    pub fn enforce_on_write(
        &self,
        json: &Value,
        fhir_version: FhirVersion,
        resource_type: &str,
    ) -> Result<(), RestError> {
        if self.mode == ProfileValidationMode::Off {
            return Ok(());
        }
        let resource = self.parse_resource(json, fhir_version)?;
        let issues = self.validate_resource_sync(&resource);
        Self::apply_write_policy(self.mode, resource_type, &issues)
    }

    pub fn apply_write_policy(
        mode: ProfileValidationMode,
        resource_type: &str,
        issues: &[ValidationIssue],
    ) -> Result<(), RestError> {
        if issues.is_empty() {
            return Ok(());
        }

        let blocking = issues
            .iter()
            .filter(|i| matches!(i.severity, Severity::Fatal | Severity::Error))
            .count();

        match mode {
            ProfileValidationMode::Warn => {
                if blocking > 0 {
                    warn!(
                        resource_type,
                        error_count = blocking,
                        total_issues = issues.len(),
                        "Profile validation errors on write (warn mode — persisting)"
                    );
                } else {
                    debug!(
                        resource_type,
                        issue_count = issues.len(),
                        "Profile validation warnings on write (persisting)"
                    );
                }
                Ok(())
            }
            ProfileValidationMode::Strict if blocking > 0 => Err(RestError::ValidationOutcome {
                outcome: validation_issues_to_operation_outcome(issues),
            }),
            ProfileValidationMode::Strict => {
                debug!(
                    resource_type,
                    issue_count = issues.len(),
                    "Profile validation warnings only on write (persisting)"
                );
                Ok(())
            }
            ProfileValidationMode::Off => Ok(()),
        }
    }

    pub fn validate_to_outcome(
        &self,
        json: &Value,
        fhir_version: FhirVersion,
    ) -> Result<Value, RestError> {
        let resource = self.parse_resource(json, fhir_version)?;
        let issues = self.validate_resource_sync(&resource);
        Ok(validation_issues_to_operation_outcome(&issues))
    }
}

/// Extract a resource JSON body from a `$validate` request (raw resource or Parameters).
pub fn extract_resource_from_validate_body(body: &Value) -> Result<&Value, RestError> {
    let rt = body
        .get("resourceType")
        .and_then(|v| v.as_str())
        .ok_or_else(|| RestError::BadRequest {
            message: "Request body must be a FHIR resource or Parameters".to_string(),
        })?;

    if rt == "Parameters" {
        let params = body
            .get("parameter")
            .and_then(|p| p.as_array())
            .ok_or_else(|| RestError::BadRequest {
                message: "Parameters resource must include parameter array".to_string(),
            })?;
        for param in params {
            if param.get("name").and_then(|n| n.as_str()) == Some("resource")
                && let Some(res) = param.get("resource")
            {
                return Ok(res);
            }
        }
        return Err(RestError::BadRequest {
            message: "Parameters must include a resource parameter for $validate".to_string(),
        });
    }

    Ok(body)
}

fn parse_fhir_resource_json(json: &Value, version: FhirVersion) -> Result<FhirResource, RestError> {
    match version {
        #[cfg(feature = "R4")]
        FhirVersion::R4 => {
            let resource: helios_fhir::r4::Resource = serde_json::from_value(json.clone())
                .map_err(|e| RestError::BadRequest {
                    message: format!("Invalid R4 resource: {e}"),
                })?;
            Ok(FhirResource::R4(Box::new(resource)))
        }
        #[cfg(feature = "R4B")]
        FhirVersion::R4B => {
            let resource: helios_fhir::r4b::Resource = serde_json::from_value(json.clone())
                .map_err(|e| RestError::BadRequest {
                    message: format!("Invalid R4B resource: {e}"),
                })?;
            Ok(FhirResource::R4B(Box::new(resource)))
        }
        #[cfg(feature = "R5")]
        FhirVersion::R5 => {
            let resource: helios_fhir::r5::Resource = serde_json::from_value(json.clone())
                .map_err(|e| RestError::BadRequest {
                    message: format!("Invalid R5 resource: {e}"),
                })?;
            Ok(FhirResource::R5(Box::new(resource)))
        }
        #[cfg(feature = "R6")]
        FhirVersion::R6 => {
            let resource: helios_fhir::r6::Resource = serde_json::from_value(json.clone())
                .map_err(|e| RestError::BadRequest {
                    message: format!("Invalid R6 resource: {e}"),
                })?;
            Ok(FhirResource::R6(Box::new(resource)))
        }
        #[allow(unreachable_patterns)]
        _ => Err(RestError::NotAcceptable {
            message: format!("FHIR version {version:?} is not enabled in this build"),
        }),
    }
}

fn evaluator_for_resource(resource: &FhirResource) -> Box<dyn FhirPathEvaluator> {
    match resource {
        #[cfg(feature = "R4")]
        FhirResource::R4(r) => Box::new(R4FhirPathEvaluator::new((**r).clone())),
        #[cfg(feature = "R4B")]
        FhirResource::R4B(r) => Box::new(R4BFhirPathEvaluator::new((**r).clone())),
        #[cfg(feature = "R5")]
        FhirResource::R5(r) => Box::new(R5FhirPathEvaluator::new((**r).clone())),
        #[cfg(feature = "R6")]
        FhirResource::R6(r) => Box::new(R6FhirPathEvaluator::new((**r).clone())),
        #[allow(unreachable_patterns)]
        _ => panic!("FHIR version not enabled in this build"),
    }
}

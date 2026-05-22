//! NDHM Patient profile validation with **live** Helios Terminology Server (HTS) lookups.
//!
//! Uses example patients under [`fixtures/r4/examples/`](../../fixtures/r4/examples/) (passing and
//! failing NDHM/R4 cases) and the NDHM R4 Patient
//! [`StructureDefinition-Patient.json`](../../fixtures/r4/profiles/StructureDefinition-Patient.json)
//! fixture. Profile URL, ValueSet binding URL, and instance codings are taken from the SD and JSON
//! fixtures — not duplicated as test constants (only the HTS base URL default remains).
//!
//! ```text
//! # Requires a running HTS (default http://localhost:9091; override with HTS_TERMINOLOGY_BASE_URL
//! or FHIR_TERMINOLOGY_BASE_URL). Tests fail fast via GET /health if the server is down.
//! cargo test -p fhir-validation --features R4 ndhm_patient_hts
//! ```

mod tests {
    use crate::common::fixtures::{
        load_fixture, load_profile, local_terminology_r4, r4_evaluator_for,
    };
    use fhir_validation::ValidationIssueDetailCode;
    use fhir_validation::profile::profile_registry::ProfileRegistry;
    use fhir_validation::profile::types::ExtractedProfile;
    use fhir_validation::profile::validate::validate_profile;
    use fhir_validation::profile::validate::validate_profile_async;
    use fhir_validation::terminology::service::{
        RemoteTerminologyService, TerminologyService, TerminologyServiceSync,
    };
    use fhir_validation::validation_context::{
        AsyncValidationContext, ValidationContext, ValidationState,
    };
    use fhir_validation::{Severity, Validator};
    use fhir_validation_types::BindingDef;
    use helios_fhir::FhirResource;
    use helios_fhir::FhirVersion;
    use reqwest::Client;
    use std::time::Duration;

    fn hts_base_url_from_env() -> String {
        std::env::var("HTS_TERMINOLOGY_BASE_URL")
            .or_else(|_| std::env::var("FHIR_TERMINOLOGY_BASE_URL"))
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_owned())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "http://localhost:9091".to_string())
    }

    fn remote_terminology_for_hts_tests() -> RemoteTerminologyService {
        let base = hts_base_url_from_env();
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(12))
            .build()
            .expect("reqwest client");
        RemoteTerminologyService::with_client(client, base, FhirVersion::R4)
    }

    async fn assert_hts_reachable() {
        let base = hts_base_url_from_env();
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client");
        let url = format!("{base}/health");
        let status = client
            .get(&url)
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {url}: {e}"))
            .status();
        assert!(
            status.is_success(),
            "expected HTS health at {url}, got HTTP {status}"
        );
    }

    fn ndhm_patient_profile() -> ExtractedProfile {
        load_profile(FhirVersion::R4, "profiles/StructureDefinition-Patient.json")
    }

    fn ndhm_identifier_type_binding(profile: &ExtractedProfile) -> &BindingDef {
        profile
            .element_rules
            .iter()
            .find(|r| r.path == "Patient.identifier.type")
            .and_then(|r| r.binding.as_ref())
            .expect("NDHM Patient SD must declare Patient.identifier.type binding")
    }

    fn load_ndhm_example(rel: &str) -> serde_json::Value {
        serde_json::from_str(&load_fixture(FhirVersion::R4, rel))
            .unwrap_or_else(|e| panic!("failed to parse fixture examples/{rel}: {e}"))
    }

    fn declared_profile_url(patient: &serde_json::Value) -> &str {
        patient["meta"]["profile"]
            .as_array()
            .and_then(|p| p.first())
            .and_then(|v| v.as_str())
            .expect("fixture must declare meta.profile[0]")
    }

    fn first_identifier_type_coding(patient: &serde_json::Value) -> (&str, &str) {
        let coding = &patient["identifier"][0]["type"]["coding"][0];
        let system = coding["system"]
            .as_str()
            .expect("identifier.type.coding[0].system");
        let code = coding["code"]
            .as_str()
            .expect("identifier.type.coding[0].code");
        (system, code)
    }

    fn ndhm_patient_registry(profile: &ExtractedProfile) -> ProfileRegistry {
        let mut registry = ProfileRegistry::new();
        registry.insert(profile.clone());
        registry
    }

    fn validator_for_ndhm_profile() -> Validator {
        let mut validator = Validator::default();
        validator.config.recurse_on_base_definition = false;
        validator.config.enable_base_definition_url_lookup = false;
        validator.config.strict_extensible_bindings = true;
        validator
    }

    struct NdhmProfileValidationHarness {
        registry: ProfileRegistry,
        evaluator: fhir_validation::R4FhirPathEvaluator,
        validator: Validator,
    }

    fn ndhm_profile_validation_harness(
        patient: &serde_json::Value,
        profile: &ExtractedProfile,
    ) -> NdhmProfileValidationHarness {
        let registry = ndhm_patient_registry(profile);
        let resource = patient_resource_from_json(patient.clone());
        let evaluator = r4_evaluator_for(&resource);
        let validator = validator_for_ndhm_profile();
        NdhmProfileValidationHarness {
            registry,
            evaluator,
            validator,
        }
    }

    fn validate_ndhm_patient_profile_sync(
        patient: &serde_json::Value,
        profile: &ExtractedProfile,
        term: Option<&dyn TerminologyServiceSync>,
    ) -> Vec<fhir_validation::ValidationIssue> {
        let h = ndhm_profile_validation_harness(patient, profile);
        let map = h.registry.as_map();
        let ctx = ValidationContext {
            fhir_version: FhirVersion::R4,
            validator: &h.validator,
            terminology: term,
            evaluator: &h.evaluator,
            runtime_profile_registry: Some(&h.registry),
            extracted_profile_map: map,
        };
        let mut state = ValidationState::default();
        validate_profile(&ctx, &mut state, patient, "Patient", profile)
    }

    async fn validate_ndhm_patient_profile_async(
        patient: &serde_json::Value,
        profile: &ExtractedProfile,
        term: Option<&dyn TerminologyService>,
    ) -> Vec<fhir_validation::ValidationIssue> {
        let h = ndhm_profile_validation_harness(patient, profile);
        let map = h.registry.as_map();
        let ctx = AsyncValidationContext {
            fhir_version: FhirVersion::R4,
            validator: &h.validator,
            terminology: term,
            evaluator: &h.evaluator,
            runtime_profile_registry: Some(&h.registry),
            extracted_profile_map: map,
        };
        let mut state = ValidationState::default();
        validate_profile_async(&ctx, &mut state, patient, "Patient", profile).await
    }

    fn patient_resource_from_json(patient: serde_json::Value) -> FhirResource {
        let patient_r4: helios_fhir::r4::Patient =
            serde_json::from_value(patient).expect("Patient JSON");
        FhirResource::R4(Box::new(helios_fhir::r4::Resource::Patient(Box::new(
            patient_r4,
        ))))
    }

    fn ndhm_identifier_type_binding_misses<'a>(
        issues: &'a [fhir_validation::ValidationIssue],
        value_set: &str,
    ) -> Vec<&'a fhir_validation::ValidationIssue> {
        issues
            .iter()
            .filter(|i| {
                i.expression.as_deref() == Some(value_set)
                    && i.detail_code == Some(ValidationIssueDetailCode::ExtensibleBindingMiss)
                    && i.fhir_path.contains("identifier")
                    && i.fhir_path.contains("type")
            })
            .collect()
    }

    fn has_required_element_missing(
        issues: &[fhir_validation::ValidationIssue],
        path: &str,
    ) -> bool {
        issues.iter().any(|i| {
            i.detail_code == Some(ValidationIssueDetailCode::RequiredElementMissing)
                && i.fhir_path.contains(path)
        })
    }

    fn has_required_binding_miss(issues: &[fhir_validation::ValidationIssue], path: &str) -> bool {
        issues.iter().any(|i| {
            i.detail_code == Some(ValidationIssueDetailCode::RequiredBindingMiss)
                && i.fhir_path.contains(path)
        })
    }

    /// HTS accepts the ADN code from the passing example patient against the profile's ValueSet binding.
    #[tokio::test]
    async fn hts_member_of_adn_in_ndhm_identifier_type_valueset() {
        assert_hts_reachable().await;
        let profile = ndhm_patient_profile();
        let binding = ndhm_identifier_type_binding(&profile);
        let patient = load_ndhm_example("examples/ndhm-richer-patient.json");
        assert_eq!(declared_profile_url(&patient), profile.url);
        let (system, code) = first_identifier_type_coding(&patient);

        let term = remote_terminology_for_hts_tests();
        let outcome = term
            .member_of(binding.value_set.as_str(), Some(system), code, None)
            .await
            .expect("HTS member_of for fixture coding");
        assert!(
            outcome.is_member,
            "expected {code} in {}, got {outcome:?}",
            binding.value_set
        );
    }

    /// Richer NDHM Patient: extensible `identifier.type` binding validated via HTS — no terminology issue.
    #[tokio::test]
    async fn ndhm_richer_patient_identifier_type_passes_with_hts() {
        assert_hts_reachable().await;
        let profile = ndhm_patient_profile();
        let binding = ndhm_identifier_type_binding(&profile);
        let patient = load_ndhm_example("examples/ndhm-richer-patient.json");
        let term = remote_terminology_for_hts_tests();
        let issues = validate_ndhm_patient_profile_async(&patient, &profile, Some(&term)).await;

        let binding_misses = ndhm_identifier_type_binding_misses(&issues, &binding.value_set);
        assert!(
            binding_misses.is_empty(),
            "fixture coding should validate against HTS; got: {binding_misses:#?}; all issues: {issues:#?}"
        );
    }

    /// Unknown identifier-type code is rejected by HTS and NDHM profile binding validation.
    #[tokio::test]
    async fn ndhm_patient_bad_identifier_type_code_fails_profile_with_hts() {
        assert_hts_reachable().await;
        let profile = ndhm_patient_profile();
        let binding = ndhm_identifier_type_binding(&profile);
        let patient = load_ndhm_example("examples/ndhm-patient-bad-identifier-type-code.json");
        let (system, code) = first_identifier_type_coding(&patient);

        let term = remote_terminology_for_hts_tests();
        let outcome = term
            .member_of(binding.value_set.as_str(), Some(system), code, None)
            .await
            .expect("member_of");
        assert!(
            !outcome.is_member,
            "HTS should reject {code} in {}, got {outcome:?}",
            binding.value_set
        );

        let issues = validate_ndhm_patient_profile_async(&patient, &profile, Some(&term)).await;
        let binding_misses = ndhm_identifier_type_binding_misses(&issues, &binding.value_set);
        assert!(
            !binding_misses.is_empty(),
            "expected extensible binding miss for {code} on Patient.identifier.type, got: {issues:#?}"
        );
    }

    /// NDHM Patient requires at least one `identifier` (min 1..* in the profile snapshot).
    #[tokio::test]
    async fn ndhm_patient_missing_identifier_fails_profile_cardinality() {
        let profile = ndhm_patient_profile();
        let patient = load_ndhm_example("examples/ndhm-patient-missing-identifier.json");
        assert_eq!(declared_profile_url(&patient), profile.url);

        let issues = validate_ndhm_patient_profile_sync(&patient, &profile, None);
        assert!(
            has_required_element_missing(&issues, "identifier"),
            "expected required-element-missing for Patient.identifier, got: {issues:#?}"
        );
    }

    /// NDHM snapshot requires `name.text` when `name` is present.
    #[test]
    fn ndhm_patient_missing_name_text_fails_profile_cardinality() {
        let profile = ndhm_patient_profile();
        let patient = load_ndhm_example("examples/ndhm-patient-missing-name-text.json");
        let issues = validate_ndhm_patient_profile_sync(&patient, &profile, None);
        assert!(
            has_required_element_missing(&issues, "name"),
            "expected required-element-missing for Patient.name.text, got: {issues:#?}"
        );
    }

    /// Invalid `gender` violates the required HL7 administrative-gender binding in the NDHM snapshot.
    #[test]
    fn ndhm_patient_invalid_gender_fails_required_binding() {
        let profile = ndhm_patient_profile();
        let patient = load_ndhm_example("examples/ndhm-patient-invalid-gender.json");
        let term = local_terminology_r4();
        let issues = validate_ndhm_patient_profile_sync(&patient, &profile, Some(&term));
        assert!(
            has_required_binding_miss(&issues, "gender"),
            "expected required-binding-miss for Patient.gender, got: {issues:#?}"
        );
    }

    /// Plain R4 Patient (no declared profile): invalid `gender` fails base resource validation.
    #[test]
    fn patient_invalid_r4_gender_fails_base_validation() {
        let json = load_ndhm_example("examples/patient-invalid-r4-gender.json");
        assert!(json.get("meta").and_then(|m| m.get("profile")).is_none());
        let resource = patient_resource_from_json(json);
        let validator = Validator::default();
        let evaluator = r4_evaluator_for(&resource);
        let term = local_terminology_r4();
        let issues = validator.validate_resource(&resource, Some(&term), &evaluator);
        assert!(
            issues.iter().any(|i| {
                i.severity == Severity::Error
                    && i.detail_code == Some(ValidationIssueDetailCode::RequiredBindingMiss)
                    && i.fhir_path.contains("gender")
            }),
            "expected required-binding-miss on Patient.gender, got: {issues:#?}"
        );
    }
}

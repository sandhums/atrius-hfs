//! Regression tests for nested extension slice validation (schedule recurrence).

use fhir_validation::Severity;
use fhir_validation::ValidationContext;
use fhir_validation::Validator;
use fhir_validation::profile::extract::extract_r4_structure_definition_profile;
use fhir_validation::profile::validate::validate_profile;
use fhir_validation::validation_context::ValidationState;
use helios_fhir::FhirVersion;
use helios_fhir::r4::StructureDefinition;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;

#[test]
fn schedule_recurrence_nested_extension_slices_validate_cleanly() {
    let sd_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../AtriusIGDraft/output/StructureDefinition-atrius-in-schedule-recurrence.json");
    if !sd_path.exists() {
        eprintln!(
            "skip: recurrence profile not found at {}",
            sd_path.display()
        );
        return;
    }

    let sd_json = std::fs::read_to_string(&sd_path).expect("read recurrence SD");
    let sd: StructureDefinition = serde_json::from_str(&sd_json).expect("parse recurrence SD");
    let profile = extract_r4_structure_definition_profile(&sd).expect("extract recurrence profile");

    let recurrence_ext = json!({
        "url": "https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-schedule-recurrence",
        "extension": [
            { "url": "RRULE", "valueString": "FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=9;BYMINUTE=0;BYSECOND=0" },
            { "url": "TZID", "valueString": "Asia/Kolkata" }
        ]
    });

    let validator = Validator::default();
    let extracted_profile_map = HashMap::new();
    let ctx = ValidationContext {
        fhir_version: FhirVersion::R4,
        validator: &validator,
        evaluator: &fhir_validation::R4FhirPathEvaluator::new(
            helios_fhir::r4::Resource::StructureDefinition(Box::new(sd)),
        ),
        runtime_profile_registry: None,
        terminology: None,
        extracted_profile_map: &extracted_profile_map,
    };

    let mut state = ValidationState::default();
    let issues = validate_profile(&ctx, &mut state, &recurrence_ext, "Extension", &profile);

    let errors: Vec<_> = issues
        .iter()
        .filter(|issue| issue.severity == Severity::Error)
        .collect();
    assert!(
        errors.is_empty(),
        "valid recurrence extension should not produce errors: {errors:#?}"
    );
}

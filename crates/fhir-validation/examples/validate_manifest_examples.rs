//! Validate FHIR example instances against profiles loaded from an Atrius manifest.
//!
//! ```text
//! cargo run -p fhir-validation --example validate_manifest_examples -- \
//!   manifests/atrius-r4-profile-manifest-core.json \
//!   /path/to/Schedule-atrius-in-schedule-opd-example.json \
//!   /path/to/Slot-atrius-in-slot-opd-example-0930.json
//! ```

use fhir_validation::{load_profile_registry_from_manifest_file, Severity, Validator};
use helios_fhir::FhirResource;
use std::path::Path;

fn main() {
    let mut args = std::env::args_os().skip(1);
    let manifest = args
        .next()
        .expect("usage: validate_manifest_examples <manifest.json> <example.json> [...]");
    let example_paths: Vec<_> = args.collect();
    if example_paths.is_empty() {
        eprintln!("usage: validate_manifest_examples <manifest.json> <example.json> [...]");
        std::process::exit(2);
    }

    let registry = load_profile_registry_from_manifest_file(Path::new(&manifest))
        .unwrap_or_else(|e| {
            eprintln!("failed to load manifest: {e}");
            std::process::exit(1);
        });

    let mut validator = Validator::default();
    validator.config.recurse_on_base_definition = false;
    validator.config.enable_base_definition_url_lookup = false;
    validator.config.strict_extensible_bindings = false;

    let mut failed = false;
    for path_os in example_paths {
        let path = Path::new(&path_os);
        let json = std::fs::read_to_string(path).unwrap_or_else(|e| {
            eprintln!("failed to read {}: {e}", path.display());
            std::process::exit(1);
        });
        let resource: helios_fhir::r4::Resource = serde_json::from_str(&json).unwrap_or_else(|e| {
            eprintln!("failed to parse {}: {e}", path.display());
            std::process::exit(1);
        });
        let evaluator = fhir_validation::R4FhirPathEvaluator::new(resource.clone());
        let resource_type = match &resource {
            helios_fhir::r4::Resource::Schedule(_) => "Schedule",
            helios_fhir::r4::Resource::Slot(_) => "Slot",
            helios_fhir::r4::Resource::Appointment(_) => "Appointment",
            other => {
                eprintln!(
                    "unsupported resource type in {}: {:?}",
                    path.display(),
                    std::mem::discriminant(other)
                );
                std::process::exit(1);
            }
        };

        let issues = validator.validate_resource_with_profiles(
            &FhirResource::R4(Box::new(resource)),
            None,
            &evaluator,
            &registry,
        );

        let errors: Vec<_> = issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .collect();

        if errors.is_empty() {
            eprintln!("OK {resource_type} {}", path.display());
        } else {
            failed = true;
            eprintln!("FAIL {resource_type} {} ({} errors)", path.display(), errors.len());
            for issue in errors {
                eprintln!("  - {}: {}", issue.fhir_path, issue.diagnostics);
            }
        }
    }

    if failed {
        std::process::exit(1);
    }
}

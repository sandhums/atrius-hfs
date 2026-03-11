use std::fs;
use std::path::PathBuf;

pub fn read_shared_r4_fixture(name: &str) -> String {
    let path = workspace_root()
        .join("crates")
        .join("fhirpath")
        .join("tests")
        .join("data")
        .join("r4")
        .join("input")
        .join(name);

    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture '{}': {e}", path.display()))
}

pub fn load_r4_resource(name: &str) -> helios_fhir::r4::Resource {
    let json = read_shared_r4_fixture(name);
    serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("failed to parse R4 resource fixture '{name}': {e}"))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .unwrap()
        .to_path_buf()
}
pub fn load_r4_patient(name: &str) -> helios_fhir::r4::Patient {
    match load_r4_resource(name) {
        helios_fhir::r4::Resource::Patient(p) => *p,
        other => panic!("expected Patient, got {:?}", other),
    }
}

pub fn load_r4_observation(name: &str) -> helios_fhir::r4::Observation {
    match load_r4_resource(name) {
        helios_fhir::r4::Resource::Observation(o) => *o,
        other => panic!("expected Observation, got {:?}", other),
    }
}
pub fn validator() -> fhir_validation::Validator {
    fhir_validation::Validator::default()
}
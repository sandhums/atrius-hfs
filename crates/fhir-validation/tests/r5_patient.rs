mod common {
    pub mod fixtures;
}
use crate::common::fixtures::{
    assert_has_invariant, assert_issue_count, load_resource, validate_resource,
};
use helios_fhir::FhirVersion;

#[test]
fn r5_patient_example_validates() {
    let r = load_resource(FhirVersion::R5, "valid/patient/patient-example.json");

    let _issues = validate_resource(&r, None);

    // assert!(issues.is_empty());
    // assert_no_errors(&issues);
}
#[test]
fn r5_patient_local_reference_but_no_contained() {
    let r = load_resource(
        FhirVersion::R5,
        "invalid/patient/patient-local_reference_but_no_contained.json",
    );

    let issues = validate_resource(&r, None);

    assert_has_invariant(
        &issues,
        "Patient.managingOrganization",
        "contained resource",
    );
}
#[test]
fn r5_dom3_no_id_in_contained() {
    let r = load_resource(
        FhirVersion::R5,
        "invalid/patient/patient_no_id_in_contained.json",
    );

    let issues = validate_resource(&r, None);

    assert_has_invariant(
        &issues,
        "Patient.managingOrganization",
        "contained resource",
    );
    assert_has_invariant(
        &issues,
        "Patient.contained[0]",
        "The organization SHALL at least have a name or an identifier, and possibly more than one",
    );
}
#[test]
fn r5_patient_malformed_reference() {
    let r = load_resource(
        FhirVersion::R5,
        "invalid/patient/patient_malformed_local_reference.json",
    );
    let issues = validate_resource(&r, None);
    assert_issue_count(&issues, 3);
    assert_has_invariant(
        &issues,
        "Patient.contained[0]",
        "The organization SHALL at least have a name or an identifier, and possibly more than one",
    );
    assert_has_invariant(
        &issues,
        "Patient",
        "If the resource is contained in another resource, it SHALL be referred to from elsewhere in the resource or SHALL refer to the containing resource",
    );
}

use fhir_validation::r5::R5Validatable;
use fhir_validation::R5FhirPathEvaluator;
use crate::common::fixtures::{assert_has_binding_issue, eval_r4_patient_expr, eval_r5_patient_expr, load_r5_patient, load_resource, validate_resource, validator};
use helios_fhir::FhirVersion;

mod common {
    pub mod fixtures;
}
#[test]
fn r5_patient_invalid_identifier() {
    let resource = load_resource(
        FhirVersion::R5,
        "invalid/patient/patient-bindings.json",
    );
    // println!("{:#?}", resource);
    let issues = validate_resource(&resource, None);
    println!("{:#?}", issues);
    assert_has_binding_issue(&issues, "Patient.identifier[0].type", "http://hl7.org/fhir/ValueSet/identifier-type");
    assert_has_binding_issue(
        &issues,
        "Patient.identifier[0].use",
         "http://hl7.org/fhir/ValueSet/identifier-use|5.0.0"
    );
    // assert_has_binding_issue(&issues, "Patient.name[0].use", "http://hl7.org/fhir/ValueSet/name-use|5.0.0");
    assert_has_binding_issue(
        &issues,
        "Patient.meta.security[0]",
        "http://hl7.org/fhir/ValueSet/security-labels"
    );
    // assert_has_error(&issues);
}
#[ignore = "This is a debug test to see what the bindings look like"]
#[test]
fn r5_debug() {
    let patient = load_r5_patient(

        "invalid/patient/patient-empty_name_ele1.json",
    );
        let exprs = [
            "name[0].hasValue()",
            "name[0].children().count()",
            "name[0].id.count()",
            "name[0].hasValue() or (name[0].children().count() > name[0].id.count())",
        ];

        for expr in exprs {
            let result = eval_r5_patient_expr(&patient, expr);
            println!("\nEXPR: {expr}\nRESULT: {result:#?}");
        }
}
#[ignore = "This is a debug test to see what the bindings look like"]
#[test]
fn r5_debug_2() {
    let patient = load_r5_patient(

        "invalid/patient/patient-empty-meta-security-code.json",
    );
    let exprs = [
        "meta.security[0].hasValue()",
        "meta.security[0].children()",
        "meta.security[0].children().count()",
        "meta.security[0].id.count()",
        "meta.security[0].hasValue() or (meta.security[0].children().count() > meta.security[0].id.count())",
    ];

    for expr in exprs {
        let result = eval_r5_patient_expr(&patient, expr);
        println!("\nEXPR: {expr}\nRESULT: {result:#?}");
    }
}


mod common {
    pub mod fixtures;
}
#[cfg(test)]
mod tests {
    use crate::common::fixtures::{assert_has_binding_issue, eval_r5_patient_expr, load_r5_patient, load_resource, validate_resource, validate_resource_async};
    use fhir_validation::{RemoteTerminologyService, TerminologyMembershipOutcome, TerminologyServiceSync, ValidationError};
    use helios_fhir::FhirVersion;
    use reqwest::Client;
    use std::time::Duration;

    struct MockTerminologyService {
        result: Result<TerminologyMembershipOutcome, ValidationError>,
    }

    impl TerminologyServiceSync for MockTerminologyService {
        fn member_of(
            &self,
            _valueset_url: &str,
            _system: Option<&str>,
            _code: &str,
            _display: Option<&str>,
        ) -> Result<TerminologyMembershipOutcome, ValidationError> {
            match &self.result {
                Ok(v) => Ok(v.clone()),
                Err(ValidationError::Terminology(msg)) => {
                    Err(ValidationError::Terminology(msg.clone()))
                }
                Err(ValidationError::Other(msg)) => Err(ValidationError::Other(msg.clone())),
                Err(ValidationError::FhirPath(_)) => Err(ValidationError::Other(
                    "unexpected fhirpath error in mock".to_string(),
                )),
                Err(ValidationError::TerminologyRemote(_)) => Err(ValidationError::Other(
                    "unexpected error in mock".to_string(),
                )),
            }
        }
    }
    #[ignore]
    #[tokio::test]
    async fn r5_patient_invalid_identifier() {
        let resource = load_resource(FhirVersion::R5, "invalid/patient/patient-bindings.json");
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(8))
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(8)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");

        let terminology = RemoteTerminologyService::with_client(
            client,
            "http://localhost:8080/fhir".to_string(),
            FhirVersion::R5,
        );

        let issues = validate_resource_async(&resource, Some(&terminology)).await;
        // println!("{:#?}", resource);
        // let issues = validate_resource(&resource, None);
        // println!("{:#?}", issues);
        assert_has_binding_issue(
            &issues,
            "Patient.identifier[0].type",
            "http://hl7.org/fhir/ValueSet/identifier-type",
        );
        assert_has_binding_issue(
            &issues,
            "Patient.identifier[0].use",
            "http://hl7.org/fhir/ValueSet/identifier-use|5.0.0",
        );
        // assert_has_binding_issue(&issues, "Patient.name[0].use", "http://hl7.org/fhir/ValueSet/name-use|5.0.0");
        // assert_has_binding_issue(
        //     &issues,
        //     "Patient.meta.security[0]",
        //     "http://hl7.org/fhir/ValueSet/security-labels"
        // );
        // assert_has_error(&issues);
    }
    #[ignore = "This is a debug test to see what the bindings look like"]
    #[test]
    fn r5_debug() {
        let patient = load_r5_patient("invalid/patient/patient-empty_name_ele1.json");
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
        let patient = load_r5_patient("invalid/patient/patient-empty-meta-security-code.json");
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
    #[ignore = "This is a debug test to see what the bindings look like"]
    #[tokio::test]
    async fn r5_sd_async() {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(8))
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(8)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");

        let terminology = RemoteTerminologyService::with_client(
            client,
            "http://localhost:8080/fhir".to_string(),
            FhirVersion::R5,
        );

        let resource = load_resource(FhirVersion::R5, "valid/structuredefinition-language.json");
        let issues = validate_resource_async(&resource, Some(&terminology)).await;
        println!("{:#?}", issues);
    }
    #[ignore]
    #[test]
    fn r5_sd_sync() {
        let term = MockTerminologyService {
            result: Ok(TerminologyMembershipOutcome {
                is_member: true,
                message: None,
                diagnostics: Vec::new(),
                system: None,
                code: None,
                version: None,
                display: None,
            }),
        };

        let resource = load_resource(FhirVersion::R5, "valid/structuredefinition-language.json");
        let issues = validate_resource(&resource, Some(&term));
        println!("{:#?}", issues);
    }
    #[tokio::test]
    async fn r5_slot_async() {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(8))
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(8)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");

        let terminology = RemoteTerminologyService::with_client(
            client,
            "http://localhost:8080/fhir".to_string(),
            FhirVersion::R5,
        );

        let resource = load_resource(FhirVersion::R5, "valid/slot-codeable-reference.json");
        let issues = validate_resource_async(&resource, Some(&terminology)).await;
        println!("{:#?}", issues);
    }
}

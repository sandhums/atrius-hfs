#[cfg(feature = "R5")]
mod tests {

    use fhir_validation::profile::extract::extract_r5_structure_definition_profile;
    use helios_fhir::r5::StructureDefinition;

    const ATRIUS_PATIENT_PROFILE_JSON: &str = r#"
    {
      "resourceType": "StructureDefinition",
      "id": "atrius-patient",
      "url": "http://atrius.health/fhir/StructureDefinition/atrius-patient",
      "version": "0.1.0",
      "name": "AtriusPatient",
      "title": "Atrius Patient Profile",
      "status": "draft",
      "date": "2026-01-01",
      "publisher": "Atrius Health",
      "kind": "resource",
      "abstract": false,
      "type": "Patient",
      "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Patient",
      "derivation": "constraint",
      "differential": {
        "element": [
          {
            "id": "Patient",
            "path": "Patient",
            "constraint": [
              {
                "key": "atrius-pat-1",
                "severity": "error",
                "human": "If patient is active, at least one name must be present",
                "expression": "active = true implies name.exists()"
              }
            ]
          },
          {
            "id": "Patient.identifier",
            "path": "Patient.identifier",
            "min": 1
          },
          {
            "id": "Patient.gender",
            "path": "Patient.gender",
            "min": 1,
            "binding": {
              "strength": "required",
              "valueSet": "http://hl7.org/fhir/ValueSet/administrative-gender"
            }
          },
          {
            "id": "Patient.birthDate",
            "path": "Patient.birthDate",
            "min": 1
          }
        ]
      }
    }
    "#;

    #[test]
    fn extracts_atrius_patient_profile() {
        let sd: StructureDefinition = serde_json::from_str(ATRIUS_PATIENT_PROFILE_JSON)
            .expect("Atrius Patient profile JSON should deserialize into R5 StructureDefinition");

        let profile = extract_r5_structure_definition_profile(&sd)
            .expect("Atrius Patient profile should extract successfully");

        assert_eq!(
            profile.url,
            "http://atrius.health/fhir/StructureDefinition/atrius-patient"
        );
        assert_eq!(profile.version.as_deref(), Some("0.1.0"));
        assert_eq!(profile.name.as_deref(), Some("AtriusPatient"));
        assert_eq!(profile.title.as_deref(), Some("Atrius Patient Profile"));
        assert_eq!(profile.resource_type, "Patient");
        assert_eq!(
            profile.base_definition.as_deref(),
            Some("http://hl7.org/fhir/StructureDefinition/Patient")
        );

        assert_eq!(profile.invariants.len(), 1);
        let invariant = &profile.invariants[0];
        assert_eq!(invariant.key, "atrius-pat-1");
        assert_eq!(invariant.path, "Patient");
        assert_eq!(
            invariant.human,
            "If patient is active, at least one name must be present"
        );
        assert_eq!(invariant.expression, "active = true implies name.exists()");

        assert_eq!(profile.element_rules.len(), 3);

        let identifier_rule = profile
            .element_rules
            .iter()
            .find(|r| r.path == "Patient.identifier")
            .expect("Patient.identifier rule should be extracted");
        assert_eq!(identifier_rule.id, "Patient.identifier");
        assert_eq!(identifier_rule.min, Some(1));
        assert!(identifier_rule.binding.is_none());
        assert!(identifier_rule.constraints.is_empty());

        let gender_rule = profile
            .element_rules
            .iter()
            .find(|r| r.path == "Patient.gender")
            .expect("Patient.gender rule should be extracted");
        assert_eq!(gender_rule.id, "Patient.gender");
        assert_eq!(gender_rule.min, Some(1));
        let binding = gender_rule
            .binding
            .as_ref()
            .expect("Patient.gender binding should be extracted");
        assert_eq!(binding.path, "Patient.gender");
        assert_eq!(
            binding.value_set,
            "http://hl7.org/fhir/ValueSet/administrative-gender"
        );
        assert!(gender_rule.constraints.is_empty());

        let birth_date_rule = profile
            .element_rules
            .iter()
            .find(|r| r.path == "Patient.birthDate")
            .expect("Patient.birthDate rule should be extracted");
        assert_eq!(birth_date_rule.id, "Patient.birthDate");
        assert_eq!(birth_date_rule.min, Some(1));
        assert!(birth_date_rule.binding.is_none());
        assert!(birth_date_rule.constraints.is_empty());
    }
}

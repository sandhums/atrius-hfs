//! Parse and build FHIR R4 [`Parameters`](https://hl7.org/fhir/R4/parameters.html) for `$apply`.

use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Default)]
pub struct ApplyInput {
    pub subject: String,
    pub encounter: Option<String>,
    pub practitioner: Option<String>,
    pub organization: Option<String>,
    pub user_type: Option<Value>,
    pub user_language: Option<Value>,
    pub user_task_context: Option<Value>,
    pub setting: Option<Value>,
    pub setting_context: Option<Value>,
    pub cql_parameters: Option<Value>,
    pub inline_definition: Option<Value>,
    pub definition_id: Option<String>,
    pub definition_url: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ParametersParseError {
    #[error("request body must be a FHIR Parameters resource")]
    NotParameters,
    #[error("missing required parameter: {0}")]
    MissingRequired(&'static str),
    #[error("invalid parameter {name}: {reason}")]
    InvalidParameter { name: &'static str, reason: String },
    #[error("{0} parameter is not allowed when $apply is invoked on an instance")]
    DisallowedOnInstance(&'static str),
}

pub fn parse_apply_parameters(
    body: &Value,
    instance_id: Option<&str>,
    definition_param: &'static str,
) -> Result<ApplyInput, ParametersParseError> {
    if body.get("resourceType").and_then(Value::as_str) != Some("Parameters") {
        return Err(ParametersParseError::NotParameters);
    }

    let params = body
        .get("parameter")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    if let Some(id) = instance_id {
        if find_param(&params, definition_param).is_some() {
            return Err(ParametersParseError::DisallowedOnInstance(definition_param));
        }
        let mut input = extract_apply_context(&params)?;
        input.definition_id = Some(id.to_string());
        return Ok(input);
    }

    let mut input = extract_apply_context(&params)?;
    if let Some(def) = find_param(&params, definition_param) {
        input.inline_definition = param_resource(def).cloned();
        if let Some(resource) = input.inline_definition.as_ref() {
            input.definition_id = resource
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string);
            input.definition_url = resource
                .get("url")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
    }

    if input.definition_id.is_none() && input.definition_url.is_none() {
        if let Some(url) = find_param(&params, "url").and_then(param_string) {
            input.definition_url = Some(url);
        }
        if let Some(canonical) = find_param(&params, "canonical").and_then(param_string) {
            input.definition_url = Some(canonical);
        }
    }

    if input.definition_id.is_none() && input.definition_url.is_none() {
        return Err(ParametersParseError::MissingRequired(definition_param));
    }

    Ok(input)
}

fn extract_apply_context(params: &[Value]) -> Result<ApplyInput, ParametersParseError> {
    let subject = find_param(params, "subject")
        .and_then(reference_or_string)
        .ok_or(ParametersParseError::MissingRequired("subject"))?;

    let cql_parameters = find_param(params, "parameters").map(|p| {
        if let Some(resource) = param_resource(p) {
            resource.clone()
        } else {
            json!({ "resourceType": "Parameters", "parameter": p.get("part").cloned().unwrap_or_default() })
        }
    });

    Ok(ApplyInput {
        subject,
        encounter: find_param(params, "encounter").and_then(reference_or_string),
        practitioner: find_param(params, "practitioner").and_then(reference_or_string),
        organization: find_param(params, "organization").and_then(reference_or_string),
        user_type: find_param(params, "userType").and_then(param_codeable_concept),
        user_language: find_param(params, "userLanguage").and_then(param_codeable_concept),
        user_task_context: find_param(params, "userTaskContext").and_then(param_codeable_concept),
        setting: find_param(params, "setting").and_then(param_codeable_concept),
        setting_context: find_param(params, "settingContext").and_then(param_codeable_concept),
        cql_parameters: cql_parameters.map(cql_parameters_to_map),
        inline_definition: None,
        definition_id: None,
        definition_url: None,
    })
}

/// Flatten nested FHIR Parameters (or map-shaped JSON) into sidecar `parameters` object.
fn cql_parameters_to_map(value: Value) -> Value {
    if let Some(obj) = value.as_object()
        && !obj.contains_key("resourceType")
        && !obj.contains_key("parameter")
    {
        return Value::Object(obj.clone());
    }

    let mut out = Map::new();
    let items = value
        .get("parameter")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for item in items {
        let Some(name) = item.get("name").and_then(Value::as_str) else {
            continue;
        };
        if let Some(cc) = param_codeable_concept(&item) {
            out.insert(name.to_string(), cc);
        } else if let Some(s) = param_string(&item) {
            out.insert(name.to_string(), Value::String(s));
        } else if let Some(period) = item.get("valuePeriod").cloned() {
            out.insert(name.to_string(), period);
        } else if let Some(b) = item.get("valueBoolean").and_then(Value::as_bool) {
            out.insert(name.to_string(), Value::Bool(b));
        }
    }
    Value::Object(out)
}

pub fn parameters_with_return(resource: Value) -> Value {
    json!({
        "resourceType": "Parameters",
        "parameter": [{
            "name": "return",
            "resource": resource
        }]
    })
}

fn find_param<'a>(params: &'a [Value], name: &str) -> Option<&'a Value> {
    params
        .iter()
        .find(|p| p.get("name").and_then(Value::as_str) == Some(name))
}

fn param_string(param: &Value) -> Option<String> {
    param
        .get("valueString")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn param_resource(param: &Value) -> Option<&Value> {
    param.get("resource").filter(|r| r.is_object())
}

fn param_codeable_concept(param: &Value) -> Option<Value> {
    param.get("valueCodeableConcept").cloned()
}

fn reference_or_string(param: &Value) -> Option<String> {
    param_string(param).or_else(|| {
        param
            .get("valueReference")
            .and_then(|r| r.get("reference"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_instance_apply_context() {
        let body = json!({
            "resourceType": "Parameters",
            "parameter": [
                { "name": "subject", "valueString": "Patient/p1" },
                { "name": "encounter", "valueReference": { "reference": "Encounter/e1" } },
                { "name": "practitioner", "valueString": "Practitioner/doc" },
                { "name": "setting", "valueCodeableConcept": { "text": "inpatient" } }
            ]
        });
        let input = parse_apply_parameters(&body, Some("cms165"), "planDefinition").unwrap();
        assert_eq!(input.definition_id.as_deref(), Some("cms165"));
        assert_eq!(input.subject, "Patient/p1");
        assert_eq!(input.encounter.as_deref(), Some("Encounter/e1"));
        assert_eq!(input.practitioner.as_deref(), Some("Practitioner/doc"));
        assert!(input.setting.is_some());
    }

    #[test]
    fn rejects_definition_param_on_instance() {
        let body = json!({
            "resourceType": "Parameters",
            "parameter": [
                { "name": "subject", "valueString": "Patient/p1" },
                { "name": "planDefinition", "resource": { "resourceType": "PlanDefinition", "id": "x" } }
            ]
        });
        let err = parse_apply_parameters(&body, Some("y"), "planDefinition").unwrap_err();
        assert!(matches!(
            err,
            ParametersParseError::DisallowedOnInstance("planDefinition")
        ));
    }

    #[test]
    fn parses_type_level_inline_definition() {
        let body = json!({
            "resourceType": "Parameters",
            "parameter": [
                { "name": "subject", "valueString": "Patient/p1" },
                { "name": "activityDefinition", "resource": {
                    "resourceType": "ActivityDefinition",
                    "id": "order-ecg",
                    "url": "http://example.org/ad/order-ecg"
                }}
            ]
        });
        let input = parse_apply_parameters(&body, None, "activityDefinition").unwrap();
        assert_eq!(input.definition_id.as_deref(), Some("order-ecg"));
        assert_eq!(
            input.definition_url.as_deref(),
            Some("http://example.org/ad/order-ecg")
        );
    }

    #[test]
    fn builds_return_parameters() {
        let ret = parameters_with_return(json!({ "resourceType": "CarePlan", "id": "cp1" }));
        assert_eq!(ret["parameter"][0]["name"], "return");
        assert_eq!(ret["parameter"][0]["resource"]["resourceType"], "CarePlan");
    }
}

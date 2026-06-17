//! Map sidecar [`NormalizedSidecarResult`](atrius_clinical_reasoning::NormalizedSidecarResult) to CDS [`Card`]s.
//!
//! Sidecar results are attached to CDS Cards via extension
//! [`EVAL_RESULT_EXTENSION_URL`] (`https://atrius.dev/cds-clinical-reasoning-eval-result`) so SMART
//! apps can read structured evaluation output without parsing markdown `detail` text.

use std::collections::HashMap;

use helios_cds_hooks::{Card, Indicator, Source};
use serde_json::json;

use atrius_clinical_reasoning::{NormalizedSidecarResult, unwrap_nested_fhir_json_strings};

const SOURCE_LABEL: &str = "Atrius clinical reasoning";
/// Machine-readable evaluation payload for SMART/EHR clients (avoid parsing markdown `detail`).
const EVAL_RESULT_EXTENSION_URL: &str = "https://atrius.dev/cds-clinical-reasoning-eval-result";

fn source() -> Source {
    Source {
        label: SOURCE_LABEL.into(),
        url: None,
        icon: None,
        topic: None,
    }
}

/// CDS summary should stay short (spec suggests &lt;140 chars); detail may hold JSON/Markdown.
pub(crate) fn truncate_summary(s: &str) -> String {
    const MAX_CHARS: usize = 135;
    let count = s.chars().count();
    if count <= 140 {
        return s.to_string();
    }
    s.chars().take(MAX_CHARS).collect::<String>() + "…"
}

fn eval_result_extension(
    expression: &str,
    result: serde_json::Value,
) -> HashMap<String, serde_json::Value> {
    HashMap::from([(
        EVAL_RESULT_EXTENSION_URL.to_string(),
        json!({
            "expression": expression,
            "result": result,
        }),
    )])
}

fn markdown_json_detail(v: &serde_json::Value) -> String {
    format!(
        "```json\n{}\n```",
        serde_json::to_string_pretty(v).unwrap_or_default()
    )
}

pub fn cards_from_normalized(expression: &str, nr: NormalizedSidecarResult) -> Vec<Card> {
    let mut base = Card {
        uuid: None,
        summary: String::new(),
        detail: None,
        indicator: Indicator::Info,
        source: source(),
        suggestions: None,
        selection_behavior: None,
        override_reasons: None,
        links: None,
        extension: None,
    };

    match nr {
        NormalizedSidecarResult::Null => {
            base.summary = truncate_summary(&format!("{expression}: null"));
            base.extension = Some(eval_result_extension(expression, serde_json::Value::Null));
        }
        NormalizedSidecarResult::Bool(b) => {
            base.summary = truncate_summary(&format!("{expression}: {b}"));
            base.extension = Some(eval_result_extension(expression, json!(b)));
        }
        NormalizedSidecarResult::Number(n) => {
            base.summary = truncate_summary(&format!("{expression}: {n}"));
            base.extension = Some(eval_result_extension(
                expression,
                serde_json::Value::Number(n.clone()),
            ));
        }
        NormalizedSidecarResult::String(s) => {
            base.summary = truncate_summary(&format!("{expression}: {s}"));
            base.extension = Some(eval_result_extension(expression, json!(s)));
        }
        NormalizedSidecarResult::Array(a) => {
            base.summary = truncate_summary(&format!("{expression}: {} items", a.len()));
            let v = unwrap_nested_fhir_json_strings(serde_json::Value::Array(a));
            base.detail = Some(markdown_json_detail(&v));
            base.extension = Some(eval_result_extension(expression, v));
        }
        NormalizedSidecarResult::Object(m) => {
            base.summary = truncate_summary(&format!("{expression}: object"));
            let v = unwrap_nested_fhir_json_strings(serde_json::Value::Object(m));
            base.detail = Some(markdown_json_detail(&v));
            base.extension = Some(eval_result_extension(expression, v));
        }
        NormalizedSidecarResult::FhirResource(v) => {
            let rt = v
                .get("resourceType")
                .and_then(|x| x.as_str())
                .unwrap_or("Resource");
            base.summary = truncate_summary(&format!("{expression}: FHIR {rt}"));
            let v = unwrap_nested_fhir_json_strings(v);
            base.detail = Some(markdown_json_detail(&v));
            base.extension = Some(eval_result_extension(expression, v));
        }
    }

    vec![base]
}

pub fn demo_card_for_patient(patient_id: &str, hook_instance: &str) -> Vec<Card> {
    vec![Card {
        uuid: None,
        summary: truncate_summary(&format!(
            "Demo CDS (configure CDS_CLINICAL_REASONING_URL): patient {patient_id}"
        )),
        detail: Some(format!(
            "Hook instance `{hook_instance}`. Set **CDS_CLINICAL_REASONING_URL**, **CDS_SERVICES_MANIFEST_PATH** (or KR **CDS_KR_SERVICES_BINARY_ID** + **CDS_LIBRARY_BASE_URL**), and FHIR bases to evaluate ELM via the sidecar."
        )),
        indicator: Indicator::Info,
        source: source(),
        suggestions: None,
        selection_behavior: None,
        override_reasons: None,
        links: None,
        extension: Some(HashMap::from([(
            "https://atrius.dev/cds-demo".to_string(),
            json!({"patientId": patient_id}),
        )])),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eval_extension_holds_unwrapped_fhir_array() {
        let arr = vec![serde_json::Value::String(
            json!({"resourceType": "Condition", "id": "c1"}).to_string(),
        )];
        let cards = cards_from_normalized("ActiveConditions", NormalizedSidecarResult::Array(arr));
        let ext = cards[0].extension.as_ref().expect("extension");
        let payload = ext.get(EVAL_RESULT_EXTENSION_URL).expect("eval payload");
        assert_eq!(payload["expression"], json!("ActiveConditions"));
        assert_eq!(payload["result"][0]["resourceType"], json!("Condition"));
    }
}

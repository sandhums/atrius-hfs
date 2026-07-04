//! Generic ValueSet binding validators parameterized by [`BindingVersionAdapter`].
//!
//! Version modules (`r4/binding.rs`, etc.) re-export these via
//! [`export_version_binding_validators`].

use crate::ValidationIssue;
use crate::ValidationSourceKind;
use crate::binding::common::{
    BindingCheckContextAsync, BindingCheckContextSync, CodeableConceptRemoteScan,
    LocalBindingDisposition, prettify_remote_terminology_error,
};
use crate::binding::engine::{
    BindingVersionAdapter, LocalBindingOutcome, evaluate_local_codeable_concept_binding,
    evaluate_local_codeable_reference_binding, evaluate_local_coding_binding,
    evaluate_local_quantity_binding,
};
use crate::issue_code;
use crate::validation_issue_detail::ValidationIssueDetailCode;
use fhir_terminology::TerminologyValidationError;
use fhir_validation_types::{BindingStrength, Severity};

fn evaluate_local_primitive_str_binding<F>(
    valueset_url: &str,
    code: &str,
    check_local: F,
) -> LocalBindingOutcome
where
    F: Fn(Option<&str>, &str, Option<&str>) -> Result<(), TerminologyValidationError>,
{
    match check_local(None, code, None) {
        Ok(()) => LocalBindingOutcome::Valid,
        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            LocalBindingOutcome::NeedsRemote {
                valueset_url: valueset_url.to_string(),
                system: None,
                code: code.to_string(),
                display: None,
            }
        }
        Err(TerminologyValidationError::NotInValueSet { .. }) => {
            LocalBindingOutcome::Error(TerminologyValidationError::NotInValueSet {
                valueset_url: valueset_url.to_string(),
                system: None,
                code: code.to_string(),
            })
        }
        Err(other) => LocalBindingOutcome::Error(other),
    }
}

fn quantity_code_without_system_issue(fhir_path: &str, valueset_url: &str) -> ValidationIssue {
    ValidationIssue {
        severity: Severity::Warning,
        code: issue_code::TERMINOLOGY.to_string(),
        fhir_path: fhir_path.to_string(),
        instance_path: None,
        expression: Some(valueset_url.to_string()),
        expression_kind: Some(ValidationSourceKind::CanonicalUri),
        source_invariant_key: None,
        summary: Some(
            crate::binding::common::binding_issue_summary::QUANTITY_CODE_WITHOUT_SYSTEM
                .to_string(),
        ),
        detail_code: Some(ValidationIssueDetailCode::CodeWithoutSystem),
        diagnostics: "A quantity code with no system has no defined meaning, and it cannot be validated. A system should be provided"
            .to_string(),
    }
}

fn finish_codeable_concept_remote_scan<A: BindingVersionAdapter>(
    validator: &crate::Validator,
    fhir_path: &str,
    valueset_url: &str,
    strength: BindingStrength,
    cc: &A::CodeableConcept,
    scan: CodeableConceptRemoteScan,
    any_usable_coding: bool,
) -> Vec<ValidationIssue> {
    if scan.any_match {
        return vec![];
    }

    if scan.any_remote_undecidable {
        return vec![
            crate::binding::common::terminology_membership_not_locally_verifiable_issue(
                fhir_path,
                valueset_url,
                strength,
                scan.remote_undecidable_message.unwrap_or_else(|| {
                    "Local terminology cannot determine ValueSet membership; use a remote terminology service for a definitive validation result.".to_string()
                }),
            ),
        ];
    }

    if !any_usable_coding {
        return vec![crate::binding::common::value_issue(
            fhir_path,
            valueset_url,
            "CodeableConcept has no usable coding with a code value for terminology validation",
            ValidationIssueDetailCode::InvalidBindableValue,
            "CodeableConcept has no usable coding with a code value for terminology validation"
                .to_string(),
        )];
    }

    if let Some(err) = scan.last_local_failure {
        return crate::binding::common::local_error_to_issues(
            validator,
            fhir_path,
            valueset_url,
            strength,
            err,
        );
    }

    let diagnostics = scan.last_miss_diagnostics.unwrap_or_else(|| {
        let coding_summary = A::summarize_codeable_concept_codings(cc);
        format!(
            "The provided coding(s) {} were not found in ValueSet {}",
            coding_summary, valueset_url
        )
    });

    match crate::binding::common::issue_for_binding_miss(
        validator,
        fhir_path,
        valueset_url,
        strength,
        diagnostics,
    ) {
        Some(issue) => vec![issue],
        None => vec![],
    }
}

fn codeable_concept_needs_remote_sync<A: BindingVersionAdapter>(
    ctx: &BindingCheckContextSync<'_>,
    cc: &A::CodeableConcept,
) -> Vec<ValidationIssue> {
    let Some(terminology) = ctx.terminology else {
        return vec![crate::binding::common::terminology_validation_issue(
            ctx.fhir_path,
            ctx.valueset_url,
            "Remote terminology validation required but no TerminologyService was provided"
                .to_string(),
        )];
    };

    let mut any_usable_coding = false;
    let mut scan = CodeableConceptRemoteScan::default();
    let codings: Vec<_> = A::codeable_concept_codings(cc).collect();

    for coding in codings {
        let system = A::coding_system(coding);
        let code = match A::coding_code(coding) {
            Some(code) => code,
            None => continue,
        };
        any_usable_coding = true;
        let display = A::coding_display(coding);

        let outcome = terminology.member_of(ctx.valueset_url, system, code, display);
        if let Err(e) = crate::binding::common::merge_remote_member_of_for_coding(
            &mut scan,
            outcome,
            system,
            code,
            ctx.valueset_url,
        ) {
            return vec![crate::binding::common::terminology_validation_issue(
                ctx.fhir_path,
                ctx.valueset_url,
                prettify_remote_terminology_error(ctx.valueset_url, &e),
            )];
        }
        if scan.any_match {
            break;
        }
    }

    finish_codeable_concept_remote_scan::<A>(
        ctx.validator,
        ctx.fhir_path,
        ctx.valueset_url,
        ctx.strength,
        cc,
        scan,
        any_usable_coding,
    )
}

async fn codeable_concept_needs_remote_async<A: BindingVersionAdapter>(
    ctx: &BindingCheckContextAsync<'_>,
    cc: &A::CodeableConcept,
) -> Vec<ValidationIssue> {
    let Some(terminology) = ctx.terminology else {
        return vec![crate::binding::common::terminology_validation_issue(
            ctx.fhir_path,
            ctx.valueset_url,
            "Remote terminology validation required but no TerminologyService was provided"
                .to_string(),
        )];
    };

    let mut any_usable_coding = false;
    let mut scan = CodeableConceptRemoteScan::default();
    let codings: Vec<_> = A::codeable_concept_codings(cc).collect();

    for coding in codings {
        let system = A::coding_system(coding);
        let code = match A::coding_code(coding) {
            Some(code) => code,
            None => continue,
        };
        any_usable_coding = true;
        let display = A::coding_display(coding);

        let outcome = terminology
            .member_of(ctx.valueset_url, system, code, display)
            .await;
        if let Err(e) = crate::binding::common::merge_remote_member_of_for_coding(
            &mut scan,
            outcome,
            system,
            code,
            ctx.valueset_url,
        ) {
            return vec![crate::binding::common::terminology_validation_issue(
                ctx.fhir_path,
                ctx.valueset_url,
                prettify_remote_terminology_error(ctx.valueset_url, &e),
            )];
        }
        if scan.any_match {
            break;
        }
    }

    finish_codeable_concept_remote_scan::<A>(
        ctx.validator,
        ctx.fhir_path,
        ctx.valueset_url,
        ctx.strength,
        cc,
        scan,
        any_usable_coding,
    )
}

pub fn validate_primitive_code_binding<A, F>(
    ctx: &BindingCheckContextSync<'_>,
    code_value: Option<&str>,
    implicit_system: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let Some(code) = code_value else {
        return vec![];
    };
    let local_outcome =
        evaluate_local_primitive_str_binding(ctx.valueset_url, code, |_, c, _| local_check(c));

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => {
            let mut req = req;
            req.system = implicit_system.map(str::to_owned);
            ctx.execute_remote_sync(&req)
        }
    }
}

pub fn validate_primitive_value_binding<A, F>(
    ctx: &BindingCheckContextSync<'_>,
    value: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let Some(value) = value else {
        return vec![];
    };
    let local_outcome =
        evaluate_local_primitive_str_binding(ctx.valueset_url, value, |_, v, _| local_check(v));

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => ctx.execute_remote_sync(&req),
    }
}

pub async fn validate_primitive_code_binding_async<A, F>(
    ctx: &BindingCheckContextAsync<'_>,
    code: Option<&str>,
    implicit_system: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let Some(code) = code else {
        return vec![];
    };
    let local_outcome =
        evaluate_local_primitive_str_binding(ctx.valueset_url, code, |_, c, _| local_check(c));

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => {
            let mut req = req;
            req.system = implicit_system.map(str::to_owned);
            ctx.execute_remote_async(&req).await
        }
    }
}

pub async fn validate_primitive_value_binding_async<A, F>(
    ctx: &BindingCheckContextAsync<'_>,
    value: Option<&str>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&str) -> Result<(), TerminologyValidationError>,
{
    let Some(value) = value else {
        return vec![];
    };
    let local_outcome =
        evaluate_local_primitive_str_binding(ctx.valueset_url, value, |_, v, _| local_check(v));

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => ctx.execute_remote_async(&req).await,
    }
}

pub fn validate_quantity_binding<A, F>(
    ctx: &BindingCheckContextSync<'_>,
    quantity: Option<&A::Quantity>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::Quantity) -> Result<(), TerminologyValidationError>,
{
    let Some(quantity) = quantity else {
        return vec![];
    };

    let system = A::quantity_system(quantity).filter(|s| !s.is_empty());
    let code = A::quantity_code(quantity).filter(|c| !c.is_empty());

    if code.is_some() && system.is_none() {
        return vec![quantity_code_without_system_issue(
            ctx.fhir_path,
            ctx.valueset_url,
        )];
    }

    let local_outcome =
        evaluate_local_quantity_binding::<A, _>(ctx.valueset_url, quantity, |_, _, _| {
            local_check(quantity)
        });

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => ctx.execute_remote_sync(&req),
    }
}

pub async fn validate_quantity_binding_async<A, F>(
    ctx: &BindingCheckContextAsync<'_>,
    quantity: Option<&A::Quantity>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::Quantity) -> Result<(), TerminologyValidationError>,
{
    let Some(quantity) = quantity else {
        return vec![];
    };

    let system = A::quantity_system(quantity).filter(|s| !s.is_empty());
    let code = A::quantity_code(quantity).filter(|c| !c.is_empty());

    if code.is_some() && system.is_none() {
        return vec![quantity_code_without_system_issue(
            ctx.fhir_path,
            ctx.valueset_url,
        )];
    }

    let local_outcome =
        evaluate_local_quantity_binding::<A, _>(ctx.valueset_url, quantity, |_, _, _| {
            local_check(quantity)
        });

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => ctx.execute_remote_async(&req).await,
    }
}

pub fn validate_coding_binding<A, F>(
    ctx: &BindingCheckContextSync<'_>,
    coding: Option<&A::Coding>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::Coding) -> Result<(), TerminologyValidationError>,
{
    let Some(coding) = coding else {
        return vec![];
    };

    let local_outcome =
        evaluate_local_coding_binding::<A, _>(ctx.valueset_url, coding, |system, code, display| {
            local_check(&A::coding_for_local_check(coding, system, code, display))
        });

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => ctx.execute_remote_sync(&req),
    }
}

pub async fn validate_coding_binding_async<A, F>(
    ctx: &BindingCheckContextAsync<'_>,
    coding: Option<&A::Coding>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::Coding) -> Result<(), TerminologyValidationError>,
{
    let Some(coding) = coding else {
        return vec![];
    };

    let local_outcome =
        evaluate_local_coding_binding::<A, _>(ctx.valueset_url, coding, |system, code, display| {
            local_check(&A::coding_for_local_check(coding, system, code, display))
        });

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(req) => ctx.execute_remote_async(&req).await,
    }
}

pub fn validate_codeable_concept_binding<A, F>(
    ctx: &BindingCheckContextSync<'_>,
    codeable_concept: Option<&A::CodeableConcept>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let Some(cc) = codeable_concept else {
        return vec![];
    };

    if !A::codeable_concept_has_codings(cc) {
        return vec![];
    }

    let local_outcome = evaluate_local_codeable_concept_binding::<A, _>(
        ctx.valueset_url,
        cc,
        |system, code, display| {
            local_check(&A::codeable_concept_for_local_check(
                cc, system, code, display,
            ))
        },
    );

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(_) => codeable_concept_needs_remote_sync::<A>(ctx, cc),
    }
}

pub async fn validate_codeable_concept_binding_async<A, F>(
    ctx: &BindingCheckContextAsync<'_>,
    codeable_concept: Option<&A::CodeableConcept>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let Some(cc) = codeable_concept else {
        return vec![];
    };

    if !A::codeable_concept_has_codings(cc) {
        return vec![];
    }

    let local_outcome = evaluate_local_codeable_concept_binding::<A, _>(
        ctx.valueset_url,
        cc,
        |system, code, display| {
            local_check(&A::codeable_concept_for_local_check(
                cc, system, code, display,
            ))
        },
    );

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(_) => {
            codeable_concept_needs_remote_async::<A>(ctx, cc).await
        }
    }
}

pub fn validate_codeable_reference_binding<A, F>(
    ctx: &BindingCheckContextSync<'_>,
    codeable_reference: Option<&A::CodeableReference>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let Some(cr) = codeable_reference else {
        return vec![];
    };

    let local_outcome = evaluate_local_codeable_reference_binding::<A, _>(
        ctx.valueset_url,
        cr,
        |system, code, display| {
            local_check(&A::single_coding_codeable_concept(system, code, display))
        },
    );

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(_) => validate_codeable_concept_binding::<A, _>(
            ctx,
            A::codeable_reference_concept(cr),
            local_check,
        ),
    }
}

pub async fn validate_codeable_reference_binding_async<A, F>(
    ctx: &BindingCheckContextAsync<'_>,
    codeable_reference: Option<&A::CodeableReference>,
    local_check: F,
) -> Vec<ValidationIssue>
where
    A: BindingVersionAdapter,
    F: Fn(&A::CodeableConcept) -> Result<(), TerminologyValidationError>,
{
    let Some(cr) = codeable_reference else {
        return vec![];
    };

    let local_outcome = evaluate_local_codeable_reference_binding::<A, _>(
        ctx.valueset_url,
        cr,
        |system, code, display| {
            local_check(&A::single_coding_codeable_concept(system, code, display))
        },
    );

    match ctx.classify_local_outcome(local_outcome) {
        LocalBindingDisposition::Valid => vec![],
        LocalBindingDisposition::Done(issues) => issues,
        LocalBindingDisposition::NeedsRemote(_) => {
            validate_codeable_concept_binding_async::<A, _>(
                ctx,
                A::codeable_reference_concept(cr),
                local_check,
            )
            .await
        }
    }
}

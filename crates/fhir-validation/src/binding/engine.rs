use helios_fhir::TerminologyValidationError;

pub trait BindingVersionAdapter {
    type Coding;
    type CodeableConcept;
    type Quantity;
    type CodeableReference;
    type PrimitiveCode;

    fn coding_system(coding: &Self::Coding) -> Option<&str>;
    fn coding_code(coding: &Self::Coding) -> Option<&str>;
    fn coding_display(coding: &Self::Coding) -> Option<&str>;

    fn codeable_concept_codings(
        cc: &Self::CodeableConcept,
    ) -> Box<dyn Iterator<Item = &Self::Coding> + '_>;

    fn codeable_concept_has_codings(cc: &Self::CodeableConcept) -> bool;

    fn quantity_system(quantity: &Self::Quantity) -> Option<&str>;
    fn quantity_code(quantity: &Self::Quantity) -> Option<&str>;

    #[allow(dead_code)]
    fn codeable_reference_concept(
        value: &Self::CodeableReference,
    ) -> Option<&Self::CodeableConcept>;

    /// Rebuild a `Coding` for generated local ValueSet checks from system/code/display parts.
    fn coding_for_local_check(
        coding: &Self::Coding,
        system: Option<&str>,
        code: &str,
        display: Option<&str>,
    ) -> Self::Coding;

    /// Rebuild a `CodeableConcept` with a single coding for local ValueSet checks.
    fn codeable_concept_for_local_check(
        cc: &Self::CodeableConcept,
        system: Option<&str>,
        code: &str,
        display: Option<&str>,
    ) -> Self::CodeableConcept;

    /// Build a one-coding `CodeableConcept` (e.g. `CodeableReference` local evaluation).
    fn single_coding_codeable_concept(
        system: Option<&str>,
        code: &str,
        display: Option<&str>,
    ) -> Self::CodeableConcept;

    fn summarize_codeable_concept_codings(cc: &Self::CodeableConcept) -> String {
        let mut rendered = Vec::new();
        for coding in Self::codeable_concept_codings(cc) {
            let system = Self::coding_system(coding).filter(|s| !s.is_empty());
            let code = Self::coding_code(coding).filter(|c| !c.is_empty());

            match (system, code) {
                (Some(system), Some(code)) => rendered.push(format!("{}#{}", system, code)),
                (None, Some(code)) => rendered.push(code.to_string()),
                (Some(system), None) => rendered.push(format!("{}#<missing-code>", system)),
                (None, None) => rendered.push("<empty-coding>".to_string()),
            }
        }

        if rendered.is_empty() {
            "CodeableConcept has no codings".to_string()
        } else {
            rendered.join(", ")
        }
    }
}

/// Implements [`BindingVersionAdapter`] local-rebuild helpers for a FHIR version module.
#[macro_export]
macro_rules! impl_binding_version_adapter_rebuild {
    ($code_ty:path, $string_ty:path, $coding:ty, $cc:ty) => {
        fn coding_for_local_check(
            coding: &$coding,
            system: Option<&str>,
            code: &str,
            display: Option<&str>,
        ) -> $coding {
            let mut local = coding.clone();
            local.system = system.map(|s| <$code_ty>::from(s.to_string()));
            local.code = Some(<$code_ty>::from(code.to_string()));
            local.display = display.map(|d| <$string_ty>::from(d.to_string()));
            local
        }

        fn codeable_concept_for_local_check(
            cc: &$cc,
            system: Option<&str>,
            code: &str,
            display: Option<&str>,
        ) -> $cc {
            let mut local_cc = cc.clone();
            let mut local_coding = <$coding as Default>::default();
            local_coding.system = system.map(|s| <$code_ty>::from(s.to_string()));
            local_coding.code = Some(<$code_ty>::from(code.to_string()));
            local_coding.display = display.map(|d| <$string_ty>::from(d.to_string()));
            local_cc.coding = Some(vec![local_coding]);
            local_cc
        }

        fn single_coding_codeable_concept(
            system: Option<&str>,
            code: &str,
            display: Option<&str>,
        ) -> $cc {
            let mut local_cc = <$cc as Default>::default();
            let mut local_coding = <$coding as Default>::default();
            local_coding.system = system.map(|s| <$code_ty>::from(s.to_string()));
            local_coding.code = Some(<$code_ty>::from(code.to_string()));
            local_coding.display = display.map(|d| <$string_ty>::from(d.to_string()));
            local_cc.coding = Some(vec![local_coding]);
            local_cc
        }
    };
}

pub enum LocalBindingOutcome {
    Valid,
    NeedsRemote {
        valueset_url: String,
        system: Option<String>,
        code: String,
        display: Option<String>,
    },
    Error(TerminologyValidationError),
}

pub fn evaluate_local_coding_binding<A, F>(
    valueset_url: &str,
    coding: &A::Coding,
    check_local: F,
) -> LocalBindingOutcome
where
    A: BindingVersionAdapter,
    F: Fn(Option<&str>, &str, Option<&str>) -> Result<(), TerminologyValidationError>,
{
    let system = A::coding_system(coding).map(str::to_owned);
    let code = match A::coding_code(coding) {
        Some(code) => code.to_string(),
        None => {
            return LocalBindingOutcome::Error(TerminologyValidationError::InvalidInput(
                "Missing code".to_string(),
            ));
        }
    };
    let display = A::coding_display(coding).map(str::to_owned);

    match check_local(system.as_deref(), &code, display.as_deref()) {
        Ok(()) => LocalBindingOutcome::Valid,
        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            LocalBindingOutcome::NeedsRemote {
                valueset_url: valueset_url.to_string(),
                system,
                code,
                display,
            }
        }
        Err(TerminologyValidationError::NotInValueSet { .. }) => {
            LocalBindingOutcome::Error(TerminologyValidationError::NotInValueSet {
                valueset_url: valueset_url.to_string(),
                system,
                code,
            })
        }
        Err(other) => LocalBindingOutcome::Error(other),
    }
}

pub fn evaluate_local_codeable_concept_binding<A, F>(
    valueset_url: &str,
    cc: &A::CodeableConcept,
    check_local: F,
) -> LocalBindingOutcome
where
    A: BindingVersionAdapter,
    F: Fn(Option<&str>, &str, Option<&str>) -> Result<(), TerminologyValidationError>,
{
    let mut first_candidate: Option<(Option<String>, String, Option<String>)> = None;
    let mut saw_remote_required = false;

    for coding in A::codeable_concept_codings(cc) {
        let system = A::coding_system(coding).map(str::to_owned);
        let code = match A::coding_code(coding) {
            Some(code) => code.to_string(),
            None => continue,
        };
        let display = A::coding_display(coding).map(str::to_owned);

        if first_candidate.is_none() {
            first_candidate = Some((system.clone(), code.clone(), display.clone()));
        }

        match check_local(system.as_deref(), &code, display.as_deref()) {
            Ok(()) => return LocalBindingOutcome::Valid,
            Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
                saw_remote_required = true;
            }
            Err(TerminologyValidationError::NotInValueSet { .. }) => {
                // keep looking through remaining codings
            }
            Err(other) => return LocalBindingOutcome::Error(other),
        }
    }

    if saw_remote_required {
        if let Some((system, code, display)) = first_candidate {
            return LocalBindingOutcome::NeedsRemote {
                valueset_url: valueset_url.to_string(),
                system,
                code,
                display,
            };
        }
    }

    match first_candidate {
        Some((system, code, _display)) => {
            LocalBindingOutcome::Error(TerminologyValidationError::NotInValueSet {
                valueset_url: valueset_url.to_string(),
                system,
                code,
            })
        }
        None => LocalBindingOutcome::Error(TerminologyValidationError::InvalidInput(format!(
            "CodeableConcept does not contain any usable codings: {}",
            A::summarize_codeable_concept_codings(cc)
        ))),
    }
}

pub fn evaluate_local_quantity_binding<A, F>(
    valueset_url: &str,
    quantity: &A::Quantity,
    check_local: F,
) -> LocalBindingOutcome
where
    A: BindingVersionAdapter,
    F: Fn(Option<&str>, &str, Option<&str>) -> Result<(), TerminologyValidationError>,
{
    let system = A::quantity_system(quantity).map(str::to_owned);
    let code = match A::quantity_code(quantity) {
        Some(code) => code.to_string(),
        None => {
            return LocalBindingOutcome::Error(TerminologyValidationError::InvalidInput(
                "Missing quantity code".to_string(),
            ));
        }
    };

    match check_local(system.as_deref(), &code, None) {
        Ok(()) => LocalBindingOutcome::Valid,
        Err(TerminologyValidationError::RemoteValidationRequired(_)) => {
            LocalBindingOutcome::NeedsRemote {
                valueset_url: valueset_url.to_string(),
                system,
                code,
                display: None,
            }
        }
        Err(TerminologyValidationError::NotInValueSet { .. }) => {
            LocalBindingOutcome::Error(TerminologyValidationError::NotInValueSet {
                valueset_url: valueset_url.to_string(),
                system,
                code,
            })
        }
        Err(other) => LocalBindingOutcome::Error(other),
    }
}
#[allow(dead_code)]
pub fn evaluate_local_codeable_reference_binding<A, F>(
    valueset_url: &str,
    value: &A::CodeableReference,
    check_local: F,
) -> LocalBindingOutcome
where
    A: BindingVersionAdapter,
    F: Fn(Option<&str>, &str, Option<&str>) -> Result<(), TerminologyValidationError>,
{
    let Some(concept) = A::codeable_reference_concept(value) else {
        return LocalBindingOutcome::Error(TerminologyValidationError::InvalidInput(
            "CodeableReference does not contain a concept".to_string(),
        ));
    };

    evaluate_local_codeable_concept_binding::<A, F>(valueset_url, concept, check_local)
}

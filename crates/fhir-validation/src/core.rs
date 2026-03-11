pub use fhir_validation_types::{
    BindingDef,
    BindingStrength,
    BindingTargetKind,
    InvariantDef,
    Severity,
};

use std::fmt;
use crate::{InvariantEvaluator, TerminologyService};
use crate::r4::R4Validatable;

/// A single validation issue that can later be mapped to
/// `OperationOutcome.issue`.
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: Severity,

    /// Category such as "value", "invariant", "structure", "terminology"
    pub code: &'static str,

    /// Logical FHIR path such as "Patient.gender"
    pub fhir_path: String,

    /// Optional expression (FHIRPath or ValueSet URL)
    pub expression: Option<String>,

    /// Human readable diagnostics
    pub diagnostics: String,
}
impl ValidationIssue {
    pub fn error(code: &'static str, path: impl Into<String>, diag: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code,
            fhir_path: path.into(),
            expression: None,
            diagnostics: diag.into(),
        }
    }

    pub fn warning(code: &'static str, path: impl Into<String>, diag: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            code,
            fhir_path: path.into(),
            expression: None,
            diagnostics: diag.into(),
        }
    }
}
/// Configuration for validation behavior.
#[derive(Debug, Clone, Copy)]
pub struct ValidationConfig {
    /// Treat extensible bindings as errors
    pub strict_extensible_bindings: bool,

    /// Emit warnings for preferred bindings
    pub warn_on_preferred_bindings: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strict_extensible_bindings: false,
            warn_on_preferred_bindings: false,
        }
    }
}

#[derive(Debug)]
pub enum ValidationError {
    FhirPath(helios_fhirpath_support::EvaluationError),
    Terminology(String),
    Other(String),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FhirPath(e) => write!(f, "{}", e),
            Self::Terminology(e) => write!(f, "{}", e),
            Self::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<helios_fhirpath_support::EvaluationError> for ValidationError {
    fn from(e: helios_fhirpath_support::EvaluationError) -> Self {
        Self::FhirPath(e)
    }
}

/// Core validator structure.
#[derive(Debug, Clone, Copy)]
pub struct Validator {
    pub config: ValidationConfig,
}

impl Default for Validator {
    fn default() -> Self {
        Self {
            config: ValidationConfig::default(),
        }
    }
}

impl Validator {
    pub fn new(config: ValidationConfig) -> Self {
        Self { config }
    }
    /// Determine severity for a binding miss depending on binding strength.
    pub fn binding_miss_severity(
        &self,
        strength: BindingStrength,
    ) -> Option<Severity> {
        match strength {
            BindingStrength::Required => Some(Severity::Error),
            BindingStrength::Extensible => Some(if self.config.strict_extensible_bindings {
                Severity::Error
            } else {
                Severity::Warning
            }),
            BindingStrength::Preferred => Some(if self.config.warn_on_preferred_bindings {
                Severity::Warning
            } else {
                Severity::Information
            }),
            BindingStrength::Example => None,
        }
    }
    pub fn validate_resource(
        &self,
        resource: &helios_fhir::FhirResource,
        terminology: Option<&dyn TerminologyService>,
        evaluator: &dyn InvariantEvaluator,
    ) -> Vec<ValidationIssue> {
        match resource {
            #[cfg(feature = "R4")]
            helios_fhir::FhirResource::R4(res) => self.validate_r4_resource(res.as_ref(), terminology, evaluator),

            #[cfg(feature = "R4B")]
            helios_fhir::FhirResource::R4B(res) => self.validate_r4b_resource(res, terminology, evaluator),

            #[cfg(feature = "R5")]
            helios_fhir::FhirResource::R5(res) => self.validate_r5_resource(res, terminology, evaluator),

            #[cfg(feature = "R6")]
            helios_fhir::FhirResource::R6(res) => self.validate_r6_resource(res, terminology, evaluator),
        }
    }
    #[cfg(feature = "R4")]
    pub fn validate_r4<T>(
        &self,
        resource: &T,
        terminology: Option<&dyn TerminologyService>,
        evaluator: &dyn InvariantEvaluator,
    ) -> Vec<ValidationIssue>
    where
        T: crate::r4::R4Validatable,
    {
        let mut issues = resource.validate_bindings(self, terminology);
        issues.extend(resource.validate_invariants(self, evaluator));
        issues
    }
    #[cfg(feature = "R4")]
    pub fn validate_r4_resource(
        &self,
        resource: &helios_fhir::r4::Resource,
        terminology: Option<&dyn TerminologyService>,
        evaluator: &dyn InvariantEvaluator,
    ) -> Vec<ValidationIssue> {
        let mut issues = self.validate_r4_resource_bindings(resource, terminology);
        issues.extend(self.validate_r4_resource_invariants(resource, evaluator));
        issues
    }
    #[cfg(feature = "R4")]
    pub fn validate_r4_resource_bindings(
        &self,
        resource: &helios_fhir::r4::Resource,
        terminology: Option<&dyn TerminologyService>,
    ) -> Vec<ValidationIssue> {
        match resource {
            helios_fhir::r4::Resource::Patient(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Account(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ActivityDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::AdverseEvent(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::AllergyIntolerance(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Appointment(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::AppointmentResponse(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::AuditEvent(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Basic(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Binary(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::BiologicallyDerivedProduct(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::BodyStructure(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Bundle(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CapabilityStatement(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CarePlan(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CareTeam(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CatalogEntry(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ChargeItem(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ChargeItemDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Claim(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ClaimResponse(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ClinicalImpression(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CodeSystem(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Communication(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CommunicationRequest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CompartmentDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Composition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ConceptMap(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Condition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Consent(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Contract(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Coverage(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CoverageEligibilityRequest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::CoverageEligibilityResponse(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DetectedIssue(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Device(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DeviceDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DeviceMetric(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DeviceRequest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DeviceUseStatement(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DiagnosticReport(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DocumentManifest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::DocumentReference(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::EffectEvidenceSynthesis(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Encounter(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Endpoint(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::EnrollmentRequest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::EnrollmentResponse(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::EpisodeOfCare(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::EventDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Evidence(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::EvidenceVariable(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ExampleScenario(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ExplanationOfBenefit(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::FamilyMemberHistory(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Flag(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Goal(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::GraphDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Group(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::GuidanceResponse(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::HealthcareService(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ImagingStudy(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Immunization(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ImmunizationEvaluation(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ImmunizationRecommendation(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ImplementationGuide(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::InsurancePlan(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Invoice(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Library(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Linkage(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::List(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Location(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Measure(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MeasureReport(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Media(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Medication(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicationAdministration(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicationDispense(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicationKnowledge(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicationRequest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicationStatement(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProduct(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductAuthorization(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductContraindication(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductIndication(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductIngredient(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductInteraction(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductManufactured(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductPackaged(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductPharmaceutical(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MedicinalProductUndesirableEffect(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MessageDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MessageHeader(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::MolecularSequence(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::NamingSystem(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::NutritionOrder(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Observation(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ObservationDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::OperationDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::OperationOutcome(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Organization(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::OrganizationAffiliation(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Parameters(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::PaymentNotice(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::PaymentReconciliation(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Person(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::PlanDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Practitioner(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::PractitionerRole(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Procedure(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Provenance(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Questionnaire(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::QuestionnaireResponse(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::RelatedPerson(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::RequestGroup(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ResearchDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ResearchElementDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ResearchStudy(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ResearchSubject(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::RiskAssessment(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::RiskEvidenceSynthesis(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Schedule(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SearchParameter(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ServiceRequest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Slot(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Specimen(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SpecimenDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::StructureDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::StructureMap(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Subscription(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Substance(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SubstanceNucleicAcid(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SubstancePolymer(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SubstanceProtein(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SubstanceReferenceInformation(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SubstanceSourceMaterial(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SubstanceSpecification(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SupplyDelivery(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::SupplyRequest(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::Task(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::TerminologyCapabilities(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::TestReport(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::TestScript(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ValueSet(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::VerificationResult(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::ViewDefinition(p) => p.validate_bindings(self, terminology),
            helios_fhir::r4::Resource::VisionPrescription(p) => p.validate_bindings(self, terminology),
        }
    }

    #[cfg(feature = "R4")]
    pub fn validate_r4_resource_invariants(
        &self,
        resource: &helios_fhir::r4::Resource,
        evaluator: &dyn InvariantEvaluator,
    ) -> Vec<ValidationIssue> {
        match resource {
            helios_fhir::r4::Resource::Patient(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Account(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ActivityDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::AdverseEvent(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::AllergyIntolerance(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Appointment(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::AppointmentResponse(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::AuditEvent(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Basic(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Binary(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::BiologicallyDerivedProduct(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::BodyStructure(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Bundle(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CapabilityStatement(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CarePlan(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CareTeam(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CatalogEntry(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ChargeItem(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ChargeItemDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Claim(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ClaimResponse(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ClinicalImpression(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CodeSystem(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Communication(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CommunicationRequest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CompartmentDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Composition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ConceptMap(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Condition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Consent(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Contract(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Coverage(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CoverageEligibilityRequest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::CoverageEligibilityResponse(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DetectedIssue(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Device(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DeviceDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DeviceMetric(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DeviceRequest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DeviceUseStatement(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DiagnosticReport(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DocumentManifest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::DocumentReference(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::EffectEvidenceSynthesis(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Encounter(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Endpoint(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::EnrollmentRequest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::EnrollmentResponse(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::EpisodeOfCare(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::EventDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Evidence(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::EvidenceVariable(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ExampleScenario(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ExplanationOfBenefit(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::FamilyMemberHistory(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Flag(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Goal(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::GraphDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Group(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::GuidanceResponse(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::HealthcareService(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ImagingStudy(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Immunization(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ImmunizationEvaluation(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ImmunizationRecommendation(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ImplementationGuide(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::InsurancePlan(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Invoice(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Library(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Linkage(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::List(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Location(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Measure(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MeasureReport(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Media(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Medication(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicationAdministration(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicationDispense(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicationKnowledge(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicationRequest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicationStatement(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProduct(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductAuthorization(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductContraindication(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductIndication(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductIngredient(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductInteraction(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductManufactured(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductPackaged(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductPharmaceutical(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MedicinalProductUndesirableEffect(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MessageDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MessageHeader(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::MolecularSequence(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::NamingSystem(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::NutritionOrder(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Observation(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ObservationDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::OperationDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::OperationOutcome(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Organization(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::OrganizationAffiliation(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Parameters(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::PaymentNotice(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::PaymentReconciliation(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Person(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::PlanDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Practitioner(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::PractitionerRole(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Procedure(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Provenance(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Questionnaire(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::QuestionnaireResponse(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::RelatedPerson(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::RequestGroup(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ResearchDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ResearchElementDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ResearchStudy(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ResearchSubject(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::RiskAssessment(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::RiskEvidenceSynthesis(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Schedule(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SearchParameter(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ServiceRequest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Slot(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Specimen(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SpecimenDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::StructureDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::StructureMap(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Subscription(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Substance(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SubstanceNucleicAcid(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SubstancePolymer(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SubstanceProtein(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SubstanceReferenceInformation(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SubstanceSourceMaterial(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SubstanceSpecification(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SupplyDelivery(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::SupplyRequest(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::Task(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::TerminologyCapabilities(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::TestReport(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::TestScript(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ValueSet(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::VerificationResult(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::ViewDefinition(p) => p.validate_invariants(self, evaluator),
            helios_fhir::r4::Resource::VisionPrescription(p) => p.validate_invariants(self, evaluator),
        }
    }
}
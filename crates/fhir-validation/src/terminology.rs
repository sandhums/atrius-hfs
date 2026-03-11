use crate::ValidationError;

/// Trait representing a terminology validation service.
///
/// This allows the validator to remain independent of the specific
/// terminology backend (Snowstorm, HAPI, local cache, etc.).
pub trait TerminologyService {
    fn member_of(
        &self,
        valueset_url: &str,
        system: Option<&str>,
        code: &str,
        display: Option<&str>,
    ) -> Result<bool, ValidationError>;
}
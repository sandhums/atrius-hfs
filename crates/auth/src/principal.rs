use chrono::{DateTime, Utc};

use crate::scope::ScopeSet;

/// Represents an authenticated identity extracted from a validated JWT.
///
/// Injected into Axum request extensions by the auth middleware after
/// successful token validation.
#[derive(Debug, Clone)]
pub struct Principal {
    /// The `sub` (subject) claim from the JWT.
    pub subject: String,
    /// The `iss` (issuer) claim from the JWT.
    pub issuer: String,
    /// SMART `fhirUser` claim when present in the access token.
    pub fhir_user: Option<String>,
    /// The tenant ID extracted from the configured JWT claim.
    pub tenant_id: Option<String>,
    /// Parsed SMART v2 scopes granted to this principal.
    pub scopes: ScopeSet,
    /// The `jti` (JWT ID) claim, if the token carried one. Informational —
    /// bearer access tokens are reusable, so this is not a single-use marker.
    pub jti: Option<String>,
    /// Token expiration time.
    pub expires_at: DateTime<Utc>,
    /// Additional claims from the JWT not captured in other fields.
    pub custom_claims: serde_json::Map<String, serde_json::Value>,
}

impl Principal {
    /// Returns the client/subject identifier.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the token issuer.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the SMART `fhirUser` claim when set on the access token.
    pub fn fhir_user(&self) -> Option<&str> {
        self.fhir_user.as_deref()
    }

    /// Returns the tenant ID if present in the token.
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Identity for audit `agent.who`: valid FHIR `fhirUser` reference, else `sub`.
    #[must_use]
    pub fn audit_agent_identity(&self) -> Option<&str> {
        if let Some(ref fu) = self.fhir_user
            && is_fhir_relative_reference(fu)
        {
            return Some(fu.as_str());
        }
        if !self.subject.is_empty() {
            return Some(self.subject.as_str());
        }
        None
    }
}

/// True when `value` looks like a FHIR relative reference (`ResourceType/id`).
///
/// Rejects pseudo-values such as `frontdesk/sweety` (resource type must start
/// with an uppercase ASCII letter).
#[must_use]
pub fn is_fhir_relative_reference(value: &str) -> bool {
    let Some((resource_type, id)) = value.split_once('/') else {
        return false;
    };
    if resource_type.is_empty() || id.is_empty() || id.contains('/') {
        return false;
    }
    resource_type
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_uppercase())
        && resource_type.chars().all(|c| c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn principal(subject: &str, fhir_user: Option<&str>) -> Principal {
        Principal {
            subject: subject.to_string(),
            issuer: "https://idp.example/realms/fhir".to_string(),
            fhir_user: fhir_user.map(str::to_string),
            tenant_id: None,
            scopes: ScopeSet::empty(),
            jti: None,
            expires_at: Utc::now(),
            custom_claims: serde_json::Map::new(),
        }
    }

    #[test]
    fn audit_agent_identity_prefers_valid_fhir_user() {
        let p = principal("uuid-sub", Some("Practitioner/dr-patel"));
        assert_eq!(p.audit_agent_identity(), Some("Practitioner/dr-patel"));
    }

    #[test]
    fn audit_agent_identity_falls_back_to_sub_for_invalid_fhir_user() {
        let p = principal("uuid-sub", Some("frontdesk/sweety"));
        assert_eq!(p.audit_agent_identity(), Some("uuid-sub"));
    }

    #[test]
    fn audit_agent_identity_uses_sub_when_fhir_user_absent() {
        let p = principal("uuid-sub", None);
        assert_eq!(p.audit_agent_identity(), Some("uuid-sub"));
    }

    #[test]
    fn is_fhir_relative_reference_accepts_practitioner() {
        assert!(is_fhir_relative_reference("Practitioner/dr-patel"));
        assert!(is_fhir_relative_reference("RelatedPerson/abc"));
    }

    #[test]
    fn is_fhir_relative_reference_rejects_lowercase_type() {
        assert!(!is_fhir_relative_reference("frontdesk/sweety"));
    }
}

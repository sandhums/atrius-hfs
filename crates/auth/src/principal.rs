use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use crate::scope::ScopeSet;

/// Default `iss` used by [`Principal::stub`]. Override with
/// [`Principal::with_issuer`] when a test asserts on the issuer.
const STUB_ISSUER: &str = "https://idp.example/realms/fhir";

/// Represents an authenticated identity extracted from a validated JWT.
///
/// Injected into Axum request extensions by the auth middleware after
/// successful token validation.
///
/// Marked `non_exhaustive` so fork-only fields (`fhir_user`, …) do not break
/// Helios tests that construct a `Principal` with a struct literal. Outside
/// this crate, build test principals with [`Principal::stub`]. Production
/// tokens still come from `JwksBearerAuthProvider` (same crate, so the
/// literal there is allowed).
#[derive(Debug, Clone)]
#[non_exhaustive]
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
    pub custom_claims: Map<String, Value>,
    /// SMART App Launch context bound to the token (`patient`, `encounter`,
    /// `fhirUser`), or `None` when the token carries none of those claims. The
    /// claims also stay in [`custom_claims`](Self::custom_claims).
    pub launch_context: Option<LaunchContext>,
}

impl Principal {
    /// Test helper: a principal with `subject` and `scopes`, and otherwise
    /// empty optional claims.
    ///
    /// Defaults: issuer [`STUB_ISSUER`], no `fhir_user` / tenant / `jti`,
    /// expiry one hour from now, empty custom claims. Chain
    /// [`with_issuer`](Self::with_issuer), [`with_tenant_id`](Self::with_tenant_id),
    /// or [`with_fhir_user`](Self::with_fhir_user) when a test needs those.
    #[must_use]
    pub fn stub(subject: impl Into<String>, scopes: ScopeSet) -> Self {
        Self {
            subject: subject.into(),
            issuer: STUB_ISSUER.to_string(),
            fhir_user: None,
            tenant_id: None,
            scopes,
            jti: None,
            expires_at: Utc::now() + chrono::Duration::hours(1),
            custom_claims: serde_json::Map::new(),
            launch_context: None,
        }
    }

    /// Override the token issuer (SMART `iss`).
    #[must_use]
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = issuer.into();
        self
    }

    /// Set the tenant claim used by path/header resolution tests.
    #[must_use]
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set the SMART `fhirUser` claim.
    #[must_use]
    pub fn with_fhir_user(mut self, fhir_user: impl Into<String>) -> Self {
        self.fhir_user = Some(fhir_user.into());
        self
    }

    /// Set the SMART launch context (`patient`, `encounter`, `fhirUser`).
    #[must_use]
    pub fn with_launch_context(mut self, launch_context: Option<LaunchContext>) -> Self {
        self.launch_context = launch_context;
        self
    }

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
    use serde_json::json;

    fn principal(subject: &str, fhir_user: Option<&str>) -> Principal {
        let p = Principal::stub(subject, ScopeSet::empty());
        match fhir_user {
            Some(fu) => p.with_fhir_user(fu),
            None => p,
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

    #[test]
    fn stub_defaults_fhir_user_and_tenant_to_none() {
        let p = Principal::stub("sub", ScopeSet::empty());
        assert_eq!(p.subject(), "sub");
        assert_eq!(p.issuer(), super::STUB_ISSUER);
        assert_eq!(p.fhir_user(), None);
        assert_eq!(p.tenant_id(), None);
        let p = p.with_tenant_id("acme").with_fhir_user("Practitioner/1");
        assert_eq!(p.tenant_id(), Some("acme"));
        assert_eq!(p.fhir_user(), Some("Practitioner/1"));
    }

    fn claims(value: Value) -> Map<String, Value> {
        value.as_object().expect("object").clone()
    }

    fn read_default(map: &Map<String, Value>) -> Option<LaunchContext> {
        LaunchContext::from_claims(map, "patient", "encounter", "fhirUser")
    }

    fn with_patient(patient: Option<&str>) -> LaunchContext {
        LaunchContext {
            patient: patient.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn from_claims_reads_all_three_default_claims() {
        let map = claims(json!({
            "patient": "p1",
            "encounter": "e1",
            "fhirUser": "Practitioner/123",
        }));
        assert_eq!(
            read_default(&map),
            Some(LaunchContext {
                patient: Some("p1".into()),
                encounter: Some("e1".into()),
                fhir_user: Some("Practitioner/123".into()),
            })
        );
    }

    #[test]
    fn from_claims_is_none_when_nothing_is_present() {
        assert_eq!(read_default(&claims(json!({"sub": "u"}))), None);
    }

    #[test]
    fn from_claims_ignores_empty_and_non_string_values() {
        let map = claims(json!({
            "patient": "",
            "encounter": 42,
            "fhirUser": {"reference": "Practitioner/1"},
        }));
        assert_eq!(read_default(&map), None);

        let map = claims(json!({"patient": "", "encounter": "e1"}));
        let ctx = read_default(&map).expect("encounter present");
        assert_eq!(ctx.patient, None);
        assert_eq!(ctx.encounter.as_deref(), Some("e1"));
    }

    #[test]
    fn from_claims_honours_custom_names_only() {
        let map = claims(json!({"patient": "p1", "launch_patient": "p9"}));
        let ctx = LaunchContext::from_claims(&map, "launch_patient", "encounter", "fhirUser")
            .expect("custom claim present");
        assert_eq!(ctx.patient.as_deref(), Some("p9"));

        // The default-named claim is not read when a different name is configured.
        let map = claims(json!({"patient": "p1"}));
        assert_eq!(
            LaunchContext::from_claims(&map, "launch_patient", "encounter", "fhirUser"),
            None
        );
    }

    #[test]
    fn patient_id_accepts_bare_and_relative_reference_forms() {
        assert_eq!(with_patient(Some("p1")).patient_id(), Some("p1"));
        assert_eq!(with_patient(Some("Patient/p1")).patient_id(), Some("p1"));
        assert_eq!(
            with_patient(Some("Patient/a.b-C9")).patient_id(),
            Some("a.b-C9")
        );
    }

    #[test]
    fn patient_id_rejects_unusable_values() {
        let too_long = "a".repeat(65);
        for bad in [
            None,
            Some(""),
            Some("Patient/"),
            Some("Group/g1"),
            Some("https://x.example/fhir/Patient/p1"),
            Some("Patient/p1/_history/1"),
            Some("p 1"),
            Some(too_long.as_str()),
        ] {
            assert_eq!(with_patient(bad).patient_id(), None, "{bad:?}");
        }
        // The 64-character boundary is accepted.
        let max = "a".repeat(64);
        assert_eq!(with_patient(Some(&max)).patient_id(), Some(max.as_str()));
    }
}

/// The SMART App Launch context the authorization server bound to an access
/// token, read from the claims named by `HFS_AUTH_PATIENT_CLAIM`,
/// `HFS_AUTH_ENCOUNTER_CLAIM` and `HFS_AUTH_FHIR_USER_CLAIM`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaunchContext {
    /// The patient claim as issued, normally a bare Patient id.
    pub patient: Option<String>,
    /// The encounter claim as issued.
    pub encounter: Option<String>,
    /// The fhirUser claim as issued, e.g. `Practitioner/123`.
    pub fhir_user: Option<String>,
}

impl LaunchContext {
    /// Reads the launch context from the named claims.
    ///
    /// Only non-empty string claims count. Returns `None` when none of the
    /// three is present.
    pub fn from_claims(
        claims: &Map<String, Value>,
        patient_claim: &str,
        encounter_claim: &str,
        fhir_user_claim: &str,
    ) -> Option<Self> {
        let read = |name: &str| {
            claims
                .get(name)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .map(String::from)
        };
        let ctx = Self {
            patient: read(patient_claim),
            encounter: read(encounter_claim),
            fhir_user: read(fhir_user_claim),
        };
        if ctx.patient.is_none() && ctx.encounter.is_none() && ctx.fhir_user.is_none() {
            None
        } else {
            Some(ctx)
        }
    }

    /// The launch patient's logical id, if the patient claim names a usable one.
    ///
    /// A bare `<id>` or a `Patient/<id>` relative reference is accepted; any
    /// other form (another resource type, an absolute URL, `_history`, empty)
    /// yields `None`.
    pub fn patient_id(&self) -> Option<&str> {
        let raw = self.patient.as_deref()?;
        let id = raw.strip_prefix("Patient/").unwrap_or(raw);
        is_fhir_id(id).then_some(id)
    }
}

/// FHIR `id` syntax: 1 to 64 characters of `[A-Za-z0-9\-.]`.
fn is_fhir_id(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
}

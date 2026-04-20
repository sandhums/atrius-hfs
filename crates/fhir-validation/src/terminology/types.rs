#[derive(Debug, Clone)]
pub struct TerminologyMembershipOutcome {
    pub is_member: bool,
    /// True when local terminology cannot decide membership (e.g. ValueSet composition not
    /// materialized locally). This is **not** a proven non-member result.
    pub remote_validation_required: bool,
    pub message: Option<String>,
    pub diagnostics: Vec<String>,
    pub system: Option<String>,
    pub code: Option<String>,
    pub version: Option<String>,
    pub display: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TerminologyRemoteError {
    pub status: Option<u16>,
    pub diagnostics: Vec<String>,
    pub raw_body: Option<String>,
}

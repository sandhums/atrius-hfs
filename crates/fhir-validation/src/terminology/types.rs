#[derive(Debug, Clone)]
pub struct TerminologyMembershipOutcome {
    pub is_member: bool,
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
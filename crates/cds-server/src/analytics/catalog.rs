//! Schema-grounded ViewDefinition / SQLQuery catalog for `$nl-views`.
//!
//! Ranking is token overlap against a checked-in catalog — the same fail-closed
//! idea as Helios `$nl-search` (grounded vocabulary, no PHI, no execute), without
//! calling an LLM or touching `crates/rest`.

use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

const EMBEDDED_CATALOG_JSON: &str = include_str!("default_catalog.json");

/// Maximum natural-language input length (characters).
pub const MAX_NL_TEXT_CHARS: usize = 512;

const STOPWORDS: &[&str] = &[
    "a", "an", "the", "of", "to", "for", "and", "or", "in", "on", "per", "with", "from", "that",
    "this", "those", "these", "is", "are", "be", "by", "at", "as", "it", "its", "vs",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ViewCatalog {
    pub version: String,
    pub entries: Vec<CatalogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogEntry {
    pub kind: CatalogKind,
    pub id: String,
    pub canonical: String,
    pub title: String,
    pub description: String,
    pub resource: String,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    pub run_hint: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogKind {
    ViewDefinition,
    SqlQuery,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogSuggestion {
    pub score: u32,
    #[serde(flatten)]
    pub entry: CatalogEntry,
}

impl ViewCatalog {
    /// Bronze + SQLQuery pack shipped with cds-server.
    pub fn embedded() -> Self {
        serde_json::from_str(EMBEDDED_CATALOG_JSON)
            .expect("embedded analytics catalog is valid JSON")
    }

    pub fn load_file(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read analytics catalog {}: {e}", path.display()))?;
        let catalog: Self = serde_json::from_str(&text)
            .map_err(|e| format!("parse analytics catalog {}: {e}", path.display()))?;
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.entries.is_empty() {
            return Err("analytics catalog has no entries".into());
        }
        let mut seen = HashSet::new();
        for entry in &self.entries {
            if entry.id.trim().is_empty() || entry.canonical.trim().is_empty() {
                return Err("catalog entry missing id or canonical".into());
            }
            if !seen.insert(entry.canonical.clone()) {
                return Err(format!("duplicate catalog canonical {}", entry.canonical));
            }
        }
        Ok(())
    }

    /// Rank catalog entries for `text`. Never executes a view or search.
    pub fn suggest(&self, text: &str, limit: usize) -> Vec<CatalogSuggestion> {
        let tokens = tokenize(text);
        if tokens.is_empty() {
            return Vec::new();
        }
        let mut scored: Vec<CatalogSuggestion> = self
            .entries
            .iter()
            .filter_map(|entry| {
                let score = score_entry(entry, &tokens);
                (score > 0).then(|| CatalogSuggestion {
                    score,
                    entry: entry.clone(),
                })
            })
            .collect();
        scored.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.entry.id.cmp(&b.entry.id))
        });
        scored.truncate(limit.max(1));
        scored
    }
}

fn tokenize(text: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    for raw in text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '-')) {
        let token = raw.trim().trim_matches('-').to_ascii_lowercase();
        if token.len() < 2 && token != "bp" {
            continue;
        }
        if STOPWORDS.contains(&token.as_str()) {
            continue;
        }
        out.insert(token);
    }
    out
}

fn score_entry(entry: &CatalogEntry, tokens: &HashSet<String>) -> u32 {
    let mut score = 0u32;
    let resource = entry.resource.to_ascii_lowercase();
    if tokens.contains(&resource) {
        score += 6;
    }
    if tokens.contains(&entry.id.to_ascii_lowercase()) {
        score += 5;
    }
    for kw in &entry.keywords {
        let k = kw.to_ascii_lowercase();
        if tokens.contains(&k) {
            score += 4;
        }
    }
    for token in tokenize(&entry.title) {
        if tokens.contains(&token) {
            score += 2;
        }
    }
    for token in tokenize(&entry.description) {
        if tokens.contains(&token) {
            score += 1;
        }
    }
    if matches!(entry.kind, CatalogKind::SqlQuery)
        && (tokens.contains("latest")
            || tokens.contains("join")
            || tokens.contains("diagnoses")
            || tokens.contains("line"))
    {
        score = score.saturating_add(2);
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_is_valid() {
        let catalog = ViewCatalog::embedded();
        catalog.validate().unwrap();
        assert!(catalog.entries.len() >= 10);
    }

    #[test]
    fn suggests_latest_hba1c_sqlquery() {
        let catalog = ViewCatalog::embedded();
        let hits = catalog.suggest("latest hba1c per patient", 5);
        assert!(!hits.is_empty(), "{hits:?}");
        assert_eq!(hits[0].entry.id, "atrius-in-patient-latest-observation");
        assert_eq!(hits[0].entry.kind, CatalogKind::SqlQuery);
    }

    #[test]
    fn suggests_encounter_diagnoses_for_join_language() {
        let catalog = ViewCatalog::embedded();
        let hits = catalog.suggest("encounter diagnoses with charge totals", 5);
        assert_eq!(hits[0].entry.id, "atrius-in-encounter-diagnoses");
    }

    #[test]
    fn unknown_text_has_no_suggestions() {
        let catalog = ViewCatalog::embedded();
        let hits = catalog.suggest("launch the missiles", 5);
        assert!(hits.is_empty());
    }

    #[test]
    fn tokenize_drops_stopwords() {
        let tokens = tokenize("the latest of the Observation");
        assert!(tokens.contains("latest"));
        assert!(tokens.contains("observation"));
        assert!(!tokens.contains("the"));
        assert!(!tokens.contains("of"));
    }
}

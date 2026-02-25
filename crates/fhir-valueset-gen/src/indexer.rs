// Some HL7 CodeSystems are normative but their actual code list is generated from
// other parts of the specification and therefore is not published as `concept[]`
// in the `valuesets.json` bundle (e.g. http://hl7.org/fhir/message-events).
// For generation and local membership checks these must be treated as non-finite.
const DYNAMIC_CODE_SYSTEMS: &[&str] = &[
    "http://hl7.org/fhir/message-events",
];

fn is_dynamic_codesystem(url: &str) -> bool {
    DYNAMIC_CODE_SYSTEMS.iter().any(|u| *u == url)
}
use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Result};

use crate::models::{
    CodeSystem, CodeSystemConcept, ValueSet, ValueSetComposeIncludeConcept,
};

/// Information about a generated CodeSystem module/type.
#[derive(Debug, Clone)]
pub struct CodeSystemGenInfo {
    /// Canonical URL of the CodeSystem.
    pub url: String,
    /// Snake-case module/file stem (e.g. `observation_status`).
    pub module: String,
    /// Public Rust type stem (e.g. `ObservationStatus`).
    pub type_name: String,
    /// True when this CodeSystem has a finite concept list we can index locally.
    pub has_concepts: bool,
}

/// Flattened concept metadata for local lookups and for generating hierarchy tables.
#[derive(Debug, Clone)]
pub struct FlatConcept {
    pub code: String,
    pub display: Option<String>,
    pub definition: Option<String>,
    pub parent: Option<String>,
    pub level: u16,
}

/// Index result used by codegen.
#[derive(Debug)]
pub struct TerminologyIndex {
    /// Deterministic iteration order by module name.
    pub code_systems_by_module: BTreeMap<String, CodeSystemIndexed>,
    pub value_sets_by_module: BTreeMap<String, ValueSetIndexed>,

    /// Canonical URL -> CodeSystem generation info.
    pub cs_by_url: HashMap<String, CodeSystemGenInfo>,

    /// Canonical URL -> flattened finite concept list (only when we have concepts).
    pub cs_concepts_by_url: HashMap<String, Vec<FlatConcept>>,

    /// Canonical URL -> ValueSet module/type info.
    pub vs_by_url: HashMap<String, (String /*module*/, String /*type_name*/)> ,
}

#[derive(Debug)]
pub struct CodeSystemIndexed {
    pub info: CodeSystemGenInfo,
    pub cs: CodeSystem,
}

#[derive(Debug)]
pub struct ValueSetIndexed {
    pub url: String,
    pub module: String,
    pub type_name: String,
    /// True when this ValueSet is marked/used as an example in the spec bundles.
    /// These should generally not be enforced as hard errors for bindings with strength "example".
    pub is_example: bool,
    pub vs: ValueSet,
}

/// Build all indices required for terminology code generation.
///
/// This layer is intentionally deterministic and collision-safe:
/// - file/module stems are derived from URL tail
/// - public type names prefer `name`, but fall back to URL tail
/// - collisions get a stable `__<hash8>` suffix derived from the canonical URL
pub fn build_index(code_systems: Vec<CodeSystem>, value_sets: Vec<ValueSet>) -> Result<TerminologyIndex> {
    // -----------------------------
    // CodeSystems
    // -----------------------------
    let mut cs_by_module: BTreeMap<String, CodeSystemIndexed> = BTreeMap::new();
    let mut cs_by_url: HashMap<String, CodeSystemGenInfo> = HashMap::new();
    let mut cs_concepts_by_url: HashMap<String, Vec<FlatConcept>> = HashMap::new();

    let mut used_type_names: HashMap<String, String> = HashMap::new(); // type_name -> url
    let mut used_modules: HashMap<String, String> = HashMap::new(); // module -> url

    for cs in code_systems {
        // Skip examples (common in spec bundles)
        if let Some(id) = cs.id.as_deref() {
            if id.starts_with("example-") {
                continue;
            }
        }
        // Skip content-not-present entries
        if let Some(content) = cs.content.as_deref() {
            if content.starts_with("not-present") {
                continue;
            }
        }
        let Some(url) = cs.url.clone() else {
            // Skip entries without canonical URL.
            continue;
        };

        let base_module = codesystem_module_stem(&url);
        let module = make_unique_module(&base_module, &url, &mut used_modules);

        let base_type = codesystem_type_stem(&cs, &url);
        let type_name = make_unique_type(&base_type, &url, &mut used_type_names);

        // Finite means we have an explicit enumerated concept list we can flatten.
        // Some spec-generated (dynamic) systems are legitimately `content=complete`
        // but omit `concept[]` in the published bundle; treat those as non-finite.
        let has_concepts = cs.concept.as_ref().map(|v| !v.is_empty()).unwrap_or(false)
            && !is_dynamic_codesystem(&url);

        if !has_concepts {
            if let Some(content) = cs.content.as_deref() {
                if content == "complete" && !is_dynamic_codesystem(&url) {
                    println!("⚠️  Complete but no concepts: {}", url);
                }
            }
        }
        let cs_info = CodeSystemGenInfo {
            url: url.clone(),
            module: module.clone(),
            type_name: type_name.clone(),
            has_concepts,
        };

        // Build flattened concepts for finite systems.
        if has_concepts {
            if let Some(concepts) = cs.concept.as_ref() {
                let mut flat = Vec::new();
                flatten_codesystem_concepts(concepts, &mut flat, None, 0);
                cs_concepts_by_url.insert(url.clone(), flat);
            }
        }

        // Index by URL (dedupe: first wins)
        cs_by_url.entry(url.clone()).or_insert_with(|| cs_info.clone());

        // Deterministic by module
        cs_by_module.insert(
            module.clone(),
            CodeSystemIndexed {
                info: cs_info,
                cs,
            },
        );
    }

    // -----------------------------
    // ValueSets
    // -----------------------------
    let mut vs_by_module: BTreeMap<String, ValueSetIndexed> = BTreeMap::new();
    let mut vs_by_url: HashMap<String, (String, String)> = HashMap::new();

    for vs in value_sets {
        let Some(url) = vs.url.clone() else {
            continue;
        };

        // Keep example ValueSets, but tag them so downstream binding enforcement
        // can treat them as guidance (warning-only) instead of hard constraints.
        let is_example = is_example_valueset(&vs, &url);

        let base_module = valueset_module_stem(&url);
        let module = make_unique_module(&base_module, &url, &mut used_modules);

        let type_name = valueset_type_stem(&vs, &url);

        vs_by_url.entry(url.clone()).or_insert_with(|| (module.clone(), type_name.clone()));

        vs_by_module.insert(
            module.clone(),
            ValueSetIndexed {
                url,
                module,
                type_name,
                is_example,
                vs,
            },
        );
    }

    Ok(TerminologyIndex {
        code_systems_by_module: cs_by_module,
        value_sets_by_module: vs_by_module,
        cs_by_url,
        cs_concepts_by_url,
        vs_by_url,
    })
}

// -----------------------------------------------------------------------------
// Naming helpers
// -----------------------------------------------------------------------------

fn codesystem_module_stem(url: &str) -> String {
    url_tail_to_snake(url)
}

fn valueset_module_stem(url: &str) -> String {
    url_tail_to_snake(url)
}

fn codesystem_type_stem(cs: &CodeSystem, url: &str) -> String {
    if let Some(name) = cs.name.as_deref() {
        let s = sanitize_type_name(name);
        if !s.is_empty() { return s; }
    }
    sanitize_type_name(&url_tail_to_pascal(url))
}

fn valueset_type_stem(vs: &ValueSet, url: &str) -> String {
    if let Some(name) = vs.name.as_deref() {
        let s = sanitize_type_name(name);
        if !s.is_empty() { return s; }
    }
    sanitize_type_name(&url_tail_to_pascal(url))
}

fn make_unique_module(base: &str, url: &str, used: &mut HashMap<String, String>) -> String {
    if let Some(existing) = used.get(base) {
        if existing == url {
            return base.to_string();
        }
        let suffix = hash8(url);
        let candidate = format!("{base}__{suffix}");
        used.insert(candidate.clone(), url.to_string());
        return candidate;
    }
    used.insert(base.to_string(), url.to_string());
    base.to_string()
}

fn make_unique_type(base: &str, url: &str, used: &mut HashMap<String, String>) -> String {
    if let Some(existing) = used.get(base) {
        if existing == url {
            return base.to_string();
        }
        let suffix = hash8(url);
        let candidate = format!("{base}__{suffix}");
        used.insert(candidate.clone(), url.to_string());
        return candidate;
    }
    used.insert(base.to_string(), url.to_string());
    base.to_string()
}

fn url_tail_to_snake(url: &str) -> String {
    // Take last non-empty path segment.
    let tail = url
        .split('/')
        .filter(|s| !s.is_empty())
        .last()
        .unwrap_or("unnamed");

    let mut out = String::new();
    for ch in tail.chars() {
        let c = match ch {
            '-' | '.' => '_',
            c if c.is_ascii_alphanumeric() => c.to_ascii_lowercase(),
            _ => '_',
        };
        if !(out.ends_with('_') && c == '_') {
            out.push(c);
        }
    }

    if out.is_empty() { out = "unnamed".to_string(); }
    if out.chars().next().unwrap().is_ascii_digit() {
        out = format!("_{out}");
    }

    out
}

fn url_tail_to_pascal(url: &str) -> String {
    let snake = url_tail_to_snake(url);
    snake
        .split('_')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            match chars.next() {
                None => String::new(),
                Some(f) => f.to_ascii_uppercase().to_string() + chars.as_str(),
            }
        })
        .collect::<String>()
}

fn sanitize_type_name(name: &str) -> String {
    // Keep only alphanumerics, split on separators into PascalCase.
    let mut parts = Vec::<String>::new();
    let mut cur = String::new();

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            cur.push(ch);
        } else {
            if !cur.is_empty() {
                parts.push(cur.clone());
                cur.clear();
            }
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }

    let mut out = String::new();
    for p in parts {
        let mut chars = p.chars();
        if let Some(f) = chars.next() {
            out.push_str(&f.to_ascii_uppercase().to_string());
            out.push_str(chars.as_str());
        }
    }

    if out.is_empty() {
        return out;
    }

    // Rust keyword protection (minimal set; extend if needed)
    let lower = out.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "as" | "async" | "await" | "break" | "const" | "continue" | "crate" | "dyn" | "else" | "enum" | "extern" | "false"
        | "fn" | "for" | "if" | "impl" | "in" | "let" | "loop" | "match" | "mod" | "move" | "mut" | "pub" | "ref" | "return"
        | "self" | "Self" | "static" | "struct"  | "super" | "trait" | "true" | "type" | "unsafe" | "use" | "where" | "while"
        |  "union" | "box" | "abstract" | "become" | "do" | "final" | "gen" | "macro" | "override" | "priv" | "try" | "typeof" | "unsized" | "virtual"
        | "yield" | "raw" | "safe"
    ) {
        out.push_str("Type");
    }

    // Cannot start with digit
    if out.chars().next().unwrap().is_ascii_digit() {
        out = format!("T{out}");
    }

    out
}

// Stable hash (FNV-1a 64-bit), rendered as 8 hex chars.
fn hash8(s: &str) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:08x}", (h & 0xffff_ffff) as u32)
}
fn is_example_valueset(vs: &ValueSet, url: &str) -> bool {
    // Common patterns in spec bundles.
    if let Some(id) = vs.id.as_deref() {
        if id.starts_with("example-") || id == "example" {
            return true;
        }
    }
    // Some examples use url tail naming.
    let tail = url.split('/').filter(|s| !s.is_empty()).last().unwrap_or("");
    if tail.starts_with("example") {
        return true;
    }
    // Fallback: description text sometimes explicitly says it's an example.
    if let Some(desc) = vs.description.as_deref() {
        let d = desc.to_ascii_lowercase();
        if d.contains("example") {
            return true;
        }
    }
    false
}
// -----------------------------------------------------------------------------
// Concept flattening
// -----------------------------------------------------------------------------

fn flatten_codesystem_concepts(
    concepts: &[CodeSystemConcept],
    out: &mut Vec<FlatConcept>,
    parent: Option<&str>,
    level: u16,
) {
    for c in concepts {
        let code = c.code.clone();
        out.push(FlatConcept {
            code: code.clone(),
            display: c.display.clone(),
            definition: c.definition.clone(),
            parent: parent.map(|p| p.to_string()),
            level,
        });

        if let Some(children) = c.concept.as_ref() {
            if !children.is_empty() {
                flatten_codesystem_concepts(children, out, Some(&code), level.saturating_add(1));
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Optional: small helper for ValueSet inline concept extraction (used by codegen)
// -----------------------------------------------------------------------------

/// Extract explicit concepts from a ValueSet include block (system+concept list).
/// This is useful for local membership checks and for generating small enums.
pub fn extract_valueset_concepts(
    include: &crate::models::ValueSetComposeInclude,
) -> &[ValueSetComposeIncludeConcept] {
    include.concept.as_deref().unwrap_or(&[])
}

/// Basic sanity checks that can be used by generators to decide when local validation is possible.
pub fn valueset_has_nonlocal_rules(
    vs: &ValueSet,
    is_finite_codesystem: impl Fn(&str) -> bool,
) -> bool {
    let Some(compose) = vs.compose.as_ref() else { return true; };

    for inc in compose.include.as_deref().unwrap_or(&[]) {
        if inc.filter.as_ref().map(|f| !f.is_empty()).unwrap_or(false) {
            return true;
        }
        if inc.value_set.as_ref().map(|v| !v.is_empty()).unwrap_or(false) {
            return true;
        }

        // whole-system include: local ONLY if the CodeSystem is finite
        if let Some(sys) = inc.system.as_deref() {
            let has_inline = inc.concept.as_ref().map(|c| !c.is_empty()).unwrap_or(false);
            let has_filters = inc.filter.as_ref().map(|f| !f.is_empty()).unwrap_or(false);
            let has_vs_refs = inc.value_set.as_ref().map(|v| !v.is_empty()).unwrap_or(false);

            if !has_inline && !has_filters && !has_vs_refs {
                if !is_finite_codesystem(sys) {
                    return true;
                }
            }
        }
    }

    if compose.exclude.as_ref().map(|e| !e.is_empty()).unwrap_or(false) {
        return true;
    }

    false
}

// Not currently used, but handy when you later want to validate that URLs are unique.
#[allow(dead_code)]
fn assert_unique_urls(urls: impl Iterator<Item = String>) -> Result<()> {
    let mut seen = HashSet::new();
    for u in urls {
        if !seen.insert(u.clone()) {
            return Err(anyhow!("duplicate canonical url: {u}"));
        }
    }
    Ok(())
}
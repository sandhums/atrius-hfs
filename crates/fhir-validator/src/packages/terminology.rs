//! Offline ValueSet expansion from FHIR NPM packages.
//!
//! Closed expansions only: an include or exclude that needs a filter, an
//! incomplete CodeSystem, or a missing dependency is skipped. A skipped
//! ValueSet is unknown here (`member` returns `None`), which means "not
//! disproved" — the same contract as [`CoreTerminology`](crate::CoreTerminology).
//! When two packages define one url, the first listed package wins.

use crate::effects::{CodedValue, TerminologyError, TerminologyProvider};
use crate::packages::cache::PackageCache;
use crate::packages::error::PackageError;
use crate::packages::manifest::PackageId;
use crate::packages::scan::scan_package_dir;
use crate::resolver::strip_canonical_version;
use crate::terminology::{CoreTerminology, fhir_release};
use async_trait::async_trait;
use flate2::Compression;
use flate2::write::GzEncoder;
use helios_fhir::FhirVersion;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

const UNCLOSED_EXT: &str = "http://hl7.org/fhir/StructureDefinition/valueset-unclosed";

/// One code in an expanded ValueSet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminologyCode {
    pub system: String,
    pub code: String,
    pub display: String,
}

/// A ValueSet this layer could expand offline.
#[derive(Debug, Clone)]
pub struct ExpandedValueSet {
    pub url: String,
    pub version: String,
    pub name: String,
    /// Sorted by system, then code.
    pub codes: Vec<TerminologyCode>,
    /// `system|version` of each CodeSystem the expansion drew on, when the
    /// system was loaded. Enumerated includes of an external system contribute
    /// the bare system url.
    pub systems: Vec<String>,
    /// ValueSets named by `include.valueSet` / `exclude.valueSet`, bare urls.
    pub nested: Vec<String>,
    index: HashMap<String, Vec<String>>,
}

impl ExpandedValueSet {
    fn contains(&self, system: Option<&str>, code: &str) -> Option<bool> {
        let Some(systems) = self.index.get(code) else {
            return Some(false);
        };
        match system {
            Some(sys) => Some(systems.iter().any(|s| s == sys)),
            None => Some(true),
        }
    }
}

/// How many ValueSets one package contributed to the merged layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageReport {
    pub package: PackageId,
    pub expanded: Vec<String>,
    pub skipped: Vec<SkippedValueSet>,
}

/// A ValueSet whose compose could not be expanded offline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedValueSet {
    pub url: String,
    pub reason: String,
}

/// Per-package counts from [`PackageTerminology::from_packages`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminologyReport {
    pub packages: Vec<PackageReport>,
}

/// Expansions merged from one or more packages. The first package to define
/// a url owns it.
#[derive(Debug, Clone)]
pub struct PackageTerminology {
    by_url: BTreeMap<String, ExpandedValueSet>,
}

impl PackageTerminology {
    /// Load `packages` from `cache` in list order and expand every closed
    /// ValueSet. A package missing from the cache fails the load.
    pub fn from_packages(
        cache: &PackageCache,
        packages: &[PackageId],
    ) -> Result<(Self, TerminologyReport), PackageError> {
        let mut seen = BTreeSet::new();
        let mut loaded = Vec::new();
        for id in packages {
            if !seen.insert(id.clone()) {
                continue;
            }
            loaded.push(load_package(cache, id)?);
        }

        let mut systems: BTreeMap<String, CodeSystemDef> = BTreeMap::new();
        let mut value_sets: BTreeMap<String, ValueSetDef> = BTreeMap::new();
        for (index, pkg) in loaded.iter().enumerate() {
            for (url, system) in &pkg.systems {
                systems.entry(url.clone()).or_insert_with(|| system.clone());
            }
            for (url, vs) in &pkg.value_sets {
                value_sets
                    .entry(url.clone())
                    .or_insert_with(|| ValueSetDef {
                        package_index: index,
                        version: vs.version.clone(),
                        name: vs.name.clone(),
                        body: vs.body.clone(),
                    });
            }
        }

        let mut ctx = ExpandCtx {
            systems: &systems,
            value_sets: &value_sets,
            memo: HashMap::new(),
            stack: HashSet::new(),
        };
        let urls: Vec<String> = value_sets.keys().cloned().collect();
        for url in urls {
            let _ = ctx.expand(&url);
        }

        let mut by_url = BTreeMap::new();
        let mut expanded_by_pkg: Vec<Vec<String>> = vec![Vec::new(); loaded.len()];
        let mut skipped_by_pkg: Vec<Vec<SkippedValueSet>> = vec![Vec::new(); loaded.len()];
        for (url, def) in &value_sets {
            match ctx.memo.get(url) {
                Some(Ok(exp)) => {
                    expanded_by_pkg[def.package_index].push(url.clone());
                    by_url.insert(url.clone(), exp.to_value_set(url, def));
                }
                Some(Err(reason)) => skipped_by_pkg[def.package_index].push(SkippedValueSet {
                    url: url.clone(),
                    reason: reason.clone(),
                }),
                None => skipped_by_pkg[def.package_index].push(SkippedValueSet {
                    url: url.clone(),
                    reason: "not-expanded".to_string(),
                }),
            }
        }
        for list in &mut expanded_by_pkg {
            list.sort();
        }
        for list in &mut skipped_by_pkg {
            list.sort_by(|a, b| a.url.cmp(&b.url));
        }

        let report = TerminologyReport {
            packages: loaded
                .iter()
                .enumerate()
                .map(|(i, pkg)| PackageReport {
                    package: pkg.id.clone(),
                    expanded: std::mem::take(&mut expanded_by_pkg[i]),
                    skipped: std::mem::take(&mut skipped_by_pkg[i]),
                })
                .collect(),
        };
        Ok((Self { by_url }, report))
    }

    /// `Some` when this layer expanded `value_set` (version pin must match).
    pub fn get(&self, value_set: &str) -> Option<&ExpandedValueSet> {
        self.lookup(value_set)
    }

    pub fn knows(&self, value_set: &str) -> bool {
        self.lookup(value_set).is_some()
    }

    /// `Some(true/false)` when this layer expanded the ValueSet; `None` when
    /// it did not, or the value is not a coded shape this layer can judge.
    pub fn member(&self, value_set: &str, coded: &CodedValue) -> Option<bool> {
        let set = self.lookup(value_set)?;
        judge(set, coded)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ExpandedValueSet> {
        self.by_url.values()
    }

    /// Write the pack HIS and the profile generator read.
    ///
    /// `fhir_version` is the FHIR release string (`4.0.1`). `packages` is the
    /// header list (`name@version`), in the order they were loaded.
    /// `bound` empty writes every expansion. Otherwise only ValueSets
    /// reachable from those StructureDefinition bindings, plus nested
    /// `include` / `exclude` value sets, are written.
    pub fn write_pack(
        &self,
        fhir_version: &str,
        packages: &[PackageId],
        bound: &[BoundSource],
        cache: &PackageCache,
        out: &Path,
    ) -> Result<(), PackageError> {
        let selected = if bound.is_empty() {
            self.by_url.keys().cloned().collect::<BTreeSet<_>>()
        } else {
            let mut urls = binding_urls(cache, bound)?;
            urls.retain(|url| self.by_url.contains_key(url));
            close_nested(&urls, self)
        };
        let value_sets: Vec<PackValueSet> = selected
            .iter()
            .filter_map(|url| self.by_url.get(url).map(PackValueSet::from_expanded))
            .collect();
        let pack = PackFile {
            fhir_version: fhir_version.to_string(),
            packages: packages.iter().map(|id| id.to_string()).collect(),
            value_sets,
        };
        if let Some(parent) = out.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|e| PackageError::io(parent, e))?;
        }
        let file = File::create(out).map_err(|e| PackageError::io(out, e))?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        serde_json::to_writer(&mut encoder, &pack)
            .map_err(|e| PackageError::Invalid(format!("terminology pack encode: {e}")))?;
        encoder.finish().map_err(|e| PackageError::io(out, e))?;
        Ok(())
    }

    fn lookup(&self, value_set: &str) -> Option<&ExpandedValueSet> {
        if let Some((bare, pin)) = value_set.split_once('|') {
            let set = self.by_url.get(bare)?;
            (set.version == pin).then_some(set)
        } else {
            self.by_url.get(value_set)
        }
    }
}

/// StructureDefinitions whose bindings limit a pack.
///
/// `types` empty collects every StructureDefinition in `package`. Otherwise
/// only those whose `type` is listed.
#[derive(Debug, Clone)]
pub struct BoundSource {
    pub package: PackageId,
    pub types: Vec<String>,
}

/// Package layers, then the embedded core pack for one FHIR version.
/// The first layer that expanded the ValueSet answers.
#[derive(Clone)]
pub struct OfflineTerminology {
    pub packages: Vec<Arc<PackageTerminology>>,
    pub core: Option<Arc<CoreTerminology>>,
}

impl std::fmt::Debug for OfflineTerminology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OfflineTerminology")
            .field("packages", &self.packages.len())
            .field("core", &self.core.is_some())
            .finish()
    }
}

impl OfflineTerminology {
    pub fn member(&self, value_set: &str, coded: &CodedValue) -> Option<bool> {
        for pkg in &self.packages {
            if pkg.knows(value_set) {
                return pkg.member(value_set, coded);
            }
        }
        self.core
            .as_ref()
            .and_then(|core| core.member(value_set, coded))
    }
}

#[async_trait]
impl TerminologyProvider for OfflineTerminology {
    async fn validate_code(
        &self,
        value_set: &str,
        coded: &CodedValue,
    ) -> Result<bool, TerminologyError> {
        Ok(self.member(value_set, coded).unwrap_or(true))
    }
}

/// FHIR versions a package's terminology joins.
///
/// `explicit` is set for `HFS_TERMINOLOGY_PACKAGES_<VER>`: that version only,
/// even when `package.json` `fhirVersions` does not list it. Otherwise the
/// declared `fhirVersions` are mapped onto the versions this build enables.
pub fn terminology_layers(declared: &[String], explicit: Option<FhirVersion>) -> Vec<FhirVersion> {
    if let Some(version) = explicit {
        return vec![version];
    }
    let mut out = Vec::new();
    for raw in declared {
        if let Some(version) = fhir_version_for_package(raw)
            && !out.contains(&version)
        {
            out.push(version);
        }
    }
    out
}

/// `4.0.1` → `R4`, `4.3.0` → `R4B`, `5.0.0` → `R5`, `6.0.0` → `R6`.
/// `None` when this build did not enable that version.
pub fn fhir_version_for_package(declared: &str) -> Option<FhirVersion> {
    match fhir_release(declared)? {
        #[cfg(feature = "R4")]
        "4.0" => Some(FhirVersion::R4),
        #[cfg(feature = "R4B")]
        "4.3" => Some(FhirVersion::R4B),
        #[cfg(feature = "R5")]
        "5.0" => Some(FhirVersion::R5),
        #[cfg(feature = "R6")]
        "6.0" => Some(FhirVersion::R6),
        _ => None,
    }
}

#[derive(Clone)]
struct CodeSystemDef {
    version: String,
    complete: bool,
    codes: BTreeMap<String, String>,
}

struct ValueSetDef {
    package_index: usize,
    version: String,
    name: String,
    body: Value,
}

struct LoadedPackage {
    id: PackageId,
    systems: BTreeMap<String, CodeSystemDef>,
    value_sets: BTreeMap<String, RawValueSet>,
}

struct RawValueSet {
    version: String,
    name: String,
    body: Value,
}

struct ExpandCtx<'a> {
    systems: &'a BTreeMap<String, CodeSystemDef>,
    value_sets: &'a BTreeMap<String, ValueSetDef>,
    memo: HashMap<String, Result<Expansion, String>>,
    stack: HashSet<String>,
}

#[derive(Clone)]
struct Expansion {
    codes: BTreeMap<(String, String), String>,
    nested: BTreeSet<String>,
    systems: BTreeSet<String>,
}

impl Expansion {
    fn to_value_set(&self, url: &str, def: &ValueSetDef) -> ExpandedValueSet {
        let codes = self
            .codes
            .iter()
            .map(|((system, code), display)| TerminologyCode {
                system: system.clone(),
                code: code.clone(),
                display: display.clone(),
            })
            .collect::<Vec<_>>();
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        for code in &codes {
            index
                .entry(code.code.clone())
                .or_default()
                .push(code.system.clone());
        }
        ExpandedValueSet {
            url: url.to_string(),
            version: def.version.clone(),
            name: def.name.clone(),
            codes,
            systems: self.systems.iter().cloned().collect(),
            nested: self.nested.iter().cloned().collect(),
            index,
        }
    }
}

impl ExpandCtx<'_> {
    fn expand(&mut self, url: &str) -> Result<Expansion, String> {
        if let Some(done) = self.memo.get(url) {
            return done.clone();
        }
        if !self.stack.insert(url.to_string()) {
            return Err("cycle".to_string());
        }
        let outcome = self.expand_uncached(url);
        self.stack.remove(url);
        self.memo.insert(url.to_string(), outcome.clone());
        outcome
    }

    fn expand_uncached(&mut self, url: &str) -> Result<Expansion, String> {
        let Some(body) = self.value_sets.get(url).map(|def| def.body.clone()) else {
            return Err("missing-valueset".to_string());
        };
        if unclosed(&body) {
            return Err("unclosed".to_string());
        }
        let Some(compose) = body.get("compose").cloned() else {
            return Err("no-compose".to_string());
        };
        let includes = compose
            .get("include")
            .and_then(Value::as_array)
            .ok_or_else(|| "no-compose".to_string())?;
        let mut codes: BTreeMap<(String, String), String> = BTreeMap::new();
        let mut nested = BTreeSet::new();
        let mut systems = BTreeSet::new();
        if includes.is_empty() {
            return Err("no-compose".to_string());
        }
        for include in includes {
            let piece = self.include_codes(include, &mut nested, &mut systems)?;
            for (key, display) in piece {
                codes.entry(key).or_insert(display);
            }
        }
        if let Some(excludes) = compose.get("exclude").and_then(Value::as_array) {
            for exclude in excludes {
                self.apply_exclude(exclude, &mut codes, &mut nested, &mut systems)?;
            }
        }
        Ok(Expansion {
            codes,
            nested,
            systems,
        })
    }

    fn include_codes(
        &mut self,
        include: &Value,
        nested: &mut BTreeSet<String>,
        systems_out: &mut BTreeSet<String>,
    ) -> Result<BTreeMap<(String, String), String>, String> {
        if include.get("filter").is_some() {
            return Err("filter".to_string());
        }
        let mut parts: Vec<BTreeMap<(String, String), String>> = Vec::new();
        if let Some(system) = include.get("system").and_then(Value::as_str) {
            parts.push(self.system_codes(include, system, systems_out)?);
        }
        if let Some(refs) = include.get("valueSet").and_then(Value::as_array) {
            if refs.is_empty() && parts.is_empty() {
                return Err("unresolved-valueset".to_string());
            }
            for refer in refs {
                let Some(canonical) = refer.as_str() else {
                    return Err("unresolved-valueset".to_string());
                };
                parts.push(self.nested_codes(canonical, nested, systems_out)?);
            }
        }
        if parts.is_empty() {
            return Err("no-compose".to_string());
        }
        Ok(intersect(parts))
    }

    fn system_codes(
        &self,
        include: &Value,
        system: &str,
        systems_out: &mut BTreeSet<String>,
    ) -> Result<BTreeMap<(String, String), String>, String> {
        let pin = include.get("version").and_then(Value::as_str);
        let defined = resolve_system(self.systems, system, pin);
        if let Some(list) = include.get("concept").and_then(Value::as_array) {
            let mut out = BTreeMap::new();
            for concept in list {
                let Some(code) = concept.get("code").and_then(Value::as_str) else {
                    continue;
                };
                let display = concept
                    .get("display")
                    .and_then(Value::as_str)
                    .or_else(|| defined.and_then(|cs| cs.codes.get(code).map(String::as_str)))
                    .unwrap_or("")
                    .to_string();
                out.insert((system.to_string(), code.to_string()), display);
            }
            systems_out.insert(system_token(system, defined));
            return Ok(out);
        }
        let Some(cs) = defined else {
            return Err(if self.systems.contains_key(system) {
                "incomplete-codesystem".to_string()
            } else {
                "missing-codesystem".to_string()
            });
        };
        if !cs.complete {
            return Err("incomplete-codesystem".to_string());
        }
        systems_out.insert(system_token(system, Some(cs)));
        Ok(cs
            .codes
            .iter()
            .map(|(code, display)| ((system.to_string(), code.clone()), display.clone()))
            .collect())
    }

    fn nested_codes(
        &mut self,
        canonical: &str,
        nested: &mut BTreeSet<String>,
        systems_out: &mut BTreeSet<String>,
    ) -> Result<BTreeMap<(String, String), String>, String> {
        let bare = strip_canonical_version(canonical).to_string();
        nested.insert(bare.clone());
        if resolve_value_set(self.value_sets, canonical).is_none() {
            return Err("unresolved-valueset".to_string());
        }
        let expanded = self.expand(&bare)?;
        systems_out.extend(expanded.systems.iter().cloned());
        Ok(expanded.codes)
    }

    fn apply_exclude(
        &mut self,
        exclude: &Value,
        codes: &mut BTreeMap<(String, String), String>,
        nested: &mut BTreeSet<String>,
        systems_out: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        if exclude.get("filter").is_some() {
            return Err("exclude-filter".to_string());
        }
        let system = exclude.get("system").and_then(Value::as_str);
        let concepts = exclude.get("concept").and_then(Value::as_array);
        let refs = exclude.get("valueSet").and_then(Value::as_array);
        if system.is_none() && concepts.is_none() && refs.is_none() {
            return Err("bad-exclude".to_string());
        }
        if let Some(system) = system
            && concepts.is_none()
            && refs.is_none()
        {
            codes.retain(|(sys, _), _| sys != system);
            return Ok(());
        }
        if let Some(system) = system
            && let Some(concepts) = concepts
            && refs.is_none()
        {
            for concept in concepts {
                if let Some(code) = concept.get("code").and_then(Value::as_str) {
                    codes.remove(&(system.to_string(), code.to_string()));
                }
            }
            return Ok(());
        }
        let described = self.include_codes(exclude, nested, systems_out)?;
        for key in described.keys() {
            codes.remove(key);
        }
        Ok(())
    }
}

fn intersect(parts: Vec<BTreeMap<(String, String), String>>) -> BTreeMap<(String, String), String> {
    let mut iter = parts.into_iter();
    let Some(mut acc) = iter.next() else {
        return BTreeMap::new();
    };
    for other in iter {
        acc.retain(|key, display| match other.get(key) {
            Some(alt) => {
                if display.is_empty() {
                    *display = alt.clone();
                }
                true
            }
            None => false,
        });
    }
    acc
}

fn resolve_system<'a>(
    systems: &'a BTreeMap<String, CodeSystemDef>,
    system: &str,
    pin: Option<&str>,
) -> Option<&'a CodeSystemDef> {
    let (bare, embedded) = split_pin(system);
    let pin = pin.or(embedded);
    let def = systems.get(bare)?;
    match pin {
        Some(pin) if pin != def.version => None,
        _ => Some(def),
    }
}

fn resolve_value_set<'a>(
    value_sets: &'a BTreeMap<String, ValueSetDef>,
    canonical: &str,
) -> Option<&'a ValueSetDef> {
    let (bare, pin) = split_pin(canonical);
    let def = value_sets.get(bare)?;
    match pin {
        Some(pin) if pin != def.version => None,
        _ => Some(def),
    }
}

fn system_token(system: &str, def: Option<&CodeSystemDef>) -> String {
    match def {
        Some(def) if !def.version.is_empty() => format!("{system}|{}", def.version),
        _ => system.to_string(),
    }
}

fn split_pin(canonical: &str) -> (&str, Option<&str>) {
    match canonical.split_once('|') {
        Some((url, version)) if !version.is_empty() => (url, Some(version)),
        _ => (canonical, None),
    }
}

fn unclosed(vs: &Value) -> bool {
    let Some(exts) = vs.get("extension").and_then(Value::as_array) else {
        return false;
    };
    exts.iter().any(|ext| {
        let url = ext.get("url").and_then(Value::as_str).unwrap_or("");
        (url == UNCLOSED_EXT || url.ends_with("/valueset-unclosed"))
            && ext.get("valueBoolean").and_then(Value::as_bool) == Some(true)
    })
}

fn load_package(cache: &PackageCache, id: &PackageId) -> Result<LoadedPackage, PackageError> {
    let root = cache.get(id)?;
    let scanned = scan_package_dir(&root)?;
    let mut paths = scanned.code_system_paths;
    paths.extend(scanned.value_set_paths);
    paths.sort();
    paths.dedup();

    let mut systems = BTreeMap::new();
    let mut value_sets = BTreeMap::new();
    for path in paths {
        let text = fs::read_to_string(&path).map_err(|e| PackageError::io(&path, e))?;
        let value: Value = serde_json::from_str(&text).map_err(|e| {
            PackageError::Invalid(format!("{}: JSON parse error: {e}", path.display()))
        })?;
        collect_terminology(&value, &mut systems, &mut value_sets);
    }
    Ok(LoadedPackage {
        id: id.clone(),
        systems,
        value_sets,
    })
}

fn collect_terminology(
    value: &Value,
    systems: &mut BTreeMap<String, CodeSystemDef>,
    value_sets: &mut BTreeMap<String, RawValueSet>,
) {
    match value.get("resourceType").and_then(Value::as_str) {
        Some("CodeSystem") => {
            if let Some(def) = code_system(value) {
                systems.entry(def.0).or_insert(def.1);
            }
        }
        Some("ValueSet") => {
            if let Some(url) = value.get("url").and_then(Value::as_str) {
                value_sets.entry(url.to_string()).or_insert(RawValueSet {
                    version: value
                        .get("version")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    name: value
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    body: value.clone(),
                });
            }
        }
        Some("Bundle") => {
            if let Some(entries) = value.get("entry").and_then(Value::as_array) {
                for entry in entries {
                    if let Some(resource) = entry.get("resource") {
                        collect_terminology(resource, systems, value_sets);
                    }
                }
            }
        }
        _ => {}
    }
}

fn code_system(value: &Value) -> Option<(String, CodeSystemDef)> {
    let url = value.get("url").and_then(Value::as_str)?.to_string();
    let mut codes = BTreeMap::new();
    collect_concepts(value.get("concept").and_then(Value::as_array), &mut codes);
    Some((
        url,
        CodeSystemDef {
            version: value
                .get("version")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            complete: value.get("content").and_then(Value::as_str) == Some("complete"),
            codes,
        },
    ))
}

fn collect_concepts(concepts: Option<&Vec<Value>>, out: &mut BTreeMap<String, String>) {
    let Some(concepts) = concepts else {
        return;
    };
    for concept in concepts {
        if let Some(code) = concept.get("code").and_then(Value::as_str) {
            let display = concept
                .get("display")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            out.entry(code.to_string()).or_insert(display);
        }
        collect_concepts(concept.get("concept").and_then(Value::as_array), out);
    }
}

fn binding_urls(
    cache: &PackageCache,
    bound: &[BoundSource],
) -> Result<BTreeSet<String>, PackageError> {
    let mut urls = BTreeSet::new();
    for source in bound {
        let root = cache.get(&source.package)?;
        let scanned = scan_package_dir(&root)?;
        for sd in &scanned.structure_definitions {
            if !type_selected(sd, &source.types) {
                continue;
            }
            let elements = sd
                .get("snapshot")
                .and_then(|s| s.get("element"))
                .and_then(Value::as_array)
                .filter(|els| !els.is_empty())
                .or_else(|| {
                    sd.get("differential")
                        .and_then(|d| d.get("element"))
                        .and_then(Value::as_array)
                });
            let Some(elements) = elements else {
                continue;
            };
            for element in elements {
                if let Some(vs) = element
                    .get("binding")
                    .and_then(|b| b.get("valueSet"))
                    .and_then(Value::as_str)
                {
                    urls.insert(strip_canonical_version(vs).to_string());
                }
            }
        }
    }
    Ok(urls)
}

fn type_selected(sd: &Value, types: &[String]) -> bool {
    if types.is_empty() {
        return true;
    }
    sd.get("type")
        .and_then(Value::as_str)
        .is_some_and(|ty| types.iter().any(|want| want == ty))
}

fn close_nested(start: &BTreeSet<String>, term: &PackageTerminology) -> BTreeSet<String> {
    let mut out = start.clone();
    let mut stack: Vec<String> = start.iter().cloned().collect();
    while let Some(url) = stack.pop() {
        let Some(vs) = term.by_url.get(&url) else {
            continue;
        };
        for nested in &vs.nested {
            if out.insert(nested.clone()) {
                stack.push(nested.clone());
            }
        }
    }
    out
}

fn judge(set: &ExpandedValueSet, coded: &CodedValue) -> Option<bool> {
    match coded {
        CodedValue::Code(code) => set.contains(None, code),
        CodedValue::Coding(coding) => {
            let (system, code) = coding_parts(coding);
            set.contains(system, code?)
        }
        CodedValue::CodeableConcept(cc) => {
            let codings = cc
                .get("coding")
                .and_then(Value::as_array)
                .filter(|c| !c.is_empty())?;
            let mut saw_false = false;
            for coding in codings {
                let (system, code) = coding_parts(coding);
                let Some(code) = code else { continue };
                match set.contains(system, code) {
                    Some(true) => return Some(true),
                    Some(false) => saw_false = true,
                    None => return None,
                }
            }
            saw_false.then_some(false)
        }
    }
}

fn coding_parts(coding: &Value) -> (Option<&str>, Option<&str>) {
    (
        coding.get("system").and_then(Value::as_str),
        coding.get("code").and_then(Value::as_str),
    )
}

#[derive(Serialize)]
struct PackFile {
    #[serde(rename = "fhirVersion")]
    fhir_version: String,
    packages: Vec<String>,
    #[serde(rename = "valueSets")]
    value_sets: Vec<PackValueSet>,
}

#[derive(Serialize)]
struct PackValueSet {
    url: String,
    version: String,
    name: String,
    systems: Vec<String>,
    codes: Vec<PackCode>,
}

impl PackValueSet {
    fn from_expanded(vs: &ExpandedValueSet) -> Self {
        Self {
            url: vs.url.clone(),
            version: vs.version.clone(),
            name: vs.name.clone(),
            systems: vs.systems.clone(),
            codes: vs
                .codes
                .iter()
                .map(|code| PackCode {
                    system: code.system.clone(),
                    code: code.code.clone(),
                    display: code.display.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct PackCode {
    system: String,
    code: String,
    display: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write_package(root: &Path, name: &str, version: &str, files: &[(&str, Value)]) {
        let dir = root.join(name).join(version);
        fs::create_dir_all(&dir).unwrap();
        let manifest = json!({
            "name": name,
            "version": version,
            "fhirVersions": ["4.0.1"]
        });
        fs::write(dir.join("package.json"), manifest.to_string()).unwrap();
        for (file, body) in files {
            fs::write(dir.join(file), body.to_string()).unwrap();
        }
    }

    fn colors() -> Value {
        json!({
            "resourceType": "CodeSystem",
            "url": "http://example/colors",
            "version": "1.0.0",
            "content": "complete",
            "concept": [
                {
                    "code": "parent",
                    "display": "Parent",
                    "property": [{ "code": "notSelectable", "valueBoolean": true }],
                    "concept": [{ "code": "red", "display": "Red" }]
                },
                { "code": "blue", "display": "Blue" }
            ]
        })
    }

    fn vs(url: &str, name: &str, compose: Value) -> Value {
        json!({
            "resourceType": "ValueSet",
            "url": url,
            "version": "1.0.0",
            "name": name,
            "compose": compose
        })
    }

    fn load(cache: &PackageCache, ids: &[&str]) -> (PackageTerminology, TerminologyReport) {
        let ids: Vec<PackageId> = ids
            .iter()
            .map(|raw| PackageId::parse(raw).unwrap())
            .collect();
        PackageTerminology::from_packages(cache, &ids).unwrap()
    }

    fn codes_of(term: &PackageTerminology, url: &str) -> Vec<String> {
        term.get(url)
            .unwrap()
            .codes
            .iter()
            .map(|c| c.code.clone())
            .collect()
    }

    #[test]
    fn expands_closed_sets_and_skips_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        write_package(
            tmp.path(),
            "example.term",
            "1.0.0",
            &[
                ("CodeSystem-colors.json", colors()),
                (
                    "CodeSystem-fragment.json",
                    json!({
                        "resourceType": "CodeSystem",
                        "url": "http://example/fragment",
                        "content": "fragment",
                        "concept": [{ "code": "a", "display": "A" }]
                    }),
                ),
                (
                    "ValueSet-colors.json",
                    vs(
                        "http://example/vs/colors",
                        "Colors",
                        json!({ "include": [{ "system": "http://example/colors" }] }),
                    ),
                ),
                (
                    "ValueSet-enumerated.json",
                    vs(
                        "http://example/vs/enumerated",
                        "Enumerated",
                        json!({
                            "include": [{
                                "system": "http://snomed.info/sct",
                                "concept": [{ "code": "123", "display": "Snomed" }]
                            }]
                        }),
                    ),
                ),
                (
                    "ValueSet-filter.json",
                    vs(
                        "http://example/vs/filter",
                        "Filtered",
                        json!({
                            "include": [{
                                "system": "http://example/colors",
                                "filter": [{ "property": "parent", "op": "is-a", "value": "parent" }]
                            }]
                        }),
                    ),
                ),
                (
                    "ValueSet-child.json",
                    vs(
                        "http://example/vs/child",
                        "Child",
                        json!({
                            "include": [{
                                "system": "http://example/colors",
                                "concept": [
                                    { "code": "red", "display": "Red" },
                                    { "code": "green", "display": "Green" }
                                ]
                            }]
                        }),
                    ),
                ),
                (
                    "ValueSet-nested.json",
                    vs(
                        "http://example/vs/nested",
                        "Nested",
                        json!({ "include": [{ "valueSet": ["http://example/vs/colors"] }] }),
                    ),
                ),
                (
                    "ValueSet-both.json",
                    vs(
                        "http://example/vs/both",
                        "Both",
                        json!({
                            "include": [{
                                "valueSet": ["http://example/vs/colors", "http://example/vs/child"]
                            }]
                        }),
                    ),
                ),
                (
                    "ValueSet-exclude-filter.json",
                    vs(
                        "http://example/vs/exclude-filter",
                        "ExcludeFilter",
                        json!({
                            "include": [{ "system": "http://example/colors" }],
                            "exclude": [{
                                "system": "http://example/colors",
                                "filter": [{ "property": "parent", "op": "is-a", "value": "parent" }]
                            }]
                        }),
                    ),
                ),
                (
                    "ValueSet-intersect.json",
                    vs(
                        "http://example/vs/intersect",
                        "Intersect",
                        json!({
                            "include": [{
                                "system": "http://example/colors",
                                "valueSet": ["http://example/vs/child"]
                            }]
                        }),
                    ),
                ),
                (
                    "ValueSet-exclude.json",
                    vs(
                        "http://example/vs/exclude",
                        "Exclude",
                        json!({
                            "include": [{ "system": "http://example/colors" }],
                            "exclude": [{
                                "system": "http://example/colors",
                                "concept": [{ "code": "blue" }]
                            }]
                        }),
                    ),
                ),
                (
                    "ValueSet-exclude-system.json",
                    vs(
                        "http://example/vs/exclude-system",
                        "ExcludeSystem",
                        json!({
                            "include": [{ "system": "http://example/colors" }],
                            "exclude": [{ "system": "http://example/colors" }]
                        }),
                    ),
                ),
                (
                    "ValueSet-unclosed.json",
                    json!({
                        "resourceType": "ValueSet",
                        "url": "http://example/vs/unclosed",
                        "name": "Unclosed",
                        "extension": [{
                            "url": "http://hl7.org/fhir/StructureDefinition/valueset-unclosed",
                            "valueBoolean": true
                        }],
                        "compose": { "include": [{ "system": "http://example/colors" }] }
                    }),
                ),
                (
                    "ValueSet-incomplete.json",
                    vs(
                        "http://example/vs/incomplete",
                        "Incomplete",
                        json!({ "include": [{ "system": "http://example/fragment" }] }),
                    ),
                ),
                (
                    "StructureDefinition-demo.json",
                    json!({
                        "resourceType": "StructureDefinition",
                        "url": "http://example/StructureDefinition/demo",
                        "name": "Demo",
                        "type": "Observation",
                        "snapshot": {
                            "element": [{
                                "path": "Observation.code",
                                "binding": { "valueSet": "http://example/vs/nested|1.0.0" }
                            }]
                        }
                    }),
                ),
            ],
        );
        let cache = PackageCache::new(tmp.path());
        let (term, report) = load(&cache, &["example.term@1.0.0"]);

        assert_eq!(
            codes_of(&term, "http://example/vs/colors"),
            vec!["blue", "parent", "red"]
        );
        let displays: Vec<_> = term
            .get("http://example/vs/colors")
            .unwrap()
            .codes
            .iter()
            .map(|c| c.display.as_str())
            .collect();
        assert_eq!(displays, vec!["Blue", "Parent", "Red"]);
        assert_eq!(codes_of(&term, "http://example/vs/enumerated"), vec!["123"]);
        assert!(!term.knows("http://example/vs/filter"));
        assert_eq!(
            codes_of(&term, "http://example/vs/nested"),
            vec!["blue", "parent", "red"]
        );
        assert_eq!(codes_of(&term, "http://example/vs/intersect"), vec!["red"]);
        assert_eq!(codes_of(&term, "http://example/vs/both"), vec!["red"]);
        assert!(!term.knows("http://example/vs/exclude-filter"));
        assert_eq!(
            codes_of(&term, "http://example/vs/exclude"),
            vec!["parent", "red"]
        );
        assert!(
            term.get("http://example/vs/exclude-system")
                .unwrap()
                .codes
                .is_empty()
        );
        assert!(!term.knows("http://example/vs/unclosed"));
        assert!(!term.knows("http://example/vs/incomplete"));

        let skipped: Vec<_> = report.packages[0]
            .skipped
            .iter()
            .map(|s| (s.url.as_str(), s.reason.as_str()))
            .collect();
        assert!(skipped.contains(&("http://example/vs/filter", "filter")));
        assert!(skipped.contains(&("http://example/vs/unclosed", "unclosed")));
        assert!(skipped.contains(&("http://example/vs/incomplete", "incomplete-codesystem")));
        assert!(skipped.contains(&("http://example/vs/exclude-filter", "exclude-filter")));

        let red = CodedValue::Code("red".into());
        assert_eq!(term.member("http://example/vs/colors", &red), Some(true));
        assert_eq!(
            term.member("http://example/vs/colors", &CodedValue::Code("nope".into())),
            Some(false)
        );
        assert_eq!(term.member("http://example/vs/missing", &red), None);
        assert_eq!(
            term.member(
                "http://example/vs/colors|1.0.0",
                &CodedValue::Coding(json!({"system": "http://example/colors", "code": "red"}))
            ),
            Some(true)
        );
        assert_eq!(term.member("http://example/vs/colors|9.9.9", &red), None);

        let out = tmp.path().join("pack.json.gz");
        term.write_pack(
            "4.0.1",
            &[PackageId::new("example.term", "1.0.0")],
            &[BoundSource {
                package: PackageId::new("example.term", "1.0.0"),
                types: vec![],
            }],
            &cache,
            &out,
        )
        .unwrap();
        let file = File::open(&out).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut text = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut text).unwrap();
        let pack: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(pack["fhirVersion"], "4.0.1");
        assert_eq!(pack["packages"][0], "example.term@1.0.0");
        let urls: Vec<&str> = pack["valueSets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["url"].as_str().unwrap())
            .collect();
        assert!(urls.contains(&"http://example/vs/nested"));
        assert!(urls.contains(&"http://example/vs/colors"));
        assert!(!urls.contains(&"http://example/vs/enumerated"));
        assert!(!urls.contains(&"http://example/vs/filter"));
        assert_eq!(pack["valueSets"][0]["name"], "Colors");
    }

    #[test]
    fn first_listed_package_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let vs_a = vs(
            "http://example/vs/shared",
            "FromA",
            json!({
                "include": [{
                    "system": "http://example/sys",
                    "concept": [{ "code": "from-a" }]
                }]
            }),
        );
        let vs_b = vs(
            "http://example/vs/shared",
            "FromB",
            json!({
                "include": [{
                    "system": "http://example/sys",
                    "concept": [{ "code": "from-b" }]
                }]
            }),
        );
        write_package(
            tmp.path(),
            "example.a",
            "1.0.0",
            &[("ValueSet-shared.json", vs_a)],
        );
        write_package(
            tmp.path(),
            "example.b",
            "1.0.0",
            &[("ValueSet-shared.json", vs_b)],
        );
        let cache = PackageCache::new(tmp.path());
        let (ab, _) = load(&cache, &["example.a@1.0.0", "example.b@1.0.0"]);
        let (ba, _) = load(&cache, &["example.b@1.0.0", "example.a@1.0.0"]);
        assert_eq!(codes_of(&ab, "http://example/vs/shared"), vec!["from-a"]);
        assert_eq!(ab.get("http://example/vs/shared").unwrap().name, "FromA");
        assert_eq!(codes_of(&ba, "http://example/vs/shared"), vec!["from-b"]);
    }

    #[test]
    fn version_layers_follow_the_package_unless_pinned() {
        let declared = vec!["4.0.1".to_string()];
        assert_eq!(terminology_layers(&declared, None), vec![FhirVersion::R4]);
        #[cfg(feature = "R5")]
        assert_eq!(
            terminology_layers(&declared, Some(FhirVersion::R5)),
            vec![FhirVersion::R5]
        );

        let tmp = tempfile::tempdir().unwrap();
        write_package(
            tmp.path(),
            "example.term",
            "1.0.0",
            &[(
                "ValueSet-only.json",
                vs(
                    "http://example/vs/only",
                    "Only",
                    json!({
                        "include": [{
                            "system": "http://example/sys",
                            "concept": [{ "code": "only" }]
                        }]
                    }),
                ),
            )],
        );
        let cache = PackageCache::new(tmp.path());
        let (pkg, _) = load(&cache, &["example.term@1.0.0"]);
        let pkg = Arc::new(pkg);
        let r4 = OfflineTerminology {
            packages: vec![Arc::clone(&pkg)],
            core: Some(crate::core_terminology(FhirVersion::R4)),
        };
        let without = OfflineTerminology {
            packages: vec![],
            core: Some(crate::core_terminology(FhirVersion::R4)),
        };
        let only = CodedValue::Code("only".into());
        assert_eq!(r4.member("http://example/vs/only", &only), Some(true));
        assert_eq!(without.member("http://example/vs/only", &only), None);
        // A package that defines a core url answers before the embedded pack.
        let gender = "http://hl7.org/fhir/ValueSet/administrative-gender";
        assert_eq!(
            without.member(gender, &CodedValue::Code("male".into())),
            Some(true)
        );
        assert_eq!(
            r4.member(gender, &CodedValue::Code("male".into())),
            Some(true)
        );
    }

    #[test]
    fn package_layer_answers_before_core() {
        let tmp = tempfile::tempdir().unwrap();
        write_package(
            tmp.path(),
            "example.term",
            "1.0.0",
            &[(
                "ValueSet-gender.json",
                vs(
                    "http://hl7.org/fhir/ValueSet/administrative-gender",
                    "AdministrativeGender",
                    json!({
                        "include": [{
                            "system": "http://hl7.org/fhir/administrative-gender",
                            "concept": [{ "code": "custom" }]
                        }]
                    }),
                ),
            )],
        );
        let cache = PackageCache::new(tmp.path());
        let (pkg, _) = load(&cache, &["example.term@1.0.0"]);
        let offline = OfflineTerminology {
            packages: vec![Arc::new(pkg)],
            core: Some(crate::core_terminology(FhirVersion::R4)),
        };
        assert_eq!(
            offline.member(
                "http://hl7.org/fhir/ValueSet/administrative-gender",
                &CodedValue::Code("custom".into())
            ),
            Some(true)
        );
        assert_eq!(
            offline.member(
                "http://hl7.org/fhir/ValueSet/administrative-gender",
                &CodedValue::Code("male".into())
            ),
            Some(false)
        );
    }

    #[tokio::test]
    async fn unknown_value_set_is_not_disproved() {
        let offline = OfflineTerminology {
            packages: vec![],
            core: Some(crate::core_terminology(FhirVersion::R4)),
        };
        let ok = offline
            .validate_code(
                "http://example/vs/needs-a-server",
                &CodedValue::Code("x".into()),
            )
            .await
            .unwrap();
        assert!(ok);
    }
}

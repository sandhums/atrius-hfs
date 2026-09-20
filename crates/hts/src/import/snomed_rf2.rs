//! SNOMED CT RF2 importer.
//!
//! Reads a SNOMED CT RF2 distribution ZIP and imports active concepts,
//! preferred terms, and `Is-a` hierarchy edges into the HTS normalized schema.
//!
//! All active descriptions — in every language present in the archive,
//! including per-language description files shipped by national editions —
//! are imported as FHIR `concept.designation` entries tagged with the RF2
//! `languageCode` and a `use` Coding carrying the SNOMED description type
//! (FSN / synonym). Language reference sets are consulted to pick the
//! preferred synonym for the concept `display` (US English, then GB English)
//! and to emit `preferredForLanguage` designations for every
//! refset-preferred synonym — tagged with the synonym's bare RF2 language
//! and, for the known national-edition refsets, a BCP-47 dialect tag
//! (`en-US`, `da-DK`, `fr-CA`, …) — so `displayLanguage` /
//! `Accept-Language` requests resolve to the right term in any language.
//!
//! The imported language set can be restricted via [`LanguageFilter`]
//! (`HTS_IMPORT_LANGUAGES` / `--languages`); excluded per-language
//! Description and Language-refset files are skipped without being parsed,
//! and English is always retained.
//!
//! # Extension packages (`--extends`)
//!
//! A SNOMED *extension* release (e.g. the NRCeS India Drug Extension) ships
//! only its own module's components. Its `Is-a` and attribute relationships
//! point at concepts that live in the International release it was built
//! against, and those concepts are **not** in the extension ZIP. Imported on
//! its own, such a package therefore loses every relationship that crosses
//! the edition boundary — the extension becomes a forest of orphaned
//! subtrees with no path to the International hierarchy and no ingredient /
//! dose-form attributes, so `$subsumes` and `is-a` filters from International
//! groupers can never reach an extension concept.
//!
//! [`import_snomed_rf2_with`] with [`SnomedImportOptions::extends`] set layers
//! the extension **into** the already-loaded base edition row instead:
//! extension concepts are written under the base `(url, version)`, a
//! relationship destination is accepted when it exists in either the
//! extension or the base, and the base closure is rebuilt once afterwards.
//! This is how a SNOMED *edition* is defined (International + one or more
//! extensions) and costs no duplication of the base content. The extension's
//! module-dependency refset is checked against the base version and any
//! mismatch, together with the count of relationships that still could not be
//! resolved, is reported in [`ImportStats::errors`] as a warning.
//!
//! # ⚠️  LICENSE REQUIRED
//!
//! Real SNOMED CT data requires a license from SNOMED International.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::Path;

use helios_persistence::tenant::TenantContext;
use zip::ZipArchive;

use crate::error::HtsError;
use crate::import::BundleImportBackend;
use crate::import::ImportStats;
use crate::import::LanguageFilter;
use crate::import::bundle_builder::{
    BuilderConcept, BuilderDesignation, BuilderProperty, CodeSystemMeta, build_parsed_code_system,
};

// ── SNOMED CT constants ───────────────────────────────────────────────────────

const SNOMED_URL: &str = "http://snomed.info/sct";
const SNOMED_ID: &str = "snomed-ct";
const SNOMED_NAME: &str = "SNOMED_CT";
const SNOMED_TITLE: &str = "SNOMED CT";

const TYPE_FSN: &str = "900000000000003001";
const TYPE_SYNONYM: &str = "900000000000013009";
const IS_A_TYPE: &str = "116680003";
/// SNOMED CT core module — the `referencedComponentId` an extension's
/// module-dependency refset uses to declare which International release it
/// was built against.
const INTERNATIONAL_CORE_MODULE: &str = "900000000000207008";
/// Module dependency reference set id.
const MODULE_DEPENDENCY_REFSET: &str = "900000000000534007";

/// Language refset acceptability: preferred.
const ACCEPTABILITY_PREFERRED: &str = "900000000000548007";
/// US English language refset.
const REFSET_EN_US: &str = "900000000000509007";
/// GB English language refset.
const REFSET_EN_GB: &str = "900000000000508004";
/// Known language refsets with their BCP-47 dialect tags, covering the
/// published national-edition refsets. SCTIDs follow the dialect map shipped
/// by SNOMED International's Snowstorm server (`search.dialect.config.*`).
/// Refsets whose tag would add nothing over the description's own RF2
/// `languageCode` (e.g. the bare-`de` German refset) are intentionally
/// absent — every refset-preferred synonym already receives a
/// `preferredForLanguage` designation in its bare language.
const LANG_REFSET_DIALECTS: &[(&str, &str)] = &[
    (REFSET_EN_US, "en-US"),
    (REFSET_EN_GB, "en-GB"),
    ("554461000005103", "da-DK"),
    ("32570271000036106", "en-AU"),
    ("19491000087109", "en-CA"),
    ("21000220103", "en-IE"),
    ("271000210107", "en-NZ"),
    ("5641000179103", "es-UY"),
    ("71000181105", "et-EE"),
    ("21000172104", "fr-BE"),
    ("20581000087109", "fr-CA"),
    ("10031000315102", "fr-FR"),
    ("61000202103", "nb-NO"),
    ("91000202106", "nn-NO"),
    ("31000172101", "nl-BE"),
    ("31000146106", "nl-NL"),
    ("46011000052107", "sv-SE"),
];

/// FHIR designation-use system for `preferredForLanguage` (matches the
/// coding the `$expand` displayLanguage swap emits).
const HL7_TERM_MAINT_INFRA: &str = "http://terminology.hl7.org/CodeSystem/hl7TermMaintInfra";

/// Map from concept code to a list of `(type_id, destination_code)` pairs.
type RoleProps = HashMap<String, Vec<(String, String)>>;

/// Known SNOMED association refset IDs with their FHIR equivalence codes.
/// Each entry is (refset_id, fhir_equivalence, label_for_logging).
const ASSOC_REFSET_EQUIVALENCES: &[(&str, &str, &str)] = &[
    ("900000000000526001", "replaced-by", "REPLACED_BY"),
    ("900000000000527005", "equal", "SAME_AS"),
    ("900000000000528000", "wider", "WAS_A"),
    ("900000000000523009", "inexact", "POSSIBLY_EQUIVALENT_TO"),
];

// ── Public entry point ────────────────────────────────────────────────────────

/// One active RF2 description row.
#[derive(Debug, Clone)]
struct ParsedDescription {
    desc_id: String,
    concept_id: String,
    language: String,
    type_id: String,
    term: String,
}

/// Owned designation, converted to a borrowed [`BuilderDesignation`] per batch.
#[derive(Debug, Clone)]
struct OwnedDesignation {
    /// BCP-47 tag — RF2 `languageCode` (`en`, `de`, …) or a dialect tag
    /// (`en-US`) for language-refset-preferred synonyms.
    language: String,
    use_system: &'static str,
    /// SNOMED description type id, or `preferredForLanguage`.
    use_code: String,
    value: String,
}

/// Display term plus designations for one concept.
#[derive(Debug, Clone, Default)]
struct ConceptTerms {
    display: String,
    designations: Vec<OwnedDesignation>,
}

#[derive(Debug)]
struct SnomedParseResult {
    /// concept id → display term + designations.
    concept_terms: HashMap<String, ConceptTerms>,
    /// (child, parent) is-a edges.
    is_a_edges: Vec<(String, String)>,
    /// source_concept_id → Vec<(type_id, destination_concept_id)> for non-IS_A relationships.
    role_relationships: RoleProps,
    /// refset_id → Vec<(source_concept_id, target_concept_id)> from association refset files.
    association_refsets: RoleProps,
    release_version: Option<String>,
    /// RF2 namespace token from the concept file name (`INT`, `IN1000189`, …).
    namespace_token: String,
    /// International release dates the package's module-dependency refset
    /// declares for the core module (empty when the refset is absent).
    declared_base_versions: Vec<String>,
    /// Active relationships skipped because their destination was in neither
    /// the extension nor the base concept set.
    unresolved_relationships: usize,
    parse_errors: Vec<String>,
}

/// Options for [`import_snomed_rf2_with`].
#[derive(Debug, Clone, Default)]
pub struct SnomedImportOptions {
    /// Layer this package onto the already-loaded base edition stored under
    /// `http://snomed.info/sct` with exactly this `version` (e.g. `20260501`),
    /// instead of storing it as a standalone version. See the module docs.
    pub extends: Option<String>,
}

/// Import a SNOMED CT RF2 distribution ZIP through the given backend.
///
/// `languages` restricts which description languages are ingested as
/// designations (see [`LanguageFilter`]). English descriptions are always
/// retained because concept display selection and the `en-US`/`en-GB`
/// preference chain depend on them.
///
/// Equivalent to [`import_snomed_rf2_with`] with default options: the package
/// is stored as its own `(url, version)` row.
pub async fn import_snomed_rf2(
    backend: &dyn BundleImportBackend,
    ctx: &TenantContext,
    path: &Path,
    batch_size: usize,
    dry_run: bool,
    languages: &LanguageFilter,
) -> Result<ImportStats, HtsError> {
    import_snomed_rf2_with(
        backend,
        ctx,
        path,
        batch_size,
        dry_run,
        languages,
        &SnomedImportOptions::default(),
    )
    .await
}

/// [`import_snomed_rf2`] with [`SnomedImportOptions`].
pub async fn import_snomed_rf2_with(
    backend: &dyn BundleImportBackend,
    ctx: &TenantContext,
    path: &Path,
    batch_size: usize,
    dry_run: bool,
    languages: &LanguageFilter,
    options: &SnomedImportOptions,
) -> Result<ImportStats, HtsError> {
    const FORMAT: &str = "snomed-rf2";
    let batch_size = batch_size.max(1);

    // Layered mode: the base edition must already be loaded, and its concept
    // set is what makes cross-edition relationship destinations resolvable.
    let base_concepts: Option<HashSet<String>> = match options.extends.as_deref() {
        Some(base_version) => {
            let codes = backend
                .code_system_concept_codes(ctx, SNOMED_URL, base_version)
                .await?;
            if codes.is_empty() {
                return Err(HtsError::InvalidRequest(format!(
                    "--extends {base_version}: no concepts stored for {SNOMED_URL} version \
                     '{base_version}'. Import the base SNOMED CT release first, then layer \
                     the extension onto it."
                )));
            }
            eprintln!(
                "[{FORMAT}] layering onto {SNOMED_URL} version {base_version} ({} base concepts)",
                codes.len()
            );
            Some(codes)
        }
        None => None,
    };

    let path_owned = path.to_path_buf();
    let languages = languages.clone();
    let parsed = tokio::task::spawn_blocking(move || -> Result<SnomedParseResult, HtsError> {
        let (concept_path, desc_paths, rel_path, assoc_refset_paths, lang_refset_paths) =
            find_rf2_paths(&path_owned, &languages)?;
        let module_dependency_path = find_module_dependency_path(&path_owned)?;

        tracing::info!(
            concept_file = %concept_path,
            description_files = desc_paths.len(),
            relationship_file = %rel_path,
            assoc_refset_files = assoc_refset_paths.len(),
            lang_refset_files = lang_refset_paths.len(),
            "Located RF2 files in archive"
        );

        let mut parse_errors: Vec<String> = Vec::new();

        let active_concepts = {
            let mut zip = open_zip(&path_owned)?;
            let entry = zip
                .by_name(&concept_path)
                .map_err(|e| HtsError::InvalidRequest(format!("Cannot open concept file: {e}")))?;
            parse_active_concepts(BufReader::new(entry), &mut parse_errors)
        };

        let concept_terms = {
            let mut by_desc_id: HashMap<String, (usize, ParsedDescription)> = HashMap::new();
            let mut next_order = 0usize;
            for desc_path in &desc_paths {
                let mut zip = open_zip(&path_owned)?;
                let entry = zip.by_name(desc_path).map_err(|e| {
                    HtsError::InvalidRequest(format!("Cannot open description file: {e}"))
                })?;
                parse_descriptions(
                    BufReader::new(entry),
                    &active_concepts,
                    &languages,
                    &mut next_order,
                    &mut by_desc_id,
                    &mut parse_errors,
                );
            }

            let mut preferred_in: HashMap<String, HashSet<String>> = HashMap::new();
            for refset_path in &lang_refset_paths {
                let mut zip = open_zip(&path_owned)?;
                let entry = zip.by_name(refset_path).map_err(|e| {
                    HtsError::InvalidRequest(format!("Cannot open language refset file: {e}"))
                })?;
                parse_language_refsets(BufReader::new(entry), &mut preferred_in, &mut parse_errors);
            }

            build_concept_terms(by_desc_id, &preferred_in, &active_concepts)
        };

        let (is_a_edges, role_relationships, unresolved_relationships) = {
            let mut zip = open_zip(&path_owned)?;
            let entry = zip.by_name(&rel_path).map_err(|e| {
                HtsError::InvalidRequest(format!("Cannot open relationship file: {e}"))
            })?;
            parse_relationships(
                BufReader::new(entry),
                &active_concepts,
                base_concepts.as_ref(),
                &mut parse_errors,
            )
        };

        let declared_base_versions = match module_dependency_path {
            Some(ref mdr_path) => {
                let mut zip = open_zip(&path_owned)?;
                let entry = zip.by_name(mdr_path).map_err(|e| {
                    HtsError::InvalidRequest(format!(
                        "Cannot open module dependency refset file: {e}"
                    ))
                })?;
                parse_module_dependency_targets(BufReader::new(entry), &mut parse_errors)
            }
            None => Vec::new(),
        };

        let association_refsets = {
            let mut merged: RoleProps = HashMap::new();
            for refset_path in &assoc_refset_paths {
                let mut zip = open_zip(&path_owned)?;
                let entry = zip.by_name(refset_path).map_err(|e| {
                    HtsError::InvalidRequest(format!("Cannot open association refset file: {e}"))
                })?;
                let partial = parse_association_refsets(BufReader::new(entry), &mut parse_errors);
                for (refset_id, mappings) in partial {
                    merged.entry(refset_id).or_default().extend(mappings);
                }
            }
            merged
        };

        let release_version = extract_release_date(&concept_path);
        let namespace_token = extension_namespace_token(&concept_path);

        Ok(SnomedParseResult {
            concept_terms,
            is_a_edges,
            role_relationships,
            association_refsets,
            release_version,
            namespace_token,
            declared_base_versions,
            unresolved_relationships,
            parse_errors,
        })
    })
    .await
    .map_err(|e| HtsError::Internal(format!("SNOMED parser panicked: {e}")))??;

    let SnomedParseResult {
        concept_terms,
        is_a_edges,
        role_relationships,
        association_refsets,
        release_version,
        namespace_token,
        declared_base_versions,
        unresolved_relationships,
        parse_errors,
    } = parsed;

    let concept_count = concept_terms.len() as u32;
    let edge_count = is_a_edges.len();
    let role_count: usize = role_relationships.values().map(|v| v.len()).sum();
    let assoc_count: usize = association_refsets.values().map(|v| v.len()).sum();

    let mut stats = ImportStats {
        code_systems: 1,
        errors: parse_errors,
        ..Default::default()
    };

    // Layered mode: the row we write into is the base edition's; the package's
    // own release date is recorded in the title so the stored CodeSystem says
    // what it contains. Report the dependency declaration and anything that
    // still failed to resolve — a mismatch between the declared and the loaded
    // International release is expected to leave a few dangling destinations
    // (concepts inactivated between the two releases), a large count means
    // the wrong base was chosen.
    let extension_release = release_version.clone().unwrap_or_else(|| "current".into());
    let layered_title;
    let (meta_version, title): (String, &str) = match options.extends.as_deref() {
        Some(base_version) => {
            if !declared_base_versions.is_empty()
                && !declared_base_versions.iter().any(|v| v == base_version)
            {
                stats.errors.push(format!(
                    "[{FORMAT}] module-dependency mismatch: the package declares it was built \
                     against SNOMED CT International {}, but is being layered onto version \
                     {base_version}. Concepts inactivated between those releases will leave \
                     unresolved relationships (see the unresolved count below).",
                    declared_base_versions.join(", ")
                ));
            }
            if unresolved_relationships > 0 {
                stats.errors.push(format!(
                    "[{FORMAT}] {unresolved_relationships} active relationship(s) skipped: \
                     destination concept is in neither the extension nor base version \
                     {base_version}"
                ));
            }
            layered_title =
                format!("{SNOMED_TITLE} ({base_version} + {namespace_token} {extension_release})");
            (base_version.to_string(), layered_title.as_str())
        }
        None => {
            if unresolved_relationships > 0 {
                stats.errors.push(format!(
                    "[{FORMAT}] {unresolved_relationships} active relationship(s) skipped: \
                     destination concept is not in this package. If this is an extension \
                     release, re-import with --extends <base-version> so its hierarchy \
                     joins the International edition it depends on{}.",
                    if declared_base_versions.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " (its module-dependency refset declares International {})",
                            declared_base_versions.join(", ")
                        )
                    }
                ));
            }
            (extension_release.clone(), SNOMED_TITLE)
        }
    };

    if dry_run {
        stats.concepts = concept_count;
        eprintln!(
            "[{FORMAT}] dry-run — would import {concept_count} concepts, {edge_count} Is-a edges, \
             {role_count} role relationships, {assoc_count} association refset mappings \
             into {SNOMED_URL} version {meta_version}"
        );
        return Ok(stats);
    }

    // Build child → parents map.
    let mut parents_of: HashMap<String, Vec<String>> = HashMap::new();
    for (child, parent) in &is_a_edges {
        parents_of
            .entry(child.clone())
            .or_default()
            .push(parent.clone());
    }

    let meta = CodeSystemMeta {
        id: SNOMED_ID,
        url: SNOMED_URL,
        version: Some(&meta_version),
        name: Some(SNOMED_NAME),
        title: Some(title),
        status: "active",
        content: "complete",
    };

    // Probe once before loading: a SNOMED edition is overwhelmingly the largest
    // designation source, so skipping the per-concept delete-before-reinsert on
    // a fresh load is the biggest single import win. Probe by canonical URL —
    // when no concepts exist yet this is a first-time load and every concept is
    // brand-new. A re-import (or a second edition sharing the URL) keeps the
    // safe replacement path. Probe failure falls back to the safe path.
    let fresh_load = !backend
        .code_system_has_concepts(ctx, SNOMED_URL)
        .await
        .unwrap_or(true);

    // Seed empty CodeSystem.
    let seed = build_parsed_code_system(&meta, &[]);
    let seed_stats = backend.import_parsed(ctx, seed).await?;
    stats.code_systems = seed_stats.code_systems;
    stats.errors.extend(seed_stats.errors);

    let concept_list: Vec<(String, ConceptTerms)> = concept_terms.into_iter().collect();
    let total = concept_list.len();
    let total_batches = total.div_ceil(batch_size).max(1);

    for (i, chunk) in concept_list.chunks(batch_size).enumerate() {
        let extras_per: Vec<Vec<BuilderProperty<'_>>> = chunk
            .iter()
            .map(|(code, _)| {
                // Additional parent edges (beyond the first, which goes in parent_code).
                let parent_extras = parents_of
                    .get(code)
                    .map(|parents| {
                        parents
                            .iter()
                            .skip(1)
                            .map(|p| BuilderProperty {
                                code: "parent",
                                value_key: "valueCode",
                                value: p.as_str(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                // Non-IS_A role relationships stored as concept properties.
                let role_extras = role_relationships
                    .get(code)
                    .map(|roles| {
                        roles
                            .iter()
                            .map(|(type_id, dest_id)| BuilderProperty {
                                code: type_id.as_str(),
                                value_key: "valueCode",
                                value: dest_id.as_str(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                [parent_extras, role_extras].concat()
            })
            .collect();

        let desig_sets: Vec<Vec<BuilderDesignation<'_>>> = chunk
            .iter()
            .map(|(_, terms)| {
                terms
                    .designations
                    .iter()
                    .map(|d| BuilderDesignation {
                        language: Some(d.language.as_str()),
                        use_system: Some(d.use_system),
                        use_code: Some(d.use_code.as_str()),
                        value: d.value.as_str(),
                    })
                    .collect()
            })
            .collect();

        let builder: Vec<BuilderConcept<'_>> = chunk
            .iter()
            .enumerate()
            .map(|(idx, (code, terms))| BuilderConcept {
                code: code.as_str(),
                display: Some(terms.display.as_str()).filter(|s| !s.is_empty()),
                parent_code: parents_of
                    .get(code)
                    .and_then(|p| p.first().map(|s| s.as_str())),
                extra_properties: extras_per[idx].as_slice(),
                designations: desig_sets[idx].as_slice(),
                ..Default::default()
            })
            .collect();

        let mut parsed = build_parsed_code_system(&meta, &builder);
        parsed.fresh_load = fresh_load;
        let chunk_stats = backend.import_parsed(ctx, parsed).await?;
        stats.errors.extend(chunk_stats.errors);
        stats.concepts += chunk.len() as u32;

        eprintln!(
            "[{FORMAT}] concept batch {}/{total_batches} — +{} concepts (total: {})",
            i + 1,
            chunk.len(),
            stats.concepts
        );
    }

    // Import association refsets as ConceptMap resources.
    if !association_refsets.is_empty() {
        eprintln!(
            "[{FORMAT}] importing {} association refset(s) as ConceptMaps…",
            association_refsets.len()
        );
        for (refset_id, mappings) in &association_refsets {
            let equivalence = ASSOC_REFSET_EQUIVALENCES
                .iter()
                .find(|(id, _, _)| *id == refset_id.as_str())
                .map(|(_, eq, _)| *eq)
                .unwrap_or("related-to");

            let bytes = build_assoc_refset_concept_map_bundle(
                refset_id,
                equivalence,
                mappings,
                &meta_version,
            );
            let cm_stats = backend.import_bundle(ctx, &bytes).await?;
            stats.concept_maps += cm_stats.concept_maps;
            stats.errors.extend(cm_stats.errors);
            eprintln!(
                "[{FORMAT}] imported ConceptMap for refset {refset_id} ({} mappings, equivalence={equivalence})",
                mappings.len()
            );
        }
    }

    Ok(stats)
}

// ── ZIP helpers ───────────────────────────────────────────────────────────────

fn open_zip(path: &Path) -> Result<ZipArchive<std::fs::File>, HtsError> {
    let file = std::fs::File::open(path)
        .map_err(|e| HtsError::InvalidRequest(format!("Cannot open {}: {e}", path.display())))?;
    ZipArchive::new(file)
        .map_err(|e| HtsError::InvalidRequest(format!("Not a valid ZIP archive: {e}")))
}

/// When an archive carries Full/Snapshot/Delta variants of the same content
/// (the standard RF2 distribution layout), keep only the Snapshot files so a
/// component's state is read exactly once.
fn prefer_snapshot(paths: Vec<String>) -> Vec<String> {
    let snapshots: Vec<String> = paths
        .iter()
        .filter(|p| p.to_lowercase().contains("snapshot"))
        .cloned()
        .collect();
    if snapshots.is_empty() {
        paths
    } else {
        snapshots
    }
}

/// Pick a single file from candidate paths, preferring Snapshot variants and
/// sorting for determinism. Production RF2 ZIPs ship `Full/` and `Snapshot/`
/// side by side; the Full variant must not be parsed by the snapshot-oriented
/// single-pass parsers.
fn pick_single(paths: Vec<String>) -> Option<String> {
    let mut paths = prefer_snapshot(paths);
    paths.sort();
    paths.into_iter().next()
}

/// `true` when descriptions in RF2 language `tag` should be imported under
/// `filter`. English always passes — display selection and the
/// `en-US`/`en-GB` preference chain depend on the English descriptions and
/// language refsets.
fn language_retained(filter: &LanguageFilter, tag: &str) -> bool {
    crate::language::lang_matches("en", tag) || filter.allows(tag)
}

/// Extract the language tag from an RF2 Description / Language-refset file
/// name, e.g. `sct2_Description_Snapshot-en_INT_20240101.txt` → `en`,
/// `der2_cRefset_LanguageSnapshot-da_DK1000005_20240331.txt` → `da`.
/// Returns `None` when the name carries no recognizable tag (such files are
/// kept and filtered row-by-row instead).
fn rf2_filename_language(path: &str) -> Option<String> {
    let fname = path.rsplit('/').next().unwrap_or(path).to_lowercase();
    for marker in ["snapshot-", "full-", "delta-"] {
        if let Some(pos) = fname.find(marker) {
            let tag: String = fname[pos + marker.len()..]
                .chars()
                .take_while(|c| c.is_ascii_alphabetic() || *c == '-')
                .collect();
            if !tag.is_empty() {
                return Some(tag);
            }
        }
    }
    None
}

/// Drop files whose filename language tag is excluded by `filter`. Files
/// without a recognizable tag are kept (their rows are filtered during
/// parsing instead).
fn retain_languages(paths: Vec<String>, filter: &LanguageFilter) -> Vec<String> {
    if filter.allows_all() {
        return paths;
    }
    paths
        .into_iter()
        .filter(|p| {
            let keep = rf2_filename_language(p)
                .map(|tag| language_retained(filter, &tag))
                .unwrap_or(true);
            if !keep {
                tracing::info!(file = %p, "skipping RF2 file excluded by language filter");
            }
            keep
        })
        .collect()
}

#[allow(clippy::type_complexity)]
fn find_rf2_paths(
    path: &Path,
    languages: &LanguageFilter,
) -> Result<(String, Vec<String>, String, Vec<String>, Vec<String>), HtsError> {
    let mut zip = open_zip(path)?;

    let mut concept_paths: Vec<String> = Vec::new();
    let mut desc_paths: Vec<String> = Vec::new();
    let mut rel_paths: Vec<String> = Vec::new();
    let mut assoc_refset_paths: Vec<String> = Vec::new();
    let mut lang_refset_paths: Vec<String> = Vec::new();

    for i in 0..zip.len() {
        let entry = zip
            .by_index(i)
            .map_err(|e| HtsError::InvalidRequest(format!("ZIP entry error: {e}")))?;
        let name = entry.name().to_string();

        if !name.ends_with(".txt") {
            continue;
        }
        let lower = name.to_lowercase();
        if lower.contains("refset") {
            if lower.contains("association") {
                assoc_refset_paths.push(name);
            } else if lower.contains("language") {
                lang_refset_paths.push(name);
            }
            continue;
        }

        if lower.contains("concept_") {
            concept_paths.push(name);
        } else if lower.contains("description_") {
            desc_paths.push(name);
        } else if lower.contains("relationship_") && !lower.contains("statedrelationship") {
            rel_paths.push(name);
        }
    }

    // National editions ship one Description file per language; sort for a
    // deterministic read order across platforms. Files in languages excluded
    // by the import filter are dropped here so they are never parsed.
    let mut desc_paths = retain_languages(prefer_snapshot(desc_paths), languages);
    desc_paths.sort();
    let mut lang_refset_paths = retain_languages(prefer_snapshot(lang_refset_paths), languages);
    lang_refset_paths.sort();
    let mut assoc_refset_paths = prefer_snapshot(assoc_refset_paths);
    assoc_refset_paths.sort();

    if desc_paths.is_empty() {
        return Err(HtsError::InvalidRequest(
            "No Description RF2 file found. Expected a file containing 'Description_' in its path."
                .into(),
        ));
    }

    Ok((
        pick_single(concept_paths).ok_or_else(|| {
            HtsError::InvalidRequest(
                "No Concept RF2 file found. Expected a file containing 'Concept_' in its path."
                    .into(),
            )
        })?,
        desc_paths,
        pick_single(rel_paths).ok_or_else(|| {
            HtsError::InvalidRequest(
                "No Relationship RF2 file found. Expected a file containing 'Relationship_' in its path."
                    .into(),
            )
        })?,
        assoc_refset_paths,
        lang_refset_paths,
    ))
}

/// Locate the module-dependency refset (`der2_ssRefset_ModuleDependency*`),
/// preferring the Snapshot copy. Absent in some minimal / test packages, so
/// `Ok(None)` is not an error.
fn find_module_dependency_path(path: &Path) -> Result<Option<String>, HtsError> {
    let mut zip = open_zip(path)?;
    let mut candidates: Vec<String> = Vec::new();
    for i in 0..zip.len() {
        let entry = zip
            .by_index(i)
            .map_err(|e| HtsError::InvalidRequest(format!("ZIP entry error: {e}")))?;
        let name = entry.name().to_string();
        let lower = name.to_lowercase();
        if lower.ends_with(".txt") && lower.contains("moduledependency") {
            candidates.push(name);
        }
    }
    Ok(pick_single(candidates))
}

/// The RF2 release-file namespace token — the `_`-delimited segment
/// immediately before the date in the concept file name, e.g. `IN1000189`
/// from `sct2_Concept_Snapshot_IN1000189_20260313T120000Z.txt` or `INT`
/// from `sct2_Concept_Snapshot_INT_20260501.txt`. Falls back to `extension`
/// when the name does not follow the RF2 pattern.
fn extension_namespace_token(concept_path: &str) -> String {
    let stem = concept_path.rsplit('/').next().unwrap_or(concept_path);
    let stem = stem.strip_suffix(".txt").unwrap_or(stem);
    let parts: Vec<&str> = stem.split('_').collect();
    match parts.as_slice() {
        [.., token, date]
            if date.len() >= 8
                && date.chars().take(8).all(|c| c.is_ascii_digit())
                && !token.is_empty() =>
        {
            (*token).to_string()
        }
        _ => "extension".to_string(),
    }
}

// ── RF2 parsers ───────────────────────────────────────────────────────────────

/// Parse the module-dependency refset and return the International core
/// module release dates (`targetEffectiveTime`) the package declares it
/// depends on, de-duplicated, in file order.
///
/// Columns: `id effectiveTime active moduleId refsetId referencedComponentId
/// sourceEffectiveTime targetEffectiveTime`.
fn parse_module_dependency_targets(reader: impl BufRead, errors: &mut Vec<String>) -> Vec<String> {
    let mut targets: Vec<String> = Vec::new();
    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line_num == 0 || line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(9, '\t').collect();
        if parts.len() < 8 {
            errors.push(format!(
                "Module dependency refset line {}: expected ≥8 fields, got {} — skipped",
                line_num + 1,
                parts.len()
            ));
            continue;
        }
        let active = parts[2].trim() == "1";
        let refset_id = parts[4].trim();
        let referenced = parts[5].trim();
        let target = parts[7].trim();
        if !active
            || refset_id != MODULE_DEPENDENCY_REFSET
            || referenced != INTERNATIONAL_CORE_MODULE
            || target.is_empty()
        {
            continue;
        }
        if !targets.iter().any(|t| t == target) {
            targets.push(target.to_string());
        }
    }
    targets
}

fn parse_active_concepts(reader: impl BufRead, errors: &mut Vec<String>) -> HashSet<String> {
    let mut active = HashSet::new();

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line_num == 0 || line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(6, '\t').collect();
        if parts.len() < 3 {
            errors.push(format!(
                "Concept RF2 line {}: expected ≥3 fields, got {} — skipped",
                line_num + 1,
                parts.len()
            ));
            continue;
        }

        let id = parts[0].trim().to_string();
        let is_active = parts[2].trim() == "1";

        if is_active {
            active.insert(id);
        } else {
            active.remove(&id);
        }
    }
    active
}

/// Parse one RF2 Description file into `by_desc_id`, keyed on description id
/// so re-stated rows (Full files, overlapping releases) keep last-state-wins
/// semantics. Inactive rows remove any earlier state. `next_order` preserves
/// first-seen file order for stable designation ordering.
fn parse_descriptions(
    reader: impl BufRead,
    active_concepts: &HashSet<String>,
    languages: &LanguageFilter,
    next_order: &mut usize,
    by_desc_id: &mut HashMap<String, (usize, ParsedDescription)>,
    errors: &mut Vec<String>,
) {
    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line_num == 0 || line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(10, '\t').collect();
        if parts.len() < 9 {
            errors.push(format!(
                "Description RF2 line {}: expected ≥9 fields, got {} — skipped",
                line_num + 1,
                parts.len()
            ));
            continue;
        }

        let desc_id = parts[0].trim();
        let active = parts[2].trim() == "1";
        let concept_id = parts[4].trim();
        let language = parts[5].trim();
        let type_id = parts[6].trim();
        let term = parts[7].trim();

        if !active {
            by_desc_id.remove(desc_id);
            continue;
        }
        if !active_concepts.contains(concept_id) || term.is_empty() {
            continue;
        }
        // Row-level language gate for mixed-language Description files
        // (file-level filtering already dropped per-language files whose
        // name carries an excluded tag).
        if !language_retained(languages, language) {
            continue;
        }

        let desc = ParsedDescription {
            desc_id: desc_id.to_string(),
            concept_id: concept_id.to_string(),
            language: language.to_string(),
            type_id: type_id.to_string(),
            term: term.to_string(),
        };
        match by_desc_id.entry(desc_id.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().1 = desc;
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert((*next_order, desc));
                *next_order += 1;
            }
        }
    }
}

/// Parse an RF2 Language refset file, recording which descriptions are
/// *preferred* in which language refset.
///
/// Columns: `id effectiveTime active moduleId refsetId referencedComponentId
/// acceptabilityId`.
fn parse_language_refsets(
    reader: impl BufRead,
    preferred_in: &mut HashMap<String, HashSet<String>>,
    errors: &mut Vec<String>,
) {
    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line_num == 0 || line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(8, '\t').collect();
        if parts.len() < 7 {
            errors.push(format!(
                "Language refset line {}: expected ≥7 fields, got {} — skipped",
                line_num + 1,
                parts.len()
            ));
            continue;
        }

        let active = parts[2].trim() == "1";
        let refset_id = parts[4].trim();
        let desc_id = parts[5].trim();
        let acceptability = parts[6].trim();

        if desc_id.is_empty() || refset_id.is_empty() {
            continue;
        }
        let entry = preferred_in.entry(desc_id.to_string()).or_default();
        if active && acceptability == ACCEPTABILITY_PREFERRED {
            entry.insert(refset_id.to_string());
        } else {
            // Inactive or demoted-to-acceptable rows revoke a previously
            // recorded preference (last-state-wins across Full files).
            entry.remove(refset_id);
        }
    }
}

/// Assemble per-concept display + designations from parsed descriptions.
///
/// Display selection: US-preferred English synonym → GB-preferred → any
/// refset-preferred English synonym → first English synonym → English FSN →
/// first synonym in any language → first FSN in any language.
///
/// Designations carry every active description (all languages), synonyms
/// before FSNs and refset-preferred terms first within each group, so
/// language-keyed first-match lookups resolve to the preferred term.
/// Every refset-preferred synonym additionally gets a `preferredForLanguage`
/// designation in its bare RF2 language, plus dialect-tagged copies
/// (`en-US`, `da-DK`, `fr-CA`, …) for refsets in [`LANG_REFSET_DIALECTS`].
fn build_concept_terms(
    by_desc_id: HashMap<String, (usize, ParsedDescription)>,
    preferred_in: &HashMap<String, HashSet<String>>,
    active_concepts: &HashSet<String>,
) -> HashMap<String, ConceptTerms> {
    let is_preferred_in = |desc: &ParsedDescription, refset: &str| {
        preferred_in
            .get(&desc.desc_id)
            .is_some_and(|s| s.contains(refset))
    };
    let is_preferred_any = |desc: &ParsedDescription| {
        preferred_in
            .get(&desc.desc_id)
            .is_some_and(|s| !s.is_empty())
    };

    // Group by concept, preserving file order.
    let mut by_concept: HashMap<String, Vec<(usize, ParsedDescription)>> = HashMap::new();
    for (_, (order, desc)) in by_desc_id {
        by_concept
            .entry(desc.concept_id.clone())
            .or_default()
            .push((order, desc));
    }

    let mut terms: HashMap<String, ConceptTerms> = HashMap::with_capacity(active_concepts.len());
    for (concept_id, mut descs) in by_concept {
        // Synonyms before FSNs before anything else; refset-preferred first
        // within each group; original file order as the tie-breaker.
        descs.sort_by_key(|(order, d)| {
            let type_rank = match d.type_id.as_str() {
                TYPE_SYNONYM => 0u8,
                TYPE_FSN => 1,
                _ => 2,
            };
            (type_rank, u8::from(!is_preferred_any(d)), *order)
        });

        let en_synonym = |pred: &dyn Fn(&ParsedDescription) -> bool| {
            descs
                .iter()
                .find(|(_, d)| d.language == "en" && d.type_id == TYPE_SYNONYM && pred(d))
                .map(|(_, d)| d.term.clone())
        };
        let first_term = |pred: &dyn Fn(&ParsedDescription) -> bool| {
            descs
                .iter()
                .find(|(_, d)| pred(d))
                .map(|(_, d)| d.term.clone())
        };

        let display = en_synonym(&|d| is_preferred_in(d, REFSET_EN_US))
            .or_else(|| en_synonym(&|d| is_preferred_in(d, REFSET_EN_GB)))
            .or_else(|| en_synonym(&|d| is_preferred_any(d)))
            .or_else(|| en_synonym(&|_| true))
            .or_else(|| first_term(&|d| d.language == "en" && d.type_id == TYPE_FSN))
            .or_else(|| first_term(&|d| d.type_id == TYPE_SYNONYM))
            .or_else(|| first_term(&|d| d.type_id == TYPE_FSN))
            .unwrap_or_default();

        let mut designations: Vec<OwnedDesignation> = Vec::with_capacity(descs.len());
        for (_, d) in &descs {
            designations.push(OwnedDesignation {
                language: d.language.clone(),
                use_system: SNOMED_URL,
                use_code: d.type_id.clone(),
                value: d.term.clone(),
            });
            if d.type_id == TYPE_SYNONYM && is_preferred_any(d) {
                // One `preferredForLanguage` per distinct BCP-47 tag: the
                // known-dialect tags of the refsets this synonym is preferred
                // in (sorted for determinism), plus the bare RF2
                // `languageCode` so preference stays queryable for editions
                // whose refset is not in the dialect table (German, Spanish,
                // Swiss, …).
                let mut tags: Vec<&str> = LANG_REFSET_DIALECTS
                    .iter()
                    .filter(|(refset_id, _)| is_preferred_in(d, refset_id))
                    .map(|(_, dialect)| *dialect)
                    .collect();
                tags.push(d.language.as_str());
                tags.sort_unstable();
                tags.dedup();
                for tag in tags {
                    designations.push(OwnedDesignation {
                        language: tag.to_string(),
                        use_system: HL7_TERM_MAINT_INFRA,
                        use_code: "preferredForLanguage".to_string(),
                        value: d.term.clone(),
                    });
                }
            }
        }

        terms.insert(
            concept_id,
            ConceptTerms {
                display,
                designations,
            },
        );
    }

    // Active concepts without any description keep an (empty-display) entry.
    for concept_id in active_concepts {
        terms.entry(concept_id.clone()).or_default();
    }
    terms
}

/// Parse the RF2 Relationship file, returning both IS_A edges and role relationships.
///
/// A relationship is kept when it is active, its **source** is an active
/// concept of this package, and its **destination** is either an active
/// concept of this package or — in layered mode — a concept of the base
/// edition (`base_concepts`). Sources are never widened to the base: only the
/// package's own concepts are written, so an edge hanging off a base concept
/// would have nowhere to go.
///
/// Returns `(is_a_edges, role_props, unresolved)` where:
/// - `is_a_edges`: Vec of `(child_code, parent_code)` for active IS_A relationships.
/// - `role_props`: Map of `source_code → Vec<(type_id, destination_code)>` for all
///   other kept relationships.
/// - `unresolved`: active relationships with a known source whose destination
///   was in neither set — exactly the edges a standalone extension import
///   silently loses.
fn parse_relationships(
    reader: impl BufRead,
    active_concepts: &HashSet<String>,
    base_concepts: Option<&HashSet<String>>,
    errors: &mut Vec<String>,
) -> (Vec<(String, String)>, RoleProps, usize) {
    let mut is_a_edges: Vec<(String, String)> = Vec::new();
    let mut is_a_seen: HashSet<(String, String)> = HashSet::new();
    let mut role_props: RoleProps = HashMap::new();
    let mut unresolved = 0usize;

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line_num == 0 || line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.splitn(11, '\t').collect();
        if parts.len() < 9 {
            errors.push(format!(
                "Relationship RF2 line {}: expected ≥9 fields, got {} — skipped",
                line_num + 1,
                parts.len()
            ));
            continue;
        }

        let active = parts[2].trim() == "1";
        let source = parts[4].trim();
        let destination = parts[5].trim();
        let type_id = parts[7].trim();

        if !active || !active_concepts.contains(source) {
            continue;
        }
        let destination_known = active_concepts.contains(destination)
            || base_concepts.is_some_and(|base| base.contains(destination));
        if !destination_known {
            unresolved += 1;
            continue;
        }

        if type_id == IS_A_TYPE {
            let edge = (source.to_string(), destination.to_string());
            if is_a_seen.insert(edge.clone()) {
                is_a_edges.push(edge);
            }
        } else {
            role_props
                .entry(source.to_string())
                .or_default()
                .push((type_id.to_string(), destination.to_string()));
        }
    }

    (is_a_edges, role_props, unresolved)
}

/// Parse an RF2 association refset file (7-column format).
///
/// Returns a map of `refset_id → Vec<(source_concept_id, target_concept_id)>`
/// for all active entries.
fn parse_association_refsets(reader: impl BufRead, errors: &mut Vec<String>) -> RoleProps {
    let mut result: RoleProps = HashMap::new();

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line_num == 0 || line.is_empty() {
            continue;
        }

        // Columns: id effectiveTime active moduleId refsetId referencedComponentId targetComponentId
        let parts: Vec<&str> = line.splitn(8, '\t').collect();
        if parts.len() < 7 {
            errors.push(format!(
                "Association refset line {}: expected ≥7 fields, got {} — skipped",
                line_num + 1,
                parts.len()
            ));
            continue;
        }

        let active = parts[2].trim() == "1";
        let refset_id = parts[4].trim();
        let source_id = parts[5].trim();
        let target_id = parts[6].trim();

        if !active || source_id.is_empty() || target_id.is_empty() {
            continue;
        }

        result
            .entry(refset_id.to_string())
            .or_default()
            .push((source_id.to_string(), target_id.to_string()));
    }

    result
}

/// Build a FHIR Bundle containing a ConceptMap for a SNOMED association refset.
///
/// The ConceptMap URL follows the FHIR implicit pattern:
/// `http://snomed.info/sct?fhir_cm=<refset_id>`
fn build_assoc_refset_concept_map_bundle(
    refset_id: &str,
    equivalence: &str,
    mappings: &[(String, String)],
    version: &str,
) -> Vec<u8> {
    use serde_json::json;

    let url = format!("{SNOMED_URL}?fhir_cm={refset_id}");
    let id = format!("snomed-assoc-{refset_id}");

    let elements: Vec<serde_json::Value> = mappings
        .iter()
        .map(|(source, target)| {
            json!({
                "code": source,
                "target": [{"code": target, "equivalence": equivalence}]
            })
        })
        .collect();

    let cm = json!({
        "resourceType": "ConceptMap",
        "id": id,
        "url": url,
        "version": version,
        "status": "active",
        "group": [{
            "source": SNOMED_URL,
            "target": SNOMED_URL,
            "element": elements
        }]
    });

    let bundle = json!({
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{"resource": cm}]
    });

    serde_json::to_vec(&bundle).expect("serialise ConceptMap bundle")
}

fn extract_release_date(path: &str) -> Option<String> {
    let stem = path.rsplit('/').next().unwrap_or(path);
    let without_ext = stem.strip_suffix(".txt")?;
    let date_part = without_ext.rsplit('_').next()?;
    // Standard RF2 suffix: `_20260501`
    if date_part.len() == 8 && date_part.chars().all(|c| c.is_ascii_digit()) {
        return Some(date_part.to_string());
    }
    // India / some national Snapshot zips use `_20260313T120000Z`; take the date prefix.
    if date_part.len() >= 8 {
        let prefix: String = date_part.chars().take(8).collect();
        if prefix.chars().all(|c| c.is_ascii_digit()) {
            return Some(prefix);
        }
    }
    None
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::backends::SqliteTerminologyBackend;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const CONCEPT_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\r\n\
123456001\t20240101\t1\t900000000000207008\t900000000000074008\r\n\
789012001\t20240101\t1\t900000000000207008\t900000000000074008\r\n\
999999001\t20240101\t0\t900000000000207008\t900000000000074008\r\n";

    const DESCRIPTION_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId\r\n\
111001\t20240101\t1\t900000000000207008\t123456001\ten\t900000000000013009\tFoo disorder\t900000000000448009\r\n\
111002\t20240101\t1\t900000000000207008\t123456001\ten\t900000000000003001\tFoo disorder (disorder)\t900000000000448009\r\n\
111003\t20240101\t1\t900000000000207008\t789012001\ten\t900000000000003001\tBar finding (finding)\t900000000000448009\r\n\
111004\t20240101\t1\t900000000000207008\t999999001\ten\t900000000000013009\tInactive concept\t900000000000448009\r\n\
111005\t20240101\t1\t900000000000207008\t123456001\ten\t900000000000013009\tFoo malady\t900000000000448009\r\n";

    /// Per-language Description file as shipped by national editions.
    const DESCRIPTION_DE_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId\r\n\
211001\t20240101\t1\t900000000000207008\t123456001\tde\t900000000000013009\tFoo Erkrankung\t900000000000448009\r\n\
211002\t20240101\t1\t900000000000207008\t789012001\tde\t900000000000003001\tBar Befund (Befund)\t900000000000448009\r\n";

    /// `Foo malady` (111005) is US-preferred; `Foo disorder` (111001) is
    /// GB-preferred — display selection must pick the US term despite file order.
    const LANGUAGE_REFSET_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId\tacceptabilityId\r\n\
L1\t20240101\t1\t900000000000207008\t900000000000509007\t111005\t900000000000548007\r\n\
L2\t20240101\t1\t900000000000207008\t900000000000508004\t111001\t900000000000548007\r\n";

    const RELATIONSHIP_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId\r\n\
444001\t20240101\t1\t900000000000207008\t789012001\t123456001\t0\t116680003\t900000000000011006\t900000000000451002\r\n";

    fn make_test_rf2_zip() -> NamedTempFile {
        let tmp = NamedTempFile::with_suffix(".zip").unwrap();
        {
            let mut zip = zip::ZipWriter::new(tmp.reopen().unwrap());
            let opts = zip::write::FileOptions::default();

            zip.start_file(
                "Snapshot/Terminology/sct2_Concept_Snapshot_INT_20240101.txt",
                opts,
            )
            .unwrap();
            zip.write_all(CONCEPT_TSV.as_bytes()).unwrap();

            zip.start_file(
                "Snapshot/Terminology/sct2_Description_Snapshot-en_INT_20240101.txt",
                opts,
            )
            .unwrap();
            zip.write_all(DESCRIPTION_TSV.as_bytes()).unwrap();

            zip.start_file(
                "Snapshot/Terminology/sct2_Description_Snapshot-de_INT_20240101.txt",
                opts,
            )
            .unwrap();
            zip.write_all(DESCRIPTION_DE_TSV.as_bytes()).unwrap();

            zip.start_file(
                "Snapshot/Refset/Language/der2_cRefset_LanguageSnapshot-en_INT_20240101.txt",
                opts,
            )
            .unwrap();
            zip.write_all(LANGUAGE_REFSET_TSV.as_bytes()).unwrap();

            zip.start_file(
                "Snapshot/Terminology/sct2_Relationship_Snapshot_INT_20240101.txt",
                opts,
            )
            .unwrap();
            zip.write_all(RELATIONSHIP_TSV.as_bytes()).unwrap();

            zip.finish().unwrap();
        }
        tmp
    }

    /// Like [`make_test_rf2_zip`], but the archive also carries `Full/`
    /// variants (as production RF2 distributions do) whose extra rows would
    /// corrupt the import if they were parsed alongside the Snapshot files.
    fn make_full_and_snapshot_rf2_zip() -> NamedTempFile {
        // Poison rows: an extra concept, description, and relationship that
        // exist only in the Full files.
        const FULL_CONCEPT_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\r\n\
123456001\t20240101\t1\t900000000000207008\t900000000000074008\r\n\
789012001\t20240101\t1\t900000000000207008\t900000000000074008\r\n\
555555001\t20230101\t1\t900000000000207008\t900000000000074008\r\n";
        const FULL_DESCRIPTION_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId\r\n\
311001\t20230101\t1\t900000000000207008\t123456001\ten\t900000000000013009\tStale full-file term\t900000000000448009\r\n";
        const FULL_RELATIONSHIP_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId\r\n\
444001\t20230101\t1\t900000000000207008\t123456001\t789012001\t0\t116680003\t900000000000011006\t900000000000451002\r\n";

        let tmp = make_test_rf2_zip();
        {
            let mut zip = zip::ZipWriter::new_append(tmp.reopen().unwrap()).unwrap();
            let opts = zip::write::FileOptions::default();

            zip.start_file("Full/Terminology/sct2_Concept_Full_INT_20240101.txt", opts)
                .unwrap();
            zip.write_all(FULL_CONCEPT_TSV.as_bytes()).unwrap();

            zip.start_file(
                "Full/Terminology/sct2_Description_Full-en_INT_20240101.txt",
                opts,
            )
            .unwrap();
            zip.write_all(FULL_DESCRIPTION_TSV.as_bytes()).unwrap();

            zip.start_file(
                "Full/Terminology/sct2_Relationship_Full_INT_20240101.txt",
                opts,
            )
            .unwrap();
            zip.write_all(FULL_RELATIONSHIP_TSV.as_bytes()).unwrap();

            zip.finish().unwrap();
        }
        tmp
    }

    /// Run the description/refset parsing pipeline over the test fixtures.
    fn parse_fixture_terms() -> HashMap<String, ConceptTerms> {
        let mut errors = Vec::new();
        let active = parse_active_concepts(CONCEPT_TSV.as_bytes(), &mut errors);

        let mut by_desc_id = HashMap::new();
        let mut next_order = 0;
        parse_descriptions(
            DESCRIPTION_TSV.as_bytes(),
            &active,
            &LanguageFilter::default(),
            &mut next_order,
            &mut by_desc_id,
            &mut errors,
        );
        parse_descriptions(
            DESCRIPTION_DE_TSV.as_bytes(),
            &active,
            &LanguageFilter::default(),
            &mut next_order,
            &mut by_desc_id,
            &mut errors,
        );

        let mut preferred_in = HashMap::new();
        parse_language_refsets(
            LANGUAGE_REFSET_TSV.as_bytes(),
            &mut preferred_in,
            &mut errors,
        );

        assert!(errors.is_empty(), "unexpected parse errors: {errors:?}");
        build_concept_terms(by_desc_id, &preferred_in, &active)
    }

    fn count_rows(backend: &SqliteTerminologyBackend, table: &str) -> i64 {
        let conn = backend.pool().get().unwrap();
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap_or(0)
    }

    // ── Parser unit tests ─────────────────────────────────────────────────────

    #[test]
    fn parse_active_concepts_returns_only_active_ids() {
        let mut errors = Vec::new();
        let active = parse_active_concepts(CONCEPT_TSV.as_bytes(), &mut errors);
        assert!(active.contains("123456001"));
        assert!(active.contains("789012001"));
        assert!(!active.contains("999999001"));
        assert!(errors.is_empty());
    }

    #[test]
    fn display_prefers_us_preferred_synonym_then_gb_then_fsn() {
        let terms = parse_fixture_terms();

        // 111005 "Foo malady" is US-preferred and must win over the
        // GB-preferred "Foo disorder" that appears earlier in the file.
        assert_eq!(terms.get("123456001").unwrap().display, "Foo malady");
        // No synonym at all → FSN fallback.
        assert_eq!(
            terms.get("789012001").unwrap().display,
            "Bar finding (finding)"
        );
        assert!(!terms.contains_key("999999001"));
    }

    #[test]
    fn display_without_language_refsets_keeps_first_synonym() {
        let mut errors = Vec::new();
        let active = parse_active_concepts(CONCEPT_TSV.as_bytes(), &mut errors);
        let mut by_desc_id = HashMap::new();
        let mut next_order = 0;
        parse_descriptions(
            DESCRIPTION_TSV.as_bytes(),
            &active,
            &LanguageFilter::default(),
            &mut next_order,
            &mut by_desc_id,
            &mut errors,
        );
        let terms = build_concept_terms(by_desc_id, &HashMap::new(), &active);

        assert_eq!(terms.get("123456001").unwrap().display, "Foo disorder");
    }

    #[test]
    fn descriptions_in_all_languages_become_designations() {
        let terms = parse_fixture_terms();
        let foo = terms.get("123456001").unwrap();

        // German synonym from the per-language description file.
        let de: Vec<_> = foo
            .designations
            .iter()
            .filter(|d| d.language == "de")
            .collect();
        assert_eq!(de.len(), 1);
        assert_eq!(de[0].value, "Foo Erkrankung");
        assert_eq!(de[0].use_system, SNOMED_URL);
        assert_eq!(de[0].use_code, TYPE_SYNONYM);

        // English FSN retained as a designation with the FSN type code.
        assert!(foo.designations.iter().any(|d| d.language == "en"
            && d.use_code == TYPE_FSN
            && d.value == "Foo disorder (disorder)"));

        // Dialect-preferred synonyms get en-US / en-GB preferredForLanguage rows.
        assert!(foo.designations.iter().any(|d| d.language == "en-US"
            && d.use_code == "preferredForLanguage"
            && d.use_system == HL7_TERM_MAINT_INFRA
            && d.value == "Foo malady"));
        assert!(
            foo.designations
                .iter()
                .any(|d| d.language == "en-GB" && d.value == "Foo disorder")
        );
    }

    #[test]
    fn refset_preferred_synonym_ordered_before_other_designations_per_language() {
        let terms = parse_fixture_terms();
        let foo = terms.get("123456001").unwrap();

        // First "en" designation must be a refset-preferred synonym, and all
        // synonyms must precede the FSN (language-keyed lookups take the
        // first match in stored order).
        let first_en = foo
            .designations
            .iter()
            .find(|d| d.language == "en")
            .unwrap();
        assert_eq!(first_en.use_code, TYPE_SYNONYM);
        assert!(["Foo malady", "Foo disorder"].contains(&first_en.value.as_str()));

        let fsn_pos = foo
            .designations
            .iter()
            .position(|d| d.use_code == TYPE_FSN)
            .unwrap();
        let last_syn_pos = foo
            .designations
            .iter()
            .rposition(|d| d.use_code == TYPE_SYNONYM)
            .unwrap();
        assert!(last_syn_pos < fsn_pos, "synonyms must precede the FSN");
    }

    #[test]
    fn inactive_description_row_revokes_earlier_state() {
        let mut errors = Vec::new();
        let active = parse_active_concepts(CONCEPT_TSV.as_bytes(), &mut errors);

        // Full-file style: the synonym is later inactivated.
        let full_tsv = "\
id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId\r\n\
111001\t20240101\t1\t900000000000207008\t123456001\ten\t900000000000013009\tFoo disorder\t900000000000448009\r\n\
111001\t20250101\t0\t900000000000207008\t123456001\ten\t900000000000013009\tFoo disorder\t900000000000448009\r\n";
        let mut by_desc_id = HashMap::new();
        let mut next_order = 0;
        parse_descriptions(
            full_tsv.as_bytes(),
            &active,
            &LanguageFilter::default(),
            &mut next_order,
            &mut by_desc_id,
            &mut errors,
        );

        assert!(by_desc_id.is_empty());
    }

    #[test]
    fn parse_relationships_returns_correct_is_a_pairs() {
        let mut errors = Vec::new();
        let active = parse_active_concepts(CONCEPT_TSV.as_bytes(), &mut errors);
        let (edges, roles, unresolved) =
            parse_relationships(RELATIONSHIP_TSV.as_bytes(), &active, None, &mut errors);

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0], ("789012001".to_string(), "123456001".to_string()));
        assert!(
            roles.is_empty(),
            "no role relationships expected in test data"
        );
        assert_eq!(unresolved, 0);
    }

    /// An extension package: its concepts point at destinations that live in
    /// the base edition and are absent from the package itself.
    const EXTENSION_CONCEPT_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\r\n\
1546271000189105\t20260313\t1\t13941000189108\t900000000000074008\r\n\
1547941000189105\t20260313\t1\t13941000189108\t900000000000074008\r\n";

    /// pack is-a brand-family (in package) and is-a 407855002 (base); brand
    /// family is-a 776239008 (base); pack has-ingredient 387069000 (base);
    /// one is-a to 999999999 (in neither) must be reported as unresolved.
    const EXTENSION_RELATIONSHIP_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId\r\n\
r1\t20260313\t1\t13941000189108\t1546271000189105\t1547941000189105\t0\t116680003\t900000000000011006\t900000000000451002\r\n\
r2\t20260313\t1\t13941000189108\t1546271000189105\t407855002\t0\t116680003\t900000000000011006\t900000000000451002\r\n\
r3\t20260313\t1\t13941000189108\t1547941000189105\t776239008\t0\t116680003\t900000000000011006\t900000000000451002\r\n\
r4\t20260313\t1\t13941000189108\t1546271000189105\t387069000\t1\t762949000\t900000000000011006\t900000000000451002\r\n\
r5\t20260313\t1\t13941000189108\t1546271000189105\t999999999\t0\t116680003\t900000000000011006\t900000000000451002\r\n\
r6\t20260313\t1\t13941000189108\t407855002\t1546271000189105\t0\t116680003\t900000000000011006\t900000000000451002\r\n";

    const MODULE_DEPENDENCY_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId\tsourceEffectiveTime\ttargetEffectiveTime\r\n\
a\t20260313\t1\t13941000189108\t900000000000534007\t900000000000012004\t20260313\t20260301\r\n\
b\t20260313\t1\t13941000189108\t900000000000534007\t900000000000207008\t20260313\t20260301\r\n\
c\t20260313\t0\t13941000189108\t900000000000534007\t900000000000207008\t20250313\t20250301\r\n";

    #[test]
    fn parse_relationships_standalone_extension_drops_cross_edition_edges() {
        let mut errors = Vec::new();
        let active = parse_active_concepts(EXTENSION_CONCEPT_TSV.as_bytes(), &mut errors);
        let (edges, roles, unresolved) = parse_relationships(
            EXTENSION_RELATIONSHIP_TSV.as_bytes(),
            &active,
            None,
            &mut errors,
        );

        // Only the in-package is-a survives; r2, r3, r4, r5 all dangle.
        assert_eq!(
            edges,
            vec![(
                "1546271000189105".to_string(),
                "1547941000189105".to_string()
            )]
        );
        assert!(roles.is_empty());
        assert_eq!(
            unresolved, 4,
            "r2 r3 r4 r5 have destinations outside the package"
        );
    }

    #[test]
    fn parse_relationships_layered_keeps_edges_into_base_edition() {
        let mut errors = Vec::new();
        let active = parse_active_concepts(EXTENSION_CONCEPT_TSV.as_bytes(), &mut errors);
        let base: HashSet<String> = ["407855002", "776239008", "387069000"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let (edges, roles, unresolved) = parse_relationships(
            EXTENSION_RELATIONSHIP_TSV.as_bytes(),
            &active,
            Some(&base),
            &mut errors,
        );

        let mut edges_sorted = edges.clone();
        edges_sorted.sort();
        assert_eq!(
            edges_sorted,
            vec![
                (
                    "1546271000189105".to_string(),
                    "1547941000189105".to_string()
                ),
                ("1546271000189105".to_string(), "407855002".to_string()),
                ("1547941000189105".to_string(), "776239008".to_string()),
            ]
        );
        assert_eq!(
            roles.get("1546271000189105"),
            Some(&vec![("762949000".to_string(), "387069000".to_string())])
        );
        // r5 (unknown destination) is unresolved; r6 (base-concept source) is
        // ignored silently — sources are never widened to the base.
        assert_eq!(unresolved, 1);
    }

    #[test]
    fn parse_module_dependency_targets_returns_active_core_module_release() {
        let mut errors = Vec::new();
        let targets =
            parse_module_dependency_targets(MODULE_DEPENDENCY_TSV.as_bytes(), &mut errors);
        assert_eq!(targets, vec!["20260301".to_string()]);
        assert!(errors.is_empty());
    }

    #[test]
    fn extension_namespace_token_reads_rf2_concept_file_name() {
        assert_eq!(
            extension_namespace_token(
                "Snapshot/Terminology/sct2_Concept_Snapshot_IN1000189_20260313T120000Z.txt"
            ),
            "IN1000189"
        );
        assert_eq!(
            extension_namespace_token(
                "Snapshot/Terminology/sct2_Concept_Snapshot_INT_20260501.txt"
            ),
            "INT"
        );
        assert_eq!(extension_namespace_token("random.txt"), "extension");
    }

    #[test]
    fn parse_concept_malformed_line_recorded_in_errors() {
        let concept_data = "\
id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\r\n\
123456001\t20240101\t1\t900000000000207008\t900000000000074008\r\n\
BADLINE\r\n";
        let mut errors = Vec::new();
        let active = parse_active_concepts(concept_data.as_bytes(), &mut errors);
        assert_eq!(active.len(), 1);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("line 3"));
    }

    #[test]
    fn extract_release_date_parses_standard_rf2_filename() {
        assert_eq!(
            extract_release_date("Snapshot/Terminology/sct2_Concept_Snapshot_INT_20240101.txt"),
            Some("20240101".to_string())
        );
    }

    #[test]
    fn extract_release_date_parses_snomed_release_timestamp_suffix() {
        assert_eq!(
            extract_release_date(
                "Snapshot/Terminology/sct2_Concept_Snapshot_IN1000189_20260313T120000Z.txt"
            ),
            Some("20260313".to_string())
        );
    }

    #[test]
    fn extract_release_date_returns_none_for_unknown_format() {
        assert_eq!(extract_release_date("random_file.txt"), None);
    }

    // ── Importer integration tests ────────────────────────────────────────────

    #[tokio::test]
    async fn import_snomed_rf2_dry_run_does_not_write_to_db() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_test_rf2_zip();

        let stats = import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            true,
            &LanguageFilter::default(),
        )
        .await
        .expect("dry-run should succeed");

        assert_eq!(stats.code_systems, 1);
        assert_eq!(stats.concepts, 2);

        assert_eq!(count_rows(&backend, "code_systems"), 0);
        assert_eq!(count_rows(&backend, "concepts"), 0);
        assert_eq!(count_rows(&backend, "concept_hierarchy"), 0);
    }

    #[tokio::test]
    async fn import_snomed_rf2_live_writes_concepts_and_hierarchy() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_test_rf2_zip();

        let stats = import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .expect("live import should succeed");

        assert_eq!(stats.code_systems, 1);
        assert_eq!(stats.concepts, 2);

        assert_eq!(count_rows(&backend, "code_systems"), 1);
        assert_eq!(count_rows(&backend, "concepts"), 2);
        assert_eq!(count_rows(&backend, "concept_hierarchy"), 1);
    }

    /// Extension package whose concepts hang off `123456001` / `789012001`
    /// from [`make_test_rf2_zip`], with a module-dependency refset declaring
    /// a base release that does not match what will be loaded.
    fn make_extension_rf2_zip() -> NamedTempFile {
        const EXT_CONCEPTS: &str = "\
id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\r\n\
1546271000189105\t20260313\t1\t13941000189108\t900000000000074008\r\n\
1547941000189105\t20260313\t1\t13941000189108\t900000000000074008\r\n";
        const EXT_DESCRIPTIONS: &str = "\
id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId\r\n\
d1\t20260313\t1\t13941000189108\t1546271000189105\ten\t900000000000013009\tTelma H 12.5 mg + 40 mg oral tablet\t900000000000448009\r\n\
d2\t20260313\t1\t13941000189108\t1547941000189105\ten\t900000000000013009\tTelma H (brand family)\t900000000000448009\r\n";
        // pack is-a brand family (ext) and is-a 789012001 (base);
        // brand family is-a 123456001 (base); pack has-ingredient 123456001;
        // one edge to 555555555 exists in neither.
        const EXT_RELATIONSHIPS: &str = "\
id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId\r\n\
r1\t20260313\t1\t13941000189108\t1546271000189105\t1547941000189105\t0\t116680003\t900000000000011006\t900000000000451002\r\n\
r2\t20260313\t1\t13941000189108\t1546271000189105\t789012001\t0\t116680003\t900000000000011006\t900000000000451002\r\n\
r3\t20260313\t1\t13941000189108\t1547941000189105\t123456001\t0\t116680003\t900000000000011006\t900000000000451002\r\n\
r4\t20260313\t1\t13941000189108\t1546271000189105\t123456001\t1\t762949000\t900000000000011006\t900000000000451002\r\n\
r5\t20260313\t1\t13941000189108\t1546271000189105\t555555555\t0\t116680003\t900000000000011006\t900000000000451002\r\n";
        const EXT_MODULE_DEPENDENCY: &str = "\
id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId\tsourceEffectiveTime\ttargetEffectiveTime\r\n\
a\t20260313\t1\t13941000189108\t900000000000534007\t900000000000207008\t20260313\t20231201\r\n";

        let tmp = NamedTempFile::with_suffix(".zip").unwrap();
        {
            let mut zip = zip::ZipWriter::new(tmp.reopen().unwrap());
            let opts = zip::write::FileOptions::default();
            zip.start_file(
                "Snapshot/Terminology/sct2_Concept_Snapshot_IN1000189_20260313T120000Z.txt",
                opts,
            )
            .unwrap();
            zip.write_all(EXT_CONCEPTS.as_bytes()).unwrap();
            zip.start_file(
                "Snapshot/Terminology/sct2_Description_Snapshot-en_IN1000189_20260313T120000Z.txt",
                opts,
            )
            .unwrap();
            zip.write_all(EXT_DESCRIPTIONS.as_bytes()).unwrap();
            zip.start_file(
                "Snapshot/Terminology/sct2_Relationship_Snapshot_IN1000189_20260313T120000Z.txt",
                opts,
            )
            .unwrap();
            zip.write_all(EXT_RELATIONSHIPS.as_bytes()).unwrap();
            zip.start_file(
                "Snapshot/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_IN1000189_20260313T120000Z.txt",
                opts,
            )
            .unwrap();
            zip.write_all(EXT_MODULE_DEPENDENCY.as_bytes()).unwrap();
            zip.finish().unwrap();
        }
        tmp
    }

    #[tokio::test]
    async fn import_snomed_rf2_extension_standalone_loses_cross_edition_hierarchy() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        import_snomed_rf2(
            &backend,
            &ctx,
            make_test_rf2_zip().path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .unwrap();

        let stats = import_snomed_rf2(
            &backend,
            &ctx,
            make_extension_rf2_zip().path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .expect("standalone extension import should succeed");

        // Two sibling versions; only the intra-extension edge survives.
        assert_eq!(count_rows(&backend, "code_systems"), 2);
        assert_eq!(count_rows(&backend, "concepts"), 4);
        assert_eq!(count_rows(&backend, "concept_hierarchy"), 2);
        assert!(
            stats
                .errors
                .iter()
                .any(|e| e.contains("4 active relationship(s) skipped")
                    && e.contains("--extends")
                    && e.contains("20231201")),
            "standalone import must hint at --extends and name the declared base: {:?}",
            stats.errors
        );
    }

    #[tokio::test]
    async fn import_snomed_rf2_extends_layers_extension_into_base_edition() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        import_snomed_rf2(
            &backend,
            &ctx,
            make_test_rf2_zip().path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .unwrap();

        let stats = import_snomed_rf2_with(
            &backend,
            &ctx,
            make_extension_rf2_zip().path(),
            500,
            false,
            &LanguageFilter::default(),
            &SnomedImportOptions {
                extends: Some("20240101".into()),
            },
        )
        .await
        .expect("layered import should succeed");

        assert_eq!(stats.concepts, 2);
        // One row, four concepts, and every resolvable edge kept:
        // base 1 + ext→ext 1 + ext→base 2.
        assert_eq!(count_rows(&backend, "code_systems"), 1);
        assert_eq!(count_rows(&backend, "concepts"), 4);
        assert_eq!(count_rows(&backend, "concept_hierarchy"), 4);

        let conn = backend.pool().get().unwrap();
        let (version, title): (String, String) = conn
            .query_row(
                "SELECT version, title FROM code_systems WHERE url = ?1",
                rusqlite::params![SNOMED_URL],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(version, "20240101");
        assert_eq!(title, "SNOMED CT (20240101 + IN1000189 20260313)");

        // Ingredient attribute into the base survived as a property.
        let ingredient: String = conn
            .query_row(
                "SELECT p.value FROM concept_properties p
                 JOIN concepts c ON c.id = p.concept_id
                 WHERE c.code = '1546271000189105' AND p.property = '762949000'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ingredient, "123456001");

        // After the closure rebuild the base root subsumes the extension pack
        // through both paths (direct is-a 789012001 → 123456001, and via the
        // brand family).
        let system_id: String = conn
            .query_row(
                "SELECT id FROM code_systems WHERE url = ?1",
                rusqlite::params![SNOMED_URL],
                |r| r.get(0),
            )
            .unwrap();
        crate::backends::sqlite::schema::build_concept_closure(&conn, &system_id).unwrap();
        let subsumed: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM concept_closure
                 WHERE system_id = ?1 AND ancestor_code = '123456001'
                   AND descendant_code = '1546271000189105')",
                rusqlite::params![system_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            subsumed,
            "base grouper must reach the extension pack via the closure"
        );

        // Diagnostics: declared 20231201 ≠ loaded 20240101, and r5 dangles.
        assert!(
            stats
                .errors
                .iter()
                .any(|e| e.contains("module-dependency mismatch")
                    && e.contains("20231201")
                    && e.contains("20240101")),
            "{:?}",
            stats.errors
        );
        assert!(
            stats
                .errors
                .iter()
                .any(|e| e.contains("1 active relationship(s) skipped")),
            "{:?}",
            stats.errors
        );
    }

    #[tokio::test]
    async fn import_snomed_rf2_extends_requires_loaded_base() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let err = import_snomed_rf2_with(
            &backend,
            &ctx,
            make_extension_rf2_zip().path(),
            500,
            false,
            &LanguageFilter::default(),
            &SnomedImportOptions {
                extends: Some("20240101".into()),
            },
        )
        .await
        .expect_err("layering onto an absent base must fail");
        assert!(
            err.to_string()
                .contains("Import the base SNOMED CT release first"),
            "{err}"
        );
        assert_eq!(count_rows(&backend, "concepts"), 0);
    }

    #[tokio::test]
    async fn import_snomed_rf2_ignores_full_files_when_snapshot_present() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_full_and_snapshot_rf2_zip();

        let stats = import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .expect("import should succeed");

        // Identical to the snapshot-only archive: the Full files' poison rows
        // (extra concept 555555001, stale description, duplicate edge) must
        // not be parsed.
        assert_eq!(stats.concepts, 2);
        assert_eq!(count_rows(&backend, "concepts"), 2);
        assert_eq!(count_rows(&backend, "concept_hierarchy"), 1);
        assert_eq!(count_rows(&backend, "concept_designations"), 10);
    }

    #[tokio::test]
    async fn import_snomed_rf2_writes_multilingual_designations() {
        use crate::traits::CodeSystemOperations;
        use crate::types::LookupRequest;

        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_test_rf2_zip();

        import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .unwrap();

        // 123456001: en×2 + FSN + de = 4 descriptions, plus per preferred
        // synonym a bare-`en` and a dialect preferredForLanguage row
        // (111005 → en + en-US, 111001 → en + en-GB) = 8;
        // 789012001: en FSN + de FSN = 2.
        assert_eq!(count_rows(&backend, "concept_designations"), 10);

        // displayLanguage=de resolves the German synonym as the display.
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    display_language: Some("de".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.display.as_deref(), Some("Foo Erkrankung"));

        // Without displayLanguage the US-preferred English synonym wins.
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.display.as_deref(), Some("Foo malady"));

        // Dialect-tagged lookup resolves the en-GB preferred term.
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    display_language: Some("en-GB".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.display.as_deref(), Some("Foo disorder"));

        // A region-qualified request falls back to the bare RF2 language
        // tag (RFC 4647 truncation: de-DE → de) — what a browser's
        // Accept-Language header typically produces.
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    display_language: Some("de-DE".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.display.as_deref(), Some("Foo Erkrankung"));
    }

    /// `--languages en` drops the German per-language Description file while
    /// leaving all English content (including `preferredForLanguage` rows)
    /// intact.
    #[tokio::test]
    async fn import_snomed_rf2_language_filter_drops_excluded_languages() {
        use crate::traits::CodeSystemOperations;
        use crate::types::LookupRequest;

        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_test_rf2_zip();

        import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            false,
            &LanguageFilter::parse("en"),
        )
        .await
        .unwrap();

        // Full import yields 10 designation rows (see
        // import_snomed_rf2_writes_multilingual_designations); the 2 German
        // synonyms/FSNs from the per-language Description file are dropped.
        assert_eq!(count_rows(&backend, "concept_designations"), 8);

        // English lookups behave exactly as without a filter.
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.display.as_deref(), Some("Foo malady"));
        assert!(
            resp.designations
                .iter()
                .all(|d| d.language.as_deref() != Some("de"))
        );
    }

    /// English is always retained even when the filter names only other
    /// languages — display selection depends on it.
    #[tokio::test]
    async fn import_snomed_rf2_language_filter_always_keeps_english() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_test_rf2_zip();

        import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            false,
            &LanguageFilter::parse("de"),
        )
        .await
        .unwrap();

        // en is force-retained alongside the requested de, so the result is
        // identical to an unfiltered import.
        assert_eq!(count_rows(&backend, "concept_designations"), 10);
    }

    #[test]
    fn rf2_filename_language_extraction() {
        assert_eq!(
            rf2_filename_language(
                "Snapshot/Terminology/sct2_Description_Snapshot-en_INT_20240101.txt"
            )
            .as_deref(),
            Some("en")
        );
        assert_eq!(
            rf2_filename_language(
                "Snapshot/Refset/Language/der2_cRefset_LanguageSnapshot-da_DK1000005_20240331.txt"
            )
            .as_deref(),
            Some("da")
        );
        assert_eq!(
            rf2_filename_language("Full/Terminology/sct2_Description_Full-en-gb_INT_20240101.txt")
                .as_deref(),
            Some("en-gb")
        );
        // Concept/Relationship files carry no language tag.
        assert_eq!(
            rf2_filename_language("Snapshot/Terminology/sct2_Concept_Snapshot_INT_20240101.txt"),
            None
        );
    }

    /// A national-edition style archive with no English content at all:
    /// Danish synonyms preferred via the Danish language refset (which is in
    /// the dialect table → tagged `da-DK` + `da`) and German synonyms
    /// preferred via the German refset (not in the table → bare `de` only).
    #[tokio::test]
    async fn import_snomed_rf2_national_edition_languages() {
        use crate::traits::CodeSystemOperations;
        use crate::types::LookupRequest;

        const NE_CONCEPT_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\r\n\
123456001\t20240101\t1\t900000000000207008\t900000000000074008\r\n";
        const NE_DESCRIPTION_DA_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId\r\n\
411001\t20240101\t1\t900000000000207008\t123456001\tda\t900000000000013009\tFoo sygdom\t900000000000448009\r\n";
        const NE_DESCRIPTION_DE_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId\r\n\
411002\t20240101\t1\t900000000000207008\t123456001\tde\t900000000000013009\tFoo Erkrankung\t900000000000448009\r\n";
        // 554461000005103 = Danish language refset (in LANG_REFSET_DIALECTS),
        // 722130004 = German language refset (deliberately not in the table).
        const NE_LANGUAGE_REFSET_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId\tacceptabilityId\r\n\
N1\t20240101\t1\t900000000000207008\t554461000005103\t411001\t900000000000548007\r\n\
N2\t20240101\t1\t900000000000207008\t722130004\t411002\t900000000000548007\r\n";
        const NE_RELATIONSHIP_TSV: &str = "\
id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId\r\n";

        let tmp = NamedTempFile::new().unwrap();
        {
            let mut zip = zip::ZipWriter::new(tmp.reopen().unwrap());
            let opts = zip::write::FileOptions::default();
            for (name, content) in [
                (
                    "Snapshot/Terminology/sct2_Concept_Snapshot_NE_20240101.txt",
                    NE_CONCEPT_TSV,
                ),
                (
                    "Snapshot/Terminology/sct2_Description_Snapshot-da_NE_20240101.txt",
                    NE_DESCRIPTION_DA_TSV,
                ),
                (
                    "Snapshot/Terminology/sct2_Description_Snapshot-de_NE_20240101.txt",
                    NE_DESCRIPTION_DE_TSV,
                ),
                (
                    "Snapshot/Refset/Language/der2_cRefset_LanguageSnapshot-da_NE_20240101.txt",
                    NE_LANGUAGE_REFSET_TSV,
                ),
                (
                    "Snapshot/Terminology/sct2_Relationship_Snapshot_NE_20240101.txt",
                    NE_RELATIONSHIP_TSV,
                ),
            ] {
                zip.start_file(name, opts).unwrap();
                zip.write_all(content.as_bytes()).unwrap();
            }
            zip.finish().unwrap();
        }

        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        import_snomed_rf2(
            &backend,
            &ctx,
            tmp.path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .unwrap();

        // Danish refset is in the dialect table: a region-qualified request
        // resolves via the da-DK preferredForLanguage designation.
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    display_language: Some("da-DK".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.display.as_deref(), Some("Foo sygdom"));
        assert!(resp.designations.iter().any(|d| {
            d.language.as_deref() == Some("da-DK")
                && d.use_code.as_deref() == Some("preferredForLanguage")
        }));
        assert!(
            resp.designations
                .iter()
                .any(|d| d.language.as_deref() == Some("da"))
        );

        // German refset is not in the dialect table: preference is still
        // recorded under the bare RF2 language, and a region-qualified
        // request reaches it via RFC 4647 truncation (de-DE → de).
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    display_language: Some("de-DE".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.display.as_deref(), Some("Foo Erkrankung"));
        assert!(resp.designations.iter().any(|d| {
            d.language.as_deref() == Some("de")
                && d.use_code.as_deref() == Some("preferredForLanguage")
        }));

        // No English anywhere: the display falls back to a preferred
        // synonym in a non-English language instead of coming up empty.
        let resp = backend
            .lookup(
                &ctx,
                LookupRequest {
                    system: SNOMED_URL.into(),
                    code: "123456001".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(resp.display.is_some());
    }

    #[tokio::test]
    async fn import_snomed_rf2_idempotent_reimport() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_test_rf2_zip();

        import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .unwrap();
        import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await
        .unwrap();

        assert_eq!(count_rows(&backend, "code_systems"), 1);
        assert_eq!(count_rows(&backend, "concepts"), 2);
        assert_eq!(count_rows(&backend, "concept_hierarchy"), 1);
    }

    #[tokio::test]
    async fn import_snomed_rf2_batching_preserves_all_concepts() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();
        let zip_file = make_test_rf2_zip();

        let stats = import_snomed_rf2(
            &backend,
            &ctx,
            zip_file.path(),
            1,
            false,
            &LanguageFilter::default(),
        )
        .await
        .unwrap();

        assert_eq!(stats.concepts, 2);
        assert_eq!(count_rows(&backend, "concepts"), 2);
    }

    #[tokio::test]
    async fn import_snomed_rf2_missing_file_returns_error() {
        let backend = SqliteTerminologyBackend::in_memory().unwrap();
        let ctx = TenantContext::system();

        let result = import_snomed_rf2(
            &backend,
            &ctx,
            Path::new("/nonexistent/snomed.zip"),
            500,
            false,
            &LanguageFilter::default(),
        )
        .await;
        assert!(result.is_err());
    }
}

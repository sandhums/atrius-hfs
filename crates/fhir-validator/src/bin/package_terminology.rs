//! Expand closed ValueSets from FHIR packages into one gzipped JSON pack.
//!
//! ```text
//! cargo run -p helios-fhir-validator --features cli --bin package-terminology -- \
//!     --cache data/fhir-packages \
//!     --fhir-version R4 \
//!     --package hl7.terminology.r4@7.2.0 \
//!     --package hl7.fhir.r4.core@4.0.1 \
//!     --bound-by atrius.fhir.india.r4@0.1.0 \
//!     --bound-type AuditEvent \
//!     --out r4.json.gz
//! ```
//!
//! `--bound-by` is also loaded, ahead of `--package`, so its urls win.
//! `--bound-type` adds bindings from every loaded package's StructureDefinitions
//! of that type. With neither flag the pack contains every expansion.
//! Core at HFS runtime still comes from the embedded pack; pass
//! `hl7.fhir.r4.core@4.0.1` here when the file itself must carry core codes.

use clap::Parser;
use helios_fhir::FhirVersion;
use helios_fhir_validator::{BoundSource, PackageCache, PackageId, PackageRef, PackageTerminology};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "package-terminology",
    about = "Expand closed ValueSets from FHIR packages into a gzipped JSON pack",
    version
)]
struct Args {
    /// Curated package cache (`{cache}/{name}/{version}/package.json`).
    #[arg(long)]
    cache: PathBuf,

    /// FHIR version written into the pack header (`R4` → `4.0.1`).
    #[arg(long, value_enum, default_value_t = FhirVersion::R4)]
    fhir_version: FhirVersion,

    /// Package `name@version` to expand (repeatable). Order is first-listed wins.
    #[arg(long = "package")]
    packages: Vec<String>,

    /// Also expand this package, prepended so its urls win, and keep only
    /// ValueSets bound by its StructureDefinitions.
    #[arg(long = "bound-by")]
    bound_by: Option<String>,

    /// Also keep ValueSets bound by StructureDefinitions of this type in every
    /// loaded package (repeatable). Combined with `--bound-by`.
    #[arg(long = "bound-type")]
    bound_types: Vec<String>,

    /// Gzipped JSON output path.
    #[arg(long)]
    out: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(error) = run(&args) {
        eprintln!("error: {error}");
        return ExitCode::from(2);
    }
    ExitCode::SUCCESS
}

fn run(args: &Args) -> Result<(), String> {
    let mut raws = Vec::new();
    if let Some(bound) = &args.bound_by {
        raws.push(bound.clone());
    }
    raws.extend(args.packages.iter().cloned());

    let mut ids: Vec<PackageId> = Vec::new();
    for raw in &raws {
        let id = PackageRef::parse(raw).map_err(|e| format!("package '{raw}': {e}"))?;
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    if ids.is_empty() {
        return Err("pass --package or --bound-by".into());
    }

    let cache = PackageCache::new(&args.cache);
    let (terminology, report) = PackageTerminology::from_packages(&cache, &ids)
        .map_err(|e| format!("package terminology failed: {e}"))?;
    for package in &report.packages {
        eprintln!(
            "{} expanded {} skipped {}",
            package.package,
            package.expanded.len(),
            package.skipped.len()
        );
    }

    let mut bound = Vec::new();
    if let Some(raw) = &args.bound_by {
        bound.push(BoundSource {
            package: PackageRef::parse(raw).map_err(|e| format!("--bound-by '{raw}': {e}"))?,
            types: Vec::new(),
        });
    }
    if !args.bound_types.is_empty() {
        for id in &ids {
            bound.push(BoundSource {
                package: id.clone(),
                types: args.bound_types.clone(),
            });
        }
    }

    terminology
        .write_pack(
            args.fhir_version.full_version(),
            &ids,
            &bound,
            &cache,
            &args.out,
        )
        .map_err(|e| format!("writing {}: {e}", args.out.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use serde_json::{Value, json};
    use std::fs;
    use std::io::Read;

    fn write_package(root: &std::path::Path, name: &str, version: &str, files: &[(&str, Value)]) {
        let dir = root.join(name).join(version);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("package.json"),
            json!({
                "name": name,
                "version": version,
                "fhirVersions": ["4.0.1"]
            })
            .to_string(),
        )
        .unwrap();
        for (file, body) in files {
            fs::write(dir.join(file), body.to_string()).unwrap();
        }
    }

    fn read_pack(path: &std::path::Path) -> Value {
        let file = fs::File::open(path).unwrap();
        let mut text = String::new();
        GzDecoder::new(file).read_to_string(&mut text).unwrap();
        serde_json::from_str(&text).unwrap()
    }

    #[test]
    fn bound_by_is_loaded_first_and_bound_type_adds_other_packages() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("cache");
        write_package(
            &cache,
            "example.ig",
            "1.0.0",
            &[
                (
                    "cs.json",
                    json!({
                        "resourceType": "CodeSystem",
                        "url": "http://example/colors",
                        "content": "complete",
                        "concept": [{ "code": "red" }, { "code": "blue" }]
                    }),
                ),
                (
                    "vs.json",
                    json!({
                        "resourceType": "ValueSet",
                        "url": "http://example/colors-vs",
                        "name": "Colors",
                        "version": "1.0.0",
                        "compose": { "include": [{ "system": "http://example/colors" }] }
                    }),
                ),
                (
                    "sd.json",
                    json!({
                        "resourceType": "StructureDefinition",
                        "url": "http://example/StructureDefinition/demo",
                        "type": "Observation",
                        "snapshot": {
                            "element": [{
                                "path": "Observation.code",
                                "binding": { "valueSet": "http://example/colors-vs" }
                            }]
                        }
                    }),
                ),
            ],
        );
        write_package(
            &cache,
            "example.core",
            "1.0.0",
            &[
                (
                    "shapes-cs.json",
                    json!({
                        "resourceType": "CodeSystem",
                        "url": "http://example/shapes",
                        "content": "complete",
                        "concept": [{ "code": "circle" }]
                    }),
                ),
                (
                    "shapes-vs.json",
                    json!({
                        "resourceType": "ValueSet",
                        "url": "http://example/shapes-vs",
                        "name": "Shapes",
                        "version": "1.0.0",
                        "compose": { "include": [{ "system": "http://example/shapes" }] }
                    }),
                ),
                (
                    "ignored-vs.json",
                    json!({
                        "resourceType": "ValueSet",
                        "url": "http://example/ignored-vs",
                        "name": "Ignored",
                        "version": "1.0.0",
                        "compose": { "include": [{ "system": "http://example/shapes", "concept": [{ "code": "circle" }] }] }
                    }),
                ),
                (
                    "obs.json",
                    json!({
                        "resourceType": "StructureDefinition",
                        "url": "http://example/StructureDefinition/obs",
                        "type": "AuditEvent",
                        "snapshot": {
                            "element": [{
                                "path": "AuditEvent.type",
                                "binding": { "valueSet": "http://example/shapes-vs" }
                            }]
                        }
                    }),
                ),
                (
                    "patient.json",
                    json!({
                        "resourceType": "StructureDefinition",
                        "url": "http://example/StructureDefinition/patient",
                        "type": "Patient",
                        "snapshot": {
                            "element": [{
                                "path": "Patient.gender",
                                "binding": { "valueSet": "http://example/ignored-vs" }
                            }]
                        }
                    }),
                ),
            ],
        );

        let out = tmp.path().join("r4.json.gz");
        run(&Args {
            cache: cache.clone(),
            fhir_version: FhirVersion::R4,
            packages: vec!["example.core@1.0.0".into()],
            bound_by: Some("example.ig@1.0.0".into()),
            bound_types: vec!["AuditEvent".into()],
            out: out.clone(),
        })
        .unwrap();

        let pack = read_pack(&out);
        assert_eq!(pack["fhirVersion"], "4.0.1");
        assert_eq!(
            pack["packages"],
            json!(["example.ig@1.0.0", "example.core@1.0.0"])
        );
        let urls: Vec<&str> = pack["valueSets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|vs| vs["url"].as_str().unwrap())
            .collect();
        assert_eq!(
            urls,
            vec!["http://example/colors-vs", "http://example/shapes-vs"]
        );
    }
}

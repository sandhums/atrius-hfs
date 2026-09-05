use crate::packages::cache::PackageCache;
use crate::packages::error::PackageError;
use crate::packages::manifest::{PackageId, PackageManifest, PackageRef};
use std::collections::BTreeSet;
use std::path::PathBuf;

/// A package located in the cache, ready to materialize.
#[derive(Debug, Clone)]
pub struct ResolvedPackage {
    pub id: PackageId,
    pub path: PathBuf,
    pub manifest: PackageManifest,
}

/// Resolve `roots` from `cache` only (offline).
///
/// `package.json` `dependencies` are **not** walked. The list is the
/// validation overlay (`HFS_FHIR_PACKAGES` or an equivalent caller list);
/// sushi / IG Publisher `dependsOn` pins stay authoring metadata.
///
/// Returns packages in **list order** (first listed wins in
/// [`CompositeResolver`](crate::CompositeResolver)). Duplicate refs are
/// skipped after the first occurrence. Fails if a listed root is missing
/// from the cache.
pub fn resolve_packages(
    cache: &PackageCache,
    roots: &[PackageRef],
) -> Result<Vec<ResolvedPackage>, PackageError> {
    let mut seen: BTreeSet<PackageId> = BTreeSet::new();
    let mut out = Vec::with_capacity(roots.len());
    for id in roots {
        if !seen.insert(id.clone()) {
            continue;
        }
        let path = cache.get(id)?;
        let manifest = PackageManifest::load(&path.join("package.json"))?;
        out.push(ResolvedPackage {
            id: id.clone(),
            path,
            manifest,
        });
    }
    Ok(out)
}

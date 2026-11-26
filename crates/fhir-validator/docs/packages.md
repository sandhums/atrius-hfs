# FHIR NPM package materialization

Package overlays use the same `SchemaRegistry` + `CompositeResolver` path as
core packs and tenant-uploaded StructureDefinitions (#232). This document
covers **materialization proper**: cache layout, listed-package resolution, and
operator configuration.

## Cache layout

```text
{HFS_FHIR_PACKAGE_CACHE}/{package-name}/{version}/
  package.json
  StructureDefinition-….json
  …
  .sha256          # optional integrity of the source .tgz
```

Populate with `PackageCache::ensure_from_tgz` / `ensure_from_dir`, or any
external seed that expands a FHIR NPM `.tgz` (with the `package/` prefix
stripped) into that directory. **Validation never fetches from the network.**

## Configuration

| Variable | Purpose |
|----------|---------|
| `HFS_FHIR_PACKAGE_CACHE` | Cache root |
| `HFS_FHIR_PACKAGES` | Comma-separated `name@version` packages to overlay |

If `HFS_FHIR_PACKAGES` is set, boot **fails** when a listed package is
missing from the cache or materialization fails (no silent empty overlay).

`package.json` `dependencies` (sushi / IG Publisher `dependsOn`) are **not**
resolved or overlaid. That list is authoring metadata — NDHM terminology,
THO, the Extensions Pack, CRMI, and so on. Add a package to
`HFS_FHIR_PACKAGES` only when its StructureDefinitions should be on the
runtime validation surface. **Do not** add `hl7.fhir.uv.extensions.r4` for
clinical HFS — typed extension instances get structure from the Atrius IG
snapshot; the Extensions Pack is SUSHI authoring-only. CodeSystem / ValueSet
resources still go through HTS, not the schema registry.

## Resolver order

`CompositeResolver` (earlier wins):

1. Tenant stored-StructureDefinition overlay (optional)
2. Package layers — `HFS_FHIR_PACKAGES` in list order
3. Embedded core schema pack

Lookup accepts both the unversioned canonical and the IG-Publisher
`url|version` form (`http://hl7.org/fhir/StructureDefinition/Patient|4.0.1`).
The registry is keyed without the `|version` suffix.

## What is loaded

Only **StructureDefinition** resources become schemas. Abstract infrastructure
roots (`Element`, `BackboneElement`, `Resource`, `DomainResource`) are skipped.
CodeSystem / ValueSet resources are not part of the schema registry.

## Terminology

Closed ValueSet expansions from the configured packages are loaded at startup
for required-binding checks and for slice binding discriminators. Lookup order
for one FHIR version is the package layers (first listed wins), then the
embedded core pack for that version, then HTS when
`HFS_VALIDATION_TERMINOLOGY=tiered` and neither offline layer expanded the
ValueSet. A filter, an incomplete CodeSystem, a missing dependency, or
`valueset-unclosed` is not expanded: the code is not disproved offline, so
tiered mode asks HTS.

| Variable | Purpose |
|----------|---------|
| `HFS_FHIR_PACKAGES` | These packages contribute terminology for the FHIR versions in their `package.json` |
| `HFS_TERMINOLOGY_PACKAGES` | Extra `name@version` packages, joined the same way. Requires `HFS_FHIR_PACKAGE_CACHE`. A missing package fails startup |
| `HFS_TERMINOLOGY_PACKAGES_<VER>` | Attaches packages to one version (`R4`, `R4B`, `R5`, `R6`) even when `package.json` does not declare it |

`HFS_FHIR_PACKAGE_CACHE` is required whenever either package list is set.
Validation never fetches packages from the network.

The `package-terminology` binary (feature `cli`) writes the same expansions
as a gzipped JSON pack: `fhirVersion`, the `name@version` list, and each
expanded ValueSet's url, version, name, and codes. `--bound-by` keeps only
ValueSets reachable from that package's StructureDefinition bindings,
including nested `include` / `exclude` value sets. At runtime the core pack
stays the embedded one; the CLI accepts `hl7.fhir.r4.core@4.0.1` like any
other package so a caller can put core codes in the file.

## Library API

See `helios_fhir_validator::packages`: `PackageCache`, `resolve_packages`,
`materialize_package`, `materialize_package_layers`, `PackageTerminology`,
`OfflineTerminology`.

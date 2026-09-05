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
runtime validation surface. CodeSystem / ValueSet resources still go through
HTS, not the schema registry.

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
CodeSystem / ValueSet files are discovered for operators but must be imported
via HTS, not the schema registry.

## Library API

See `helios_fhir_validator::packages`: `PackageCache`, `resolve_packages`,
`materialize_package`, `materialize_package_layers`.

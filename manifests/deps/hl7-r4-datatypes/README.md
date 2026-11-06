# HL7 R4 datatype StructureDefinitions (Atrius profile registry deps)

Materialized by [`scripts/build-atrius-profile-manifest.sh`](../../scripts/build-atrius-profile-manifest.sh)
from `crates/fhir-gen/resources/R4/profiles-types.json`.

## Included

Primitive and complex datatypes used by Atrius/NDHM snapshots (`Quantity`, `Coding`,
`Period`, `SimpleQuantity`, …) so `type.profile` references resolve offline.

## Excluded (do not add back)

| Id | Reason |
|----|--------|
| `Element` | Abstract root; **no `derivation`** — aborts ProfileRegistry load |
| `BackboneElement` | Abstract infrastructure, not a clinical `type.profile` target |
| `Resource` / `DomainResource` | Resource bases (same class of exclusion; Atrius snapshots cover resources) |

If these files reappear, `setup-atrius-profile-registry.sh` fails the verify step. Full
write-up: `crates/fhir-validation/docs/Profile_registry_and_IG_materialization.md`.

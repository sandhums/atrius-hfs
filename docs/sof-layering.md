# SQL-on-FHIR layering: `helios-sof` vs `persistence`

**Decision:** do **not** split the ViewDefinition-to-SQL compiler out of
`helios-persistence`. The `persistence → sof` crate dependency is not a
layering inversion.

Recorded 8 Sep 2026 so a later architecture review does not "fix" this.

## What each crate owns

| Crate | Role |
|-------|------|
| `helios-sof` | SQL-on-FHIR **spec model** (ViewDefinition, constants, lint) and the **in-process** FHIRPath evaluator (`sof-cli`, standalone `sof-server`, `InProcessSofRunner` for stores with no query engine). |
| `helios-persistence` (`crates/persistence/src/sof/`) | ViewDefinition-to-SQL **compiler**: `compile_path` → `compile_view` → `ir::PlanNode` → `dialect` → `emit`. Runners (`SqliteInDbRunner`, `PgInDbRunner`, `MongoInDbRunner`) execute that SQL/aggregation **inside** the backend. |

SQL generation is a storage concern because it needs the backend dialect
(JSON accessors, parameter syntax, type casts). Flattening inside Postgres
is what makes analytics over large clinical stores viable; that cannot live
in a dialect-agnostic spec crate without either duplicating the ViewDefinition
model or inventing a dialect-registration indirection for no behavioral gain.

## Why this stays put on a Helios fork

`crates/persistence/` and `crates/sof/` are Helios-owned. File-boundary
shuffles here make every `upstream/main` merge a hand reconciliation. Leave
the compiler in persistence; keep Atrius work in ViewDefinition **authoring**
and REST/HTS wiring, not in relocating this module.

## What *would* be a layering bug

Putting FHIR REST handlers, SMART auth, or CDS inside `persistence`. Those
stay in `helios-rest` / `helios-auth` / `cds-server`. The compiler is the
exception because its output *is* backend SQL.

//! Named schema-migration ledger shared by the SQL backends.
//!
//! The recorded `schema_version` integer is "how many steps have run" under
//! **one** numbering. This fork inserted `subscription_outbox` as v16→v17 and
//! shifted every later Helios step by +1, so the same integer means different
//! applied DDL on an upstream-numbered database versus a fork-numbered one.
//!
//! Dispatch keys off stable step **names**. The integer is kept as an operator
//! stamp (`SCHEMA_VERSION`) and as a one-time bootstrap for databases that
//! predate this ledger. Once `schema_migrations` has rows, those names are the
//! source of truth.

use std::collections::HashSet;

/// Written into `schema_version.flavour` after this fork has touched a database.
pub const SCHEMA_FLAVOUR: &str = "atrius";

/// Ledger row for the v1 catalog (`create_schema_v1`). Not a migrate step.
pub const BASE_STEP: &str = "base";

/// Fork-only step inserted as v16→v17. Helios numbering has no such step; its
/// v16→v17 is this fork's `bulk_provider_submissions`.
pub const OUTBOX_STEP: &str = "subscription_outbox";

/// Fork-only tip step: `subscription_outbox.dead_at` so exhausted claims are
/// tombstoned instead of retried hourly forever.
pub const OUTBOX_DEAD_LETTER_STEP: &str = "subscription_outbox_dead_letter";

/// Index of [`OUTBOX_STEP`] in both SQL backends' ordered step lists.
pub const OUTBOX_STEP_INDEX: usize = 15;

/// Which integer sequence `schema_version` was stamped with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Numbering {
    /// This fork's ladder (outbox is v17).
    Fork,
    /// Upstream Helios ladder (outbox was never a numbered step).
    Upstream,
}

/// Decide how to interpret a legacy integer when `schema_migrations` is empty.
///
/// - `flavour = atrius` or `version >= tip` → fork numbering.
/// - Outbox table present → fork numbering (that step ran under this fork).
/// - `version >= 17` and no outbox → upstream numbering.
/// - Otherwise the integers still coincide (pre-insertion).
pub fn classify_numbering(
    flavour: Option<&str>,
    recorded_version: i32,
    tip: i32,
    outbox_exists: bool,
) -> Numbering {
    if flavour == Some(SCHEMA_FLAVOUR) || recorded_version >= tip || outbox_exists {
        Numbering::Fork
    } else if recorded_version >= 17 {
        Numbering::Upstream
    } else {
        Numbering::Fork
    }
}

/// Zero-based indices into the backend step list implied by a legacy integer.
///
/// Recorded version `V` means migrate steps that produce versions `2..=V` have
/// run. [`BASE_STEP`] is not included.
///
/// Under [`Numbering::Upstream`] with `V >= 17`, index [`OUTBOX_STEP_INDEX`] is
/// omitted and Helios v16+ maps onto fork indices `16..` (provider onwards).
pub fn implied_applied_indices(
    recorded_version: i32,
    numbering: Numbering,
    n_steps: usize,
) -> HashSet<usize> {
    if recorded_version <= 1 {
        return HashSet::new();
    }
    match numbering {
        Numbering::Fork => {
            let n = ((recorded_version - 1) as usize).min(n_steps);
            (0..n).collect()
        }
        Numbering::Upstream => {
            if recorded_version <= 16 {
                let n = ((recorded_version - 1) as usize).min(n_steps);
                (0..n).collect()
            } else {
                let mut set: HashSet<usize> = (0..OUTBOX_STEP_INDEX).collect();
                for k in (OUTBOX_STEP_INDEX + 1)..n_steps {
                    if recorded_version >= k as i32 + 1 {
                        set.insert(k);
                    }
                }
                set
            }
        }
    }
}

/// [`implied_applied_indices`] resolved to step names.
pub fn implied_applied_names<'a>(
    recorded_version: i32,
    numbering: Numbering,
    steps: &[&'a str],
) -> HashSet<&'a str> {
    implied_applied_indices(recorded_version, numbering, steps.len())
        .into_iter()
        .map(|i| steps[i])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_v17_includes_outbox_excludes_provider() {
        let idx = implied_applied_indices(17, Numbering::Fork, 37);
        assert!(idx.contains(&15), "outbox");
        assert!(!idx.contains(&16), "provider is fork v18");
        assert_eq!(idx.len(), 16);
    }

    #[test]
    fn upstream_v17_excludes_outbox_includes_provider() {
        let idx = implied_applied_indices(17, Numbering::Upstream, 37);
        assert!(!idx.contains(&15), "Helios never ran the outbox step");
        assert!(idx.contains(&16), "Helios v16→v17 is provider");
        assert_eq!(idx.len(), 16);
    }

    #[test]
    fn pre_insertion_integers_agree() {
        let fork = implied_applied_indices(16, Numbering::Fork, 18);
        let up = implied_applied_indices(16, Numbering::Upstream, 18);
        assert_eq!(fork, up);
        assert!(!fork.contains(&15));
        assert_eq!(fork.len(), 15);
    }

    #[test]
    fn upstream_sqlite_v18_runs_only_outbox_as_missing() {
        let idx = implied_applied_indices(18, Numbering::Upstream, 18);
        assert!(!idx.contains(&15));
        assert!(idx.contains(&16));
        assert!(idx.contains(&17));
        assert_eq!(idx.len(), 17, "all except outbox");
    }

    #[test]
    fn upstream_postgres_v36_skips_outbox_and_later_helios_steps() {
        let idx = implied_applied_indices(36, Numbering::Upstream, 41);
        assert!(!idx.contains(&15));
        assert!(idx.contains(&16));
        assert!(idx.contains(&35), "autovacuum is Helios v36");
        assert!(!idx.contains(&36), "publication is Helios v37");
        assert!(!idx.contains(&38), "slot-2 is fork-only");
        assert_eq!(idx.len(), 35);
    }

    #[test]
    fn classify_flavour_and_tip_are_fork() {
        assert_eq!(
            classify_numbering(Some(SCHEMA_FLAVOUR), 36, 42, false),
            Numbering::Fork
        );
        assert_eq!(classify_numbering(None, 42, 42, false), Numbering::Fork);
        assert_eq!(classify_numbering(None, 20, 42, true), Numbering::Fork);
    }

    #[test]
    fn classify_no_outbox_below_tip_is_upstream() {
        assert_eq!(classify_numbering(None, 36, 42, false), Numbering::Upstream);
        assert_eq!(classify_numbering(None, 18, 19, false), Numbering::Upstream);
    }

    #[test]
    fn classify_below_insertion_is_fork() {
        assert_eq!(classify_numbering(None, 16, 42, false), Numbering::Fork);
    }

    #[test]
    fn upstream_postgres_v36_skips_later_fork_tip_steps() {
        let idx = implied_applied_indices(36, Numbering::Upstream, 41);
        assert!(!idx.contains(&15), "outbox");
        assert!(!idx.contains(&36), "publication");
        assert!(!idx.contains(&37), "types");
        assert!(!idx.contains(&38), "slot-2");
        assert!(!idx.contains(&39), "manifest phase");
        assert!(!idx.contains(&40), "outbox dead-letter");
        assert_eq!(idx.len(), 35);
    }

    #[test]
    fn upstream_postgres_v38_runs_fork_steps_after_types() {
        let idx = implied_applied_indices(38, Numbering::Upstream, 41);
        assert!(!idx.contains(&15), "outbox");
        assert!(idx.contains(&35), "autovacuum is Helios v36");
        assert!(idx.contains(&36), "publication is Helios v37");
        assert!(idx.contains(&37), "types is Helios v38");
        assert!(!idx.contains(&38), "slot-2 is fork-only");
        assert!(!idx.contains(&39), "manifest phase");
        assert!(!idx.contains(&40), "outbox dead-letter");
        assert_eq!(idx.len(), 37, "all Helios steps through v38 except outbox");
    }

    #[test]
    fn upstream_sqlite_v23_runs_later_steps_after_helios_fts_map() {
        // 28 steps: Helios v23 maps onto fork indices 16..=22 (provider …
        // resource_fts_map). live_type (23), phase (24), publication (25),
        // types (26) and dead_letter (27) must still run.
        let idx = implied_applied_indices(23, Numbering::Upstream, 28);
        assert!(!idx.contains(&15), "outbox");
        assert!(idx.contains(&22), "resource_fts_map is Helios v23");
        assert!(!idx.contains(&23), "live_type is Helios v24");
        assert!(!idx.contains(&24), "phase is Helios v25");
        assert!(!idx.contains(&25), "publication is Helios v26");
        assert!(!idx.contains(&26), "types is Helios v27");
        assert!(!idx.contains(&27), "dead_letter is fork-only tip");
        assert_eq!(idx.len(), 22, "all Helios steps through v23 except outbox");
    }

    #[test]
    fn upstream_sqlite_v25_runs_later_steps_after_helios_phase() {
        let idx = implied_applied_indices(25, Numbering::Upstream, 28);
        assert!(!idx.contains(&15), "outbox");
        assert!(idx.contains(&23), "live_type is Helios v24");
        assert!(idx.contains(&24), "phase is Helios v25");
        assert!(!idx.contains(&25), "publication is Helios v26");
        assert!(!idx.contains(&26), "types is Helios v27");
        assert!(!idx.contains(&27), "dead_letter is fork-only tip");
        assert_eq!(idx.len(), 24, "all Helios steps through v25 except outbox");
    }

    #[test]
    fn upstream_sqlite_v27_runs_dead_letter_after_types() {
        let idx = implied_applied_indices(27, Numbering::Upstream, 28);
        assert!(!idx.contains(&15), "outbox");
        assert!(idx.contains(&26), "types is Helios v27");
        assert!(!idx.contains(&27), "dead_letter is fork-only tip");
        assert_eq!(idx.len(), 26, "all Helios steps except outbox");
    }
}

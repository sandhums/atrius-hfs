# Helios follow-on sync: main → feat-clinical-reasoning (Aug 2026)

Paste-ready addendum for *HELIOS vs Atrius.docx*. Covers the sync after the
large earlier merge already documented in that file (dual validators cutover,
JTI revocation keep, `$reindex`, HTS FTS).

## Topology

| Ref | Tip / note |
|---|---|
| `upstream/main` (HeliosSoftware/hfs) | Merged into atrius `main` on 2026-08-06 |
| atrius `main` | `e857179e1` — merge of upstream (+85 Helios commits since Aug 1 sync `f0a8095c4`) |
| `feat-clinical-reasoning` before merge | `7bc8fd963` (tagged `pre-merge-main-2026-08-06`) |
| Merge commit | `4ae9891cc` — *Merge main (Helios upstream sync) into feat-clinical-reasoning* |
| Distance at merge | main 86 ahead / feat 35 ahead of merge-base `f0a8095c4` |

Direction: **checkout feat → merge main into it** (no PR).

## What's in the upstream delta (85 commits)

Themes that matter for Atrius:

- **Validator / UI** — slicing + terminology pickers in the guided editor, profiled extensions, live-vs-save validation split, conformance CRUD, Import/Export nav, heavier validator CI (pack smoke + official FHIR example corpus).
- **Subscriptions `#357`** — server-driven status transitions written back to the stored `Subscription` via `ResourceStorage::update`; `EnteredInError` terminal handling; `$status` storage fallback with request tenant context; new `tests/status_persistence.rs`.
- **Persistence / REST** — Elasticsearch tenant→index naming made injective; statement timeouts → 504; bulk-submit respects `submissionStatus=completed`; `If-Match: "0"` for first user-settings version; `__system__` tenant reservation non-bypassable (`#317`).
- **Infra** — docker/bulk-submit compose fixes, CI host-GC / benchmark guards, fhir-codegen shared serde bodies.

## Critical overlap: status persistence

Both sides independently made status durable. They are **not** the same shape:

| Side | Mechanism |
|---|---|
| Helios `#357` (main) | `SubscriptionEngine::with_status_store(Arc<dyn ResourceStorage>)`, `persist_status` / concurrency semaphore / write timeout, `HFS_SUBSCRIPTION_PERSIST_STATUS` |
| Atrius (feat) | Separate `SubscriptionStatusStore` trait + `ResourceStorageStatusStore` wrapper + `tests/status_persist_integration.rs` |

### Resolution

- **Adopt Helios `#357`** — ResourceStorage write-back, `EnteredInError`, upstream `status_persistence` tests, `$status` tenant fallback.
- **Drop** Atrius `status_store.rs` trait and `status_persist_integration.rs` (no dual write-back paths).
- **Keep Atrius** FHIRPath criteria evaluation (`topics/fhirpath_criteria.rs` + evaluator gating + `fhirpath_triggers_integration`), durable **outbox**, and **heartbeat** workers.
- Manager `register` keeps Helios `is_rearm` counter reset **and** preserves `last_notification_at` across non-rearm re-registrations (needed so status write-back / outbox feedback does not stall error/off).

## Kept vs adopted (this sync)

| Piece | Status |
|---|---|
| Upstream `#357` status write-back + rehydrate `off`/`error`/`entered-in-error` policy | Adopted |
| Atrius `SubscriptionStatusStore` trait | Removed |
| Atrius outbox + heartbeat | Kept |
| Atrius `fhirPathCriteria` evaluation (`%previous` / fail-closed) | Kept (main only *parsed* the field) |
| Validator/UI/ES/tenant Helios fixes | Adopted |
| Atrius cds-server / JTI revocation / package validation cutover | Untouched (no conflict) |

## Verification

```text
cargo check -p helios-subscriptions -p helios-rest   # ok
cargo test -p helios-subscriptions --test status_persistence
  # 8 passed
cargo test -p helios-subscriptions --test fhirpath_triggers_integration
  # 4 passed
cargo test -p helios-subscriptions --test rehydrate_integration
  # 13 passed
```

Do not commit `data/audit-clinical.ndjson`.

## Follow-ups

- Filter comparators / notification `include` (subscriptions framework guide).
- Push merged `feat-clinical-reasoning` only when ready; `main` is still ahead of `origin/main` by the upstream merge if not pushed separately.
- Optional: teach staging smoke to assert `$status` matches stored `Subscription.status` after handshake.

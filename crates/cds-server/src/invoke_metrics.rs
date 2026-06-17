//! Minimal structured metrics for CDS invoke (production hardening — logs, not Prometheus).

use std::time::Instant;

/// Log one completed CDS Hooks invoke for aggregation (grep `cds_invoke_metrics` in journald).
pub fn log_invoke_completed(
    service_id: &str,
    eval_path: &str,
    library_id: &str,
    library_version: Option<&str>,
    started: Instant,
    outcome: &str,
    http_status: Option<u16>,
) {
    tracing::info!(
        target: "cds_invoke_metrics",
        service_id = %service_id,
        eval_path = %eval_path,
        library_id = %library_id,
        library_version = ?library_version,
        duration_ms = started.elapsed().as_millis() as u64,
        outcome = %outcome,
        http_status = ?http_status,
        "cds invoke completed"
    );
}

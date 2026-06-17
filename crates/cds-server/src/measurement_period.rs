//! eCQM **Measurement Period** — the measure reporting interval passed as an explicit CQL parameter.
//!
//! Set on each CDS Hooks invoke via [`helios_cds_hooks::MeasurementPeriodContext`] in hook
//! `context.measurementPeriod`. CQL filters clinical events to those falling within the interval.

use std::collections::HashMap;

use chrono::NaiveDate;
use helios_cds_hooks::MeasurementPeriodContext;
use serde_json::Value;

/// CDS Hooks `extension` key (legacy per-request override).
pub const MEASUREMENT_PERIOD_EXTENSION: &str = "https://atrius.dev/cds-measurement-period";

/// Configured reporting interval (inclusive calendar dates).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasurementPeriod {
    pub low: NaiveDate,
    pub high: NaiveDate,
}

impl MeasurementPeriod {
    pub fn parse_bounds(low: &str, high: &str) -> Option<Self> {
        let low = parse_date(low)?;
        let high = parse_date(high)?;
        if low > high {
            return None;
        }
        Some(Self { low, high })
    }

    pub fn from_hook_context(ctx: &MeasurementPeriodContext) -> Option<Self> {
        Self::parse_bounds(&ctx.low, &ctx.high)
    }

    pub fn to_cql_parameters(&self) -> Value {
        // Full datetimes so PlanDefinition/$apply binds Interval<DateTime> (date-only breaks AgeInYearsAt).
        serde_json::json!({
            "Measurement Period": {
                "low": format!("{}T00:00:00.000+00:00", self.low.format("%Y-%m-%d")),
                "high": format!("{}T23:59:59.999+00:00", self.high.format("%Y-%m-%d")),
                "lowClosed": true,
                "highClosed": true
            }
        })
    }

    pub fn from_extension_value(value: &Value) -> Option<Self> {
        let obj = value.as_object()?;
        let low = obj
            .get("low")
            .or_else(|| obj.get("start"))
            .and_then(|v| v.as_str())?;
        let high = obj
            .get("high")
            .or_else(|| obj.get("end"))
            .and_then(|v| v.as_str())?;
        Self::parse_bounds(low, high)
    }

    pub fn from_cds_extension(extension: &Option<HashMap<String, Value>>) -> Option<Self> {
        let value = extension.as_ref()?.get(MEASUREMENT_PERIOD_EXTENSION)?;
        Self::from_extension_value(value)
    }
}

/// Resolve reporting interval for a CDS invoke: hook context → extension → server default.
pub fn resolve_measurement_period_parameters(
    hook: &Option<MeasurementPeriodContext>,
    extension: &Option<HashMap<String, Value>>,
    server_default: &Option<MeasurementPeriod>,
) -> Option<Value> {
    hook.as_ref()
        .and_then(MeasurementPeriod::from_hook_context)
        .or_else(|| MeasurementPeriod::from_cds_extension(extension))
        .or_else(|| server_default.clone())
        .map(|period| period.to_cql_parameters())
}

fn parse_date(raw: &str) -> Option<NaiveDate> {
    let date_part = raw.split('T').next()?.trim();
    if date_part.len() >= 10 {
        NaiveDate::parse_from_str(&date_part[..10], "%Y-%m-%d").ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hook_context_is_primary_source() {
        let hook = MeasurementPeriodContext {
            low: "2026-01-01".into(),
            high: "2026-12-31".into(),
        };
        let server = MeasurementPeriod::parse_bounds("2025-01-01", "2025-12-31");
        let params =
            resolve_measurement_period_parameters(&Some(hook), &None, &server).expect("params");
        assert_eq!(
            params["Measurement Period"]["low"],
            "2026-01-01T00:00:00.000+00:00"
        );
    }

    #[test]
    fn extension_overrides_server_default_when_hook_absent() {
        let server = MeasurementPeriod::parse_bounds("2025-01-01", "2025-12-31");
        let mut extension = HashMap::new();
        extension.insert(
            MEASUREMENT_PERIOD_EXTENSION.to_string(),
            json!({"low": "2026-01-01", "high": "2026-12-31"}),
        );
        let params = resolve_measurement_period_parameters(&None, &Some(extension), &server)
            .expect("params");
        assert_eq!(
            params["Measurement Period"]["low"],
            "2026-01-01T00:00:00.000+00:00"
        );
    }

    #[test]
    fn returns_none_when_unconfigured() {
        assert!(resolve_measurement_period_parameters(&None, &None, &None).is_none());
    }
}

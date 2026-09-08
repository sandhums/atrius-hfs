//! Cost estimation for query routing.
//!
//! This module provides cost estimation for routing queries to backends.
//! Costs are derived from Criterion benchmarks and used to make optimal
//! routing decisions.
//!
//! # Cost Model
//!
//! The cost model considers:
//! - **Base cost**: Inherent cost of using a particular backend
//! - **Feature multipliers**: Additional cost for specific query features
//! - **Data volume**: Expected number of results
//! - **Network latency**: For distributed backends
//!
//! # Example
//!
//! ```ignore
//! use helios_persistence::composite::cost::{CostEstimator, QueryCost};
//!
//! let estimator = CostEstimator::with_defaults();
//!
//! let cost = estimator.estimate(&query, &config);
//! println!("Estimated cost: {}", cost.total);
//! println!("Estimated latency: {}ms", cost.estimated_latency_ms);
//! ```

use std::collections::HashMap;

use crate::core::BackendKind;
use crate::types::SearchQuery;

use super::analyzer::{QueryAnalyzer, QueryFeature};
use super::config::{BackendEntry, CompositeConfig, CostConfig};

/// Estimated cost of executing a query.
#[derive(Debug, Clone)]
pub struct QueryCost {
    /// Total cost (arbitrary units, lower is better).
    pub total: f64,

    /// Estimated latency in milliseconds.
    pub estimated_latency_ms: u64,

    /// Estimated number of results.
    pub estimated_results: EstimatedCount,

    /// Confidence in the estimate (0.0 to 1.0).
    pub confidence: f64,

    /// Breakdown by component.
    pub breakdown: CostBreakdown,
}

/// Estimated result count.
#[derive(Debug, Clone)]
pub enum EstimatedCount {
    /// Exact count known.
    Exact(u64),
    /// Approximate count.
    Approximate(u64),
    /// Range estimate.
    Range {
        /// Minimum count.
        min: u64,
        /// Maximum count.
        max: u64,
    },
    /// Unknown count.
    Unknown,
}

impl EstimatedCount {
    /// Returns the expected value.
    pub fn expected(&self) -> u64 {
        match self {
            EstimatedCount::Exact(n) => *n,
            EstimatedCount::Approximate(n) => *n,
            EstimatedCount::Range { min, max } => (min + max) / 2,
            EstimatedCount::Unknown => 100, // Default assumption
        }
    }
}

/// Breakdown of cost components.
#[derive(Debug, Clone, Default)]
pub struct CostBreakdown {
    /// Base cost for the backend.
    pub base: f64,

    /// Cost from query features.
    pub feature_costs: HashMap<QueryFeature, f64>,

    /// Cost from data volume.
    pub volume_cost: f64,

    /// Cost from network/latency.
    pub latency_cost: f64,

    /// Cost from resource usage.
    pub resource_cost: f64,
}

impl CostBreakdown {
    /// Returns the total cost.
    pub fn total(&self) -> f64 {
        self.base
            + self.feature_costs.values().sum::<f64>()
            + self.volume_cost
            + self.latency_cost
            + self.resource_cost
    }
}

/// Cost estimator for query routing decisions.
pub struct CostEstimator {
    /// Cost configuration.
    config: CostConfig,

    /// Query analyzer.
    analyzer: QueryAnalyzer,

    /// Benchmark results (if available).
    benchmarks: Option<BenchmarkResults>,
}

impl CostEstimator {
    /// Creates a new cost estimator with default costs.
    pub fn with_defaults() -> Self {
        Self {
            config: CostConfig::default(),
            analyzer: QueryAnalyzer::new(),
            benchmarks: None,
        }
    }

    /// Creates a cost estimator with custom configuration.
    pub fn new(config: CostConfig) -> Self {
        Self {
            config,
            analyzer: QueryAnalyzer::new(),
            benchmarks: None,
        }
    }

    /// Adds benchmark results for more accurate estimation.
    pub fn with_benchmarks(mut self, benchmarks: BenchmarkResults) -> Self {
        self.benchmarks = Some(benchmarks);
        self
    }

    /// Estimates the cost of executing a query on a specific backend.
    pub fn estimate(&self, query: &SearchQuery, backend: &BackendEntry) -> QueryCost {
        let analysis = self.analyzer.analyze(query);

        // Calculate base cost
        let base_cost = self
            .config
            .base_costs
            .get(&backend.kind)
            .copied()
            .unwrap_or(1.0);

        // Calculate feature costs
        let mut feature_costs = HashMap::new();
        for feature in &analysis.features {
            let multiplier = self
                .config
                .feature_multipliers
                .get(feature)
                .copied()
                .unwrap_or(1.0);

            feature_costs.insert(*feature, base_cost * multiplier);
        }

        // Estimate volume cost based on query specificity
        let specificity = self.estimate_specificity(query);
        let volume_cost = base_cost * (1.0 - specificity) * 2.0;

        // Estimate latency based on backend type
        let estimated_latency_ms = self.estimate_latency(&backend.kind, &analysis);

        // Calculate total with weights
        let total = base_cost * self.config.weights.latency
            + feature_costs.values().sum::<f64>()
            + volume_cost * self.config.weights.resource_usage;

        let breakdown = CostBreakdown {
            base: base_cost,
            feature_costs,
            volume_cost,
            latency_cost: estimated_latency_ms as f64 * 0.01,
            resource_cost: 0.0,
        };

        QueryCost {
            total,
            estimated_latency_ms,
            estimated_results: EstimatedCount::Unknown,
            confidence: self.estimate_confidence(&analysis),
            breakdown,
        }
    }

    /// Estimates the cost of a query across all backends in a config.
    pub fn estimate_all(
        &self,
        query: &SearchQuery,
        config: &CompositeConfig,
    ) -> HashMap<String, QueryCost> {
        config
            .backends
            .iter()
            .filter(|b| b.enabled)
            .map(|backend| (backend.id.clone(), self.estimate(query, backend)))
            .collect()
    }

    /// Returns the backend with the lowest estimated cost.
    pub fn cheapest_backend<'a>(
        &self,
        query: &SearchQuery,
        backends: &'a [BackendEntry],
    ) -> Option<&'a BackendEntry> {
        backends
            .iter()
            .filter(|b| b.enabled)
            .map(|b| (b, self.estimate(query, b)))
            .min_by(|(_, cost_a), (_, cost_b)| {
                cost_a
                    .total
                    .partial_cmp(&cost_b.total)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(backend, _)| backend)
    }

    /// Estimates query specificity (how selective the query is).
    fn estimate_specificity(&self, query: &SearchQuery) -> f64 {
        let mut specificity: f64 = 0.0;

        for param in &query.parameters {
            // More specific params add more specificity
            match param.name.as_str() {
                "_id" => specificity += 0.9,
                "identifier" => specificity += 0.7,
                _ => specificity += 0.1,
            }

            // More values means less specific
            if param.values.len() > 1 {
                specificity *= 0.8;
            }
        }

        // Cap at 1.0
        specificity.min(1.0)
    }

    /// Estimates latency based on backend type.
    fn estimate_latency(
        &self,
        backend_kind: &BackendKind,
        analysis: &super::analyzer::QueryAnalysis,
    ) -> u64 {
        // Base latency by backend type
        let base_latency = match backend_kind {
            BackendKind::Sqlite => 1,
            BackendKind::Postgres => 5,
            BackendKind::Elasticsearch => 10,
            BackendKind::S3 => 50,
            _ => 10,
        };

        // Add latency for complex features
        let feature_latency: u64 = analysis
            .features
            .iter()
            .map(|f| match f {
                QueryFeature::ChainedSearch => 20,
                QueryFeature::ReverseChaining => 25,
                QueryFeature::FullTextSearch => 15,
                QueryFeature::TerminologySearch => 30,
                QueryFeature::Include | QueryFeature::Revinclude => 10,
                _ => 0,
            })
            .sum();

        base_latency + feature_latency
    }

    /// Estimates confidence in the cost estimate.
    fn estimate_confidence(&self, analysis: &super::analyzer::QueryAnalysis) -> f64 {
        let mut confidence = 0.8;

        // Less confidence for complex queries
        if analysis.complexity_score > 5 {
            confidence *= 0.8;
        }

        // Less confidence without benchmarks
        if self.benchmarks.is_none() {
            confidence *= 0.7;
        }

        confidence
    }
}

impl Default for CostEstimator {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Results from Criterion benchmarks.
#[derive(Debug, Clone, Default)]
pub struct BenchmarkResults {
    /// Measured operation costs by backend and operation.
    pub operations: HashMap<(BackendKind, BenchmarkOperation), BenchmarkMeasurement>,
}

/// Types of benchmark operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BenchmarkOperation {
    /// Simple ID lookup.
    IdLookup,
    /// Basic string search.
    StringSearch,
    /// Token search.
    TokenSearch,
    /// Date range search.
    DateSearch,
    /// Chained search (1 level).
    ChainedSearch1,
    /// Chained search (2 levels).
    ChainedSearch2,
    /// Chained search (3 levels).
    ChainedSearch3,
    /// Full-text search.
    FullTextSearch,
    /// Terminology expansion.
    TerminologyExpand,
    /// Include resolution.
    IncludeResolve,
    /// Revinclude resolution.
    RevincludeResolve,
}

/// A benchmark measurement.
#[derive(Debug, Clone)]
pub struct BenchmarkMeasurement {
    /// Mean execution time in microseconds.
    pub mean_us: f64,

    /// Standard deviation in microseconds.
    pub std_dev_us: f64,

    /// Number of iterations.
    pub iterations: u64,

    /// Throughput (operations per second).
    pub throughput: f64,
}

impl BenchmarkResults {
    /// Creates empty benchmark results.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a measurement.
    pub fn add(
        &mut self,
        backend: BackendKind,
        operation: BenchmarkOperation,
        measurement: BenchmarkMeasurement,
    ) {
        self.operations.insert((backend, operation), measurement);
    }

    /// Gets the cost multiplier for an operation.
    pub fn cost_multiplier(
        &self,
        backend: BackendKind,
        operation: BenchmarkOperation,
    ) -> Option<f64> {
        self.operations
            .get(&(backend, operation))
            .map(|m| m.mean_us / 1000.0) // Convert to ms
    }
}

/// Cost comparison between routing options.
#[derive(Debug)]
pub struct CostComparison {
    /// Routing options with their costs.
    pub options: Vec<(String, QueryCost)>,

    /// Recommended option.
    pub recommended: String,

    /// Savings compared to worst option.
    pub savings_percent: f64,
}

impl CostComparison {
    /// Creates a comparison from cost estimates.
    pub fn from_estimates(estimates: HashMap<String, QueryCost>) -> Self {
        let mut options: Vec<_> = estimates.into_iter().collect();
        options.sort_by(|a, b| {
            a.1.total
                .partial_cmp(&b.1.total)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let recommended = options
            .first()
            .map(|(id, _)| id.clone())
            .unwrap_or_default();

        let best_cost = options.first().map(|(_, c)| c.total).unwrap_or(1.0);
        let worst_cost = options.last().map(|(_, c)| c.total).unwrap_or(1.0);
        let savings_percent = if worst_cost > 0.0 {
            ((worst_cost - best_cost) / worst_cost) * 100.0
        } else {
            0.0
        };

        Self {
            options,
            recommended,
            savings_percent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_estimator_default() {
        let estimator = CostEstimator::with_defaults();
        assert!(
            estimator
                .config
                .base_costs
                .contains_key(&BackendKind::Sqlite)
        );
    }

    #[test]
    fn test_estimated_count_expected() {
        assert_eq!(EstimatedCount::Exact(50).expected(), 50);
        assert_eq!(EstimatedCount::Approximate(100).expected(), 100);
        assert_eq!(EstimatedCount::Range { min: 10, max: 30 }.expected(), 20);
        assert_eq!(EstimatedCount::Unknown.expected(), 100);
    }

    #[test]
    fn test_cost_breakdown_total() {
        let mut breakdown = CostBreakdown {
            base: 1.0,
            feature_costs: HashMap::new(),
            volume_cost: 0.5,
            latency_cost: 0.2,
            resource_cost: 0.1,
        };
        breakdown
            .feature_costs
            .insert(QueryFeature::BasicSearch, 0.2);

        assert!((breakdown.total() - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_estimate_simple_query() {
        let estimator = CostEstimator::with_defaults();
        let backend = BackendEntry::new(
            "test",
            super::super::config::BackendRole::Primary,
            BackendKind::Sqlite,
        );
        let query = SearchQuery::new("Patient");

        let cost = estimator.estimate(&query, &backend);
        assert!(cost.total > 0.0);
        assert!(cost.confidence > 0.0);
    }

    #[test]
    fn test_benchmark_results() {
        let mut results = BenchmarkResults::new();
        results.add(
            BackendKind::Sqlite,
            BenchmarkOperation::IdLookup,
            BenchmarkMeasurement {
                mean_us: 100.0,
                std_dev_us: 10.0,
                iterations: 1000,
                throughput: 10000.0,
            },
        );

        let multiplier = results
            .cost_multiplier(BackendKind::Sqlite, BenchmarkOperation::IdLookup)
            .unwrap();
        assert!((multiplier - 0.1).abs() < 0.01);
    }

    #[test]
    fn test_cost_comparison() {
        let mut estimates = HashMap::new();
        estimates.insert(
            "fast".to_string(),
            QueryCost {
                total: 1.0,
                estimated_latency_ms: 10,
                estimated_results: EstimatedCount::Unknown,
                confidence: 0.8,
                breakdown: CostBreakdown::default(),
            },
        );
        estimates.insert(
            "slow".to_string(),
            QueryCost {
                total: 2.0,
                estimated_latency_ms: 20,
                estimated_results: EstimatedCount::Unknown,
                confidence: 0.8,
                breakdown: CostBreakdown::default(),
            },
        );

        let comparison = CostComparison::from_estimates(estimates);
        assert_eq!(comparison.recommended, "fast");
        assert!((comparison.savings_percent - 50.0).abs() < 0.01);
    }
}

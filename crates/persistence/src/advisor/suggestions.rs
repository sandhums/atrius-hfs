//! Optimization suggestions for composite storage configurations.
//!
//! This module provides intelligent suggestions for improving composite
//! storage configurations based on workload patterns and requirements.

use std::collections::HashMap;

use crate::composite::{BackendRole, CompositeConfig, QueryFeature};
use crate::core::{BackendCapability, BackendKind};

/// Engine for generating optimization suggestions.
pub struct SuggestionEngine {
    /// Backend cost profiles.
    #[allow(dead_code)]
    backend_costs: HashMap<BackendKind, BackendCostProfile>,
}

impl SuggestionEngine {
    /// Creates a new suggestion engine with default profiles.
    pub fn new() -> Self {
        Self {
            backend_costs: Self::default_cost_profiles(),
        }
    }

    /// Generates suggestions based on workload pattern.
    pub fn suggest(
        &self,
        config: &CompositeConfig,
        workload: &WorkloadPattern,
    ) -> Vec<OptimizationSuggestion> {
        let mut suggestions = Vec::new();

        // Analyze current config
        let current_capabilities = self.analyze_capabilities(config);

        // Suggest backends based on workload
        suggestions.extend(self.suggest_for_workload(config, workload, &current_capabilities));

        // Suggest performance optimizations
        suggestions.extend(self.suggest_performance_optimizations(config, workload));

        // Suggest cost optimizations
        suggestions.extend(self.suggest_cost_optimizations(config, workload));

        // Sort by priority
        suggestions.sort_by_key(|s| std::cmp::Reverse(s.priority));

        suggestions
    }

    /// Generates suggestions for a specific workload pattern.
    fn suggest_for_workload(
        &self,
        config: &CompositeConfig,
        workload: &WorkloadPattern,
        _current_capabilities: &[BackendCapability],
    ) -> Vec<OptimizationSuggestion> {
        let mut suggestions = Vec::new();

        // Full-text search heavy workload
        if workload.fulltext_search_ratio > 0.3 {
            if !config
                .backends
                .iter()
                .any(|b| b.kind == BackendKind::Elasticsearch && b.role == BackendRole::Search)
            {
                suggestions.push(OptimizationSuggestion {
                    priority: SuggestionPriority::High,
                    category: SuggestionCategory::Performance,
                    title: "Add Elasticsearch for full-text search".to_string(),
                    description: format!(
                        "Your workload has {:.0}% full-text search queries. \
                         Elasticsearch is optimized for this use case.",
                        workload.fulltext_search_ratio * 100.0
                    ),
                    estimated_improvement: Some("3-10x faster full-text queries".to_string()),
                    implementation: Some(
                        "Add a secondary backend with role=Search, kind=Elasticsearch".to_string(),
                    ),
                });
            }
        }

        // High write workload
        if workload.write_ratio > 0.5 {
            let primary = config
                .backends
                .iter()
                .find(|b| b.role == BackendRole::Primary);
            if let Some(p) = primary {
                if p.kind == BackendKind::Sqlite {
                    suggestions.push(OptimizationSuggestion {
                        priority: SuggestionPriority::High,
                        category: SuggestionCategory::Scalability,
                        title: "Consider PostgreSQL for write-heavy workloads".to_string(),
                        description: format!(
                            "Your workload has {:.0}% write operations. \
                             PostgreSQL handles concurrent writes better than SQLite.",
                            workload.write_ratio * 100.0
                        ),
                        estimated_improvement: Some(
                            "Better concurrent write performance".to_string(),
                        ),
                        implementation: Some("Replace SQLite primary with PostgreSQL".to_string()),
                    });
                }
            }
        }

        // Large data volume
        if workload.estimated_data_size_gb > 100.0 {
            if !config
                .backends
                .iter()
                .any(|b| b.kind == BackendKind::S3 && b.role == BackendRole::Archive)
            {
                suggestions.push(OptimizationSuggestion {
                    priority: SuggestionPriority::Medium,
                    category: SuggestionCategory::Cost,
                    title: "Add S3 for archival storage".to_string(),
                    description: format!(
                        "With {:.0}GB of data, S3 can significantly reduce storage costs \
                         for historical/archived data.",
                        workload.estimated_data_size_gb
                    ),
                    estimated_improvement: Some(
                        "70-90% storage cost reduction for archives".to_string(),
                    ),
                    implementation: Some(
                        "Add a secondary backend with role=Archive, kind=S3".to_string(),
                    ),
                });
            }
        }

        // Terminology operations
        if workload.terminology_search_ratio > 0.1 {
            suggestions.push(OptimizationSuggestion {
                priority: SuggestionPriority::Low,
                category: SuggestionCategory::Feature,
                title: "Consider dedicated terminology service".to_string(),
                description: format!(
                    "Your workload has {:.0}% terminology operations. \
                     A dedicated terminology service can improve expansion performance.",
                    workload.terminology_search_ratio * 100.0
                ),
                estimated_improvement: Some("Faster code expansion and validation".to_string()),
                implementation: Some("Add a secondary backend with role=Terminology".to_string()),
            });
        }

        suggestions
    }

    /// Suggests performance optimizations.
    fn suggest_performance_optimizations(
        &self,
        config: &CompositeConfig,
        workload: &WorkloadPattern,
    ) -> Vec<OptimizationSuggestion> {
        let mut suggestions = Vec::new();

        // Check sync mode
        if workload.read_ratio > 0.8
            && config.sync_config.mode == crate::composite::SyncMode::Synchronous
        {
            suggestions.push(OptimizationSuggestion {
                priority: SuggestionPriority::Medium,
                category: SuggestionCategory::Performance,
                title: "Consider asynchronous sync for read-heavy workloads".to_string(),
                description:
                    "With mostly read operations, asynchronous sync can reduce write latency \
                             without impacting read consistency."
                        .to_string(),
                estimated_improvement: Some("Lower write latency".to_string()),
                implementation: Some("Set sync_config.mode to Asynchronous".to_string()),
            });
        }

        // Single backend bottleneck
        let enabled_count = config.backends.iter().filter(|b| b.enabled).count();
        if enabled_count == 1 && workload.concurrent_users > 50 {
            suggestions.push(OptimizationSuggestion {
                priority: SuggestionPriority::High,
                category: SuggestionCategory::Scalability,
                title: "Add read replicas for high concurrency".to_string(),
                description: format!(
                    "With {} concurrent users and a single backend, \
                     consider adding read replicas.",
                    workload.concurrent_users
                ),
                estimated_improvement: Some("Better concurrent query performance".to_string()),
                implementation: Some("Add secondary backends for read distribution".to_string()),
            });
        }

        suggestions
    }

    /// Suggests cost optimizations.
    fn suggest_cost_optimizations(
        &self,
        config: &CompositeConfig,
        workload: &WorkloadPattern,
    ) -> Vec<OptimizationSuggestion> {
        let mut suggestions = Vec::new();

        // Check for over-provisioned backends
        if workload.queries_per_day < 100 {
            let expensive_backends: Vec<_> = config
                .backends
                .iter()
                .filter(|b| matches!(b.kind, BackendKind::Elasticsearch | BackendKind::Postgres))
                .collect();

            if !expensive_backends.is_empty() {
                suggestions.push(OptimizationSuggestion {
                    priority: SuggestionPriority::Low,
                    category: SuggestionCategory::Cost,
                    title: "Consider simpler setup for low volume".to_string(),
                    description: format!(
                        "With only {} queries/day, a SQLite-only setup may be sufficient \
                         and reduce operational costs.",
                        workload.queries_per_day
                    ),
                    estimated_improvement: Some("Reduced infrastructure costs".to_string()),
                    implementation: Some("Use SQLite as primary without secondaries".to_string()),
                });
            }
        }

        suggestions
    }

    /// Analyzes capabilities of current configuration.
    fn analyze_capabilities(&self, config: &CompositeConfig) -> Vec<BackendCapability> {
        config
            .backends
            .iter()
            .filter(|b| b.enabled)
            .flat_map(|b| b.effective_capabilities())
            .collect()
    }

    /// Creates default backend cost profiles.
    fn default_cost_profiles() -> HashMap<BackendKind, BackendCostProfile> {
        let mut profiles = HashMap::new();

        profiles.insert(
            BackendKind::Sqlite,
            BackendCostProfile {
                setup_cost: 0.0,
                monthly_cost: 0.0,
                cost_per_query: 0.0001,
                best_for: vec![
                    "Development".to_string(),
                    "Low volume".to_string(),
                    "Single node".to_string(),
                ],
            },
        );

        profiles.insert(
            BackendKind::Postgres,
            BackendCostProfile {
                setup_cost: 50.0,
                monthly_cost: 50.0,
                cost_per_query: 0.00005,
                best_for: vec![
                    "Production CRUD".to_string(),
                    "Concurrent writes".to_string(),
                    "ACID transactions".to_string(),
                ],
            },
        );

        profiles.insert(
            BackendKind::Elasticsearch,
            BackendCostProfile {
                setup_cost: 100.0,
                monthly_cost: 200.0,
                cost_per_query: 0.00001,
                best_for: vec![
                    "Full-text search".to_string(),
                    "Analytics".to_string(),
                    "Log aggregation".to_string(),
                ],
            },
        );

        profiles.insert(
            BackendKind::S3,
            BackendCostProfile {
                setup_cost: 10.0,
                monthly_cost: 0.023, // per GB
                cost_per_query: 0.0004,
                best_for: vec![
                    "Archival".to_string(),
                    "Large data".to_string(),
                    "Cost efficiency".to_string(),
                ],
            },
        );

        profiles
    }
}

impl Default for SuggestionEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Workload pattern describing usage characteristics.
#[derive(Debug, Clone, Default)]
pub struct WorkloadPattern {
    /// Ratio of read operations (0.0 to 1.0).
    pub read_ratio: f64,

    /// Ratio of write operations (0.0 to 1.0).
    pub write_ratio: f64,

    /// Ratio of full-text search queries.
    pub fulltext_search_ratio: f64,

    /// Ratio of chained/relationship search queries.
    pub chained_search_ratio: f64,

    /// Ratio of terminology-based searches.
    pub terminology_search_ratio: f64,

    /// Estimated data size in GB.
    pub estimated_data_size_gb: f64,

    /// Number of queries per day.
    pub queries_per_day: u64,

    /// Peak concurrent users.
    pub concurrent_users: u64,

    /// Required features.
    pub required_features: Vec<QueryFeature>,

    /// Latency requirements in ms.
    pub max_latency_ms: Option<u64>,

    /// Budget constraints (monthly).
    pub budget_monthly: Option<f64>,
}

impl WorkloadPattern {
    /// Creates a development workload pattern.
    pub fn development() -> Self {
        Self {
            read_ratio: 0.7,
            write_ratio: 0.3,
            fulltext_search_ratio: 0.1,
            chained_search_ratio: 0.05,
            terminology_search_ratio: 0.02,
            estimated_data_size_gb: 1.0,
            queries_per_day: 100,
            concurrent_users: 5,
            required_features: vec![],
            max_latency_ms: Some(1000),
            budget_monthly: Some(0.0),
        }
    }

    /// Creates a production workload pattern.
    pub fn production() -> Self {
        Self {
            read_ratio: 0.8,
            write_ratio: 0.2,
            fulltext_search_ratio: 0.2,
            chained_search_ratio: 0.1,
            terminology_search_ratio: 0.05,
            estimated_data_size_gb: 100.0,
            queries_per_day: 10000,
            concurrent_users: 100,
            required_features: vec![QueryFeature::BasicSearch, QueryFeature::FullTextSearch],
            max_latency_ms: Some(200),
            budget_monthly: Some(500.0),
        }
    }

    /// Creates a high-volume workload pattern.
    pub fn high_volume() -> Self {
        Self {
            read_ratio: 0.9,
            write_ratio: 0.1,
            fulltext_search_ratio: 0.3,
            chained_search_ratio: 0.15,
            terminology_search_ratio: 0.1,
            estimated_data_size_gb: 1000.0,
            queries_per_day: 1000000,
            concurrent_users: 1000,
            required_features: vec![
                QueryFeature::BasicSearch,
                QueryFeature::FullTextSearch,
                QueryFeature::ChainedSearch,
            ],
            max_latency_ms: Some(100),
            budget_monthly: Some(5000.0),
        }
    }
}

/// An optimization suggestion.
#[derive(Debug, Clone)]
pub struct OptimizationSuggestion {
    /// Priority of the suggestion.
    pub priority: SuggestionPriority,

    /// Category of optimization.
    pub category: SuggestionCategory,

    /// Suggestion title.
    pub title: String,

    /// Detailed description.
    pub description: String,

    /// Estimated improvement.
    pub estimated_improvement: Option<String>,

    /// Implementation guidance.
    pub implementation: Option<String>,
}

/// Priority level for suggestions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SuggestionPriority {
    /// Low priority (nice to have).
    Low,
    /// Medium priority.
    Medium,
    /// High priority.
    High,
    /// Critical (should address).
    Critical,
}

/// Category of optimization suggestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestionCategory {
    /// Performance improvement.
    Performance,
    /// Scalability improvement.
    Scalability,
    /// Cost optimization.
    Cost,
    /// Feature addition.
    Feature,
    /// Reliability improvement.
    Reliability,
}

/// Cost profile for a backend type.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BackendCostProfile {
    /// Initial setup cost.
    setup_cost: f64,

    /// Monthly operational cost.
    monthly_cost: f64,

    /// Cost per query (approximate).
    cost_per_query: f64,

    /// Best use cases.
    best_for: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composite::CompositeConfigBuilder;

    #[test]
    fn test_suggestion_engine_creation() {
        let engine = SuggestionEngine::new();
        assert!(!engine.backend_costs.is_empty());
    }

    #[test]
    fn test_suggest_for_development() {
        let engine = SuggestionEngine::new();
        let config = CompositeConfigBuilder::new()
            .primary("sqlite", BackendKind::Sqlite)
            .build()
            .unwrap();

        let workload = WorkloadPattern::development();
        let suggestions = engine.suggest(&config, &workload);

        // Should have minimal suggestions for dev workload with SQLite
        assert!(suggestions.len() < 5);
    }

    #[test]
    fn test_suggest_elasticsearch_for_fulltext() {
        let engine = SuggestionEngine::new();
        let config = CompositeConfigBuilder::new()
            .primary("sqlite", BackendKind::Sqlite)
            .build()
            .unwrap();

        let mut workload = WorkloadPattern::production();
        workload.fulltext_search_ratio = 0.5; // High full-text search ratio

        let suggestions = engine.suggest(&config, &workload);

        // Should suggest Elasticsearch
        assert!(
            suggestions
                .iter()
                .any(|s| s.title.contains("Elasticsearch"))
        );
    }

    #[test]
    fn test_workload_patterns() {
        let dev = WorkloadPattern::development();
        assert!(dev.queries_per_day < 1000);

        let prod = WorkloadPattern::production();
        assert!(prod.queries_per_day >= 1000);

        let high = WorkloadPattern::high_volume();
        assert!(high.queries_per_day >= 100000);
    }
}

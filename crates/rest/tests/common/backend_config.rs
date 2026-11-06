//! Backend configuration for tests.
//!
//! Provides configuration types for running tests against different backends.

use helios_persistence::core::BackendKind;

/// Test backend configuration.
#[derive(Debug, Clone)]
pub enum TestBackendConfig {
    /// A single backend configuration.
    Single(SingleBackendConfig),
    /// A composite (multi-backend) configuration.
    Composite(CompositeTestConfig),
}

/// Configuration for a single backend.
#[derive(Debug, Clone)]
pub struct SingleBackendConfig {
    /// The kind of backend.
    pub kind: BackendKind,
    /// Connection string (if applicable).
    pub connection: Option<String>,
    /// Additional options.
    pub options: std::collections::HashMap<String, String>,
}

impl SingleBackendConfig {
    /// Creates a new in-memory SQLite configuration.
    pub fn sqlite_memory() -> Self {
        Self {
            kind: BackendKind::Sqlite,
            connection: Some(":memory:".to_string()),
            options: std::collections::HashMap::new(),
        }
    }

    /// Creates a new SQLite configuration with a file path.
    pub fn sqlite_file(path: &str) -> Self {
        Self {
            kind: BackendKind::Sqlite,
            connection: Some(path.to_string()),
            options: std::collections::HashMap::new(),
        }
    }

    /// Creates a new PostgreSQL configuration.
    pub fn postgres(connection_string: &str) -> Self {
        Self {
            kind: BackendKind::Postgres,
            connection: Some(connection_string.to_string()),
            options: std::collections::HashMap::new(),
        }
    }
}

/// Configuration for a composite (multi-backend) setup.
#[derive(Debug, Clone)]
pub struct CompositeTestConfig {
    /// Configuration name.
    pub name: String,
    /// Primary backend.
    pub primary: SingleBackendConfig,
    /// Secondary backends.
    pub secondaries: Vec<SecondaryConfig>,
}

/// Configuration for a secondary backend in a composite setup.
#[derive(Debug, Clone)]
pub struct SecondaryConfig {
    /// Role of this backend (e.g., "search", "graph").
    pub role: String,
    /// Backend configuration.
    pub config: SingleBackendConfig,
}

impl TestBackendConfig {
    /// Returns the backend kind string for filtering.
    pub fn kind_str(&self) -> &str {
        match self {
            TestBackendConfig::Single(s) => s.kind.as_str(),
            TestBackendConfig::Composite(c) => c.primary.kind.as_str(),
        }
    }

    /// Returns true if this is an in-memory configuration.
    pub fn is_memory(&self) -> bool {
        match self {
            TestBackendConfig::Single(s) => s.connection.as_deref() == Some(":memory:"),
            TestBackendConfig::Composite(_) => false,
        }
    }
}

/// Extension trait for BackendKind to get string representation.
trait BackendKindExt {
    fn as_str(&self) -> &'static str;
}

impl BackendKindExt for BackendKind {
    fn as_str(&self) -> &'static str {
        match self {
            BackendKind::Sqlite => "sqlite",
            BackendKind::Postgres => "postgres",
            BackendKind::MongoDB => "mongodb",
            BackendKind::Elasticsearch => "elasticsearch",
            BackendKind::S3 => "s3",
            BackendKind::Custom(name) => name,
        }
    }
}

/// Gets the default test backend configuration.
pub fn default_test_config() -> TestBackendConfig {
    TestBackendConfig::Single(SingleBackendConfig::sqlite_memory())
}

/// Gets all available test backend configurations.
///
/// This returns configurations for backends that are enabled via features.
#[allow(clippy::vec_init_then_push)]
pub fn available_configs() -> Vec<TestBackendConfig> {
    let mut configs = Vec::new();

    #[cfg(feature = "sqlite")]
    configs.push(TestBackendConfig::Single(
        SingleBackendConfig::sqlite_memory(),
    ));

    // Add more configurations as backends are enabled
    // #[cfg(feature = "postgres")]
    // configs.push(TestBackendConfig::Single(SingleBackendConfig::postgres("...")));

    configs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_memory_config() {
        let config = SingleBackendConfig::sqlite_memory();
        assert_eq!(config.kind, BackendKind::Sqlite);
        assert_eq!(config.connection.as_deref(), Some(":memory:"));
    }

    #[test]
    fn test_backend_config_kind_str() {
        let config = TestBackendConfig::Single(SingleBackendConfig::sqlite_memory());
        assert_eq!(config.kind_str(), "sqlite");
    }

    #[test]
    fn test_is_memory() {
        let memory = TestBackendConfig::Single(SingleBackendConfig::sqlite_memory());
        let file = TestBackendConfig::Single(SingleBackendConfig::sqlite_file("/tmp/test.db"));

        assert!(memory.is_memory());
        assert!(!file.is_memory());
    }
}

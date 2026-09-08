//! Database backend implementations.
//!
//! This module contains implementations of the storage traits for various
//! database backends. Each backend is gated behind a feature flag.
//!
//! # Available Backends
//!
//! | Backend | Feature | Description |
//! |---------|---------|-------------|
//! | SQLite | `sqlite` | Lightweight embedded database, great for development |
//! | PostgreSQL | `postgres` | Full-featured RDBMS with JSONB support |
//! | MongoDB | `mongodb` | Document store with native JSON support |
//! | Elasticsearch | `elasticsearch` | Full-text search optimized |
//! | S3 | `s3` | Object storage for bulk data |
//!
//! The local filesystem module ([`local_fs`]) is always compiled and provides an
//! export output store rather than a full resource-storage backend.
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(feature = "sqlite")]
//! use helios_persistence::backends::sqlite::SqliteBackend;
//!
//! # #[cfg(feature = "sqlite")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create an in-memory SQLite backend
//! let backend = SqliteBackend::in_memory()?;
//!
//! // Or use a file-based database
//! let backend = SqliteBackend::open("./data/fhir.db")?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "postgres")]
pub mod postgres;

/// Local filesystem [`ExportOutputStore`](crate::core::bulk_export_output::ExportOutputStore).
pub mod local_fs;

#[cfg(feature = "mongodb")]
pub mod mongodb;

#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;

#[cfg(feature = "s3")]
pub mod s3;

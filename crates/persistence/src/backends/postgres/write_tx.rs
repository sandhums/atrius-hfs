//! Same-connection write transaction for direct REST CRUD.
//!
//! Bundle writes already run inside [`super::transaction::PostgresTransaction`].
//! Direct `create` / `update` / `delete` / `restore` used to autocommit the
//! resource statement, then index, then the outbox insert as three independent
//! transactions. A crash between them left a committed resource with no
//! subscription event.
//!
//! This type is the slim equivalent of that bundle helper: simple-query
//! `BEGIN` / `COMMIT`, and a `Drop` that issues `ROLLBACK` before the
//! connection returns to the pool.

use crate::error::{StorageError, StorageResult, TransactionError};

/// RAII write transaction on a pooled client.
///
/// Statements issued through [`Self::client`] participate in the transaction
/// until [`Self::finish`] commits. Dropping without commit rolls the
/// transaction back (panic / early-return paths that skip `finish`).
pub(super) struct WriteTx {
    client: Option<deadpool_postgres::Client>,
    done: bool,
}

impl WriteTx {
    /// `BEGIN` on an already-checked-out pooled client.
    ///
    /// `batch_execute` and not `execute`: the same reason as
    /// [`super::transaction::PostgresTransaction::new`] — `BEGIN` has no
    /// parameters and must not pay Parse + Describe for a prepared statement.
    pub(super) async fn begin(client: deadpool_postgres::Client) -> StorageResult<Self> {
        client.batch_execute("BEGIN").await.map_err(|e| {
            StorageError::Transaction(TransactionError::RolledBack {
                reason: format!("Failed to begin write transaction: {e}"),
            })
        })?;
        Ok(Self {
            client: Some(client),
            done: false,
        })
    }

    pub(super) fn client(&self) -> &deadpool_postgres::Client {
        self.client
            .as_ref()
            .expect("write transaction client already taken")
    }

    pub(super) async fn commit(mut self) -> StorageResult<()> {
        let result = {
            let client = self.client();
            client.batch_execute("COMMIT").await
        };
        match result {
            Ok(()) => {
                self.done = true;
                Ok(())
            }
            Err(e) => Err(StorageError::Transaction(TransactionError::RolledBack {
                reason: format!("Failed to commit write transaction: {e}"),
            })),
        }
    }

    /// Commit on success; roll back synchronously on error so the connection
    /// is idle before it returns to the pool (Drop only covers panics).
    pub(super) async fn finish<T>(self, result: StorageResult<T>) -> StorageResult<T> {
        match result {
            Ok(value) => {
                self.commit().await?;
                Ok(value)
            }
            Err(error) => {
                self.rollback().await;
                Err(error)
            }
        }
    }

    /// `ROLLBACK` and mark the guard done so [`Drop`] is a no-op.
    pub(super) async fn rollback(mut self) {
        self.done = true;
        if let Some(client) = self.client.as_ref()
            && let Err(e) = client.batch_execute("ROLLBACK").await
        {
            tracing::error!(error = %e, "failed to roll back write transaction");
        }
    }
}

impl Drop for WriteTx {
    fn drop(&mut self) {
        // deadpool's default recycling does not reset session state. A
        // connection handed back with an open transaction poisons the pool
        // (`there is already a transaction in progress`). Same obligation as
        // `PostgresTransaction`: ROLLBACK before the client drops back in.
        if self.done {
            return;
        }
        self.done = true;
        let Some(client) = self.client.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                tracing::debug!(
                    "PostgreSQL write transaction dropped without commit; rolling back before pool return"
                );
                handle.spawn(async move {
                    let _ = client.batch_execute("ROLLBACK").await;
                });
            }
            Err(_) => {
                tracing::warn!(
                    "PostgreSQL write transaction dropped without commit and no runtime available to roll back; connection may re-enter the pool with an open transaction"
                );
            }
        }
    }
}

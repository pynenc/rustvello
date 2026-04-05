use mongodb::{Client, Database};
use tokio::sync::OnceCell;

use rustvello_core::error::{RustvelloError, RustvelloResult};

/// Shared MongoDB connection pool.
///
/// Uses `OnceCell` for lazy, one-time async initialization of the
/// MongoDB `Client`. All backends share the same pool instance via `Arc`.
///
/// Data is isolated by `app_id`: the effective database name is
/// `{db_name}_{app_id}`, so two pools created with different `app_id`
/// values against the same MongoDB instance will not see each other's data.
#[non_exhaustive]
pub struct MongoPool {
    uri: String,
    db_name: String,
    client: OnceCell<Client>,
}

impl MongoPool {
    /// Create a new pool targeting the given MongoDB URI and database name.
    ///
    /// The `app_id` is appended to `db_name` (as `{db_name}_{app_id}`) so
    /// that different applications sharing the same MongoDB cluster are
    /// fully isolated.
    pub fn new(uri: &str, db_name: &str, app_id: &str) -> Self {
        Self::with_options(uri, db_name, app_id, None)
    }

    /// Create a new pool with an explicit maximum pool size.
    ///
    /// When `max_pool_size` is `None`, the MongoDB driver's default (100) is used.
    pub fn with_options(
        uri: &str,
        db_name: &str,
        app_id: &str,
        max_pool_size: Option<u32>,
    ) -> Self {
        let mut uri_string = uri.to_string();
        if let Some(size) = max_pool_size {
            // Append maxPoolSize as a connection-string option
            let sep = if uri_string.contains('?') { '&' } else { '?' };
            uri_string = format!("{uri_string}{sep}maxPoolSize={size}");
        }
        let effective_db_name = format!("{db_name}_{app_id}");
        Self {
            uri: uri_string,
            db_name: effective_db_name,
            client: OnceCell::new(),
        }
    }

    /// Get (or lazily create) a handle to the configured database.
    ///
    /// On first access this also creates recommended indexes. Missing
    /// indexes only affect performance, so creation failures are logged
    /// but do not prevent startup.
    pub async fn db(&self) -> RustvelloResult<Database> {
        let client = self
            .client
            .get_or_try_init(|| async {
                let c = Client::with_uri_str(&self.uri).await.map_err(|e| {
                    RustvelloError::state_backend(format!("MongoDB connect: {}", e))
                })?;
                // Best-effort index creation on first connect
                if let Err(e) = ensure_indexes(&c.database(&self.db_name)).await {
                    tracing::warn!("MongoDB index creation failed (non-fatal): {e}");
                }
                Ok(c)
            })
            .await?;
        Ok(client.database(&self.db_name))
    }

    /// Get (or lazily create) a reference to the underlying [`Client`].
    ///
    /// Needed for starting sessions/transactions (the v2 driver's `Database`
    /// does not expose its `Client` publicly).
    pub async fn client(&self) -> RustvelloResult<&Client> {
        // Ensure the client is initialized via db() first
        let _ = self.db().await?;
        Ok(self.client.get().expect("client initialized by db()"))
    }
}

/// Create indexes that are important for query performance.
async fn ensure_indexes(db: &Database) -> Result<(), mongodb::error::Error> {
    use mongodb::IndexModel;

    let status_col = db.collection::<mongodb::bson::Document>("orch_status");
    status_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "task_id": 1 })
                .build(),
            None,
        )
        .await?;
    status_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "call_id": 1 })
                .build(),
            None,
        )
        .await?;
    status_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "status_name": 1 })
                .build(),
            None,
        )
        .await?;

    let cond_col = db.collection::<mongodb::bson::Document>("trg_conditions");
    cond_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "condition_type": 1 })
                .build(),
            None,
        )
        .await?;
    cond_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "event_code": 1 })
                .build(),
            None,
        )
        .await?;
    cond_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "task_ids": 1 })
                .build(),
            None,
        )
        .await?;

    let trigger_col = db.collection::<mongodb::bson::Document>("trg_definitions");
    trigger_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "task_id": 1 })
                .build(),
            None,
        )
        .await?;
    trigger_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "condition_ids": 1 })
                .build(),
            None,
        )
        .await?;

    // State backend: history indexes for runner and time-range queries
    let history_col = db.collection::<mongodb::bson::Document>("state_history");
    history_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "runner_id": 1 })
                .build(),
            None,
        )
        .await?;
    history_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "timestamp": 1 })
                .build(),
            None,
        )
        .await?;
    history_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "invocation_id": 1 })
                .build(),
            None,
        )
        .await?;

    // State backend: runner contexts index for parent lookups
    let runner_col = db.collection::<mongodb::bson::Document>("state_runner_contexts");
    runner_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "parent_runner_id": 1 })
                .build(),
            None,
        )
        .await?;

    // State backend: workflow runs index
    let wf_runs_col = db.collection::<mongodb::bson::Document>("state_workflow_runs");
    wf_runs_col
        .create_index(
            IndexModel::builder()
                .keys(mongodb::bson::doc! { "workflow_type": 1 })
                .build(),
            None,
        )
        .await?;

    Ok(())
}

pub(crate) fn mongo_err(e: mongodb::error::Error) -> RustvelloError {
    RustvelloError::state_backend(format!("MongoDB: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_stores_uri_and_db_name() {
        let pool = MongoPool::new("mongodb://localhost:27017", "test_db", "my_app");
        assert_eq!(pool.uri, "mongodb://localhost:27017");
        assert_eq!(pool.db_name, "test_db_my_app");
    }

    #[test]
    fn mongo_err_maps_to_storage() {
        use mongodb::error::Error as MongoError;
        let err = MongoError::custom("test error");
        let mapped = mongo_err(err);
        assert!(
            matches!(mapped, RustvelloError::Infrastructure { .. }),
            "expected Infrastructure, got {:?}",
            mapped
        );
    }
}

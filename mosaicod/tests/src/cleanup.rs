use mosaicod_core::{params, types};
use mosaicod_db as db;
use mosaicod_grpc_common as grpc_common;
use mosaicod_store as store;
use mosaicod_task as task;
pub struct Builder {
    time_interval: types::Duration,
    retention_period: types::Duration,
    db: db::testing::Database,
}

impl Builder {
    pub fn new(pool: sqlx::Pool<db::DatabaseType>) -> Self {
        let db = db::testing::Database::new(pool.clone());

        Self {
            time_interval: types::Duration::seconds(0),
            retention_period: types::Duration::seconds(0),
            db,
        }
    }

    pub fn with_time_interval(mut self, time_interval: types::Duration) -> Self {
        self.time_interval = time_interval;
        self
    }

    pub fn with_retention_period(mut self, retention_period: types::Duration) -> Self {
        self.retention_period = retention_period;
        self
    }

    pub async fn build_with_store(self, store: &store::testing::Store) -> Handle {
        // Ensure that params are loaded
        params::load_params_from_env(params::ParamsLoadOptions::testing()).unwrap();

        let shutdown = grpc_common::ShutdownNotifier::default();
        let db = self.db;

        let task_handle = tokio::task::spawn({
            let store = (**store).clone();
            let db = db.clone();
            let shutdown = shutdown.clone();

            async move {
                let cleanup = task::Cleanup::new(db, store)
                    .with_time_interval(self.time_interval)
                    .with_retention_duration(self.retention_period);

                cleanup.run(shutdown.token()).await;
            }
        });

        Handle {
            join_handle: task_handle,
            shutdown,
        }
    }
}

/// A handle for the store optimizer routine.
pub struct Handle {
    shutdown: grpc_common::ShutdownNotifier,
    join_handle: tokio::task::JoinHandle<()>,
}

impl Handle {
    /// Signals the cleanup to stop and waits for the background task to complete.
    pub async fn shutdown(self) {
        self.shutdown.shutdown();

        if let Err(e) = self.join_handle.await {
            println!("Cleanup failed: {}", e)
        }
    }

    /// Check if the cleanup is running.
    pub async fn is_shutdown(&self) -> bool {
        self.join_handle.is_finished()
    }
}

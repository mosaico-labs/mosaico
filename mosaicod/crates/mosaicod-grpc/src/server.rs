use mosaicod_core::{error::PublicResult as Result, params};
use mosaicod_db as db;
use mosaicod_grpc_common as grpc_common;
use mosaicod_grpc_flight as grpc_flight;
use mosaicod_store as store;
use mosaicod_task as task;
use tracing::{debug, error};

/// Mosaico server.
/// Handles incoming requests and manages the database and store.
pub struct Server {
    /// Shutdown notifier used to signal server shutdown
    pub shutdown: grpc_common::ShutdownNotifier,
    pub flight_config: grpc_flight::Config,
    /// Store engine
    store: store::StoreRef,
    /// Database handler
    db: db::Database,
}

impl Server {
    /// Creates a new server.
    pub fn new(host: String, port: u16, store: store::StoreRef, db: db::Database) -> Self {
        Self {
            flight_config: grpc_flight::Config::new(host, port),
            store,
            db,
            shutdown: grpc_common::ShutdownNotifier::default(),
        }
    }

    /// Start the server and wait for it to finish.
    ///
    /// The `on_start` callback is called once the server has started.
    ///
    /// This method startup a Tokio runtime to handle async operations.
    pub fn start_and_wait<F>(&self, rt: tokio::runtime::Runtime, on_start: F) -> Result<()>
    where
        F: FnOnce(),
    {
        let shutdown = self.shutdown.clone();
        let shutdown_cleanup = self.shutdown.clone();
        let config = self.flight_config.clone();

        rt.block_on(async {
            let cleanup_time_interval =
                task::cleanup::Duration::seconds(params::params().cleanup_time_interval.value);

            let cleanup_retention_duration =
                task::cleanup::Duration::seconds(params::params().cleanup_retention_duration.value);

            let cleanup_store = self.store.clone();
            let cleanup_db = self.db.clone();

            // Start cleanup background task.
            let handle_cleanup_task = rt.spawn(async move {
                let cleanup = task::Cleanup::new(cleanup_db, cleanup_store)
                    .with_time_interval(cleanup_time_interval)
                    .with_retention_duration(cleanup_retention_duration);

                cleanup.run((shutdown_cleanup.token()).clone()).await
            });

            let server_store = self.store.clone();
            let server_db = self.db.clone();

            // Create a thread in tokio runtime to handle flight requests
            let handle_flight = rt.spawn(async move {
                debug!("grpc server starting");
                if let Err(err) =
                    grpc_flight::start(config, server_store, server_db, Some(shutdown)).await
                {
                    error!("{}", err);
                }
            });

            on_start();

            let _ = tokio::join!(handle_flight, handle_cleanup_task);
        });

        debug!("grpc server stopped");

        Ok(())
    }
}

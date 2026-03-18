use super::flight;
use mosaicod_db as db;
use mosaicod_store as store;
use tracing::{debug, error};

/// Mosaico server.
/// Handles incoming requests and manages the database and store.
pub struct Server {
    /// Shutdown notifier used to signal server shutdown
    pub shutdown: flight::ShutdownNotifier,

    pub flight_config: flight::Config,

    /// Store engine
    store: store::StoreRef,

    /// Database handler
    db: db::Database,
}

impl Server {
    /// Creates a new server.
    pub fn new(host: String, port: u16, store: store::StoreRef, db: db::Database) -> Self {
        Self {
            flight_config: flight::Config::new(host, port),
            store,
            db,
            shutdown: flight::ShutdownNotifier::default(),
        }
    }

    /// Start the server and wait for it to finish.
    ///
    /// The `on_start` callback is called once the server has started.
    ///
    /// This method startup a Tokio runtime to handle async operations.
    pub fn start_and_wait<F>(
        &self,
        rt: tokio::runtime::Runtime,
        on_start: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnOnce(),
    {
        let shutdown = self.shutdown.clone();

        let store = self.store.clone();
        let database = self.db.clone();

        let config = self.flight_config.clone();

        rt.block_on(async {
            // Create a thread in tokio runtime to handle flight requests
            let handle_flight = rt.spawn(async move {
                debug!("flight service starting");
                if let Err(err) = flight::start(config, store, database, Some(shutdown)).await {
                    error!("{}", err);
                }
            });

            on_start();

            let _ = tokio::join!(handle_flight);
        });

        debug!("flight service stopped");

        Ok(())
    }
}

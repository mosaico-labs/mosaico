use super::flight;
use log::{error, info, trace};
use mosaicod_db as db;
use mosaicod_store as store;

/// Mosaico server.
/// Handles incoming requests and manages the database and store.
pub struct Server {
    /// Listen on all addresses, including LAN and public addresses
    pub host: bool,

    pub port: u16,
    /// Shutdown notifier used to signal server shutdown
    pub shutdown: flight::ShutdownNotifier,
    /// Store engine
    store: store::StoreRef,
    /// database configuration params
    pub db_config: db::Config,
}

impl Server {
    pub fn new(host: bool, port: u16, store: store::StoreRef, db_config: db::Config) -> Self {
        Self {
            host,
            port,
            store,
            db_config,
            shutdown: flight::ShutdownNotifier::default(),
        }
    }

    /// Start the server and wait for it to finish.
    ///
    /// The `on_start` callback is called once the server has started.
    ///
    /// This method startup a Tokio runtime to handle async operations.
    ///
    /// Since the `database` requires an async context to be initialized,
    /// the initialization of the [`db::Database`] is done inside this method.
    pub fn start_and_wait<F>(&self, on_start: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnOnce(),
    {
        let host = if self.host { "0.0.0.0" } else { "127.0.0.1" };

        let config = flight::Config {
            host: host.to_owned(),
            port: self.port,
        };

        let shutdown = self.shutdown.clone();

        info!("startup multi-threaded runtime");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        info!("startup store connection");

        // Database connection needs to be done in a async context
        // this is the main reason for which in Server::new we pass
        // `db::Config` instead of the database directly.
        info!("startup database connection");
        let database = rt.block_on(async {
            let database = db::Database::try_new(&self.db_config)
                .await
                .inspect_err(|e| error!("{}", e))?;

            // Bootstrap logic
            info!("database initialization");
            let mut tx = database.transaction().await?;
            db::layer_bootstrap(&mut tx).await?;
            tx.commit().await?;

            Ok::<db::Database, Box<dyn std::error::Error>>(database)
        })?;

        let store = self.store.clone();
        rt.block_on(async {
            // Create a thread in tokio runtime to handle flight requests
            let handle_flight = rt.spawn(async move {
                trace!("flight service starting");
                if let Err(err) = flight::start(config, store, database, Some(shutdown)).await {
                    error!("flight server error: {}", err);
                }
            });

            on_start();

            let _ = tokio::join!(handle_flight);
        });

        info!("stopped");

        Ok(())
    }
}

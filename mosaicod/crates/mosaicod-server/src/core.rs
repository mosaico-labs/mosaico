use super::flight;
use log::{error, info, trace};
use mosaicod_repo as repo;
use mosaicod_store as store;
use std::sync::Arc;
use tokio::sync::Notify;

/// Mosaico server.
/// Handles incoming requests and manages the repository and store.
pub struct Server {
    /// Listen on all addresses, including LAN and public addresses
    pub host: bool,

    pub port: u16,
    /// Shutdown notifier used to signal server shutdown
    pub shutdown: flight::ShutdownNotifier,
    /// Store engine
    store: store::StoreRef,
    /// Repository configuration params
    pub repo_config: repo::Config,
}

impl Server {
    pub fn new(host: bool, port: u16, store: store::StoreRef, repo_config: repo::Config) -> Self {
        Self {
            host,
            port,
            store,
            repo_config,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Start the server and wait for it to finish.
    ///
    /// The `on_start` callback is called once the server has started.
    ///
    /// This method startup a Tokio runtime to handle async operations.
    ///
    /// Since the `repo` requires an async context to be initialized,
    /// the initialization of the [`repo::Repository`] is done inside this method.
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

        info!("startup repository connection (database)");
        let repo = rt.block_on(async {
            let repo = repo::Repository::try_new(&self.repo_config)
                .await
                .inspect_err(|e| error!("{}", e))?;

            info!("repository initialization");
            let mut tx = repo.transaction().await?;

            repo::layer_bootstrap(&mut tx).await?;

            tx.commit().await?;

            Ok::<repo::Repository, Box<dyn std::error::Error>>(repo)
        })?;

        let store = self.store.clone();
        rt.block_on(async {
            // Create a thread in tokio runtime to handle flight requests
            let handle_flight = rt.spawn(async move {
                trace!("flight service starting");
                if let Err(err) = flight::start(config, store, repo, Some(shutdown)).await {
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

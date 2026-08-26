use mosaicod_core::{error::PublicResult as Result, params, types::auth::Permissions};
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_grpc_common as grpc_common;
use mosaicod_grpc_flight as grpc_flight;
use mosaicod_store as store;
use std::net::{IpAddr, SocketAddr};
use tonic::transport::Server as TonicServer;
use tracing::{debug, error, info, warn};

/// Mosaico server.
/// Handles incoming requests and manages the database and store.
pub struct Server {
    /// Shutdown notifier used to signal server shutdown
    pub shutdown: grpc_common::ShutdownNotifier,

    pub options: Options,

    /// Store engine
    store: store::StoreRef,

    /// Database handler
    db: db::Database,
}

#[derive(Clone)]
pub struct TlsConfig {
    pub certificate_file: std::path::PathBuf,
    pub private_key_file: std::path::PathBuf,
}

#[derive(Clone)]
pub struct Options {
    pub host: IpAddr,

    /// Default port
    pub port: u16,

    /// If this option is `Some` the server will try to enable TLS
    tls: Option<TlsConfig>,

    /// If this option is true the server will require API keys for every operation
    enable_api_key_management: bool,

    /// Enable gzip encoding in gRPC
    gzip: bool,
}

impl Options {
    pub fn new(host: IpAddr, port: u16) -> Self {
        Self {
            host,
            port,
            tls: None,
            enable_api_key_management: false,
            gzip: false,
        }
    }

    /// Enable TLS
    pub fn tls(&mut self, tls: TlsConfig) {
        self.tls = Some(tls);
    }

    /// Enables gzip compression for both incoming and outgoing gRPC messages.
    pub fn gzip(&mut self, enable: bool) {
        self.gzip = enable;
    }

    /// Enable API key management
    pub fn enable_api_key_management(&mut self) {
        self.enable_api_key_management = true;
    }
}

impl Server {
    /// Creates a new server.
    pub fn new(host: IpAddr, port: u16, store: store::StoreRef, db: db::Database) -> Self {
        Self {
            options: Options::new(host, port),
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
    pub fn start_and_wait<F>(&mut self, rt: tokio::runtime::Runtime, on_start: F) -> Result<()>
    where
        F: FnOnce(),
    {
        let shutdown = self.shutdown.clone();

        let opts = self.options.clone();

        rt.block_on(async {
            let server_store = self.store.clone();
            let server_db = self.db.clone();

            // Create a thread in tokio runtime to handle flight requests
            let handle_flight = rt.spawn(async move {
                debug!("grpc server starting");
                if let Err(err) = serve(server_store, server_db, opts, Some(shutdown.clone())).await
                {
                    error!("{}", err);
                }

                shutdown.shutdown();
            });

            on_start();

            let _ = handle_flight.await;
        });

        debug!("grpc server stopped");

        Ok(())
    }
}

/// Configures all gRPC services and start to serve requests.
/// This function is blocking until a shutdown signal is received, if configured.
pub async fn serve(
    store: store::StoreRef,
    db: db::Database,
    opts: Options,
    shutdown: Option<grpc_common::ShutdownNotifier>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::new(opts.host, opts.port);

    let mut grpc_flight_svc = grpc_flight::Service::try_new(store, db.clone())?;

    if opts.enable_api_key_management {
        grpc_flight_svc.enable_api_key_manegement();
    }

    let mut auth_layer = grpc_common::middleware::AuthLayer::new(grpc_flight_svc.context());

    let mut flight_svc =
        arrow_flight::flight_service_server::FlightServiceServer::new(grpc_flight_svc);

    if !opts.enable_api_key_management {
        auth_layer = auth_layer.with_permission_passthrough(Permissions::all());
    }
    let layer = tower::ServiceBuilder::new().layer(auth_layer).into_inner();

    let mut builder = TonicServer::builder();

    let mut tls_enabled = false;

    if let Some(tls) = opts.tls {
        builder = builder.tls_config(ext::tonic::load_tls_config(
            &tls.certificate_file,
            &tls.private_key_file,
        )?)?;
        tls_enabled = true;
    }

    if !tls_enabled {
        warn!("TLS is currently disabled. Traffic is being sent unencrypted.");
    }

    // If API key management is disabled define a custom permission with all permissions
    // and enable permissions passthrough in the auth middleware
    if !opts.enable_api_key_management {
        warn!("API key management is currently disabled.");
    } else if !tls_enabled {
        warn!(
            "API key management is currently enabled but TLS is disabled. Sensitive credential are sent unencrypted and could be intercepted."
        );
    }

    flight_svc = flight_svc
        .max_decoding_message_size(params::params().max_grpc_message_size.value)
        .max_encoding_message_size(params::params().max_grpc_message_size.value);

    if opts.gzip {
        flight_svc = flight_svc
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        info!("gzip compression for gRPC requests is enabled");
    }

    let server = builder.layer(layer).add_service(flight_svc);

    if let Some(shutdown_notifier) = shutdown {
        server
            .serve_with_shutdown(addr, async {
                shutdown_notifier.wait_for_shutdown().await;
                debug!("received shutdown notification");
            })
            .await?;
    } else {
        server.serve(addr).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::params;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration as StdDuration;

    /// If the flight service fails on its very first attempt to start (e.g. because of an
    /// invalid bind address), `start_and_wait` must still join both the flight and the cleanup
    /// background tasks instead of hanging forever waiting on a shutdown signal that never
    /// comes.
    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_start_and_wait_joins_all_tasks_on_first_failure(
        pool: sqlx::Pool<db::DatabaseType>,
    ) {
        // `start_and_wait` reads `params::params()` internally, so it must be initialized first.
        let _ = params::load_params_from_env(params::ParamsLoadOptions::testing());

        let test_store =
            store::testing::Store::new_random_on_tmp().expect("failed to create test store");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");

        let test_db = db::testing::Database::new(pool);

        // "123.123.123.123" is a valid IP address, but no interface has this IP associated.
        // So `serve()` fails immediately while parsing the bind address.
        let mut server = Server::new(
            "123.123.123.123".parse().unwrap(),
            0,
            (*test_store).clone(),
            (*test_db).clone(),
        );

        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let result = server.start_and_wait(rt, || {});
            // The receiver may already be gone if the test failed on the timeout below.
            let _ = tx.send(result.is_ok());
        });

        let completed = rx.recv_timeout(StdDuration::from_secs(60));

        assert!(
            completed.is_ok(),
            "start_and_wait() did not return within the timeout: the cleanup background task \
             likely never joined after the flight server failed on its first attempt to start"
        );
        assert!(
            completed.unwrap(),
            "start_and_wait() should return Ok(()) even when the flight server fails to start"
        );

        handle.join().expect("start_and_wait thread panicked");
    }
}

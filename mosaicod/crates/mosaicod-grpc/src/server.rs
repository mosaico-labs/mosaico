use mosaicod_core::{error::PublicResult as Result, params, types::auth::Permissions};
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_grpc_common as grpc_common;
use mosaicod_grpc_flight as grpc_flight;
use mosaicod_store as store;
use mosaicod_task as task;
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
    pub host: String,

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
    pub fn new(host: String, port: u16) -> Self {
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
    pub fn new(host: String, port: u16, store: store::StoreRef, db: db::Database) -> Self {
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
    pub fn start_and_wait<F>(&self, rt: tokio::runtime::Runtime, on_start: F) -> Result<()>
    where
        F: FnOnce(),
    {
        let shutdown = self.shutdown.clone();
        let shutdown_cleanup = self.shutdown.clone();
        let opts = self.options.clone();

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
                if let Err(err) = serve(server_store, server_db, opts, Some(shutdown)).await {
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

/// Configures all gRPC services and start to serve requests.
/// This function is blocking until a shutdown signal is received, if configured.
pub async fn serve(
    store: store::StoreRef,
    db: db::Database,
    opts: Options,
    shutdown: Option<grpc_common::ShutdownNotifier>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", opts.host, opts.port).parse()?;

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

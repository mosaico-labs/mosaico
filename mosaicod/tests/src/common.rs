use arrow_flight::flight_service_client::FlightServiceClient;
use mosaicod_core::{params, types};
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_grpc as grpc;
use mosaicod_grpc_common as grpc_common;
use mosaicod_query as query;
use mosaicod_store as store;
use serde::Deserialize;
use std::convert::Into;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::sync::Arc;
use std::sync::Mutex;
use tonic::service::interceptor;

/// This lock prevents a race condition between releasing the TCP listener (used to find a
/// free port) and binding the Apache Flight server to that port. Without it, a parallel
/// test could grab the port in between, breaking the Flight bind and raising a TCPError.
static PORT_LOCK: Mutex<()> = Mutex::new(());

/// The local loopback address for testing.
pub const HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

pub const TLS_CERT_FILE: &str = "./data/cert.pem";
pub const TLS_CA_FILE: &str = "./data/ca.pem";
pub const TLS_PRIVATE_KEY_FILE: &str = "./data/key.pem";

/// Formats host and port into a valid endpoint string.
///
/// FIXME:
/// We need to use `http` and `https` instead of `grpc` and `grpc+tls` since tonic has an
/// isue reagarding this https://github.com/hyperium/tonic/issues/1496
pub fn format_endpoint(host: &str, port: u16, tls: bool) -> String {
    if tls {
        return format!("https://{host}:{port}");
    }
    format!("http://{host}:{port}")
}

pub struct ServerBuilder {
    host: IpAddr,
    tls: Option<grpc::TlsConfig>,
    db: db::testing::Database,
    enable_api_key: bool,
}

impl ServerBuilder {
    pub fn new(host: IpAddr, pool: sqlx::Pool<db::DatabaseType>) -> Self {
        let db = db::testing::Database::new(pool.clone());

        Self {
            host,
            tls: None,
            db,
            enable_api_key: false,
        }
    }

    pub fn enable_api_key(mut self) -> Self {
        self.enable_api_key = true;
        self
    }

    pub fn enable_tls(mut self) -> Self {
        self.tls = Some(grpc::TlsConfig {
            certificate_file: TLS_CERT_FILE.to_owned().into(),
            private_key_file: TLS_PRIVATE_KEY_FILE.to_owned().into(),
        });
        self
    }

    pub fn enable_tls_with(mut self, cert: &str, private_key: &str) -> Self {
        self.tls = Some(grpc::TlsConfig {
            certificate_file: cert.to_owned().into(),
            private_key_file: private_key.to_owned().into(),
        });
        self
    }

    pub async fn build(self) -> Server {
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        self.build_with_store(store).await
    }

    pub async fn build_with_store(self, store: store::testing::Store) -> Server {
        // Ensure that params are loaded
        params::load_params_from_env(params::ParamsLoadOptions::testing()).unwrap();

        let ts_gw = Arc::new(
            query::TimeseriesEngine::try_new(
                (*store).clone(),
                params::params().query_engine_memory_pool_size.value,
            )
            .unwrap(),
        );

        let _guard = PORT_LOCK.lock();

        // Get a free port from the OS by binding a temporary TCP listener to port 0,
        // then drop the listener so the port is released and available for the Apache Flight
        // server to bind to.
        let port = {
            let tcp_listner = TcpListener::bind(format!("{}:0", HOST)).unwrap();
            tcp_listner.local_addr().unwrap().port()
        };

        let mut opts = grpc::Options::new(self.host, port);

        if let Some(tls) = self.tls {
            opts.tls(tls);
        }

        if self.enable_api_key {
            opts.enable_api_key_management();
        }

        let shutdown = grpc_common::ShutdownNotifier::default();
        let db = self.db;

        let flight_server_handle = tokio::task::spawn({
            let shutdown = shutdown.clone();
            let store = (*store).clone();
            let db = db.clone();

            async move {
                if let Err(err) = grpc::serve(store, db, opts, Some(shutdown.clone())).await {
                    panic!("flight server error: {}", err);
                }
                println!("server stopped");

                shutdown.shutdown();
            }
        });

        // Wait a little to be sure that server port is bound
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Server {
            server_join_handle: flight_server_handle,
            shutdown,
            port,
            db,
            store,
            ts_gw,
        }
    }
}

/// A wrapper around a mosaicod Flight server instance.
///
/// ### Usage:
/// ```no_run
/// use tests::common;
/// use mosaicod_db as db;
///
/// async fn test(pool: sqlx::Pool<db::DatabaseType>) {
///     let server = common::ServerBuilder::new(common::HOST, pool).build().await;
///     // ... run tests ...
///     server.shutdown().await;
/// }
/// ```
pub struct Server {
    shutdown: grpc_common::ShutdownNotifier,
    server_join_handle: tokio::task::JoinHandle<()>,
    port: u16,
    pub db: db::testing::Database,
    pub store: store::testing::Store,
    pub ts_gw: query::TimeseriesEngineRef,
}

impl Server {
    /// Signals the server to stop and waits for the background task to complete.
    pub async fn shutdown(self) {
        self.shutdown.shutdown();

        if let Err(e) = self.server_join_handle.await {
            println!("Flight server failed: {}", e)
        }
    }

    /// Check if the server is running.
    pub async fn is_shutdown(&self) -> bool {
        self.server_join_handle.is_finished()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn context(&self) -> facade::Context {
        facade::Context::new((*self.store).clone(), self.db.clone(), self.ts_gw.clone())
    }

    pub async fn create_api_key(
        &mut self,
        permissions: types::auth::Permissions,
        expires_at: Option<types::Timestamp>,
    ) -> types::ApiKey {
        let handle = facade::auth::create(&self.context(), permissions, "".to_string(), expires_at)
            .await
            .expect("Failed to create api.");

        handle.api_key().clone()
    }
}

type InterceptedChannel =
    interceptor::InterceptedService<tonic::transport::Channel, ApiKeyInterceptor>;

#[derive(Clone)]
pub struct ApiKeyInterceptor {
    api_key: Option<String>,
}

impl tonic::service::Interceptor for ApiKeyInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(key) = &self.api_key {
            req.metadata_mut()
                .insert("mosaico-api-key-token", key.parse().unwrap());
        }

        Ok(req)
    }
}

pub struct ClientBuilder {
    url: url::Url,
    tls: Option<tonic::transport::ClientTlsConfig>,
    api_key_interceptor: Option<ApiKeyInterceptor>,
}

impl ClientBuilder {
    pub fn new(host: IpAddr, port: u16) -> Self {
        Self {
            url: format_endpoint(&host.to_string(), port, false)
                .parse()
                .expect("unable to convert host"),
            tls: None,
            api_key_interceptor: None,
        }
    }

    pub fn enable_tls(mut self) -> Self {
        let cert_str = fs::read(TLS_CA_FILE).expect("Unable to read certificate");
        let cert = tonic::transport::Certificate::from_pem(cert_str);

        self.url = format_endpoint(
            &self.url.host().unwrap().to_string(),
            self.url.port().unwrap(),
            true,
        )
        .parse()
        .unwrap();

        dbg!(&self.url.to_string());
        dbg!(&self.url.domain());

        self.tls = Some(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(cert)
                .domain_name("127.0.0.1"),
        );

        self
    }

    pub fn enable_tls_with(
        mut self,
        tls_ca_file: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cert_str =
            fs::read(tls_ca_file).map_err(|e| format!("Unable to read certificate: {e}"))?;
        let cert = tonic::transport::Certificate::from_pem(cert_str);

        self.url = format_endpoint(
            &self.url.host().unwrap().to_string(),
            self.url.port().unwrap(),
            true,
        )
        .parse()?;

        dbg!(&self.url.to_string());
        dbg!(&self.url.domain());

        self.tls = Some(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(cert)
                .domain_name("127.0.0.1"),
        );

        Ok(self)
    }

    pub fn with_api_key(mut self, api_key: String) -> Self {
        self.api_key_interceptor = Some(ApiKeyInterceptor {
            api_key: Some(api_key),
        });
        self
    }

    /// Establishes a connection to a Flight server at the specified host and port.
    pub async fn build(self) -> Client {
        let url = self.url.as_str().trim_end_matches('/').to_owned();

        let mut channel = tonic::transport::Channel::from_shared(url.clone())
            .expect("Unable to create tonic channel");

        if let Some(tls_config) = self.tls {
            channel = channel
                .tls_config(tls_config)
                .expect("Problem running TLS configuration");
        }

        let channel = channel.connect().await.unwrap_or_else(|e| {
            if let Some(e) = std::error::Error::source(&e) {
                panic!("Unable to connect to `{}`: {}", url, e)
            } else {
                panic!("Unable to connect to `{}`: {}", url, e);
            }
        });

        let interceptor = self
            .api_key_interceptor
            .unwrap_or(ApiKeyInterceptor { api_key: None });

        Client {
            client: FlightServiceClient::with_interceptor(channel, interceptor),
        }
    }
}
/// A dummy client that communicates to mosaicod.
pub struct Client<T = InterceptedChannel> {
    client: FlightServiceClient<T>,
}

impl Client {}

impl<T> std::ops::Deref for Client<T> {
    type Target = FlightServiceClient<T>;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<T> std::ops::DerefMut for Client<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

/// Represents a standard mosaicod response from a `do_action` call.
#[derive(Deserialize, Debug)]
pub struct ActionResponse {
    /// The name of the action performed (e.g., "sequence_create").
    pub action: String,
    /// The JSON response body containing returned data.
    ///
    /// ### How to use `response`:
    /// Because this is a `serde_json::Value`, you must "downcast" the fields
    /// to the expected Rust types.
    ///
    /// **Example:**
    /// ```
    /// use tests::common;
    ///
    /// let body: &str = r#"{"action": "topic_create", "response": {"key": "some-uuid", "id": 10}}"#;
    ///
    /// let r = common::ActionResponse::from_body(body.as_bytes());
    ///
    /// // Extract a String (returns Option<&str>)
    /// let key = r.response["key"].as_str().expect("key is missing");
    ///
    /// // Extract a Number (returns Option<u64>)
    /// let id = r.response["id"].as_u64().expect("id is not a number");
    /// ```
    pub response: serde_json::Value,
}

impl ActionResponse {
    /// Deserializes a raw byte slice from a Flight `Result` into an `ActionResponse`.
    pub fn from_body(body: &[u8]) -> Self {
        serde_json::from_slice(body).expect("problem deserializing action response")
    }
}

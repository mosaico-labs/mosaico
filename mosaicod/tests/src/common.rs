use std::fs;

use arrow_flight::flight_service_client::FlightServiceClient;
use mosaicod_core::params;
use mosaicod_db as db;
use mosaicod_server::{self as server, flight::ShutdownNotifier};
use mosaicod_store as store;
use serde::Deserialize;

/// The local loopback address for testing.
pub const HOST: &str = "127.0.0.1";

pub const TLS_CERT_FILE: &str = "./data/cert.pem";
pub const TLS_CA_FILE: &str = "./data/ca.pem";
pub const TLS_PRIVATE_KEY_FILE: &str = "./data/key.pem";

/// Generates a random port in the ephemeral range [49152, 65535].
/// Use this in tests to avoid `Address In Use` errors during parallel execution.
pub fn random_port() -> u16 {
    use rand::Rng;

    let mut rng = rand::rng();
    rng.random_range(49152..=65535)
}

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

async fn start_server(
    host: &str,
    port: u16,
    pool: sqlx::Pool<db::DatabaseType>,
    shutdown: ShutdownNotifier,
    tls: Option<server::flight::TlsConfig>,
) -> tokio::task::JoinHandle<()> {
    // Ensure that params are loaded
    params::load_params_from_env().unwrap();

    let database = db::testing::Database::new(pool);
    let store = store::testing::Store::new_random_on_tmp().unwrap();
    let config = server::flight::Config {
        host: host.to_owned(),
        port,
        tls,
    };

    let handle = tokio::task::spawn(async move {
        if let Err(err) = server::flight::start(
            config,
            (*store).clone(),
            (*database).clone(),
            Some(shutdown),
        )
        .await
        {
            panic!("flight server error: {}", err);
        }
        println!("server stopped");
    });

    // Wait a little to be sure that server port is binded
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    handle
}

pub struct ServerBuilder {
    host: String,
    port: u16,
    pool: sqlx::Pool<db::DatabaseType>,
    tls: Option<server::flight::TlsConfig>,
}

impl ServerBuilder {
    pub fn new(host: &str, port: u16, pool: sqlx::Pool<db::DatabaseType>) -> Self {
        Self {
            host: host.to_owned(),
            port,
            pool,
            tls: None,
        }
    }

    pub fn enable_tls(mut self) -> Self {
        self.tls = Some(server::flight::TlsConfig {
            certificate_file: TLS_CERT_FILE.to_owned().into(),
            private_key_file: TLS_PRIVATE_KEY_FILE.to_owned().into(),
        });
        self
    }

    pub async fn build(self) -> Server {
        let shutdown = ShutdownNotifier::default();
        Server {
            server_join_handle: start_server(
                &self.host,
                self.port,
                self.pool,
                shutdown.clone(),
                self.tls,
            )
            .await,
            shutdown,
        }
    }
}

/// A wrapper around a MosaicoD Flight server instance.
///
/// ### Usage:
/// ```no_run
/// use tests::common;
/// use mosaicod_db as db;
///
/// async fn test(pool: sqlx::Pool<db::DatabaseType>) {
///     let port = common::random_port();
///     let server = common::ServerBuilder::new(common::HOST, port, pool).build().await;
///     // ... run tests ...
///     server.shutdown().await;
/// }
/// ```
pub struct Server {
    shutdown: ShutdownNotifier,
    server_join_handle: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Signals the server to stop and waits for the background task to complete.
    pub async fn shutdown(self) {
        self.shutdown.shutdown();
        let _ = tokio::join!(self.server_join_handle);
    }
}

pub struct ClientBuilder {
    url: url::Url,
    tls: Option<tonic::transport::ClientTlsConfig>,
}

impl ClientBuilder {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            url: format_endpoint(host, port, false)
                .parse()
                .expect("unable to convert host"),
            tls: None,
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

        let client = FlightServiceClient::new(channel);

        Client { client }
    }
}

/// A dummy client that communicates to mosaicod.
pub struct Client {
    client: FlightServiceClient<tonic::transport::Channel>,
}

impl Client {}

impl std::ops::Deref for Client {
    type Target = FlightServiceClient<tonic::transport::Channel>;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl std::ops::DerefMut for Client {
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
    pub response: serde_json::Value,
}

impl ActionResponse {
    /// Deserializes a raw byte slice from a Flight `Result` into an `ActionResponse`.
    pub fn from_body(body: &[u8]) -> Self {
        serde_json::from_slice(body).expect("problem deserializing action response")
    }
}

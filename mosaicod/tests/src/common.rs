use arrow_flight::flight_service_client::FlightServiceClient;
use mosaicod_core::params;
use mosaicod_repo as repo;
use mosaicod_server::{self as server, flight::ShutdownNotifier};
use mosaicod_store as store;
use serde::Deserialize;

/// The local loopback address for testing.
pub const HOST: &str = "127.0.0.1";

/// Generates a random port in the ephemeral range [49152, 65535].
/// Use this in tests to avoid `Address In Use` errors during parallel execution.
pub fn random_port() -> u16 {
    use rand::Rng;

    let mut rng = rand::rng();
    rng.random_range(49152..=65535)
}

/// Formats host and port into a valid endpoint string.
pub fn format_endpoint(host: &str, port: u16) -> String {
    format!("grpc://{host}:{port}")
}

async fn start_server(
    host: &str,
    port: u16,
    pool: sqlx::Pool<repo::Database>,
    shutdown: ShutdownNotifier,
) -> tokio::task::JoinHandle<()> {
    // Ensure that params are loaded
    params::load_configurables_from_env();

    let repo = repo::testing::Repository::new(pool);
    let store = store::testing::Store::new_random_on_tmp().unwrap();
    let config = server::flight::Config {
        host: host.to_owned(),
        port,
    };

    let handle = tokio::task::spawn(async move {
        if let Err(err) =
            server::flight::start(config, (*store).clone(), (*repo).clone(), Some(shutdown)).await
        {
            panic!("flight server error: {}", err);
        }
        println!("server stopped");
    });

    // Wait a little to be sure that server port is binded
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    handle
}

/// A wrapper around a MosaicoD Flight server instance.
///
/// ### Usage:
/// ```no_run
/// use tests::common;
/// use mosaicod_repo as repo;
///
/// async fn test(pool: sqlx::Pool<repo::Database>) {
///     let port = common::random_port();
///     let server = common::Server::new(common::HOST, port, pool).await;
///     // ... run tests ...
///     server.shutdown().await;
/// }
/// ```
pub struct Server {
    shutdown: ShutdownNotifier,
    server_join_handle: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Creates and starts a new Flight server on the specified host and port.
    pub async fn new(host: &str, port: u16, pool: sqlx::Pool<repo::Database>) -> Self {
        let shutdown = ShutdownNotifier::default();
        Self {
            server_join_handle: start_server(host, port, pool, shutdown.clone()).await,
            shutdown,
        }
    }

    /// Signals the server to stop and waits for the background task to complete.
    pub async fn shutdown(self) {
        self.shutdown.shutdown();
        let _ = tokio::join!(self.server_join_handle);
    }
}

/// A dummy client that communicates to mosaicod.
pub struct Client {
    client: FlightServiceClient<tonic::transport::Channel>,
}

impl Client {
    /// Establishes a connection to a Flight server at the specified host and port.
    pub async fn new(host: &str, port: u16) -> Self {
        let url = format_endpoint(host, port);
        let channel = tonic::transport::Channel::from_shared(url.clone())
            .expect("Unable to create tonic channel")
            .connect()
            .await
            .unwrap_or_else(|_| panic!("Unable to connect to `{}`", url));

        let client = FlightServiceClient::new(channel);

        Self { client }
    }
}

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

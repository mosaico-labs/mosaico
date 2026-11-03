//! Delivery of hook events: the HTTP `POST`s mosaicod sends when a topic is uploaded or a
//! session is finalized. See `mosaicod hook`.
//!
//! Delivery is best effort. Each server process runs one worker, fed through a bounded queue by
//! every [`Dispatcher`]. Events are dropped when the queue is full, when a hook keeps failing
//! after the last retry, and on shutdown.

use bytes::Bytes;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_marshal as marshal;
use reqwest::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// An event, with JSON user metadata.
pub type Event = types::HookEvent<marshal::JsonMetadataBlob>;

/// The worker couldn't be started.
#[derive(Debug, thiserror::Error)]
#[error("cannot build the hook HTTP client: {0}")]
pub struct Error(#[from] reqwest::Error);

/// Hands events to the worker that delivers them.
///
/// Cloning is cheap, and every clone feeds the same worker.
#[derive(Clone)]
pub struct Dispatcher {
    tx: mpsc::Sender<Event>,
}

impl Dispatcher {
    /// Spawns the worker on the current Tokio runtime and returns a dispatcher that feeds it.
    ///
    /// The worker stops when `shutdown` is cancelled or every dispatcher has been dropped,
    /// dropping the events still queued or being delivered.
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime.
    pub fn spawn(db: db::Database, shutdown: CancellationToken) -> Result<Self, Error> {
        Self::spawn_with(db, shutdown, Policy::default())
    }

    fn spawn_with(
        db: db::Database,
        shutdown: CancellationToken,
        policy: Policy,
    ) -> Result<Self, Error> {
        let client = build_client()?;
        let (tx, rx) = mpsc::channel(policy.queue_capacity);
        tokio::spawn(run_worker(rx, db, client, Arc::new(policy), shutdown));
        Ok(Self { tx })
    }

    /// Queues `event` for delivery to every hook.
    ///
    /// Never waits: when the queue is full, the event is dropped and a warning logged.
    pub fn emit(&self, event: Event) {
        let name = event.name();

        match self.tx.try_send(event) {
            Ok(()) => (),
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("hook queue is full, dropping a `{name}` event");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("hook worker has stopped, dropping a `{name}` event");
            }
        }
    }
}

fn build_client() -> Result<reqwest::Client, Error> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("mosaicod/", env!("CARGO_PKG_VERSION")))
        .build()?)
}

/// How events are queued and delivered.
struct Policy {
    /// Events waiting for the worker.
    queue_capacity: usize,
    /// Deliveries in flight at once, across all hooks.
    max_concurrent_deliveries: usize,
    /// Timeout of each attempt, connecting included.
    attempt_timeout: Duration,
    /// Waits before each retry. A delivery makes one attempt more than there are waits.
    retry_waits: Vec<Duration>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            max_concurrent_deliveries: 16,
            attempt_timeout: Duration::from_secs(5),
            retry_waits: vec![Duration::from_secs(1), Duration::from_secs(10)],
        }
    }
}

async fn run_worker(
    mut rx: mpsc::Receiver<Event>,
    db: db::Database,
    client: reqwest::Client,
    policy: Arc<Policy>,
    shutdown: CancellationToken,
) {
    let permits = Arc::new(Semaphore::new(policy.max_concurrent_deliveries));

    loop {
        let event = tokio::select! {
            _ = shutdown.cancelled() => break,
            event = rx.recv() => match event {
                Some(event) => event,
                None => break,
            },
        };

        let name = event.name();

        let hooks = match db::hook_find_all(&mut db.connection()).await {
            Ok(hooks) => hooks,
            Err(e) => {
                warn!("cannot load the hooks, dropping a `{name}` event: {e}");
                continue;
            }
        };

        if hooks.is_empty() {
            continue;
        }

        let event_id = types::Uuid::new();

        let body = match serde_json::to_vec(&marshal::JsonHookEvent::new(&event_id, event)) {
            Ok(body) => Bytes::from(body),
            Err(e) => {
                warn!("cannot serialize a `{name}` event, dropping it: {e}");
                continue;
            }
        };

        for hook in hooks {
            // Waiting here stops the worker from reading the queue, so a slow hook fills it
            // up instead of piling up deliveries.
            let permit = tokio::select! {
                _ = shutdown.cancelled() => return,
                permit = permits.clone().acquire_owned() => {
                    permit.expect("the semaphore is never closed")
                }
            };

            let client = client.clone();
            let policy = policy.clone();
            let shutdown = shutdown.clone();
            let event_id = event_id.clone();
            let body = body.clone();

            tokio::spawn(async move {
                let _permit = permit;

                tokio::select! {
                    _ = shutdown.cancelled() => (),
                    res = deliver(&client, &hook.url, body, &policy) => {
                        if let Err(e) = res {
                            warn!(
                                "delivery of `{name}` event {event_id} to hook {} ({}) failed: {e}",
                                hook.hook_id, hook.url,
                            );
                        }
                    }
                }
            });
        }
    }

    debug!("hook worker stopped");
}

/// Why the last attempt of a delivery failed.
#[derive(Debug, thiserror::Error)]
enum DeliveryError {
    /// Connecting, a timeout, or anything else before a response arrived.
    #[error("{0}")]
    Transport(reqwest::Error),
    #[error("the hook answered {0}")]
    Status(StatusCode),
}

impl DeliveryError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status(status) => {
                *status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
            }
        }
    }
}

/// Sends `body` to `url`, retrying as `policy` says.
async fn deliver(
    client: &reqwest::Client,
    url: &str,
    body: Bytes,
    policy: &Policy,
) -> Result<(), DeliveryError> {
    let mut waits = policy.retry_waits.iter();

    loop {
        let err = match send(client, url, body.clone(), policy.attempt_timeout).await {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };

        match waits.next() {
            Some(wait) if err.is_retryable() => {
                debug!("delivery to {url} failed, retrying in {wait:?}: {err}");
                tokio::time::sleep(*wait).await;
            }
            _ => return Err(err),
        }
    }
}

async fn send(
    client: &reqwest::Client,
    url: &str,
    body: Bytes,
    timeout: Duration,
) -> Result<(), DeliveryError> {
    let response = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .timeout(timeout)
        .body(body)
        .send()
        .await
        .map_err(DeliveryError::Transport)?;

    let status = response.status();

    if status.is_success() {
        Ok(())
    } else {
        Err(DeliveryError::Status(status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// How the test server answers a request.
    #[derive(Clone, Copy)]
    enum Reply {
        Status(u16),
        /// Never answers, so the attempt times out.
        Hang,
        /// A `302` to another path.
        Redirect,
    }

    struct Request {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl Request {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.as_str())
        }

        fn event_id(&self) -> String {
            let json: serde_json::Value = serde_json::from_slice(&self.body).unwrap();
            json["id"].as_str().unwrap().to_owned()
        }
    }

    /// Answers the n-th request with the n-th reply, and with `200` once the replies run out.
    struct Server {
        url: String,
        requests: Arc<Mutex<Vec<Request>>>,
    }

    impl Server {
        async fn start(replies: Vec<Reply>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/hook", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));

            let recorded = requests.clone();
            tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let request = read_request(&mut stream).await;

                    let reply = {
                        let mut recorded = recorded.lock().unwrap();
                        let reply = replies.get(recorded.len()).copied();
                        recorded.push(request);
                        reply.unwrap_or(Reply::Status(200))
                    };

                    tokio::spawn(async move {
                        let response = match reply {
                            Reply::Status(code) => format!(
                                "HTTP/1.1 {code} Test\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                            ),
                            Reply::Redirect => "HTTP/1.1 302 Found\r\nlocation: /elsewhere\r\n\
                                content-length: 0\r\nconnection: close\r\n\r\n"
                                .to_owned(),
                            Reply::Hang => {
                                tokio::time::sleep(Duration::from_secs(60)).await;
                                return;
                            }
                        };
                        let _ = stream.write_all(response.as_bytes()).await;
                    });
                }
            });

            Self { url, requests }
        }

        fn requests(&self) -> std::sync::MutexGuard<'_, Vec<Request>> {
            self.requests.lock().unwrap()
        }
    }

    async fn read_request(stream: &mut TcpStream) -> Request {
        let mut buf = Vec::new();
        let mut chunk = [0; 4096];

        let head_len = loop {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "connection closed mid-request");
            buf.extend_from_slice(&chunk[..n]);

            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };

        let head = String::from_utf8(buf[..head_len].to_vec()).unwrap();
        let mut lines = head.split("\r\n");
        let mut request_line = lines.next().unwrap().split(' ');
        let method = request_line.next().unwrap().to_owned();
        let path = request_line.next().unwrap().to_owned();

        let headers: Vec<(String, String)> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            .collect();

        let mut request = Request {
            method,
            path,
            headers,
            body: buf[head_len..].to_vec(),
        };

        let len: usize = request
            .header("content-length")
            .map_or(0, |len| len.parse().unwrap());

        while request.body.len() < len {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "connection closed mid-body");
            request.body.extend_from_slice(&chunk[..n]);
        }

        request
    }

    fn test_policy() -> Policy {
        Policy {
            queue_capacity: 8,
            max_concurrent_deliveries: 4,
            attempt_timeout: Duration::from_millis(200),
            retry_waits: vec![Duration::from_millis(10); 2],
        }
    }

    async fn deliver_to(url: &str, policy: &Policy) -> Result<(), DeliveryError> {
        let body = Bytes::from_static(br#"{"id":"test"}"#);
        deliver(&build_client().unwrap(), url, body, policy).await
    }

    fn session_uploaded() -> Event {
        types::HookEvent::SessionUploaded(types::SessionMetadata {
            locator: "drive_042:session".parse().unwrap(),
            topics: vec![],
            created_at: 0.into(),
            completed_at: None,
        })
    }

    #[tokio::test]
    async fn delivers_body_and_headers() {
        let server = Server::start(vec![]).await;

        deliver_to(&server.url, &test_policy()).await.unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 1);

        let request = &requests[0];
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/hook");
        assert_eq!(request.header("content-type"), Some("application/json"));
        assert!(
            request
                .header("user-agent")
                .unwrap()
                .starts_with("mosaicod/")
        );
        assert_eq!(request.body, br#"{"id":"test"}"#);
    }

    #[tokio::test]
    async fn retries_server_errors_and_too_many_requests() {
        let server = Server::start(vec![Reply::Status(500), Reply::Status(429)]).await;

        deliver_to(&server.url, &test_policy()).await.unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|r| r.body == requests[0].body));
    }

    #[tokio::test]
    async fn gives_up_after_the_last_attempt() {
        let server = Server::start(vec![Reply::Status(503); 4]).await;

        let err = deliver_to(&server.url, &test_policy()).await.unwrap_err();

        assert!(matches!(
            err,
            DeliveryError::Status(StatusCode::SERVICE_UNAVAILABLE)
        ));
        assert_eq!(server.requests().len(), 3);
    }

    #[tokio::test]
    async fn does_not_retry_other_statuses() {
        for code in [400, 401, 404] {
            let server = Server::start(vec![Reply::Status(code)]).await;

            let err = deliver_to(&server.url, &test_policy()).await.unwrap_err();

            assert!(matches!(err, DeliveryError::Status(s) if s.as_u16() == code));
            assert_eq!(server.requests().len(), 1, "{code}");
        }
    }

    #[tokio::test]
    async fn does_not_follow_redirects() {
        let server = Server::start(vec![Reply::Redirect]).await;

        let err = deliver_to(&server.url, &test_policy()).await.unwrap_err();

        assert!(matches!(err, DeliveryError::Status(StatusCode::FOUND)));
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn retries_timeouts() {
        let server = Server::start(vec![Reply::Hang]).await;

        deliver_to(&server.url, &test_policy()).await.unwrap();

        assert_eq!(server.requests().len(), 2);
    }

    #[tokio::test]
    async fn retries_refused_connections() {
        // Nothing listens on this port once the listener is dropped.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        drop(listener);

        let policy = Policy {
            retry_waits: vec![Duration::from_millis(100); 2],
            ..test_policy()
        };

        let start = Instant::now();
        let err = deliver_to(&url, &policy).await.unwrap_err();

        assert!(matches!(err, DeliveryError::Transport(_)));
        // It waited before both retries.
        assert!(start.elapsed() >= Duration::from_millis(200));
    }

    #[test]
    fn emit_drops_events_when_the_queue_is_full() {
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = Dispatcher { tx };

        dispatcher.emit(session_uploaded());
        dispatcher.emit(session_uploaded());

        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());

        // With the worker gone, events are dropped too.
        drop(rx);
        dispatcher.emit(session_uploaded());
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn worker_sends_each_event_to_every_hook(
        pool: sqlx::Pool<db::DatabaseType>,
    ) -> sqlx::Result<()> {
        let database = db::testing::Database::new(pool);
        let first = Server::start(vec![]).await;
        let second = Server::start(vec![]).await;

        db::hook_create(&mut database.connection(), &first.url, 0)
            .await
            .unwrap();

        let shutdown = CancellationToken::new();
        let dispatcher =
            Dispatcher::spawn_with((*database).clone(), shutdown.clone(), test_policy()).unwrap();

        // Registered after the worker started: hooks are read on every event.
        db::hook_create(&mut database.connection(), &second.url, 0)
            .await
            .unwrap();

        dispatcher.emit(session_uploaded());
        dispatcher.emit(session_uploaded());

        let deadline = Instant::now() + Duration::from_secs(5);
        while first.requests().len() < 2 || second.requests().len() < 2 {
            assert!(Instant::now() < deadline, "events not delivered in time");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Each event has its own id, and both hooks got the same ones.
        let ids = |server: &Server| {
            let mut ids: Vec<String> = server.requests().iter().map(Request::event_id).collect();
            ids.sort();
            ids
        };
        let first_ids = ids(&first);
        assert_ne!(first_ids[0], first_ids[1]);
        assert_eq!(first_ids, ids(&second));

        shutdown.cancel();

        Ok(())
    }
}

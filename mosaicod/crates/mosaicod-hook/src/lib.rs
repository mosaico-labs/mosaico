//! Delivery of hook events: the HTTP `POST`s mosaicod sends. See `mosaicod hook`.
//!
//! Delivery is best effort. Each server process runs one worker, fed through a bounded queue by
//! every [`Dispatcher`]. Events are dropped when the queue is full, when a hook keeps failing
//! after the last retry, and on shutdown.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mosaicod_config::default;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_marshal as marshal;
use reqwest::StatusCode;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

// The limits `Policy::default` sets. Each one is described on its `Policy` field.
const QUEUE_CAPACITY: usize = 1024;
const MAX_CONCURRENT_DELIVERIES: usize = 16;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_WAITS: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(10)];

/// An event, with JSON user metadata.
pub type Event = types::HookEvent<marshal::JsonMetadataBlob>;

/// The worker couldn't be started.
#[derive(Debug, thiserror::Error)]
#[error("cannot build the hook HTTP client: {0}")]
pub struct Error(#[from] reqwest::Error);

impl mosaicod_core::error::PublicError for Error {
    fn error(&self) -> mosaicod_core::Error {
        mosaicod_core::Error::internal(Some(self.to_string()))
    }
}

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
        let policy = Policy::default();
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
        .user_agent(default::user_agent())
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
            queue_capacity: QUEUE_CAPACITY,
            max_concurrent_deliveries: MAX_CONCURRENT_DELIVERIES,
            attempt_timeout: ATTEMPT_TIMEOUT,
            retry_waits: RETRY_WAITS.to_vec(),
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

        let body = match marshal::JsonHookEvent::new(&event_id, event).to_bytes() {
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
                    res = deliver(&client, hook.hook_id, &hook.url, body, &policy) => {
                        if let Err(e) = res {
                            warn!(
                                "delivery of `{name}` event {event_id} to hook {} failed: {e}",
                                hook.hook_id,
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

/// Sends `body` to `url`, the URL of hook `hook_id`, retrying as `policy` says.
async fn deliver(
    client: &reqwest::Client,
    hook_id: i32,
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
                debug!("delivery to hook {hook_id} failed, retrying in {wait:?}: {err}");
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
        .map_err(|e| DeliveryError::Transport(e.without_url()))?;

    let status = response.status();

    if status.is_success() {
        Ok(())
    } else {
        Err(DeliveryError::Status(status))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use httpmock::Method::POST;
    use httpmock::{Mock, MockServer, Then};

    use super::*;

    /// The body [`deliver_to`] sends.
    const BODY: &str = r#"{"id":"test"}"#;

    fn test_policy() -> Policy {
        Policy {
            queue_capacity: 8,
            max_concurrent_deliveries: 4,
            attempt_timeout: Duration::from_millis(200),
            retry_waits: vec![Duration::from_millis(10); 2],
        }
    }

    async fn deliver_to(url: &str, policy: &Policy) -> Result<(), DeliveryError> {
        let body = Bytes::from_static(BODY.as_bytes());
        deliver(&build_client().unwrap(), 1, url, body, policy).await
    }

    /// A hook on `receiver` that gets [`BODY`] and replies as `reply` says. A request with
    /// another body gets a 404.
    async fn hook_replying(receiver: &MockServer, reply: impl FnOnce(Then) -> Then) -> Mock<'_> {
        receiver
            .mock_async(|when, then| {
                when.body(BODY);
                reply(then);
            })
            .await
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
        let receiver = MockServer::start_async().await;
        let hook = receiver
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/hook")
                    .header("content-type", "application/json")
                    .header("user-agent", default::user_agent())
                    .body(BODY);
                then.status(200);
            })
            .await;

        let delivered = deliver_to(&receiver.url("/hook"), &test_policy()).await;

        // Before the result: a request that doesn't match gets a 404, and this says why.
        hook.assert_async().await;
        delivered.unwrap();
    }

    #[tokio::test]
    async fn retries_server_errors_and_too_many_requests() {
        let receiver = MockServer::start_async().await;
        let server_error = hook_replying(&receiver, |then| then.status(500).up_to_n_times(1)).await;
        let throttled = hook_replying(&receiver, |then| then.status(429).up_to_n_times(1)).await;
        let ok = hook_replying(&receiver, |then| then.status(200)).await;

        deliver_to(&receiver.base_url(), &test_policy())
            .await
            .unwrap();

        server_error.assert_async().await;
        throttled.assert_async().await;
        ok.assert_async().await;
    }

    #[tokio::test]
    async fn gives_up_after_the_last_attempt() {
        let receiver = MockServer::start_async().await;
        let unavailable = hook_replying(&receiver, |then| then.status(503)).await;

        let err = deliver_to(&receiver.base_url(), &test_policy())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            DeliveryError::Status(StatusCode::SERVICE_UNAVAILABLE)
        ));
        unavailable.assert_calls_async(3).await;
    }

    #[tokio::test]
    async fn does_not_retry_other_statuses() {
        for code in [400, 401, 404] {
            let receiver = MockServer::start_async().await;
            let rejecting = hook_replying(&receiver, |then| then.status(code)).await;

            let err = deliver_to(&receiver.base_url(), &test_policy())
                .await
                .unwrap_err();

            assert!(matches!(err, DeliveryError::Status(s) if s.as_u16() == code));
            assert_eq!(rejecting.calls_async().await, 1, "{code}");
        }
    }

    #[tokio::test]
    async fn does_not_follow_redirects() {
        let receiver = MockServer::start_async().await;
        let redirecting = hook_replying(&receiver, |then| {
            then.status(302).header("location", "/elsewhere")
        })
        .await;

        let err = deliver_to(&receiver.base_url(), &test_policy())
            .await
            .unwrap_err();

        assert!(matches!(err, DeliveryError::Status(StatusCode::FOUND)));
        redirecting.assert_async().await;
    }

    #[tokio::test]
    async fn retries_timeouts() {
        let receiver = MockServer::start_async().await;
        let hanging = hook_replying(&receiver, |then| {
            then.status(200)
                .delay(Duration::from_secs(60))
                .up_to_n_times(1)
        })
        .await;
        let ok = hook_replying(&receiver, |then| then.status(200)).await;

        deliver_to(&receiver.base_url(), &test_policy())
            .await
            .unwrap();

        hanging.assert_async().await;
        ok.assert_async().await;
    }

    #[tokio::test]
    async fn retries_refused_connections() {
        // Nothing listens on this port once the listener is dropped.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let url = format!("http://{addr}/hook");
        drop(listener);

        let policy = Policy {
            retry_waits: vec![Duration::from_millis(100); 2],
            ..test_policy()
        };

        let start = Instant::now();
        let err = deliver_to(&url, &policy).await.unwrap_err();

        assert!(matches!(err, DeliveryError::Transport(_)));
        assert!(!err.to_string().contains(&addr), "{err}");
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
}

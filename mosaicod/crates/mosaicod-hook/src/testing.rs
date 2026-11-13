//! A minimal HTTP server standing in for a hook in tests.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long [`Receiver::wait_for_requests`] waits before failing the test.
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// How the receiver answers a request.
#[derive(Clone, Copy)]
pub enum Reply {
    Status(u16),
    /// Never answers, so the attempt times out.
    Hang,
    /// A `302` to another path.
    Redirect,
}

/// A request the receiver got.
#[derive(Clone)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// The body, parsed as JSON.
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("the body is not JSON")
    }
}

/// Records every request, and answers the n-th one with the n-th reply, or with `200` once the
/// replies run out.
pub struct Receiver {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Receiver {
    /// A receiver that answers `200` to everything.
    pub async fn start() -> Self {
        Self::start_with(vec![]).await
    }

    pub async fn start_with(replies: Vec<Reply>) -> Self {
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

    /// The URL to register the receiver with.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A copy of the requests received so far. A copy rather than the lock, so tests can hold it
    /// across an `await`.
    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    /// Waits until at least `n` requests have arrived, and returns them.
    ///
    /// # Panics
    ///
    /// If they don't arrive within a few seconds.
    pub async fn wait_for_requests(&self, n: usize) -> Vec<Request> {
        let deadline = Instant::now() + WAIT_TIMEOUT;

        loop {
            let requests = self.requests();
            let got = requests.len();
            if got >= n {
                return requests;
            }

            assert!(
                Instant::now() < deadline,
                "got {got} hook requests out of {n} in {WAIT_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

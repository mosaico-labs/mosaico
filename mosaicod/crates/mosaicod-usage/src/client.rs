//! The HTTP client the exporter sends through, which sends no header of the customer's.
//!
//! The stock exporter always adds the headers of `OTEL_EXPORTER_OTLP_HEADERS` and
//! `OTEL_EXPORTER_OTLP_LOGS_HEADERS` to its requests: unlike its other settings, values set
//! in code can't override them. A customer who sets them for their own collector, often
//! with a vendor API key in them, would send that key to us along with the usage reports,
//! so [`OwnHeadersOnly`] drops them.

use std::time::Duration;

use async_trait::async_trait;
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};
use tracing::debug;

/// Sends each request with only the headers that describe its body, plus its own
/// `User-Agent`.
#[derive(Debug)]
pub(crate) struct OwnHeadersOnly(reqwest::Client);

impl OwnHeadersOnly {
    /// `timeout` bounds each request, connection included.
    pub(crate) fn new(user_agent: &str, timeout: Duration) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .user_agent(user_agent)
            .timeout(timeout)
            .build()?;

        Ok(Self(client))
    }
}

#[async_trait]
impl HttpClient for OwnHeadersOnly {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();

        let mut outgoing = self
            .0
            .request(parts.method, parts.uri.to_string())
            .body(body);
        for name in [CONTENT_TYPE, CONTENT_ENCODING] {
            if let Some(value) = parts.headers.get(&name) {
                outgoing = outgoing.header(name, value);
            }
        }

        // Usage reporting must never get in the way of the server, so a failure is only
        // worth a debug line: the exporter retries a few times, then drops the batch.
        let mut response = outgoing
            .send()
            .await
            .inspect_err(|e| debug!("usage report not delivered: {e}"))?;
        let status = response.status();
        if !status.is_success() {
            debug!("usage report not delivered: the endpoint answered {status}");
        }

        // Its headers too: the exporter reads `Retry-After` to back off when throttled.
        let headers = std::mem::take(response.headers_mut());
        let mut forwarded = Response::new(response.bytes().await?);
        *forwarded.status_mut() = status;
        *forwarded.headers_mut() = headers;
        Ok(forwarded)
    }
}

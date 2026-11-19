//! Tower layer recording OpenTelemetry metrics and spans for gRPC calls.
//!
//! Arrow Flight calls are mostly streams, so the call is measured until the response body
//! ends (or is dropped), not until the response headers are sent. The gRPC status code is
//! read from the response headers (trailers-only responses) or from the trailers.

use crate::{METER_NAME, RPC_SPAN_TARGET};
use http::{HeaderMap, Request, Response};
use http_body::{Body, Frame, SizeHint};
use opentelemetry::{
    KeyValue, StringValue, global,
    metrics::{Histogram, Meter, UpDownCounter},
    propagation::Extractor,
    trace::TraceContextExt as _,
};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Instant,
};
use tower::{Layer, Service};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

const GRPC_STATUS: &str = "grpc-status";

#[cfg(test)]
const CODE_OK: i64 = 0;
const CODE_CANCELLED: i64 = 1;
const CODE_UNKNOWN: i64 = 2;
const CODE_UNIMPLEMENTED: i64 = 12;

/// Replaces `rpc.service` and `rpc.method` for unknown methods, in metrics and spans alike,
/// so a client cannot create unbounded label values or span names by calling random paths.
const OTHER: &str = "_OTHER";

/// Histogram boundaries in seconds, from 5 ms to 10 minutes, to cover both unary actions
/// and long upload/download streams.
const DURATION_BOUNDARIES: [f64; 15] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0,
];

#[derive(Clone)]
struct Instruments {
    duration: Histogram<f64>,
    active: UpDownCounter<i64>,
}

impl Instruments {
    fn new(meter: &Meter) -> Self {
        Self {
            duration: meter
                .f64_histogram("rpc.server.call.duration")
                .with_description("Duration of gRPC calls, until the response stream ends.")
                .with_unit("s")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build(),
            active: meter
                .i64_up_down_counter("rpc.server.active_calls")
                .with_description("Number of gRPC calls currently in progress.")
                .with_unit("{call}")
                .build(),
        }
    }
}

/// Tower layer that measures every gRPC call. See the [module documentation](self).
#[derive(Clone)]
pub struct RpcLayer {
    instruments: Instruments,
}

impl RpcLayer {
    /// Creates the layer using the global meter provider.
    ///
    /// The global provider must be installed (see [`Telemetry`](crate::Telemetry)) before
    /// calling this, otherwise the instruments are no-op.
    pub fn new() -> Self {
        Self::with_meter(&global::meter(METER_NAME))
    }

    fn with_meter(meter: &Meter) -> Self {
        Self {
            instruments: Instruments::new(meter),
        }
    }
}

impl Default for RpcLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for RpcLayer {
    type Service = RpcService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcService {
            inner,
            instruments: self.instruments.clone(),
        }
    }
}

/// Service produced by [`RpcLayer`].
#[derive(Clone)]
pub struct RpcService<S> {
    inner: S,
    instruments: Instruments,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RpcService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = Response<RpcBody<ResBody>>;
    type Error = S::Error;
    type Future = RpcResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let (service, method) = split_path(req.uri().path());

        let span = tracing::info_span!(
            target: RPC_SPAN_TARGET,
            "rpc",
            otel.name = %SpanName(service, method),
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            rpc.grpc.status_code = tracing::field::Empty,
        );
        // Set directly on the OpenTelemetry span, unlike `tracing` fields, which copy every
        // string value. `rpc.service` and `rpc.method` are set when the call ends, with the
        // same values as the metric.
        span.set_attribute("rpc.system", "grpc");

        // Continue the client trace, if the request carries a `traceparent` header.
        let parent =
            global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(req.headers())));
        let _ = span.set_parent(parent);

        let guard = CallGuard::start(
            self.instruments.clone(),
            shared(service),
            shared(method),
            span,
        );

        let inner = guard.span.in_scope(|| self.inner.call(req));

        RpcResponseFuture {
            inner,
            guard: Some(guard),
        }
    }
}

pin_project_lite::pin_project! {
    /// Response future of [`RpcService`].
    pub struct RpcResponseFuture<F> {
        #[pin]
        inner: F,
        guard: Option<CallGuard>,
    }
}

impl<F, B, E> Future for RpcResponseFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
{
    type Output = Result<Response<RpcBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = ready!(in_span(this.guard, || this.inner.poll(cx)));
        let mut guard = this.guard.take();

        Poll::Ready(match result {
            Ok(response) => {
                if let (Some(g), Some(code)) = (guard.as_mut(), grpc_status(response.headers())) {
                    g.set_code(code);
                }
                Ok(response.map(|inner| RpcBody { inner, guard }))
            }
            Err(e) => {
                if let Some(g) = guard.as_mut() {
                    g.set_code(CODE_UNKNOWN);
                }
                Err(e)
            }
        })
    }
}

pin_project_lite::pin_project! {
    /// Response body of [`RpcService`]: it forwards every frame and closes the call
    /// measurement when the stream ends or is dropped.
    pub struct RpcBody<B> {
        #[pin]
        inner: B,
        guard: Option<CallGuard>,
    }
}

impl<B: Default> Default for RpcBody<B> {
    fn default() -> Self {
        Self {
            inner: B::default(),
            guard: None,
        }
    }
}

impl<B: Body> Body for RpcBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let result = ready!(in_span(this.guard, || this.inner.poll_frame(cx)));

        match &result {
            Some(Ok(frame)) => {
                if let Some(code) = frame.trailers_ref().and_then(grpc_status)
                    && let Some(mut guard) = this.guard.take()
                {
                    guard.set_code(code);
                }
            }
            Some(Err(_)) => {
                if let Some(mut guard) = this.guard.take() {
                    guard.set_code(CODE_UNKNOWN);
                }
            }
            None => {
                this.guard.take();
            }
        }

        Poll::Ready(result)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Runs `f` inside the span of the call, if it is still tracked.
fn in_span<R>(guard: &Option<CallGuard>, f: impl FnOnce() -> R) -> R {
    match guard {
        Some(guard) => guard.span.in_scope(f),
        None => f(),
    }
}

/// Tracks one call and records it when dropped.
struct CallGuard {
    instruments: Instruments,
    start: Instant,
    service: StringValue,
    method: StringValue,
    code: Option<i64>,
    span: Span,
}

impl CallGuard {
    fn start(
        instruments: Instruments,
        service: StringValue,
        method: StringValue,
        span: Span,
    ) -> Self {
        instruments
            .active
            .add(1, &[KeyValue::new("rpc.system", "grpc")]);

        Self {
            instruments,
            start: Instant::now(),
            service,
            method,
            code: None,
            span,
        }
    }

    /// Records the status code. The first code wins, since a trailers-only response sets it
    /// in the headers and has no trailers.
    fn set_code(&mut self, code: i64) {
        self.code.get_or_insert(code);
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        // No status means the body was dropped before its end: the client went away.
        let code = self.code.unwrap_or(CODE_CANCELLED);

        let (service, method) = if code == CODE_UNIMPLEMENTED {
            // The method is only known to be unimplemented now, so the span opened with the
            // requested path is renamed like the metric. Recording `otel.name` would not
            // work: the OpenTelemetry span already exists, and it only takes `update_name`.
            self.span.context().span().update_name(OTHER);
            (StringValue::from(OTHER), StringValue::from(OTHER))
        } else {
            (self.service.clone(), self.method.clone())
        };

        // Cheap clones: the strings are reference counted.
        self.span.set_attribute("rpc.service", service.clone());
        self.span.set_attribute("rpc.method", method.clone());

        let attributes = [
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", service),
            KeyValue::new("rpc.method", method),
            KeyValue::new("rpc.grpc.status_code", code),
        ];

        self.instruments
            .duration
            .record(self.start.elapsed().as_secs_f64(), &attributes);
        self.instruments
            .active
            .add(-1, &[KeyValue::new("rpc.system", "grpc")]);

        self.span.record("rpc.grpc.status_code", code);
        if is_server_error(code) {
            self.span.record("otel.status_code", "ERROR");
        }
    }
}

/// Splits a gRPC path (`/package.Service/Method`) into service and method.
fn split_path(path: &str) -> (&str, &str) {
    path.trim_start_matches('/')
        .split_once('/')
        .filter(|(service, method)| !service.is_empty() && !method.is_empty())
        .unwrap_or((OTHER, OTHER))
}

/// Span name following the OpenTelemetry gRPC conventions: `{service}/{method}`, or
/// [`OTHER`] when the method is unknown. Written straight into the span name, with no
/// intermediate `String`.
struct SpanName<'a>(&'a str, &'a str);

impl fmt::Display for SpanName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpanName(OTHER, _) => f.write_str(OTHER),
            SpanName(service, method) => write!(f, "{service}/{method}"),
        }
    }
}

/// Reference-counted copy of a path segment, shared by the span and the metric. [`OTHER`]
/// is static and needs no allocation.
fn shared(segment: &str) -> StringValue {
    if segment == OTHER {
        StringValue::from(OTHER)
    } else {
        StringValue::from(Arc::<str>::from(segment))
    }
}

fn grpc_status(headers: &HeaderMap) -> Option<i64> {
    headers.get(GRPC_STATUS)?.to_str().ok()?.trim().parse().ok()
}

/// Status codes that mark a server span as failed, following the OpenTelemetry gRPC
/// semantic conventions: client errors (e.g. `NOT_FOUND`) are not server errors.
fn is_server_error(code: i64) -> bool {
    // UNKNOWN, DEADLINE_EXCEEDED, UNIMPLEMENTED, INTERNAL, UNAVAILABLE, DATA_LOSS
    matches!(code, 2 | 4 | 12 | 13 | 14 | 15)
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData},
    };
    use std::convert::Infallible;

    /// Minimal body: one data frame, then optional trailers.
    struct TestBody {
        data: Option<bytes::Bytes>,
        trailers: Option<HeaderMap>,
    }

    impl Body for TestBody {
        type Data = bytes::Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if let Some(data) = self.data.take() {
                return Poll::Ready(Some(Ok(Frame::data(data))));
            }
            Poll::Ready(self.trailers.take().map(|t| Ok(Frame::trailers(t))))
        }
    }

    fn status_headers(code: i64) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_STATUS, code.to_string().parse().unwrap());
        headers
    }

    fn provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        (provider, exporter)
    }

    /// Returns `(count, rpc.method, rpc.grpc.status_code)` of every duration data point.
    fn recorded_calls(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
    ) -> Vec<(u64, String, i64)> {
        provider.force_flush().unwrap();

        let mut calls = Vec::new();
        for rm in exporter.get_finished_metrics().unwrap() {
            for sm in rm.scope_metrics() {
                for metric in sm.metrics() {
                    if metric.name() != "rpc.server.call.duration" {
                        continue;
                    }
                    let AggregatedMetrics::F64(MetricData::Histogram(h)) = metric.data() else {
                        panic!("unexpected metric data");
                    };
                    for dp in h.data_points() {
                        let attr = |k: &str| {
                            dp.attributes()
                                .find(|kv| kv.key.as_str() == k)
                                .map(|kv| kv.value.clone())
                                .unwrap()
                        };
                        let opentelemetry::Value::I64(code) = attr("rpc.grpc.status_code") else {
                            panic!("status code is not an integer");
                        };
                        calls.push((dp.count(), attr("rpc.method").to_string(), code));
                    }
                }
            }
        }
        calls
    }

    async fn drain<B: Body + Unpin>(mut body: B) {
        while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
        {
            assert!(frame.is_ok());
        }
    }

    async fn call(
        layer: &RpcLayer,
        path: &str,
        response: Response<TestBody>,
    ) -> Response<RpcBody<TestBody>> {
        let mut response = Some(response);
        let mut svc = layer.layer(tower::service_fn(move |_req: Request<()>| {
            let r = response.take().unwrap();
            async move { Ok::<_, Infallible>(r) }
        }));
        let req = Request::builder().uri(path).body(()).unwrap();
        svc.call(req).await.unwrap()
    }

    #[test]
    fn split_grpc_path() {
        assert_eq!(
            split_path("/arrow.flight.protocol.FlightService/DoPut"),
            ("arrow.flight.protocol.FlightService", "DoPut")
        );
        assert_eq!(split_path("/"), (OTHER, OTHER));
        assert_eq!(split_path("/only-service"), (OTHER, OTHER));
        assert_eq!(split_path("/service/"), (OTHER, OTHER));
    }

    #[test]
    fn span_names() {
        assert_eq!(
            SpanName("svc.Flight", "DoGet").to_string(),
            "svc.Flight/DoGet"
        );
        assert_eq!(SpanName(OTHER, OTHER).to_string(), OTHER);
    }

    #[test]
    fn parse_grpc_status() {
        assert_eq!(grpc_status(&status_headers(5)), Some(5));
        assert_eq!(grpc_status(&HeaderMap::new()), None);
    }

    #[test]
    fn server_errors() {
        assert!(is_server_error(13));
        assert!(!is_server_error(CODE_OK));
        assert!(!is_server_error(5)); // NOT_FOUND is a client error
    }

    #[tokio::test]
    async fn records_status_from_trailers() {
        let (provider, exporter) = provider();
        let layer = RpcLayer::with_meter(&provider.meter("test"));

        let body = TestBody {
            data: Some(bytes::Bytes::from_static(b"payload")),
            trailers: Some(status_headers(CODE_OK)),
        };
        let response = call(&layer, "/svc.Flight/DoGet", Response::new(body)).await;

        // Nothing is recorded until the stream ends.
        drain(response.into_body()).await;

        assert_eq!(
            recorded_calls(&provider, &exporter),
            vec![(1, "DoGet".to_owned(), CODE_OK)]
        );
    }

    #[tokio::test]
    async fn records_status_from_headers() {
        let (provider, exporter) = provider();
        let layer = RpcLayer::with_meter(&provider.meter("test"));

        let body = TestBody {
            data: None,
            trailers: None,
        };
        let mut response = Response::new(body);
        *response.headers_mut() = status_headers(16); // UNAUTHENTICATED
        let response = call(&layer, "/svc.Flight/DoAction", response).await;
        drop(response);

        assert_eq!(
            recorded_calls(&provider, &exporter),
            vec![(1, "DoAction".to_owned(), 16)]
        );
    }

    #[tokio::test]
    async fn dropped_stream_is_cancelled() {
        let (provider, exporter) = provider();
        let layer = RpcLayer::with_meter(&provider.meter("test"));

        let body = TestBody {
            data: Some(bytes::Bytes::from_static(b"payload")),
            trailers: Some(status_headers(CODE_OK)),
        };
        let response = call(&layer, "/svc.Flight/DoGet", Response::new(body)).await;
        drop(response);

        assert_eq!(
            recorded_calls(&provider, &exporter),
            vec![(1, "DoGet".to_owned(), CODE_CANCELLED)]
        );
    }

    #[tokio::test]
    async fn unimplemented_methods_are_grouped() {
        let (provider, exporter) = provider();
        let layer = RpcLayer::with_meter(&provider.meter("test"));

        for path in ["/svc.Flight/Random1", "/svc.Flight/Random2"] {
            let mut response = Response::new(TestBody {
                data: None,
                trailers: None,
            });
            *response.headers_mut() = status_headers(CODE_UNIMPLEMENTED);
            drop(call(&layer, path, response).await);
        }

        assert_eq!(
            recorded_calls(&provider, &exporter),
            vec![(2, OTHER.to_owned(), CODE_UNIMPLEMENTED)]
        );
    }

    #[tokio::test]
    async fn unimplemented_methods_are_grouped_in_spans() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing_subscriber::layer::SubscriberExt as _;

        let span_exporter = InMemorySpanExporter::default();
        let tracer_provider = SdkTracerProvider::builder()
            .with_simple_exporter(span_exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("test")));
        let _default = tracing::subscriber::set_default(subscriber);

        let (provider, _exporter) = provider();
        let layer = RpcLayer::with_meter(&provider.meter("test"));

        let mut unimplemented = Response::new(TestBody {
            data: None,
            trailers: None,
        });
        *unimplemented.headers_mut() = status_headers(CODE_UNIMPLEMENTED);
        drop(call(&layer, "/svc.Flight/Random", unimplemented).await);

        let implemented = Response::new(TestBody {
            data: None,
            trailers: Some(status_headers(CODE_OK)),
        });
        drain(
            call(&layer, "/svc.Flight/DoGet", implemented)
                .await
                .into_body(),
        )
        .await;

        tracer_provider.force_flush().unwrap();
        let spans = span_exporter.get_finished_spans().unwrap();
        assert!(spans.iter().all(|span| {
            span.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "rpc.system" && kv.value.as_str() == "grpc")
        }));
        let summary: Vec<(String, String)> = spans
            .iter()
            .map(|span| {
                let method = span
                    .attributes
                    .iter()
                    .find(|kv| kv.key.as_str() == "rpc.method")
                    .map(|kv| kv.value.to_string())
                    .unwrap();
                (span.name.to_string(), method)
            })
            .collect();

        assert_eq!(
            summary,
            vec![
                (OTHER.to_owned(), OTHER.to_owned()),
                ("svc.Flight/DoGet".to_owned(), "DoGet".to_owned()),
            ]
        );
    }
}

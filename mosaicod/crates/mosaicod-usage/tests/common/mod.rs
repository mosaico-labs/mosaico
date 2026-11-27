//! Helpers to see what the reporter sends, through an httpmock server.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use httpmock::Method::POST;
use httpmock::{HttpMockRequest, HttpMockResponse, MockServer, When};
use mosaicod_core::types;
use mosaicod_usage::{Config, UsageReporter};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use prost::Message;

/// Reports the identity event and returns the body of the one request the endpoint got.
/// The request must also meet `when`, or the endpoint answers `404` and the test fails.
pub fn report_identity(
    installation_id: Option<types::Uuid>,
    instance_id: i32,
    when: impl FnOnce(When) -> When,
) -> ExportLogsServiceRequest {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let endpoint = MockServer::start();

    let body = Arc::new(Mutex::new(Vec::new()));
    let received = body.clone();
    let export = endpoint.mock(|request, then| {
        when(request.method(POST).path("/v1/logs"));
        then.respond_with(move |request: &HttpMockRequest| {
            *received.lock().unwrap() = request.body_ref().to_vec();
            HttpMockResponse::builder().status(200).build()
        });
    });

    let reporter = UsageReporter::new(
        rt.handle(),
        Config {
            endpoint: endpoint.url("/v1/logs"),
            ..Config::new(installation_id, instance_id)
        },
    )
    .unwrap();
    reporter.emit_identity_event();
    // Returns once the export has ended.
    drop(reporter);

    export.assert();
    ExportLogsServiceRequest::decode(body.lock().unwrap().as_slice()).unwrap()
}

/// The attributes as `key → string value`, for the string-valued ones.
pub fn strings(attributes: &[KeyValue]) -> HashMap<String, String> {
    attributes
        .iter()
        .filter_map(|kv| match kv.value.as_ref() {
            Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            }) => Some((kv.key.clone(), s.clone())),
            _ => None,
        })
        .collect()
}

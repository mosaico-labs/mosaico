//! The customer's `OTEL_*` variables configure their own collector and must never shape
//! usage reports. This test changes the environment, so it lives alone in its own binary:
//! no other test runs in the same process.

#![allow(unused_crate_dependencies)]

mod common;

use mosaicod_core::types;

#[test]
fn otel_variables_never_reach_the_usage_endpoint() {
    // SAFETY: this is the only test of this binary, so no other thread reads the
    // environment while it changes.
    unsafe {
        std::env::set_var("OTEL_EXPORTER_OTLP_HEADERS", "x-vendor-key=secret");
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
            "x-vendor-logs-key=secret",
        );
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9");
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "http://127.0.0.1:9/v1/logs",
        );
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json");
        std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "customer.team=robotics");
        std::env::set_var("OTEL_SERVICE_NAME", "customer-service");
    }

    // The runtime and the endpoint start only now, so that none of their threads runs while
    // the environment changes. Reaching the endpoint at all shows the endpoint variables
    // were ignored.
    let body = common::report_identity(Some(types::Uuid::new()), 1, |when| {
        when.header("content-type", "application/x-protobuf")
            .header_missing("x-vendor-key")
            .header_missing("x-vendor-logs-key")
    });

    let resource = common::strings(&body.resource_logs[0].resource.as_ref().unwrap().attributes);
    assert_eq!(resource["service.name"], "mosaicod");
    assert!(!resource.contains_key("customer.team"));
}

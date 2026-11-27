#![allow(unused_crate_dependencies)]

mod common;

use mosaicod_config::default;
use mosaicod_core::types;

#[test]
fn identity_reaches_the_endpoint_with_ids_as_resource_attributes() {
    let installation_id = types::Uuid::new();

    let body = common::report_identity(Some(installation_id.clone()), 7, |when| {
        when.header("content-type", "application/x-protobuf")
            .header("user-agent", default::user_agent())
    });

    let [resource_logs] = body.resource_logs.as_slice() else {
        panic!("expected one resource");
    };
    let resource = common::strings(&resource_logs.resource.as_ref().unwrap().attributes);
    assert_eq!(resource["service.name"], "mosaicod");
    assert_eq!(resource["service.version"], default::version());
    assert_eq!(resource["service.namespace"], installation_id.to_string());
    assert_eq!(
        resource["mosaicod.installation_id"],
        installation_id.to_string()
    );
    assert_eq!(resource["service.instance.id"], "7");

    let [scope_logs] = resource_logs.scope_logs.as_slice() else {
        panic!("expected one scope");
    };
    let [record] = scope_logs.log_records.as_slice() else {
        panic!("expected one record");
    };
    assert_eq!(record.event_name, "mosaicod.identity");
}

#[test]
fn unknown_installation_id_is_sent_as_the_nil_uuid() {
    let body = common::report_identity(None, 7, |when| when);

    let [resource_logs] = body.resource_logs.as_slice() else {
        panic!("expected one resource");
    };
    let resource = common::strings(&resource_logs.resource.as_ref().unwrap().attributes);
    let nil = "00000000-0000-0000-0000-000000000000";
    assert_eq!(resource["service.namespace"], nil);
    assert_eq!(resource["mosaicod.installation_id"], nil);
}

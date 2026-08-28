use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    FlightDescriptor, SchemaAsIpc, SchemaResult, flight_descriptor::DescriptorType,
};
use mosaicod_core::{self as core, types};
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal as marshal;
use tracing::{info, trace};

/// Message provided when an error occurs while encoding a schema result.
const UNABLE_TO_BUILD_SCHEMA_RESULT: &str = "unable to build schema result";

/// Returns the [`SchemaResult`] for the requested Topic.
pub async fn get_schema(
    ctx: &facade::Context,
    desc: FlightDescriptor,
) -> grpc_common::Result<SchemaResult> {
    match desc.r#type() {
        DescriptorType::Cmd => {
            let cmd = marshal::flight::get_schema_cmd(&desc.cmd)?;
            let resource_name = cmd.resource_locator;

            info!("requesting schema for resource {}", resource_name);

            let topic_locator = resource_name.parse::<types::TopicLocator>().map_err(|_| {
                core::Error::locator_kind_mismatch(resource_name.clone(), "topic".to_owned())
            })?;

            let schema = facade::topic::schema(ctx, &topic_locator).await?;

            trace!("{} done", topic_locator);

            let options = IpcWriteOptions::default();
            let schema_result: SchemaResult = SchemaAsIpc::new(schema.as_ref(), &options)
                .try_into()
                .map_err(|_| {
                    core::Error::internal(Some(UNABLE_TO_BUILD_SCHEMA_RESULT.to_owned()))
                })?;

            Ok(schema_result)
        }
        _ => Err(core::Error::unsupported_descriptor())?,
    }
}

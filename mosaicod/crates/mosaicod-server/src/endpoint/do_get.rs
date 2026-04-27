use crate::error::Result;
use arrow::ipc::CompressionType;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    Ticket,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
    error::FlightError,
};
use futures::TryStreamExt;
use log::{debug, info, trace};
use mosaicod_core as core;
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;

pub async fn do_get(ctx: &facade::Context, ticket: Ticket) -> Result<FlightDataEncoder> {
    let ticket = marshal::flight::ticket_topic_from_binary(&ticket.ticket)?;

    info!("requesting data for ticket `{}`", ticket.locator);

    // Create topic handle
    let topic_locator = ticket.locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    // Read metadata from topic
    let metadata = facade::topic::metadata(ctx, &topic_handle).await?;

    trace!("{:?}", metadata);

    let batch_size = facade::topic::compute_optimal_batch_size(ctx, &topic_handle).await?;

    let path_in_store = topic_handle
        .path_in_store()
        .ok_or(core::error::Error::internal(Some(format!(
            "Path in store not set for topic {}",
            topic_handle.locator()
        ))))?;

    let mut query_result = ctx
        .timeseries_querier
        .read(
            &path_in_store.data_folder_path(),
            metadata.ontology_metadata.properties.serialization_format,
            Some(batch_size),
        )
        .await?;

    // Append JSON metadata to original data schema
    let metadata = marshal::JsonTopicMetadata::from(metadata);
    let flatten_mdata = metadata.ontology_metadata.to_flat_hashmap()?;

    let schema = query_result.schema_with_metadata(flatten_mdata);
    trace!("{:?}", schema);

    if let Some(ts_range) = ticket.timestamp_range {
        debug!("requesting timestamp range {}", ts_range);
        query_result = query_result.filter_by_timestamp_range(ts_range)?;
    }

    // Get data stream from query result
    let stream = query_result.stream().await?;

    // Convert the data stream to a flight stream casting the returned error
    let stream = stream.map_err(|e| FlightError::ExternalError(Box::new(e)));

    // We enable by default LZ4_FRAME compression for all streams.
    // As `.try_with_compression()` states the function throws an error at runtime
    // if the ipc_compression feature is not enabled. So we should never see this terror.
    let ipc_options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .map_err(|_| {
            core::Error::internal(Some("arrow ipc lz4 compression not available".to_owned()))
        })?;

    Ok(FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(ipc_options)
        .build(stream))
}

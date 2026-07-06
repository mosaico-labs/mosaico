use arrow::ipc::CompressionType;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    Ticket,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder, GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES},
    error::FlightError,
};
use futures::TryStreamExt;
use log::{debug, info, trace};
use mosaicod_core::{self as core, params, types};
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal as marshal;

pub async fn do_get(
    ctx: &facade::Context,
    ticket: Ticket,
) -> grpc_common::Result<FlightDataEncoder> {
    let ticket = marshal::flight::ticket_topic_from_binary(&ticket.ticket)?;

    info!("requesting data for ticket `{}`", ticket.locator);

    let doget_params = facade::topic::streaming_read_prepare(ctx, &ticket.locator).await?;

    trace!("{:?}", doget_params.metadata);

    // TODO: since we are calling timestamp_range(), count() and stream() on query_result,
    // in some cases it could increase I/O and computes. Maybe a better approach overall is possible?

    let mut query_result = ctx
        .timeseries_querier
        .read(
            &doget_params.data_folder_path,
            doget_params
                .metadata
                .ontology_metadata
                .properties
                .serialization_format,
            Some(doget_params.optimal_batch_size),
        )
        .await?;

    if let Some(ts_range) = ticket.timestamp_range {
        debug!("requesting timestamp range {}", ts_range);
        query_result = query_result.filter_by_timestamp_range(ts_range)?;
    }

    let mut metadata = doget_params.metadata;

    // Timestamp_range can be None only if there is no data uploaded for the topic yet.
    // In that case the entire interval_props is left empty.
    if let Some(timestamp_range) = query_result.clone().timestamp_range().await? {
        metadata = metadata.with_interval(types::TopicIntervalProperties {
            message_count: query_result.clone().count().await?,
            timestamp_range,
        });
    }

    // Append JSON metadata to original data schema
    let metadata = marshal::JsonTopicMetadata::from(metadata);
    let flatten_mdata = metadata.to_flat_hashmap()?;

    let schema = query_result.schema_with_metadata(flatten_mdata);
    trace!("{:?}", schema);

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

    // Set max flight message size to our gRPC limit minus 2MB headroom,
    // matching the same conservative margin used by the Flight default.
    //
    // If our value is below the default we keep the default.
    let max_flight_data_size = usize::max(
        GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES,
        params::params().max_grpc_message_size.value - 2_000_000,
    );

    Ok(FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(ipc_options)
        .with_max_flight_data_size(max_flight_data_size)
        .build(stream))
}

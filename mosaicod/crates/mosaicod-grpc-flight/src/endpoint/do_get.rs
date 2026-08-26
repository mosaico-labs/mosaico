use arrow::ipc::CompressionType;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    Ticket,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder, GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES},
    error::FlightError,
};
use futures::TryStreamExt;
use mosaicod_core::{self as core, params};
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal as marshal;
use mosaicod_marshal::flight;
use tracing::{debug, info, trace};

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
            doget_params.metadata.ontology_metadata.serialization_format,
            Some(doget_params.optimal_batch_size),
        )
        .await?;

    if let Some(ts_range) = ticket.timestamp_range {
        debug!("requesting timestamp range {}", ts_range);
        query_result = query_result.filter_by_timestamp_range(ts_range)?;
    }

    let mut do_get_app_metadata = None;

    // Timestamp_range can be None only if there is no data uploaded for the topic yet.
    // In that case the entire app metadata is left empty.
    if let Some(timestamp_range) = query_result.clone().timestamp_range().await? {
        let row_count = query_result.clone().count().await?;

        do_get_app_metadata = Some(flight::TopicDoGetAppMetadata::new(
            row_count,
            timestamp_range,
        ));
    }

    let schema = query_result.schema();
    trace!("{:?}", schema);

    // Get data stream from query result
    let stream = query_result.stream().await?;

    // Convert the data stream to a flight stream casting the returned error
    let stream = stream
        .inspect_ok(|batch| {
            debug!(
                target = "streaming batch",
                cols = batch.columns().len(),
                rows = batch.num_rows(),
                batch_physical_size_MB = batch.get_array_memory_size() / 1_000_000,
            );
        })
        .map_err(|e| FlightError::ExternalError(Box::new(e)));

    // We enable by default LZ4_FRAME compression for all streams.
    // As `.try_with_compression()` states the function throws an error at runtime
    // if the ipc_compression feature is not enabled. So we should never see this terror.
    let ipc_options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .map_err(|_| {
            core::Error::internal(Some("arrow ipc lz4 compression not available".to_owned()))
        })?;

    // Set max flight message size to half of our gRPC limit.
    //
    // If our value is below the default we keep the default.
    let max_flight_data_size = usize::max(
        GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES,
        params::params().target_message_size,
    );

    debug!(
        target = "streaming topic",
        cols = schema.fields().len(),
        total_rows = do_get_app_metadata
            .clone()
            .map(|am| am.row_count)
            .unwrap_or(0),
        optimal_batch_size = doget_params.optimal_batch_size,
        max_flight_data_size_MB = max_flight_data_size / 1_000_000,
    );

    let mut data_enc_builder = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(ipc_options)
        .with_max_flight_data_size(max_flight_data_size);

    if let Some(app_metadata) = do_get_app_metadata {
        data_enc_builder = data_enc_builder.with_metadata(app_metadata.into());
    }

    Ok(data_enc_builder.build(stream))
}

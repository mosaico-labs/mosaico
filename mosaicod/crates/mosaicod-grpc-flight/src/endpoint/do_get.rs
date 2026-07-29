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
use mosaicod_ext as ext;
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal as marshal;

/// Percentage of [`params::ConfigurablesParams::max_grpc_message_size`] a read
/// message is allowed to fill, leaving the rest as headroom.
///
/// The headroom exists because the two figures are in different currencies. Both the
/// pre-split and the Flight encoder decide using the batch's *in-memory* footprint,
/// while the gRPC limit is checked against the *encoded* message -- and encoding is
/// not guaranteed to shrink it. The IPC writer pads buffers to their alignment, adds
/// its own framing, and LZ4 on data that does not compress adds a wrapper instead of
/// removing bytes. A read has been observed producing a 50'180'305 byte message
/// against a 48'000'000 byte target: 4.5% above, which no in-memory figure could have
/// predicted.
///
/// Proportional rather than a fixed subtraction, because a fixed margin is a smaller
/// and smaller share of the limit as the limit grows: the 2MB this code used to
/// reserve is 4% of the 50MB default, and would be 0.4% if the limit were raised to
/// 500MB. A percentage keeps the safety factor constant instead.
///
/// 20% is the same order of caution arrow applies to its own default, which reserves
/// 2MB out of 4MB precisely because "the size calculation is somewhat inexact". If a
/// deployment still hits the limit, its data expands more than this on encoding and
/// the knob to turn is `MOSAICOD_TARGET_MESSAGE_SIZE`, downwards.
const FLIGHT_MESSAGE_BUDGET_PERCENT: usize = 80;

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

    // See `FLIGHT_MESSAGE_BUDGET_PERCENT` for why this is a share of the limit rather
    // than the limit itself. Dividing before multiplying cannot overflow; it costs at
    // most 99 bytes of precision, which is noise at these sizes.
    //
    // Never go below the Flight default: if the configured limit is small enough that
    // 80% of it lands under 2MB, that default is the more sensible floor.
    let max_flight_data_size = usize::max(
        GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES,
        params::params().max_grpc_message_size.value / 100 * FLIGHT_MESSAGE_BUDGET_PERCENT,
    );

    // Bisect any batch that would land near the limit. Read batches are sized from the
    // topic statistics and normally arrive well below this, so this catches outliers
    // rather than running on every batch. The encoder's own split divides rows evenly
    // using the batch average, which overshoots exactly when a few rows are far larger
    // than the rest.
    let stream = stream
        .map_ok(move |batch| {
            futures::stream::iter(
                ext::arrow::split_by_size(batch, max_flight_data_size)
                    .into_iter()
                    .map(Ok),
            )
        })
        .try_flatten();

    Ok(FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(ipc_options)
        .with_max_flight_data_size(max_flight_data_size)
        .build(stream))
}

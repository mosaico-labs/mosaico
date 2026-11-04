use arrow::ipc::CompressionType;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    Ticket,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder, GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES},
    error::FlightError,
};
use futures::{StreamExt, TryStreamExt};
use mosaicod_config::params;
use mosaicod_core::types::{self, flight::ImageSequenceView};
use mosaicod_core::{self as core};
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal as marshal;
use tracing::{debug, info, trace};

pub mod image_sequence;

pub async fn do_get(
    ctx: &facade::Context,
    ticket: Ticket,
) -> grpc_common::Result<FlightDataEncoder> {
    let ticket = marshal::flight::ticket_topic_from_bytes(&ticket.ticket)?;

    info!("requesting data for ticket `{}`", ticket.locator);

    let doget_params = facade::topic::streaming_read_prepare(ctx, &ticket.locator).await?;

    trace!("{:?}", doget_params.metadata);

    ticket
        .image_sequence_view
        .check_time_range(ticket.timestamp_range)?;
    let format = doget_params.metadata.ontology_metadata.serialization_format;
    if format.is_image_sequence() {
        let video =
            facade::topic::image_sequence_video(ctx, &doget_params.data_folder_path).await?;
        if let Some(video) = video {
            let (schema, stream) = image_sequence_stream(ctx, &ticket, video).await?;
            return Ok(flight_encoder(schema, stream)?);
        }
        if ticket.image_sequence_view != ImageSequenceView::Images {
            Err(core::Error::unsupported_retrieval(format!(
                "topic {} has no video yet: the store optimizer turns its images into one",
                ticket.locator
            )))?;
        }
    } else if ticket.image_sequence_view != ImageSequenceView::Images {
        Err(core::Error::unsupported_retrieval(format!(
            "topic {} is not an image sequence: only its data can be retrieved",
            ticket.locator
        )))?;
    }

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

    let schema = query_result.schema();
    trace!("{:?}", schema);
    debug!(
        target = "streaming topic",
        cols = schema.fields().len(),
        optimal_batch_size = doget_params.optimal_batch_size,
    );

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

    Ok(flight_encoder(schema, stream.boxed())?)
}

/// The size of the messages of a stream: half of our gRPC limit, or the default of Arrow Flight
/// if above.
fn max_flight_data_size() -> usize {
    usize::max(
        GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES,
        params::params().grpc_target_encode_message_size.value,
    )
}

/// The stream of an image sequence turned into a video, as the ticket asks for it (see
/// [`image_sequence`]).
async fn image_sequence_stream(
    ctx: &facade::Context,
    ticket: &types::flight::TicketTopic,
    video: facade::topic::ImageSequenceVideo,
) -> Result<(arrow::datatypes::SchemaRef, image_sequence::BatchStream), core::Error> {
    // A piece of a file, or a batch of images, in a message, with room for the other columns.
    let piece = max_flight_data_size() / 2;
    match ticket.image_sequence_view {
        ImageSequenceView::Video { interval } => {
            image_sequence::segments(ctx, video, interval, piece).await
        }
        #[cfg(feature = "video")]
        ImageSequenceView::Images => {
            image_sequence::images(ctx, video, ticket.timestamp_range, piece).await
        }
        #[cfg(not(feature = "video"))]
        ImageSequenceView::Images => {
            let _ = video;
            Err(core::Error::unsupported_retrieval(format!(
                "topic {} is a video, and this server is built without the `video` feature: \
                 only the video can be retrieved",
                ticket.locator
            )))
        }
    }
}

/// Encodes `stream` of `schema` into Flight messages, LZ4 compressed.
fn flight_encoder(
    schema: arrow::datatypes::SchemaRef,
    stream: image_sequence::BatchStream,
) -> Result<FlightDataEncoder, core::Error> {
    // We enable by default LZ4_FRAME compression for all streams.
    // As `.try_with_compression()` states the function throws an error at runtime
    // if the ipc_compression feature is not enabled. So we should never see this terror.
    let ipc_options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .map_err(|_| {
            core::Error::internal(Some("arrow ipc lz4 compression not available".to_owned()))
        })?;

    let max_flight_data_size = max_flight_data_size();
    debug!(max_flight_data_size_MB = max_flight_data_size / 1_000_000);

    Ok(FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(ipc_options)
        .with_max_flight_data_size(max_flight_data_size)
        .build(stream))
}

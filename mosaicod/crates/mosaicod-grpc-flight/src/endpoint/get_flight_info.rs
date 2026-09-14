use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_descriptor::DescriptorType,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use mosaicod_core::{
    self as core,
    error::BoxPublicError,
    params,
    types::{self},
};
use mosaicod_facade as facade;
use mosaicod_facade::Context;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal as marshal;
use mosaicod_marshal::flight;
use tracing::{info, trace};

/// Returns the [`FlightInfo`] for the requested resource (Sequence or Topic).
pub async fn get_flight_info(
    ctx: &facade::Context,
    desc: FlightDescriptor,
) -> grpc_common::Result<FlightInfo> {
    match desc.r#type() {
        DescriptorType::Cmd => {
            let cmd = marshal::flight::get_flight_info_cmd(&desc.cmd)?;
            do_get_flight_info(ctx, desc, cmd).await
        }
        _ => Err(core::Error::unsupported_descriptor())?,
    }
}

/// Internal implementation for [`get_flight_info`].
///
/// It accepts a command already parsed into [`types::flight::GetFlightInfoCmd`].
async fn do_get_flight_info(
    ctx: &facade::Context,
    desc: FlightDescriptor,
    cmd: types::flight::GetFlightInfoCmd,
) -> grpc_common::Result<FlightInfo> {
    let resource_name = &cmd.resource_locator;

    info!("requesting info for resource {}", resource_name);

    if let Some(ts_range) = &cmd.timestamp_range
        && ts_range.is_empty()
    {
        Err(core::Error::bad_timestamp_range(*ts_range))?;
    }

    return if let Ok(sequence_locator) = resource_name.parse::<types::SequenceLocator>() {
        sequence_flight_info(ctx, desc, sequence_locator, cmd.timestamp_range).await
    } else if let Ok(topic_locator) = resource_name.parse::<types::TopicLocator>() {
        topic_flight_info(ctx, desc, topic_locator, cmd.timestamp_range).await
    } else if let Ok(session_locator) = resource_name.parse::<types::SessionLocator>() {
        Err(core::Error::unsupported_locator(
            session_locator.to_string(),
        ))?
    } else {
        Err(core::Error::bad_locator(resource_name.clone()))?
    };
}

/// Creates flight info response for the given Sequence.
async fn sequence_flight_info(
    ctx: &facade::Context,
    desc: FlightDescriptor,
    sequence_locator: types::SequenceLocator,
    timestamp_range: Option<types::TimestampRange>,
) -> grpc_common::Result<FlightInfo> {
    let sequence_info = facade::sequence::info(ctx, &sequence_locator, timestamp_range).await?;

    trace!("{} generating endpoints", sequence_locator);

    // Populate endpoints
    let endpoints = stream::iter(sequence_info.topics)
        .map(async |topic_info: facade::topic::TopicInfo| {
            let topic_endpoint = build_topic_endpoint(topic_info, timestamp_range).await?;
            Ok::<FlightEndpoint, BoxPublicError>(topic_endpoint)
        })
        .buffer_unordered(params::MAX_BUFFERED_FUTURES)
        .try_collect::<Vec<FlightEndpoint>>()
        .await?;

    // Get sequence metadata and convert it to flight appmetadata.
    let app_metadata: flight::SequenceAppMetadata = sequence_info.metadata.into();

    let mut flight_info = FlightInfo::new()
        .with_descriptor(desc)
        .with_app_metadata(app_metadata);

    for endpoint in endpoints {
        flight_info = flight_info.with_endpoint(endpoint);
    }

    trace!("{} done", sequence_locator);
    Ok(flight_info)
}

/// Creates flight info response for the given Topic.
async fn topic_flight_info(
    ctx: &Context,
    desc: FlightDescriptor,
    topic_locator: types::TopicLocator,
    timestamp_range: Option<types::TimestampRange>,
) -> grpc_common::Result<FlightInfo> {
    let topic_info = facade::topic::info(ctx, &topic_locator, timestamp_range).await?;

    // Topic's metadata are stored inside endpoint and not FlightInfo::app_metadata to replicate
    // the same behavior inside sequence_flight_info.
    let endpoint = build_topic_endpoint(topic_info, timestamp_range).await?;

    let flight_info = FlightInfo::new()
        .with_descriptor(desc)
        .with_endpoint(endpoint);

    trace!("{} done", topic_locator);
    Ok(flight_info)
}

/// Builds a [`FlightEndpoint`] for the given Topic.
async fn build_topic_endpoint(
    topic_info: facade::topic::TopicInfo,
    timestamp_range: Option<types::TimestampRange>,
) -> grpc_common::Result<FlightEndpoint> {
    let topic_locator = topic_info.metadata.properties.resource_locator.clone();

    let ticket = types::flight::TicketTopic {
        locator: topic_locator.clone(),
        timestamp_range,
    };

    let app_mdata = flight::TopicAppMetadata::new(
        topic_info.metadata,
        topic_info.data_info,
        topic_info.time_window_info,
    );

    let endpoint = FlightEndpoint::new()
        .with_ticket(Ticket {
            ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
        })
        .with_app_metadata(app_mdata);

    trace!("{} generating endpoint {:?}", topic_locator, endpoint);

    Ok(endpoint)
}

use crate::error::Result;
use arrow::datatypes::{Field, Schema};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_descriptor::DescriptorType,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{info, trace};
use mosaicod_core::{
    self as core,
    error::BoxPublicError,
    params,
    types::{self, TopicOntologyMetadata},
};
use mosaicod_facade as facade;
use mosaicod_facade::Context;
use mosaicod_marshal as marshal;
use mosaicod_marshal::{JsonMetadataBlob, flight};

/// Message provided when an error occurs when building flight info data
const UNABLE_TO_BUILD_FLIGHT_INFO: &str = "unable to build flight info data";

/// Returns the [`FlightInfo`] for the requested resource (Sequence or Topic).
pub async fn get_flight_info(ctx: &facade::Context, desc: FlightDescriptor) -> Result<FlightInfo> {
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
) -> Result<FlightInfo> {
    let resource_name = &cmd.resource_locator;

    info!("requesting info for resource {}", resource_name);

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
) -> Result<FlightInfo> {
    let sequence_handle = facade::sequence::Handle::try_from_locator(ctx, sequence_locator).await?;

    let metadata = facade::sequence::metadata(ctx, &sequence_handle).await?;

    trace!(
        "{} building empty schema (+platform metadata)",
        sequence_handle.locator()
    );

    let mut schema = Schema::new(Vec::<Field>::new());

    // Collect user metadata
    if let Some(user_metadata) = &metadata.user_metadata {
        let user_metadata = marshal::JsonSequenceMetadata {
            user_metadata: user_metadata.clone(),
        };
        let flatten_user_metadata = user_metadata.to_flat_hashmap()?;

        schema = schema.with_metadata(flatten_user_metadata);
    }

    trace!("{} generating endpoints", sequence_handle.locator());
    let topics = facade::sequence::topic_list(ctx, &sequence_handle).await?;

    // Populate endpoints
    let endpoints = stream::iter(topics)
        .map(async |topic_handle: facade::topic::Handle| {
            let metadata = facade::topic::metadata(ctx, &topic_handle).await?;
            let topic_endpoint = build_topic_endpoint(
                ctx,
                &topic_handle,
                timestamp_range.clone(),
                metadata.properties,
            )
            .await?;
            Ok::<FlightEndpoint, BoxPublicError>(topic_endpoint)
        })
        .buffer_unordered(params::MAX_BUFFERED_FUTURES)
        .try_collect::<Vec<FlightEndpoint>>()
        .await?;

    // Get sequence metadata and convert it to flight appmetadata.
    let app_metadata: flight::SequenceAppMetadata = metadata.into();

    let mut flight_info = FlightInfo::new()
        .with_descriptor(desc)
        .with_app_metadata(app_metadata)
        .try_with_schema(&schema)
        .map_err(|_| core::Error::internal(Some(UNABLE_TO_BUILD_FLIGHT_INFO.to_owned())))?;

    for endpoint in endpoints {
        flight_info = flight_info.with_endpoint(endpoint);
    }

    trace!("{} done", sequence_handle.locator());
    Ok(flight_info)
}

/// Creates flight info response for the given Topic.
async fn topic_flight_info(
    ctx: &facade::Context,
    desc: FlightDescriptor,
    topic_locator: types::TopicLocator,
    timestamp_range: Option<types::TimestampRange>,
) -> Result<FlightInfo> {
    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    let metadata = facade::topic::metadata(ctx, &topic_handle).await?;

    let endpoint =
        build_topic_endpoint(ctx, &topic_handle, timestamp_range, metadata.properties).await?;

    let schema =
        topic_arrow_schema_with_metadata(metadata.ontology_metadata, &topic_handle, ctx).await?;

    let flight_info = FlightInfo::new()
        .with_descriptor(desc)
        .with_endpoint(endpoint)
        .try_with_schema(&schema)
        .map_err(|_| core::Error::internal(Some(UNABLE_TO_BUILD_FLIGHT_INFO.to_owned())))?;

    trace!("{} done", topic_handle.locator());
    Ok(flight_info)
}

/// Builds a [`FlightEndpoint`] for the given Topic.
async fn build_topic_endpoint(
    ctx: &facade::Context,
    topic_handle: &facade::topic::Handle,
    timestamp_range: Option<types::TimestampRange>,
    metadata: types::TopicMetadataProperties,
) -> Result<FlightEndpoint> {
    let ticket = types::flight::TicketTopic {
        locator: topic_handle.locator().clone(),
        timestamp_range,
    };

    let mut app_mdata = marshal::flight::TopicAppMetadata::new(metadata);
    if let Ok(info) = facade::topic::data_info(ctx, topic_handle).await {
        app_mdata = app_mdata.with_info(info);
    }

    let endpoint = FlightEndpoint::new()
        .with_ticket(Ticket {
            ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
        })
        .with_app_metadata(app_mdata);

    trace!(
        "{} generating endpoint {:?}",
        topic_handle.locator(),
        endpoint
    );

    Ok(endpoint)
}

/// Utility function to create an arrow schema with metadata for the given Topic.
async fn topic_arrow_schema_with_metadata(
    ontology_metadata: TopicOntologyMetadata<JsonMetadataBlob>,
    topic_handle: &facade::topic::Handle,
    context: &Context,
) -> Result<Schema> {
    trace!(
        "{} building schema (+platform metadata)",
        topic_handle.locator()
    );

    // Collect schema.
    let schema = facade::topic::arrow_schema(
        context,
        topic_handle,
        ontology_metadata.properties.serialization_format,
    )
    .await?;

    // Collect schema metadata
    let json_ontology_metadata = marshal::JsonTopicOntologyMetadata::from(ontology_metadata);
    let flatten_ontology_metadata = json_ontology_metadata.to_flat_hashmap()?;

    Ok(Schema::new_with_metadata(
        schema.fields().clone(),
        flatten_ontology_metadata,
    ))
}

use crate::errors::ServerError;
use arrow::datatypes::{Field, Schema};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_descriptor::DescriptorType,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{info, trace};
use mosaicod_core::params;
use mosaicod_core::types::{self, Resource, TopicOntologyMetadata};
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_facade::Context;
use mosaicod_marshal as marshal;
use mosaicod_marshal::{JsonMetadataBlob, flight};

pub async fn get_flight_info(
    ctx: &facade::Context,
    desc: FlightDescriptor,
) -> Result<FlightInfo, ServerError> {
    match desc.r#type() {
        DescriptorType::Cmd => {
            let cmd = marshal::flight::get_flight_info_cmd(&desc.cmd)?;
            let resource_name = &cmd.resource_locator;

            info!("requesting info for resource {}", resource_name);

            let resource = db::get_resource_locator_from_name(&ctx.db, resource_name).await?;

            match resource.resource_type() {
                types::ResourceType::Sequence => {
                    let sequence_locator = types::SequenceResourceLocator::from(resource.locator());

                    let sequence_handle =
                        facade::sequence::Handle::try_from_locator(ctx, sequence_locator).await?;

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
                        let flatten_user_metadata = user_metadata
                            .to_flat_hashmap()
                            .map_err(facade::Error::from)?;

                        schema = schema.with_metadata(flatten_user_metadata);
                    }

                    trace!("{} generating endpoints", sequence_handle.locator());
                    let topics = facade::sequence::topic_list(ctx, &sequence_handle).await?;

                    // Populate endpoints
                    let endpoints = stream::iter(topics)
                        .map(async |topic_handle: facade::topic::Handle| {
                            let ticket = types::flight::TicketTopic {
                                locator: topic_handle.locator().to_string(),
                                timestamp_range: cmd.timestamp_range.clone(),
                            };

                            let topic_app_mdata = build_topic_app_metadata(
                                facade::topic::metadata(ctx, &topic_handle)
                                    .await?
                                    .properties,
                                &topic_handle,
                                ctx,
                            )
                            .await;

                            let e = FlightEndpoint::new()
                                .with_ticket(Ticket {
                                    ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                                })
                                .with_location(topic_handle.locator().url()?)
                                .with_app_metadata(topic_app_mdata);

                            Ok::<FlightEndpoint, ServerError>(e)
                        })
                        .buffer_unordered(params::MAX_BUFFERED_FUTURES)
                        .try_collect::<Vec<FlightEndpoint>>()
                        .await?;

                    // Get sequence metadata and convert it to flight appmetadata.
                    let app_metadata: flight::SequenceAppMetadata = metadata.into();

                    let mut flight_info = FlightInfo::new()
                        .with_descriptor(desc.clone())
                        .with_app_metadata(app_metadata)
                        .try_with_schema(&schema)?;

                    for endpoint in endpoints {
                        flight_info = flight_info.with_endpoint(endpoint);
                    }

                    trace!("{} done", sequence_handle.locator());
                    Ok(flight_info)
                }

                types::ResourceType::Topic => {
                    let topic_locator = types::TopicResourceLocator::from(resource.locator());

                    let topic_handle =
                        facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

                    let metadata = facade::topic::metadata(ctx, &topic_handle).await?;

                    let ticket = types::flight::TicketTopic {
                        locator: topic_handle.locator().clone().into(),
                        timestamp_range: cmd.timestamp_range,
                    };

                    // building a single endpoint for topic data
                    let endpoint = FlightEndpoint::new()
                        .with_ticket(Ticket {
                            ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                        })
                        .with_location(topic_handle.locator().url()?)
                        .with_app_metadata(
                            build_topic_app_metadata(metadata.properties, &topic_handle, ctx).await,
                        );

                    trace!(
                        "{} generating endpoint {:?}",
                        topic_handle.locator(),
                        endpoint
                    );

                    let schema = topic_arrow_schema_with_metadata(
                        metadata.ontology_metadata,
                        &topic_handle,
                        ctx,
                    )
                    .await?;

                    let flight_info = FlightInfo::new()
                        .with_descriptor(desc.clone())
                        .with_endpoint(endpoint)
                        .try_with_schema(&schema)?;

                    trace!("{} done", topic_handle.locator());
                    Ok(flight_info)
                }
            }
        }
        _ => Err(ServerError::UnsupportedDescriptor),
    }
}

/// Build topic app_metadata.
async fn build_topic_app_metadata(
    metadata_props: types::TopicMetadataProperties,
    topic_handle: &facade::topic::Handle,
    context: &Context,
) -> marshal::flight::TopicAppMetadata {
    let mut app_mdata = marshal::flight::TopicAppMetadata::new(metadata_props);

    if let Ok(info) = facade::topic::data_info(context, topic_handle).await {
        app_mdata = app_mdata.with_info(info);
    }

    app_mdata
}

/// Utility function to create an arrow schema with metadata for the given Topic.
async fn topic_arrow_schema_with_metadata(
    ontology_metadata: TopicOntologyMetadata<JsonMetadataBlob>,
    topic_handle: &facade::topic::Handle,
    context: &Context,
) -> Result<Schema, facade::Error> {
    trace!(
        "{} building schema (+platform metadata)",
        topic_handle.locator()
    );

    // Collect schema, if no schema was found generate an empty schema
    let schema = match facade::topic::arrow_schema(
        context,
        topic_handle,
        ontology_metadata.properties.serialization_format,
    )
    .await
    {
        Ok(s) => s,
        Err(facade::Error::NotFound(_)) => mosaicod_ext::arrow::empty_schema_ref(),
        Err(e) => return Err(e),
    };

    // Collect schema metadata
    let json_ontology_metadata = marshal::JsonTopicOntologyMetadata::from(ontology_metadata);
    let flatten_ontology_metadata = json_ontology_metadata
        .to_flat_hashmap()
        .map_err(facade::Error::from)?;

    Ok(Schema::new_with_metadata(
        schema.fields().clone(),
        flatten_ontology_metadata,
    ))
}

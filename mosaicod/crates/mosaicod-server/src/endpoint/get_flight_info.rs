use super::Context;
use crate::errors::ServerError;
use arrow::datatypes::{Field, Schema};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_descriptor::DescriptorType,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{info, trace};
use mosaicod_core::params;
use mosaicod_core::types::{self, Resource, TopicOntologyMetadata, TopicResourceLocator};
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;
use mosaicod_marshal::{JsonMetadataBlob, flight};

pub async fn get_flight_info(
    ctx: Context,
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
                    let handle = facade::Sequence::new(
                        resource.locator().into(),
                        ctx.store.clone(),
                        ctx.db.clone(),
                    );
                    let metadata = handle.metadata().await?;

                    trace!(
                        "{} building empty schema (+platform metadata)",
                        handle.locator
                    );

                    // Collect metadata
                    let metadata = marshal::JsonSequenceMetadata::from(metadata);
                    let flatten_metadata =
                        metadata.to_flat_hashmap().map_err(facade::Error::from)?;

                    // Collect schema
                    let schema = Schema::new_with_metadata(Vec::<Field>::new(), flatten_metadata);

                    trace!("{} generating endpoints", handle.locator);
                    let topics = handle.topic_list().await?;

                    // Populate endpoints
                    let endpoints = stream::iter(topics)
                        .map(async |topic: TopicResourceLocator| {
                            let ticket = types::flight::TicketTopic {
                                locator: topic.locator().to_owned(),
                                timestamp_range: cmd.timestamp_range.clone(),
                            };

                            let topic_facade = facade::Topic::try_from_locator(
                                topic.clone(),
                                ctx.store.clone(),
                                ctx.db.clone(),
                            )
                            .await?;

                            let topic_app_mdata = build_topic_app_metadata(
                                topic_facade.manifest().await?.properties,
                                &topic_facade,
                            )
                            .await;

                            let e = FlightEndpoint::new()
                                .with_ticket(Ticket {
                                    ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                                })
                                .with_location(topic.url()?)
                                .with_app_metadata(topic_app_mdata);

                            Ok::<FlightEndpoint, ServerError>(e)
                        })
                        .buffer_unordered(params::MAX_BUFFERED_FUTURES)
                        .try_collect::<Vec<FlightEndpoint>>()
                        .await?;

                    // Get sequence manifest, if it exists, and send it as app metadata.
                    let manifest: flight::SequenceAppMetadata = match handle.manifest().await {
                        Ok(m) => m.into(),
                        Err(e) => return Err(e.into()),
                    };

                    let mut flight_info = FlightInfo::new()
                        .with_descriptor(desc.clone())
                        .with_app_metadata(manifest)
                        .try_with_schema(&schema)?;

                    for endpoint in endpoints {
                        flight_info = flight_info.with_endpoint(endpoint);
                    }

                    trace!("{} done", handle.locator);
                    Ok(flight_info)
                }

                types::ResourceType::Topic => {
                    let handle = facade::Topic::try_from_locator(
                        resource.locator().into(),
                        ctx.store,
                        ctx.db.clone(),
                    )
                    .await?;

                    let manifest = handle.manifest().await?;

                    let ticket = types::flight::TicketTopic {
                        locator: handle.locator.clone().into(),
                        timestamp_range: cmd.timestamp_range,
                    };

                    // building a single endpoint for topic data
                    let endpoint = FlightEndpoint::new()
                        .with_ticket(Ticket {
                            ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                        })
                        .with_location(handle.locator.url()?)
                        .with_app_metadata(
                            build_topic_app_metadata(manifest.properties, &handle).await,
                        );

                    trace!("{} generating endpoint {:?}", handle.locator, endpoint);

                    let schema =
                        topic_arrow_schema_with_metadata(manifest.ontology_metadata, &handle)
                            .await?;

                    let flight_info = FlightInfo::new()
                        .with_descriptor(desc.clone())
                        .with_endpoint(endpoint)
                        .try_with_schema(&schema)?;

                    trace!("{} done", handle.locator);
                    Ok(flight_info)
                }
            }
        }
        _ => Err(ServerError::UnsupportedDescriptor),
    }
}

/// Build topic app_metadata.
async fn build_topic_app_metadata(
    metadata_props: types::TopicProperties,
    topic_facade: &facade::Topic,
) -> marshal::flight::TopicAppMetadata {
    let mut app_mdata = marshal::flight::TopicAppMetadata::new(metadata_props);

    if let Ok(info) = topic_facade.data_info().await {
        app_mdata = app_mdata.with_info(info);
    }

    app_mdata
}

/// Utility function to create an arrow schema with metadata for the given Topic.
async fn topic_arrow_schema_with_metadata(
    ontology_metadata: TopicOntologyMetadata<JsonMetadataBlob>,
    topic_facade: &facade::Topic,
) -> Result<Schema, facade::Error> {
    trace!(
        "{} building schema (+platform metadata)",
        topic_facade.locator
    );

    // Collect schema, if no schema was found generate an empty schema
    let schema = match topic_facade
        .arrow_schema(ontology_metadata.properties.serialization_format)
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

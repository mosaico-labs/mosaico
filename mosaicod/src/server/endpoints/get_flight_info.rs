use super::Context;
use crate::{
    marshal,
    repo::{self, FacadeError, FacadeSequence, FacadeTopic},
    server::errors::ServerError,
    types::{self, Resource},
};
use arrow::datatypes::{Field, Schema};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_descriptor::DescriptorType,
};
use log::{info, trace};

pub async fn get_flight_info(
    ctx: Context,
    desc: FlightDescriptor,
) -> Result<FlightInfo, ServerError> {
    match desc.r#type() {
        DescriptorType::Cmd => {
            let cmd = marshal::flight::get_flight_info_cmd(&desc.cmd)?;
            let resource_name = &cmd.resource_locator;

            info!("requesting info for resource {}", resource_name);

            let resource = repo::get_resource_locator_from_name(&ctx.repo, resource_name).await?;

            match resource.resource_type() {
                types::ResourceType::Sequence => {
                    let handle = FacadeSequence::new(
                        resource.name().into(),
                        ctx.store.clone(),
                        ctx.repo.clone(),
                    );
                    let metadata = handle.metadata().await?;

                    trace!(
                        "{} building empty schema (+platform metadata)",
                        handle.locator
                    );

                    // Collect metadata
                    let metadata = marshal::JsonSequenceMetadata::from(metadata);
                    let flatten_metadata = metadata.to_flat_hashmap().map_err(FacadeError::from)?;

                    // Collect schema
                    let schema = Schema::new_with_metadata(Vec::<Field>::new(), flatten_metadata);

                    trace!("{} generating endpoints", handle.locator);
                    let topics = handle.topic_list().await?;

                    // Collect manifests
                    let manifests = collect_manifests(ctx, &topics).await?;

                    // Populate endpoints
                    let endpoints: Vec<FlightEndpoint> = topics
                        .into_iter()
                        .enumerate()
                        .map(|(index, topic)| {
                            let ticket = types::flight::TicketTopic {
                                locator: topic.name().clone(),
                                timestamp_range: cmd.timestamp_range.clone(),
                            };

                            let app_mdata =
                                marshal::flight::TopicAppMetadata::new(&manifests[index]);

                            let e = FlightEndpoint::new()
                                .with_ticket(Ticket {
                                    ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                                })
                                .with_app_metadata(app_mdata)
                                .with_location(topic.url()?);

                            Ok::<FlightEndpoint, ServerError>(e)
                        })
                        .collect::<Result<_, ServerError>>()?;

                    trace!("{} generating endpoints: {:?}", handle.locator, endpoints);
                    let mut flight_info = FlightInfo::new()
                        .with_descriptor(desc.clone())
                        .try_with_schema(&schema)?;

                    for endpoint in endpoints {
                        flight_info = flight_info.with_endpoint(endpoint);
                    }

                    trace!("{} done", handle.locator);
                    Ok(flight_info)
                }

                types::ResourceType::Topic => {
                    let handle =
                        FacadeTopic::new(resource.name().into(), ctx.store, ctx.repo.clone());
                    let metadata = handle.metadata().await?;

                    trace!("{} building schema (+platform metadata)", handle.locator);

                    // Collect schema, if no schema was found generate an create an empty schema
                    let schema = match handle
                        .arrow_schema(metadata.properties.serialization_format)
                        .await
                    {
                        Ok(s) => s,
                        Err(FacadeError::NotFound(_)) => crate::arrow::empty_schema_ref(),
                        Err(e) => return Err(e.into()),
                    };

                    // Collect metadata
                    let metadata = marshal::JsonTopicMetadata::from(metadata);
                    let flatten_metadata = metadata.to_flat_hashmap().map_err(FacadeError::from)?;

                    // Build schema to send
                    let schema =
                        Schema::new_with_metadata(schema.fields().clone(), flatten_metadata);

                    // Collect manifest, if no manifest is found an empty one is returned while
                    // other errors are propagated
                    let manifest = match handle.manifest().await {
                        Ok(m) => m,
                        Err(FacadeError::NotFound(_)) => types::TopicManifest::new(),
                        Err(e) => return Err(e.into()),
                    };

                    // We can get directly the only elements since collect_manifests ensures that
                    // there will be at least one entry returned (if no error)
                    let app_mdata = marshal::flight::TopicAppMetadata::new(&manifest);

                    let ticket = types::flight::TicketTopic {
                        locator: handle.locator.clone().into(),
                        timestamp_range: cmd.timestamp_range,
                    };

                    // building a single endpoint for topic data
                    let endpoint = FlightEndpoint::new()
                        .with_ticket(Ticket {
                            ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                        })
                        .with_app_metadata(app_mdata)
                        .with_location(handle.locator.url()?);

                    trace!("{} generating endpoint {:?}", handle.locator, endpoint);

                    let mut flight_info = FlightInfo::new()
                        .with_descriptor(desc.clone())
                        .try_with_schema(&schema)?;

                    flight_info = flight_info.with_endpoint(endpoint);

                    trace!("{} done", handle.locator);
                    Ok(flight_info)
                }
            }
        }
        _ => Err(ServerError::UnsupportedDescriptor),
    }
}

/// Retrieves the manifest for every provided topic.
///
/// This function guarantees a 1:1 mapping: the output vector will strictly correspond
/// to the input slice in both length and order.
pub async fn collect_manifests(
    ctx: Context,
    topics: &[types::TopicResourceLocator],
) -> Result<Vec<types::TopicManifest>, ServerError> {
    let mut manifests = Vec::new();

    for topic in topics {
        // (cabba) TODO: avoid cloning avery time store and repo, maybe a `.into_parts()` to reuse
        // facade resources ?
        let handler =
            FacadeTopic::new(topic.name().to_owned(), ctx.store.clone(), ctx.repo.clone());

        // Collect manifest, if no manifest is found an empty one is returned while
        // other errors are propagated
        let manifest = match handler.manifest().await {
            Ok(manifest) => manifest,
            Err(FacadeError::NotFound(_)) => types::TopicManifest::new(),
            Err(e) => return Err(e.into()),
        };

        manifests.push(manifest);
    }

    Ok(manifests)
}

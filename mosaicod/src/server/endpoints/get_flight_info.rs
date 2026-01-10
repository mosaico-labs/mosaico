use crate::{
    marshal,
    repo::{self, FacadeError, FacadeSequence, FacadeTopic},
    server::errors::ServerError,
    store,
    types::{self, Resource},
};
use arrow::datatypes::{Field, Schema};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_descriptor::DescriptorType,
};
use log::{info, trace};

pub async fn get_flight_info(
    store: store::StoreRef,
    repo: repo::Repository,
    desc: FlightDescriptor,
) -> Result<FlightInfo, ServerError> {
    match desc.r#type() {
        DescriptorType::Cmd => {
            let cmd = marshal::flight::get_flight_info_cmd(&desc.cmd)?;
            let resource_name = &cmd.resource_locator;

            info!("requesting info for resource {}", resource_name);

            let resource = repo::get_resource_locator_from_name(&repo, resource_name).await?;

            match resource.resource_type() {
                types::ResourceType::Sequence => {
                    let handle = FacadeSequence::new(resource.name().into(), store.clone(), repo);
                    let metadata = handle.metadata().await?;

                    trace!(
                        "{} building empty schema (+platform metadata)",
                        handle.locator
                    );

                    let metadata = marshal::JsonSequenceMetadata::from(metadata);
                    let flatten_metadata = metadata.to_flat_hashmap().map_err(FacadeError::from)?;
                    let schema = Schema::new_with_metadata(Vec::<Field>::new(), flatten_metadata);

                    trace!("{} generating endpoints", handle.locator);
                    let topics = handle.topic_list().await?;
                    let endpoints: Vec<FlightEndpoint> = topics
                        .into_iter()
                        .map(|topic| {
                            let ticket = types::flight::TicketTopic {
                                locator: topic.name().clone(),
                                timestamp_range: cmd.timestamp_range.clone(),
                            };

                            let e = FlightEndpoint::new()
                                .with_ticket(Ticket {
                                    ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                                })
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
                    let handle = FacadeTopic::new(resource.name().into(), store, repo);
                    let metadata = handle.metadata().await?;

                    trace!("{} building schema (+platform metadata)", handle.locator);
                    let schema = handle
                        .arrow_schema(metadata.properties.serialization_format)
                        .await?;
                    let metadata = marshal::JsonTopicMetadata::from(metadata);
                    let flatten_metadata = metadata.to_flat_hashmap().map_err(FacadeError::from)?;
                    let schema =
                        Schema::new_with_metadata(schema.fields().clone(), flatten_metadata);

                    let ticket = types::flight::TicketTopic {
                        locator: handle.locator.clone().into(),
                        timestamp_range: cmd.timestamp_range,
                    };

                    // building a single endpoint for topic data
                    let endpoint = FlightEndpoint::new()
                        .with_ticket(Ticket {
                            ticket: marshal::flight::ticket_topic_to_binary(ticket)?.into(),
                        })
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

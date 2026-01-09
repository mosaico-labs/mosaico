use arrow_flight::{
    Ticket,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
    error::FlightError,
};

use futures::TryStreamExt;
use log::{debug, info, trace};

use crate::{marshal, query, repo, server::errors::ServerError, store, types::Resource};

pub async fn do_get(
    store: store::StoreRef,
    repo: repo::Repository,
    ts_engine: query::TimeseriesGatewayRef,
    ticket: Ticket,
) -> Result<FlightDataEncoder, ServerError> {
    let ticket = marshal::flight::ticket_topic_from_binary(&ticket.ticket)?;

    info!("requesting data for ticket `{}`", ticket.locator);

    // Create topic handle
    let topic = ticket.locator;
    let tfacade = repo::FacadeTopic::new(topic, store, repo.clone());

    // Read metadata from topic
    let metadata = tfacade.metadata().await?;

    trace!("{:?}", metadata);

    let batch_size = tfacade.compute_optimal_batch_size().await?;

    let mut query_result = ts_engine
        .read(
            &tfacade.locator.name(),
            metadata.properties.serialization_format,
            Some(batch_size),
        )
        .await?;

    // Append JSON metadata to original data schema
    let metadata = marshal::JsonTopicMetadata::from(metadata);
    let flatten_mdata = metadata
        .to_flat_hashmap()
        .map_err(repo::FacadeError::from)?;
    let schema = query_result.schema_with_metadata(flatten_mdata);
    trace!("{:?}", schema);

    if let Some(ts_range) = ticket.timestamp_range {
        debug!("requesting timestamp range {}", ts_range);
        query_result = query_result.filter_by_timestamp_range(ts_range)?;
    }

    // Get data stream from query result
    let stream = query_result.stream().await?;

    // Convert the data stream to a flight stream casting the returned error
    let stream = stream.map_err(|e| FlightError::ExternalError(Box::new(e)));

    Ok(FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream))
}

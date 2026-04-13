use crate::error::Result;
use arrow_flight::{
    Ticket,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
    error::FlightError,
};
use futures::TryStreamExt;
use log::{debug, info, trace};
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_marshal as marshal;

pub async fn do_get(ctx: &facade::Context, ticket: Ticket) -> Result<FlightDataEncoder> {
    let ticket = marshal::flight::ticket_topic_from_binary(&ticket.ticket)?;

    info!("requesting data for ticket `{}`", ticket.locator);

    // Create topic handle
    let topic_locator = types::TopicResourceLocator::from(ticket.locator);

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    // Read metadata from topic
    let metadata = facade::topic::metadata(ctx, &topic_handle).await?;

    trace!("{:?}", metadata);

    let batch_size = facade::topic::compute_optimal_batch_size(ctx, &topic_handle).await?;

    let mut query_result = ctx
        .timeseries_querier
        .read(
            &topic_handle.locator().path_data_folder(topic_handle.uuid()),
            metadata.ontology_metadata.properties.serialization_format,
            Some(batch_size),
        )
        .await?;

    // Append JSON metadata to original data schema
    let metadata = marshal::JsonTopicMetadata::from(metadata);
    let flatten_mdata = metadata
        .ontology_metadata
        .to_flat_hashmap()
        .map_err(facade::Error::from)?;
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

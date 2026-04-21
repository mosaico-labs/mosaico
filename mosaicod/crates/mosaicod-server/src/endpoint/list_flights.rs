//! Implementation of the Arrow Flight `list_flights` endpoint.
//!
//! Returns a stream of all available sequences when queried at the root level.
use crate::error::*;
use arrow_flight::{Criteria, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::stream::BoxStream;
use log::{info, trace};
use mosaicod_core as core;
use mosaicod_facade as facade;

/// Lists all available flights (sequences) in the database.
///
/// When clients query with an empty or root path ("" or "/"), this function
/// returns a streamed list of all sequences. Each sequence is represented
/// as a minimal `FlightInfo` containing only the sequence identifier.
pub async fn list_flights(
    ctx: &facade::Context,
    criteria: Criteria,
) -> Result<BoxStream<'static, Result<FlightInfo>>> {
    // Validate criteria - only root-level queries are supported
    let expression = String::from_utf8_lossy(&criteria.expression);
    let is_root_query = expression.is_empty() || expression == "/";

    if !is_root_query {
        Err(core::Error::unsupported_descriptor())?
    }

    info!("listing all sequences");

    // Fetch all sequences from database
    let sequences = facade::sequence::all(ctx).await?;

    trace!("found {} sequences", sequences.len());

    // Convert each sequence locator to a minimal FlightInfo
    let flight_infos: Vec<Result<FlightInfo>> = sequences
        .into_iter()
        .map(|sequence_handle| {
            let sequence_name = sequence_handle.locator().to_string();

            // Create flight descriptor with the sequence path
            let descriptor = FlightDescriptor::new_path(vec![sequence_name.clone()]);

            // Create a ticket using the sequence name
            let endpoint = FlightEndpoint::new().with_ticket(Ticket {
                ticket: sequence_name.into(),
            });

            let flight_info = FlightInfo::new()
                .with_descriptor(descriptor)
                .with_endpoint(endpoint);

            Ok(flight_info)
        })
        .collect();

    // Create the stream from the vector
    let stream = futures::stream::iter(flight_infos);

    Ok(Box::pin(stream))
}

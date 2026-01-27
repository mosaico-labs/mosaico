//! Implementation of the Arrow Flight `list_flights` endpoint.
//!
//! Returns a stream of all available sequences when queried at the root level.
use super::Context;
use crate::errors::ServerError;
use arrow_flight::{Criteria, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::stream::BoxStream;
use log::{info, trace};
use mosaicod_core::types::Resource;
use mosaicod_repo as repo;
use tonic::Status;

/// Lists all available flights (sequences) in the repository.
///
/// When clients query with an empty or root path ("" or "/"), this function
/// returns a streamed list of all sequences. Each sequence is represented
/// as a minimal `FlightInfo` containing only the sequence identifier.
pub async fn list_flights(
    ctx: Context,
    criteria: Criteria,
) -> Result<BoxStream<'static, Result<FlightInfo, Status>>, ServerError> {
    // Validate criteria - only root-level queries are supported
    let expression = String::from_utf8_lossy(&criteria.expression);
    let is_root_query = expression.is_empty() || expression == "/";

    if !is_root_query {
        return Err(ServerError::UnsupportedDescriptor);
    }

    info!("listing all sequences");

    // Fetch all sequences from repository
    let sequences = repo::FacadeSequence::all(ctx.repo).await?;

    trace!("found {} sequences", sequences.len());

    // Convert each sequence locator to a minimal FlightInfo
    let flight_infos: Vec<Result<FlightInfo, Status>> = sequences
        .into_iter()
        .map(|locator| {
            let sequence_name = locator.name().to_string();

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

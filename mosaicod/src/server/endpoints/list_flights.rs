//! Implementation of the Arrow Flight `list_flights` endpoint.
//!
//! Returns a stream of all available sequences when queried at the root level.

use arrow_flight::{Criteria, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::stream::BoxStream;
use log::{info, trace};
use tonic::Status;

use crate::{repo, server::errors::ServerError, store};

/// Lists all available flights (sequences) in the repository.
///
/// When clients query with an empty or root path ("" or "/"), this function
/// returns a streamed list of all sequences. Each sequence is represented
/// as a minimal `FlightInfo` containing only the sequence identifier.
pub async fn list_flights(
    _store: store::StoreRef,
    repo: repo::Repository,
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
    let mut cx = repo.connection();
    let sequences = repo::sequence_find_all(&mut cx).await?;

    trace!("found {} sequences", sequences.len());

    // Convert each sequence record to a minimal FlightInfo
    let flight_infos: Vec<Result<FlightInfo, Status>> = sequences
        .into_iter()
        .map(|record| {
            let sequence_name = record.sequence_name.clone();

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

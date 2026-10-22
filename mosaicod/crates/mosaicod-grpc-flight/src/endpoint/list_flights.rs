//! Implementation of the Arrow Flight `list_flights` endpoint.
//!
//! Returns a page of sequences, as requested by the `ListFlightsCmd` carried in
//! `Criteria.expression`.
use arrow_flight::{Criteria, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use futures::stream::BoxStream;
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal as marshal;
use tracing::{info, trace};

/// Lists a page of sequences.
///
/// `Criteria.expression` is a serialized `ListFlightsCmd`. Each sequence is a minimal
/// `FlightInfo` carrying its name in the descriptor path and, in `app_metadata`, the
/// cursor pointing just past it.
pub async fn list_flights(
    ctx: &facade::Context,
    criteria: Criteria,
) -> grpc_common::Result<BoxStream<'static, grpc_common::Result<FlightInfo>>> {
    let cmd = marshal::flight::list_flights_cmd(&criteria.expression)?;

    info!("listing sequences from {:?} ({:?})", cmd.start, cmd.limit);

    let entries = facade::sequence::page(ctx, &cmd).await?;

    trace!("found {} sequences", entries.len());

    let flight_infos: Vec<grpc_common::Result<FlightInfo>> = entries
        .into_iter()
        .map(|entry| {
            let sequence_name = entry.locator.to_string();

            // Create flight descriptor with the sequence path
            let descriptor = FlightDescriptor::new_path(vec![sequence_name.clone()]);

            // Create a ticket using the sequence name
            let endpoint = FlightEndpoint::new().with_ticket(Ticket {
                ticket: sequence_name.into(),
            });

            let flight_info = FlightInfo::new()
                .with_descriptor(descriptor)
                .with_endpoint(endpoint)
                .with_app_metadata(marshal::flight::sequence_list_item_app_metadata_to_bytes(
                    &entry.cursor,
                ));
            Ok(flight_info)
        })
        .collect();

    Ok(Box::pin(futures::stream::iter(flight_infos)))
}

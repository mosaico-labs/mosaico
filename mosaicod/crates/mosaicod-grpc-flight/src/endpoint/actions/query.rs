//! Query-related actions

use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{self as marshal, ActionResponse};
use tracing::{info, trace};

/// Executes a query and returns matching groups.
pub async fn execute(
    ctx: &facade::Context,
    query: marshal::requests::QueryFilter,
) -> grpc_common::Result<ActionResponse> {
    info!("performing a query");

    let filter = marshal::query_filter_from_proto(query)?;

    trace!("query filter: {:?}", filter);

    let groups =
        facade::Query::query(filter, ctx.timeseries_querier.clone(), ctx.db.clone()).await?;

    trace!("groups found: {:?}", groups);

    Ok(ActionResponse::query(groups))
}

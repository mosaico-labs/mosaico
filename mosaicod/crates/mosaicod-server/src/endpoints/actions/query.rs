//! Query-related action handlers.

use crate::{endpoints::Context, errors::ServerError};
use log::{info, trace};
use mosaicod_marshal::{self as marshal, ActionResponse};
use mosaicod_repo::FacadeQuery;

/// Executes a query and returns matching groups.
pub async fn execute(
    ctx: &Context,
    query: serde_json::Value,
) -> Result<ActionResponse, ServerError> {
    info!("performing a query");

    let filter = marshal::query_filter_from_serde_value(query)?;

    trace!("query filter: {:?}", filter);

    let groups =
        FacadeQuery::query(filter, ctx.timeseries_querier.clone(), ctx.repo.clone()).await?;

    trace!("groups found: {:?}", groups);

    Ok(ActionResponse::Query(groups.into()))
}

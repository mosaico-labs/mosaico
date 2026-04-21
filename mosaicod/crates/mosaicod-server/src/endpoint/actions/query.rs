//! Query-related actions.

use crate::error::*;
use log::{info, trace};
use mosaicod_facade as facade;
use mosaicod_marshal::{self as marshal, ActionResponse};

/// Executes a query and returns matching groups.
pub async fn execute(ctx: &facade::Context, query: serde_json::Value) -> Result<ActionResponse> {
    info!("performing a query");

    let filter = marshal::query_filter_from_serde_value(query)?;

    trace!("query filter: {:?}", filter);

    let groups =
        facade::Query::query(filter, ctx.timeseries_querier.clone(), ctx.db.clone()).await?;

    trace!("groups found: {:?}", groups);

    Ok(ActionResponse::Query(groups.into()))
}

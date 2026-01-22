//! Query-related action handlers.

use log::{info, trace};

use crate::{
    marshal::{self, ActionResponse},
    repo::FacadeQuery,
    server::endpoints::Context,
    server::errors::ServerError,
};

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

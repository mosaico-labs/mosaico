//! Query-related action handlers.

use log::{info, trace};

use super::ActionContext;
use crate::{
    marshal::{self, ActionResponse},
    repo::FacadeQuery,
    server::errors::ServerError,
};

/// Handler for query-related actions.
pub struct QueryActionHandler;

impl QueryActionHandler {
    /// Executes a query and returns matching groups.
    pub async fn execute(
        ctx: &ActionContext,
        query: serde_json::Value,
    ) -> Result<ActionResponse, ServerError> {
        info!("performing a query");

        let filter = marshal::query_filter_from_serde_value(query)?;

        trace!("query filter: {:?}", filter);

        let groups = FacadeQuery::query(filter, ctx.ts_engine.clone(), ctx.repo.clone()).await?;

        trace!("groups found: {:?}", groups);

        Ok(ActionResponse::Query(groups.into()))
    }
}

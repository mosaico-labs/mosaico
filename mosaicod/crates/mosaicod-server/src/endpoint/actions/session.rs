//! Session related actions.
use crate::{ServerError, endpoint::Context};
use log::{info, trace, warn};
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_marshal::ActionResponse;

pub async fn create(
    ctx: &Context,
    sequence_locator: String,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", sequence_locator);

    let handle = facade::Sequence::new(sequence_locator, ctx.store.clone(), ctx.db.clone());
    let resource_key = handle.session().await?;

    trace!("created session for {}", handle.locator);

    Ok(ActionResponse::session_create(resource_key.uuid.into()))
}

pub async fn finalize(ctx: &Context, session_uuid: String) -> Result<ActionResponse, ServerError> {
    info!("finalizing session {}", session_uuid);

    let uuid: types::Uuid = session_uuid.parse()?;

    let handle = facade::Session::try_new(uuid, ctx.store.clone(), ctx.db.clone()).await?;

    handle.finalize().await?;

    trace!("session `{}` finalized", handle.uuid);

    Ok(ActionResponse::session_finalize())
}

pub async fn delete(ctx: &Context, session_uuid: String) -> Result<ActionResponse, ServerError> {
    warn!("deleting session `{}`", session_uuid);

    let uuid: types::Uuid = session_uuid.parse()?;

    let session = facade::Session::try_new(uuid, ctx.store.clone(), ctx.db.clone()).await?;

    session.delete(false, types::allow_data_loss()).await?;

    warn!("session `{}` deleted", session_uuid);

    Ok(ActionResponse::session_delete())
}

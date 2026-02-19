//! Session related actions.
use crate::{ServerError, endpoints::Context};
use log::{info, trace};
use mosaicod_core::types;
use mosaicod_marshal::ActionResponse;
use mosaicod_repo::{FacadeSequence, FacadeSession};

pub async fn create(
    ctx: &Context,
    sequence_locator: String,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", sequence_locator);

    let handle = FacadeSequence::new(sequence_locator, ctx.store.clone(), ctx.repo.clone());
    let resource_key = handle.session().await?;

    trace!("created session for {}", handle.locator);

    Ok(ActionResponse::session_create(resource_key.uuid.into()))
}

pub async fn finalize(ctx: &Context, uuid: String) -> Result<ActionResponse, ServerError> {
    info!("finalizing session {}", uuid);

    let uuid: types::Uuid = uuid.parse()?;

    let handle = FacadeSession::new(uuid, ctx.store.clone(), ctx.repo.clone());

    handle.finalize().await?;

    trace!("session `{}` finalized", handle.uuid);

    Ok(ActionResponse::session_finalize())
}

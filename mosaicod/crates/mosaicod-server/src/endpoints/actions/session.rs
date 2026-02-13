//! Session related actions.
use crate::{ServerError, endpoints::Context};
use log::{info, trace};
use mosaicod_core::types;
use mosaicod_marshal::ActionResponse;
use mosaicod_repo::FacadeSession;

pub async fn create(
    ctx: &Context,
    sequence_locator: String,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", sequence_locator);

    let handle = FacadeSession::new(
        types::ResourceLookup::Locator(sequence_locator),
        ctx.store.clone(),
        ctx.repo.clone(),
    );
    let resource_key = handle.create().await?;

    trace!("created session for {}", handle.lookup);

    Ok(ActionResponse::session_create(resource_key.uuid.into()))
}

pub async fn finalize(ctx: &Context, uuid: String) -> Result<ActionResponse, ServerError> {
    info!("finalizing session {}", uuid);

    let uuid: types::Uuid = uuid.parse()?;

    let handle = FacadeSession::new(
        types::ResourceLookup::Uuid(uuid),
        ctx.store.clone(),
        ctx.repo.clone(),
    );

    handle.finalize().await?;

    trace!("session `{}` finalized", handle.lookup);

    Ok(ActionResponse::session_finalize())
}

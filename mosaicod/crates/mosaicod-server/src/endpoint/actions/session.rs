//! Session related actions.
use crate::ServerError;
use log::{info, trace, warn};
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_facade::session;
use mosaicod_marshal::ActionResponse;

pub async fn create(
    ctx: &facade::Context,
    sequence_locator: String,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", sequence_locator);

    let sequence_locator = types::SequenceResourceLocator::from(sequence_locator);

    let session_handle = facade::session::try_create(sequence_locator, ctx).await?;

    trace!("created session for {}", session_handle.sequence_locator());

    Ok(ActionResponse::session_create(
        session_handle.uuid().clone().into(),
    ))
}

pub async fn finalize(
    ctx: &facade::Context,
    session_uuid: String,
) -> Result<ActionResponse, ServerError> {
    info!("finalizing session {}", session_uuid);

    let uuid: types::Uuid = session_uuid.parse()?;

    let session_handle = session::Handle::try_from_uuid(&uuid, ctx).await?;

    facade::session::finalize(&session_handle, ctx).await?;

    trace!("session `{}` finalized", uuid);

    Ok(ActionResponse::session_finalize())
}

pub async fn delete(
    ctx: &facade::Context,
    session_uuid: String,
) -> Result<ActionResponse, ServerError> {
    warn!("deleting session `{}`", session_uuid);

    let uuid: types::Uuid = session_uuid.parse()?;

    let session_handle = session::Handle::try_from_uuid(&uuid, ctx).await?;

    facade::session::delete(session_handle, false, types::allow_data_loss(), ctx).await?;

    warn!("session `{}` deleted", session_uuid);

    Ok(ActionResponse::session_delete())
}

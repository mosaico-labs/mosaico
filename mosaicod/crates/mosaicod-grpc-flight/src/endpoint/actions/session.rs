//! Session related actions.
use mosaicod_core::{self as core, types};
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::ActionResponse;
use tracing::{info, trace, warn};

pub async fn create(
    ctx: &facade::Context,
    sequence_locator: String,
) -> grpc_common::Result<ActionResponse> {
    info!("requested resource {} creation", sequence_locator);

    let sequence_locator = sequence_locator.parse::<types::SequenceLocator>()?;
    let (session_locator, session_uuid) =
        facade::session::try_create(ctx, sequence_locator).await?;

    trace!(
        "created session {} with uuid {}",
        session_locator, session_uuid
    );

    Ok(ActionResponse::session_create(
        session_locator,
        session_uuid,
    ))
}

pub async fn finalize(
    ctx: &facade::Context,
    session_uuid: String,
) -> grpc_common::Result<ActionResponse> {
    info!("finalizing session {}", session_uuid);

    let uuid: types::Uuid = session_uuid
        .parse()
        .map_err(|_| core::Error::bad_uuid(session_uuid))?;

    facade::session::finalize(ctx, &uuid).await?;

    trace!("session `{}` finalized", uuid);

    Ok(ActionResponse::session_finalize())
}

pub async fn delete(
    ctx: &facade::Context,
    session_locator: String,
) -> grpc_common::Result<ActionResponse> {
    warn!("deleting session `{}`", session_locator);

    let locator = session_locator.parse::<types::SessionLocator>()?;
    facade::session::delete(ctx, &locator, types::allow_data_loss()).await?;

    warn!("session `{}` deleted", session_locator);

    Ok(ActionResponse::session_delete())
}

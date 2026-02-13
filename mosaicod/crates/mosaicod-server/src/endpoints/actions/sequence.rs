//! Sequence-related actions

use crate::{endpoints::Context, errors::ServerError};
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob, Resource};
use mosaicod_marshal::{self as marshal, ActionResponse};
use mosaicod_repo::{FacadeError, FacadeSequence};

/// Creates a new sequence with the given name and metadata.
pub async fn create(
    ctx: &Context,
    locator: String,
    user_metadata_str: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", locator);

    let handle = FacadeSequence::new(locator, ctx.store.clone(), ctx.repo.clone());

    // Check if sequence exists, if so return with an error
    if handle.resource_id().await.is_ok() {
        return Err(ServerError::SequenceAlreadyExists(
            handle.locator.name().into(),
        ));
    }

    let user_mdata =
        marshal::JsonMetadataBlob::try_from_str(user_metadata_str).map_err(FacadeError::from)?;

    // No sequence record was found, let's write it
    let metadata = types::SequenceMetadata::new(user_mdata);
    let r_id = handle.create(Some(metadata)).await?;

    trace!(
        "created resource {} with uuid {}",
        handle.locator, r_id.uuid
    );

    Ok(ActionResponse::sequence_create())
}

/// Deletes an unlocked sequence.
pub async fn delete(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("requested deletion of resource {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());

    let loc = handle.locator.clone();
    handle.delete().await?;
    warn!("resource {} deleted", loc);

    Ok(ActionResponse::sequence_delete())
}

/// Aborts a sequence creation, deleting it if the key matches.
pub async fn abort(
    ctx: &Context,
    name: String,
    key: String,
) -> Result<ActionResponse, ServerError> {
    warn!("abort for {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());

    // Check that sequence id and provided key matches
    let r_id = handle.resource_id().await?;
    let received_uuid: types::Uuid = key.parse()?;
    if r_id.uuid != received_uuid {
        return Err(ServerError::BadKey);
    }

    // Save handle name (for logging) since the delete will consume the handle
    let loc = handle.locator.clone();
    handle.delete().await?;
    warn!("resource {} deleted", loc.name());

    Ok(ActionResponse::sequence_abort())
}

/// Creates a notification for a sequence.
pub async fn notify_create(
    ctx: &Context,
    name: String,
    notify_type: String,
    msg: String,
) -> Result<ActionResponse, ServerError> {
    info!("new notify for {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());
    let ntype: types::NotifyType = notify_type.parse()?;
    handle.notify(ntype, msg).await?;

    Ok(ActionResponse::sequence_notify_create())
}

/// Lists all notifications for a sequence.
pub async fn notify_list(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("notify list for {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());
    let notifies = handle.notify_list().await?;

    Ok(ActionResponse::sequence_notify_list(notifies.into()))
}

/// Purges all notifications for a sequence.
pub async fn notify_purge(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("notify purge for {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());
    handle.notify_purge().await?;

    Ok(ActionResponse::sequence_notify_purge())
}

/// Gets system information for a sequence.
pub async fn system_info(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("[{}] sequence system informations", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());
    let sysinfo = handle.system_info().await?;

    Ok(ActionResponse::sequence_system_info(sysinfo.into()))
}

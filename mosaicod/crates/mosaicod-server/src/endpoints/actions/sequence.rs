//! Sequence-related action handlers.

use crate::{endpoints::Context, errors::ServerError};
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob, Resource};
use mosaicod_marshal::{self as marshal, ActionResponse};
use mosaicod_repo::{FacadeError, FacadeSequence};

/// Creates a new sequence with the given name and metadata.
pub async fn create(
    ctx: &Context,
    name: String,
    user_metadata_str: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());

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
    Ok(ActionResponse::SequenceCreate(r_id.into()))
}

/// Deletes an unlocked sequence.
pub async fn delete(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("requested deletion of resource {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());

    if handle.is_locked().await? {
        return Err(ServerError::SequenceLocked);
    }

    let loc = handle.locator.clone();
    handle.delete().await?;
    warn!("resource {} deleted", loc);

    Ok(ActionResponse::Empty)
}

/// Aborts a sequence creation, deleting it if the key matches.
pub async fn abort(
    ctx: &Context,
    name: String,
    key: String,
) -> Result<ActionResponse, ServerError> {
    warn!("abort for {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());

    // Avoid aborting on locked sequences
    if handle.is_locked().await? {
        return Err(ServerError::SequenceLocked);
    }

    // Check that sequence id and provided key matches
    let r_id = handle.resource_id().await?;
    let received_uuid: uuid::Uuid = key.parse()?;
    if r_id.uuid != received_uuid {
        return Err(ServerError::BadKey);
    }

    // Save handle name (for logging) since the delete will consume the handle
    let loc = handle.locator.clone();
    handle.delete().await?;
    warn!("resource {} deleted", loc.name());

    Ok(ActionResponse::Empty)
}

/// Finalizes and locks a sequence.
pub async fn finalize(
    ctx: &Context,
    name: String,
    key: String,
) -> Result<ActionResponse, ServerError> {
    info!("resource {} finalized", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());

    // Check that key matches the sequence id
    let r_id = handle.resource_id().await?;
    let received_uuid: uuid::Uuid = key.parse()?;

    if r_id.uuid != received_uuid {
        return Err(ServerError::BadKey);
    }

    handle.lock().await?;
    trace!("resource {} locked", handle.locator);

    Ok(ActionResponse::Empty)
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

    Ok(ActionResponse::Empty)
}

/// Lists all notifications for a sequence.
pub async fn notify_list(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("notify list for {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());
    let notifies = handle.notify_list().await?;

    Ok(ActionResponse::SequenceNotifyList(notifies.into()))
}

/// Purges all notifications for a sequence.
pub async fn notify_purge(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("notify purge for {}", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());
    handle.notify_purge().await?;

    Ok(ActionResponse::Empty)
}

/// Gets system information for a sequence.
pub async fn system_info(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("[{}] sequence system informations", name);

    let handle = FacadeSequence::new(name, ctx.store.clone(), ctx.repo.clone());
    let sysinfo = handle.system_info().await?;

    Ok(ActionResponse::SequenceSystemInfo(sysinfo.into()))
}

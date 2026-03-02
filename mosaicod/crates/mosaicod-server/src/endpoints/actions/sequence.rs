//! Sequence-related actions

use crate::{endpoints::Context, errors::ServerError};
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob, Resource};
use mosaicod_facade as facade;
use mosaicod_marshal::{self as marshal, ActionResponse};

/// Creates a new sequence with the given name and metadata.
pub async fn create(
    ctx: &Context,
    locator: String,
    user_metadata_str: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", locator);

    let handle = facade::Sequence::new(locator, ctx.store.clone(), ctx.db.clone());

    // Check if sequence exists, if so return with an error
    if handle.resource_id().await.is_ok() {
        return Err(ServerError::SequenceAlreadyExists(
            handle.locator.name().into(),
        ));
    }

    let user_mdata =
        marshal::JsonMetadataBlob::try_from_str(user_metadata_str).map_err(facade::Error::from)?;

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

    let handle = facade::Sequence::new(name, ctx.store.clone(), ctx.db.clone());

    let loc = handle.locator.clone();
    handle.delete(types::allow_data_loss()).await?;
    warn!("resource {} deleted", loc);

    Ok(ActionResponse::sequence_delete())
}

/// Creates a notification for a sequence.
pub async fn notification_create(
    ctx: &Context,
    name: String,
    notification_type: String,
    msg: String,
) -> Result<ActionResponse, ServerError> {
    info!("new notification for {}", name);

    let handle = facade::Sequence::new(name, ctx.store.clone(), ctx.db.clone());
    let ntype: types::NotificationType = notification_type.parse()?;
    handle.notify(ntype, msg).await?;

    Ok(ActionResponse::sequence_notification_create())
}

/// Lists all notifications for a sequence.
pub async fn notification_list(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("notification list for {}", name);

    let handle = facade::Sequence::new(name, ctx.store.clone(), ctx.db.clone());
    let notifications = handle.notification_list().await?;

    Ok(ActionResponse::sequence_notification_list(
        notifications.into(),
    ))
}

/// Purges all notifications for a sequence.
pub async fn notification_purge(
    ctx: &Context,
    name: String,
) -> Result<ActionResponse, ServerError> {
    warn!("notification purge for {}", name);

    let handle = facade::Sequence::new(name, ctx.store.clone(), ctx.db.clone());
    handle.notification_purge().await?;

    Ok(ActionResponse::sequence_notification_purge())
}

/// Gets system information for a sequence.
pub async fn system_info(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("[{}] sequence system informations", name);

    let handle = facade::Sequence::new(name, ctx.store.clone(), ctx.db.clone());
    let sysinfo = handle.system_info().await?;

    Ok(ActionResponse::sequence_system_info(sysinfo.into()))
}

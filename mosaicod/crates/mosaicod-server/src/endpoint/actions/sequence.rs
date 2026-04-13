//! Sequence-related actions

use crate::errors::ServerError;
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob};
use mosaicod_facade as facade;
use mosaicod_marshal::{self as marshal, ActionResponse};

/// Creates a new sequence with the given name and metadata.
pub async fn create(
    ctx: &facade::Context,
    locator: String,
    user_metadata_str: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", locator);

    let locator = types::SequenceResourceLocator::from(locator);

    let user_mdata =
        marshal::JsonMetadataBlob::try_from_str(user_metadata_str).map_err(facade::Error::from)?;

    // No sequence record was found, let's write it
    let sequence_handle = facade::sequence::try_create(ctx, locator, Some(user_mdata))
        .await
        .inspect_err(|e| println!("error in sequence create: {}", e))?;

    trace!(
        "created resource {} with uuid {}",
        sequence_handle.locator(),
        sequence_handle.uuid()
    );

    Ok(ActionResponse::sequence_create())
}

/// Deletes an unlocked sequence.
pub async fn delete(ctx: &facade::Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("requested deletion of resource {}", name);

    let locator = types::SequenceResourceLocator::from(name);

    let handle = facade::sequence::Handle::try_from_locator(ctx, locator.clone()).await?;

    facade::sequence::delete(ctx, handle, types::allow_data_loss()).await?;
    warn!("resource {} deleted", locator);

    Ok(ActionResponse::sequence_delete())
}

/// Creates a notification for a sequence.
pub async fn notification_create(
    ctx: &facade::Context,
    name: String,
    notification_type: String,
    msg: String,
) -> Result<ActionResponse, ServerError> {
    info!("new notification for {}", name);

    let locator = types::SequenceResourceLocator::from(name);

    let handle = facade::sequence::Handle::try_from_locator(ctx, locator).await?;

    let ntype: types::NotificationType = notification_type.parse()?;
    facade::sequence::notify(ctx, &handle, ntype, msg).await?;

    Ok(ActionResponse::sequence_notification_create())
}

/// Lists all notifications for a sequence.
pub async fn notification_list(
    ctx: &facade::Context,
    name: String,
) -> Result<ActionResponse, ServerError> {
    info!("notification list for {}", name);

    let locator = types::SequenceResourceLocator::from(name);

    let handle = facade::sequence::Handle::try_from_locator(ctx, locator).await?;

    let notifications = facade::sequence::notification_list(ctx, &handle).await?;

    Ok(ActionResponse::sequence_notification_list(
        notifications.into(),
    ))
}

/// Purges all notifications for a sequence.
pub async fn notification_purge(
    ctx: &facade::Context,
    name: String,
) -> Result<ActionResponse, ServerError> {
    warn!("notification purge for {}", name);

    let locator = types::SequenceResourceLocator::from(name);

    let handle = facade::sequence::Handle::try_from_locator(ctx, locator).await?;

    facade::sequence::notification_purge(ctx, &handle).await?;

    Ok(ActionResponse::sequence_notification_purge())
}

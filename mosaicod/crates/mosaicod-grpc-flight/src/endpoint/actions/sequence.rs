//! Sequence-related actions
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob};
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{self as marshal, ActionResponse};

/// Creates a new sequence with the given name and metadata.
pub async fn create(
    ctx: &facade::Context,
    locator: String,
    user_metadata_str: &str,
) -> grpc_common::Result<ActionResponse> {
    info!("requested resource {} creation", locator);

    let locator = locator.parse::<types::SequenceLocator>()?;
    let user_mdata = marshal::JsonMetadataBlob::try_from_str(user_metadata_str)?;

    // No sequence record was found, let's write it
    let sequence_uuid = facade::sequence::try_create(ctx, &locator, Some(user_mdata)).await?;

    trace!("created resource {} with uuid {}", locator, sequence_uuid);

    Ok(ActionResponse::sequence_create())
}

/// Deletes a sequence.
pub async fn delete(ctx: &facade::Context, name: String) -> grpc_common::Result<ActionResponse> {
    warn!("requested deletion of resource {}", name);

    let locator = name.parse::<types::SequenceLocator>()?;
    facade::sequence::delete(ctx, &locator, types::allow_data_loss()).await?;

    warn!("resource {} deleted", locator);

    Ok(ActionResponse::sequence_delete())
}

/// Creates a notification for a sequence.
pub async fn notification_create(
    ctx: &facade::Context,
    name: &str,
    notification_type: &str,
    msg: &str,
) -> grpc_common::Result<ActionResponse> {
    info!("new notification for {}", name);

    let ntype: types::NotificationType = notification_type
        .parse()
        .map_err(|_| grpc_common::Error::invalid_notification_type(notification_type))?;

    let locator = name.parse::<types::SequenceLocator>()?;
    facade::sequence::notify(ctx, &locator, ntype, msg).await?;

    Ok(ActionResponse::sequence_notification_create())
}

/// Lists all notifications for a sequence.
pub async fn notification_list(
    ctx: &facade::Context,
    name: &str,
) -> grpc_common::Result<ActionResponse> {
    info!("notification list for {}", name);

    let locator = name.parse::<types::SequenceLocator>()?;
    let notifications = facade::sequence::notification_list(ctx, locator).await?;

    Ok(ActionResponse::sequence_notification_list(
        notifications.into(),
    ))
}

/// Purges all notifications for a sequence.
pub async fn notification_purge(
    ctx: &facade::Context,
    name: &str,
) -> grpc_common::Result<ActionResponse> {
    warn!("notification purge for {}", name);

    let locator = name.parse::<types::SequenceLocator>()?;
    facade::sequence::notification_purge(ctx, &locator).await?;

    Ok(ActionResponse::sequence_notification_purge())
}

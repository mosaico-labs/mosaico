//! Topic-related actions.

use crate::{endpoints::Context, errors::ServerError};
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob, Resource};
use mosaicod_facade as facade;
use mosaicod_marshal::{self as marshal, ActionResponse};

/// Creates a new topic with the given name and metadata.
pub async fn create(
    ctx: &Context,
    name: String,
    sequence_key: String,
    serialization_format: types::Format,
    ontology_tag: String,
    user_metadata_str: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", name);

    let handle = facade::Topic::new(name.clone(), ctx.store.clone(), ctx.db.clone());

    // Check if the topic has already been created
    if handle.resource_id().await.is_ok() {
        return Err(ServerError::TopicAlreadyExists(
            handle.locator.name().into(),
        ));
    }

    let user_mdata =
        marshal::JsonMetadataBlob::try_from_str(user_metadata_str).map_err(facade::Error::from)?;

    let mdata = types::TopicMetadata::new(
        types::TopicProperties::new(serialization_format, ontology_tag),
        user_mdata,
    );

    let received_uuid: types::Uuid = sequence_key.parse()?;
    let r_id = handle.create(&received_uuid, Some(mdata)).await?;

    trace!(
        "resource {} created with uuid {}",
        handle.locator, r_id.uuid,
    );

    Ok(ActionResponse::TopicCreate(r_id.into()))
}

/// Deletes an unlocked topic.
pub async fn delete(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("requested deletion of resource {}", name);

    let handle = facade::Topic::new(name.clone(), ctx.store.clone(), ctx.db.clone());

    if handle.is_locked().await? {
        return Err(ServerError::TopicLocked);
    }

    handle.delete_unlocked().await?;
    warn!("resource {} deleted", name);

    Ok(ActionResponse::Empty)
}

/// Creates a notification for a topic.
pub async fn notification_create(
    ctx: &Context,
    name: String,
    notification_type: String,
    msg: String,
) -> Result<ActionResponse, ServerError> {
    info!("notification for {}", name);

    let handle = facade::Topic::new(name, ctx.store.clone(), ctx.db.clone());
    handle.notify(notification_type.parse()?, msg).await?;

    Ok(ActionResponse::Empty)
}

/// Lists all notifications for a topic.
pub async fn notification_list(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("notification list for {}", name);

    let handle = facade::Topic::new(name, ctx.store.clone(), ctx.db.clone());
    let notifications = handle.notification_list().await?;

    Ok(ActionResponse::TopicNotificationList(notifications.into()))
}

/// Purges all notifications for a topic.
pub async fn notification_purge(
    ctx: &Context,
    name: String,
) -> Result<ActionResponse, ServerError> {
    warn!("notification purge for {}", name);

    let handle = facade::Topic::new(name, ctx.store.clone(), ctx.db.clone());
    handle.notification_purge().await?;

    Ok(ActionResponse::Empty)
}

/// Gets system information for a topic.
pub async fn system_info(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("[{}] topic system information", name);

    let handle = facade::Topic::new(name, ctx.store.clone(), ctx.db.clone());
    let sysinfo = handle.system_info().await?;

    Ok(ActionResponse::TopicSystemInfo(sysinfo.into()))
}

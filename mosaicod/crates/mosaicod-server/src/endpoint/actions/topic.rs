//! Topic-related actions.

use crate::{endpoint::Context, errors::ServerError};
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob};
use mosaicod_facade as facade;
use mosaicod_marshal::{self as marshal, ActionResponse};

/// Creates a new topic with the given name and metadata.
pub async fn create(
    ctx: &Context,
    name: String,
    session_uuid: String,
    serialization_format: types::Format,
    ontology_tag: String,
    user_metadata_str: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", name);

    let user_mdata =
        marshal::JsonMetadataBlob::try_from_str(user_metadata_str).map_err(facade::Error::from)?;

    let received_uuid: types::Uuid = session_uuid.parse()?;

    let ontology_metadata = types::TopicOntologyMetadata::new(
        types::TopicOntologyProperties {
            serialization_format,
            ontology_tag,
        },
        Some(user_mdata),
    );

    let ftopic = facade::Topic::create(
        name.into(),
        received_uuid,
        ontology_metadata,
        ctx.store.clone(),
        ctx.db.clone(),
    )
    .await?;

    trace!(
        "resource `{}` created with uuid {}",
        ftopic.locator,
        ftopic.uuid(),
    );

    Ok(ActionResponse::TopicCreate(ftopic.uuid().clone().into()))
}

/// Deletes an unlocked topic.
pub async fn delete(ctx: &Context, locator: String) -> Result<ActionResponse, ServerError> {
    warn!("requested deletion of resource `{}`", locator);

    let handle =
        facade::Topic::try_from_locator(locator.clone().into(), ctx.store.clone(), ctx.db.clone())
            .await?;

    if handle.manifest().await?.properties.locked {
        return Err(ServerError::TopicLocked);
    }

    handle.delete_unlocked().await?;
    warn!("resource {} deleted", locator);

    Ok(ActionResponse::Empty)
}

/// Creates a notification for a topic.
pub async fn notification_create(
    ctx: &Context,
    locator: String,
    notification_type: String,
    msg: String,
) -> Result<ActionResponse, ServerError> {
    info!("notification for {}", locator);

    let ftopic =
        facade::Topic::try_from_locator(locator.into(), ctx.store.clone(), ctx.db.clone()).await?;
    ftopic.notify(notification_type.parse()?, msg).await?;

    Ok(ActionResponse::Empty)
}

/// Lists all notifications for a topic.
pub async fn notification_list(
    ctx: &Context,
    locator: String,
) -> Result<ActionResponse, ServerError> {
    info!("notification list for {}", locator);

    let ftopic =
        facade::Topic::try_from_locator(locator.into(), ctx.store.clone(), ctx.db.clone()).await?;
    let notifications = ftopic.notification_list().await?;

    Ok(ActionResponse::TopicNotificationList(notifications.into()))
}

/// Purges all notifications for a topic.
pub async fn notification_purge(
    ctx: &Context,
    locator: String,
) -> Result<ActionResponse, ServerError> {
    warn!("notification purge for {}", locator);

    let ftopic =
        facade::Topic::try_from_locator(locator.into(), ctx.store.clone(), ctx.db.clone()).await?;
    ftopic.notification_purge().await?;

    Ok(ActionResponse::Empty)
}

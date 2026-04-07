//! Topic-related actions.

use crate::errors::ServerError;
use log::{info, trace, warn};
use mosaicod_core::types::{self, MetadataBlob};
use mosaicod_facade as facade;
use mosaicod_marshal::{self as marshal, ActionResponse};

/// Creates a new topic with the given name and metadata.
pub async fn create(
    ctx: &facade::Context,
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

    let topic_locator = types::TopicResourceLocator::from(name);

    let session_handle = facade::session::Handle::try_from_uuid(&received_uuid, ctx).await?;
    let topic_handle =
        facade::topic::create(&topic_locator, &session_handle, ontology_metadata, ctx).await?;

    let topic_uuid = facade::topic::uuid(&topic_handle, ctx);

    trace!(
        "resource `{}` created with uuid {}",
        topic_locator, topic_uuid,
    );

    Ok(ActionResponse::TopicCreate(topic_uuid.into()))
}

/// Deletes an unlocked topic.
pub async fn delete(ctx: &facade::Context, locator: String) -> Result<ActionResponse, ServerError> {
    warn!("requested deletion of resource `{}`", locator);

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(&topic_locator, ctx).await?;

    if facade::topic::manifest(&topic_handle, ctx)
        .await?
        .properties
        .locked
    {
        return Err(ServerError::TopicLocked);
    }

    facade::topic::delete_unlocked(topic_handle, ctx).await?;
    warn!("resource {} deleted", topic_locator);

    Ok(ActionResponse::Empty)
}

/// Creates a notification for a topic.
pub async fn notification_create(
    ctx: &facade::Context,
    locator: String,
    notification_type: String,
    msg: String,
) -> Result<ActionResponse, ServerError> {
    info!("notification for {}", locator);

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(&topic_locator, ctx).await?;

    facade::topic::notify(&topic_handle, notification_type.parse()?, msg, ctx).await?;

    Ok(ActionResponse::Empty)
}

/// Lists all notifications for a topic.
pub async fn notification_list(
    ctx: &facade::Context,
    locator: String,
) -> Result<ActionResponse, ServerError> {
    info!("notification list for {}", locator);

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(&topic_locator, ctx).await?;

    let notifications = facade::topic::notification_list(&topic_handle, ctx).await?;

    Ok(ActionResponse::TopicNotificationList(notifications.into()))
}

/// Purges all notifications for a topic.
pub async fn notification_purge(
    ctx: &facade::Context,
    locator: String,
) -> Result<ActionResponse, ServerError> {
    warn!("notification purge for {}", locator);

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(&topic_locator, ctx).await?;

    facade::topic::notification_purge(&topic_handle, ctx).await?;

    Ok(ActionResponse::Empty)
}

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

    let session_handle = facade::session::Handle::try_from_uuid(ctx, &received_uuid).await?;
    let topic_handle =
        facade::topic::try_create(ctx, topic_locator, &session_handle, ontology_metadata).await?;

    trace!(
        "resource `{}` created with uuid {}",
        topic_handle.locator(),
        topic_handle.uuid(),
    );

    Ok(ActionResponse::TopicCreate(
        topic_handle.uuid().clone().into(),
    ))
}

/// Deletes a topic (it doesn't matter if it's still open or archived).
pub async fn delete(ctx: &facade::Context, locator: String) -> Result<ActionResponse, ServerError> {
    warn!("requested deletion of resource `{}`", locator);

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator.clone()).await?;

    facade::topic::delete(ctx, topic_handle, types::allow_data_loss()).await?;
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

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    facade::topic::notify(ctx, &topic_handle, notification_type.parse()?, msg).await?;

    Ok(ActionResponse::Empty)
}

/// Lists all notifications for a topic.
pub async fn notification_list(
    ctx: &facade::Context,
    locator: String,
) -> Result<ActionResponse, ServerError> {
    info!("notification list for {}", locator);

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    let notifications = facade::topic::notification_list(ctx, &topic_handle).await?;

    Ok(ActionResponse::TopicNotificationList(notifications.into()))
}

/// Purges all notifications for a topic.
pub async fn notification_purge(
    ctx: &facade::Context,
    locator: String,
) -> Result<ActionResponse, ServerError> {
    warn!("notification purge for {}", locator);

    let topic_locator = types::TopicResourceLocator::from(locator);

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    facade::topic::notification_purge(ctx, &topic_handle).await?;

    Ok(ActionResponse::Empty)
}

//! Topic-related actions.

use crate::error::{Error, Result};
use log::{info, trace, warn};
use mosaicod_core::{
    self as core,
    types::{self, MetadataBlob},
};
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
) -> Result<ActionResponse> {
    info!("requested resource {} creation", name);

    let user_mdata = marshal::JsonMetadataBlob::try_from_str(user_metadata_str)?;

    let received_uuid: types::Uuid = session_uuid
        .parse()
        .map_err(|_| core::Error::bad_uuid(session_uuid))?;

    let ontology_metadata = types::TopicOntologyMetadata::new(
        types::TopicOntologyProperties {
            serialization_format,
            ontology_tag,
        },
        Some(user_mdata),
    );

    let topic_locator = name.parse::<types::TopicLocator>()?;

    let session_handle = facade::session::Handle::try_from_uuid(ctx, &received_uuid).await?;

    let topic_handle =
        facade::topic::try_create(ctx, topic_locator, &session_handle, ontology_metadata).await?;

    trace!(
        "resource `{}` created with uuid {}",
        topic_handle.locator(),
        topic_handle.uuid(),
    );

    Ok(ActionResponse::topic_create(
        topic_handle.uuid().clone().into(),
    ))
}

/// Deletes a topic (it doesn't matter if it's still open or archived).
pub async fn delete(ctx: &facade::Context, locator: String) -> Result<ActionResponse> {
    warn!("requested deletion of resource `{}`", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator.clone()).await?;

    facade::topic::delete(ctx, topic_handle, types::allow_data_loss()).await?;

    warn!("resource {} deleted", topic_locator);

    Ok(ActionResponse::topic_delete())
}

/// Creates a notification for a topic.
pub async fn notification_create(
    ctx: &facade::Context,
    locator: String,
    notification_type: String,
    msg: String,
) -> Result<ActionResponse> {
    info!("notification for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    let notification_type = notification_type
        .parse()
        .map_err(|_| Error::invalid_notification_type(&notification_type))?;

    facade::topic::notify(ctx, &topic_handle, notification_type, msg).await?;

    Ok(ActionResponse::topic_notification_create())
}

/// Lists all notifications for a topic.
pub async fn notification_list(ctx: &facade::Context, locator: String) -> Result<ActionResponse> {
    info!("notification list for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    let notifications = facade::topic::notification_list(ctx, &topic_handle).await?;

    Ok(ActionResponse::topic_notification_list(
        notifications.into(),
    ))
}

/// Purges all notifications for a topic.
pub async fn notification_purge(ctx: &facade::Context, locator: String) -> Result<ActionResponse> {
    warn!("notification purge for {}", locator);

    let topic_locator = locator.parse::<types::TopicLocator>()?;

    let topic_handle = facade::topic::Handle::try_from_locator(ctx, topic_locator).await?;

    facade::topic::notification_purge(ctx, &topic_handle).await?;

    Ok(ActionResponse::topic_notification_purge())
}

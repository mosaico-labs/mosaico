//! Topic-related action handlers.

use log::{info, trace, warn};

use crate::{
    marshal::{self, ActionResponse},
    repo::{FacadeError, FacadeTopic},
    rw,
    server::endpoints::Context,
    server::errors::ServerError,
    types::{self, MetadataBlob, Resource},
};

/// Creates a new topic with the given name and metadata.
pub async fn create(
    ctx: &Context,
    name: String,
    sequence_key: String,
    serialization_format: rw::Format,
    ontology_tag: String,
    user_metadata_str: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested resource {} creation", name);

    let handle = FacadeTopic::new(name.clone(), ctx.store.clone(), ctx.repo.clone());

    // Check if the topic has already been created
    if handle.resource_id().await.is_ok() {
        return Err(ServerError::TopicAlreadyExists(
            handle.locator.name().into(),
        ));
    }

    let user_mdata =
        marshal::JsonMetadataBlob::try_from_str(user_metadata_str).map_err(FacadeError::from)?;

    let mdata = types::TopicMetadata::new(
        types::TopicProperties::new(serialization_format, ontology_tag),
        user_mdata,
    );

    let received_uuid: uuid::Uuid = sequence_key.parse()?;
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

    let handle = FacadeTopic::new(name.clone(), ctx.store.clone(), ctx.repo.clone());

    if handle.is_locked().await? {
        return Err(ServerError::SequenceLocked);
    }

    handle.delete_unlocked().await?;
    warn!("resource {} deleted", name);

    Ok(ActionResponse::Empty)
}

/// Creates a notification for a topic.
pub async fn notify_create(
    ctx: &Context,
    name: String,
    notify_type: String,
    msg: String,
) -> Result<ActionResponse, ServerError> {
    info!("nofity for {}", name);

    let handle = FacadeTopic::new(name, ctx.store.clone(), ctx.repo.clone());
    handle.notify(notify_type.parse()?, msg).await?;

    Ok(ActionResponse::Empty)
}

/// Lists all notifications for a topic.
pub async fn notify_list(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("notify list for {}", name);

    let handle = FacadeTopic::new(name, ctx.store.clone(), ctx.repo.clone());
    let notifies = handle.notify_list().await?;

    Ok(ActionResponse::TopicNotifyList(notifies.into()))
}

/// Purges all notifications for a topic.
pub async fn notify_purge(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("nofity purge for {}", name);

    let handle = FacadeTopic::new(name, ctx.store.clone(), ctx.repo.clone());
    handle.notify_purge().await?;

    Ok(ActionResponse::Empty)
}

/// Gets system information for a topic.
pub async fn system_info(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    info!("[{}] topic system informations", name);

    let handle = FacadeTopic::new(name, ctx.store.clone(), ctx.repo.clone());
    let sysinfo = handle.system_info().await?;

    Ok(ActionResponse::TopicSystemInfo(sysinfo.into()))
}

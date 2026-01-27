//! Layer-related action handlers.

use crate::{endpoints::Context, errors::ServerError};
use log::{info, warn};
use mosaicod_core::types;
use mosaicod_marshal::ActionResponse;
use mosaicod_repo::FacadeLayer;

/// Creates a new layer with the given name and description.
pub async fn create(
    ctx: &Context,
    name: String,
    description: String,
) -> Result<ActionResponse, ServerError> {
    info!("creating layer `{}`", name);

    let handle = FacadeLayer::new(
        types::LayerLocator::from(name.as_str()),
        ctx.store.clone(),
        ctx.repo.clone(),
    );
    handle.create(description).await?;

    Ok(ActionResponse::Empty)
}

/// Deletes a layer.
pub async fn delete(ctx: &Context, name: String) -> Result<ActionResponse, ServerError> {
    warn!("deleting layer `{}`", name);

    let handle = FacadeLayer::new(
        types::LayerLocator::from(name.as_str()),
        ctx.store.clone(),
        ctx.repo.clone(),
    );
    handle.delete().await?;

    Ok(ActionResponse::Empty)
}

/// Updates a layer's name and description.
pub async fn update(
    ctx: &Context,
    prev_name: String,
    curr_name: String,
    curr_description: String,
) -> Result<ActionResponse, ServerError> {
    info!(
        "updating layer `{}` with new name `{}` and new description `{}`",
        prev_name, curr_name, curr_description
    );

    let handle = FacadeLayer::new(
        types::LayerLocator::from(prev_name.as_str()),
        ctx.store.clone(),
        ctx.repo.clone(),
    );
    handle
        .update(
            types::LayerLocator::from(curr_name.as_str()),
            &curr_description,
        )
        .await?;

    Ok(ActionResponse::Empty)
}

/// Lists all layers.
pub async fn list(ctx: &Context) -> Result<ActionResponse, ServerError> {
    info!("request layer list");

    let layers = FacadeLayer::all(ctx.repo.clone()).await?;

    Ok(ActionResponse::LayerList(layers.into()))
}

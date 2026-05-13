use crate::error::*;
use log::info;
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_marshal::ActionResponse;

/// Creates a new api key with the given name and metadata.
pub async fn api_key_create(
    ctx: &facade::Context,
    permissions: String,
    expires_at: Option<types::Timestamp>,
    description: String,
) -> Result<ActionResponse> {
    info!("requested new api key");
    let handle = facade::auth::create(ctx, permissions.parse()?, description, expires_at).await?;
    Ok(ActionResponse::api_key_create(handle.api_key().key.into()))
}

/// Returns the status for the given api key.
pub async fn api_key_status(ctx: &facade::Context, fingerprint: &str) -> Result<ActionResponse> {
    info!("requested api key status");
    let handle = facade::auth::Handle::try_from_fingerprint(ctx, fingerprint).await?;
    Ok(ActionResponse::api_key_status(handle.api_key().into()))
}

/// Revokes the selected api key.
pub async fn api_key_revoke(ctx: &facade::Context, fingerprint: &str) -> Result<ActionResponse> {
    info!("requested api key revocation");
    let handle = facade::auth::Handle::try_from_fingerprint(ctx, fingerprint).await?;
    facade::auth::delete(ctx, handle).await?;
    Ok(ActionResponse::api_key_revoke())
}

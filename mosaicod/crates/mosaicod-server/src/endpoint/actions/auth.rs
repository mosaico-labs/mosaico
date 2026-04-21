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

    let auth = facade::Auth::create(
        permissions.parse()?,
        description,
        expires_at,
        ctx.db.clone(),
    )
    .await?;

    Ok(ActionResponse::api_key_create(auth.api_key().key.into()))
}

/// Returns the status for the given api key.
pub async fn api_key_status(ctx: &facade::Context, fingerprint: &str) -> Result<ActionResponse> {
    info!("requested api key status");
    let auth = facade::Auth::try_from_fingerprint(fingerprint, ctx.db.clone()).await?;
    Ok(ActionResponse::api_key_status(auth.api_key().into()))
}

/// Revokes the selected api key.
pub async fn api_key_revoke(ctx: &facade::Context, fingerprint: &str) -> Result<ActionResponse> {
    info!("requested api key revocation");
    let auth = facade::Auth::try_from_fingerprint(fingerprint, ctx.db.clone()).await?;
    auth.delete().await?;
    Ok(ActionResponse::api_key_revoke())
}

use crate::ServerError;
use crate::endpoint::Context;
use log::info;
use mosaicod_core::types;
use mosaicod_facade as facade;
use mosaicod_marshal::ActionResponse;

/// Creates a new api key with the given name and metadata.
pub async fn api_key_create(
    ctx: &Context,
    permissions: Vec<String>,
    expires_at: Option<types::Timestamp>,
    description: String,
) -> Result<ActionResponse, ServerError> {
    info!("requested new api key");

    let auth = facade::Auth::create(
        permissions
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<&str>>()
            .as_slice()
            .try_into()?,
        description,
        expires_at,
        ctx.db.clone(),
    )
    .await?;

    Ok(ActionResponse::api_key_create(auth.api_key().key.into()))
}

/// Returns the status for the given api key.
pub async fn api_key_status(
    ctx: &Context,
    fingerprint: &str,
) -> Result<ActionResponse, ServerError> {
    info!("requested api key status");
    let auth = facade::Auth::try_from_fingerprint(fingerprint, ctx.db.clone()).await?;
    Ok(ActionResponse::api_key_status(auth.api_key().into()))
}

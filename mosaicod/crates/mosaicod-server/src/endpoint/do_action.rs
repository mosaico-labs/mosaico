//! Flight DoAction endpoint implementation.
//!
//! This module implements the main dispatcher for Flight DoAction requests,
//! delegating to specialized handler functions for each action category.

use super::actions::{misc, query as query_action, sequence, session, topic};
use crate::error::Result;
use crate::{endpoint::actions::auth, flight::DoActionStream};
use mosaicod_core::{self as core, types::auth::Permission};
use mosaicod_facade as facade;
use mosaicod_marshal::{ActionRequest, ActionResponse};

/// Adapter that wraps a single ActionResponse into a one-item
/// DoActionStream, as expected by Arrow Flight's do_action endpoint.
///
/// Use this when the handler produces a single payload rather than a
/// stream of results.
pub fn single_stream(resp: ActionResponse) -> Result<DoActionStream> {
    let bytes = resp.bytes()?;
    Ok(Box::pin(futures::stream::once(async move {
        Ok(arrow_flight::Result::new(bytes))
    })))
}

/// Dispatches a Flight action request to the appropriate handler.
///
/// This function serves as the main entry point for all Flight DoAction requests,
/// routing each action type to its specialized handler function.
pub async fn do_action(
    ctx: &facade::Context,
    action: ActionRequest,
    perm: &Permission,
) -> Result<DoActionStream> {
    if !has_permissions(&action, perm) {
        let err_msg = format!(
            "provided API key has not enough permissions to execute {} action.",
            action
        );
        Err(core::Error::unauthorized(err_msg))?;
    }

    match action {
        // ////////
        // Sequence
        ActionRequest::SequenceCreate(data) => {
            let user_metadata = data.user_metadata()?;
            single_stream(sequence::create(ctx, data.locator, user_metadata.as_str()).await?)
        }
        ActionRequest::SequenceDelete(data) => {
            single_stream(sequence::delete(ctx, data.locator).await?)
        }
        ActionRequest::SequenceNotificationCreate(data) => single_stream(
            sequence::notification_create(ctx, data.locator, data.notification_type, data.msg)
                .await?,
        ),
        ActionRequest::SequenceNotificationList(data) => {
            single_stream(sequence::notification_list(ctx, data.locator).await?)
        }
        ActionRequest::SequenceNotificationPurge(data) => {
            single_stream(sequence::notification_purge(ctx, data.locator).await?)
        }

        // ///////
        // Session
        ActionRequest::SessionCreate(data) => {
            single_stream(session::create(ctx, data.locator).await?)
        }
        ActionRequest::SessionFinalize(data) => {
            single_stream(session::finalize(ctx, data.session_uuid).await?)
        }
        ActionRequest::SessionDelete(data) => {
            single_stream(session::delete(ctx, data.locator).await?)
        }

        // /////
        // Topic
        ActionRequest::TopicCreate(data) => {
            let user_metadata = data.user_metadata()?;
            single_stream(
                topic::create(
                    ctx,
                    data.locator,
                    data.session_uuid,
                    data.serialization_format.into(),
                    data.ontology_tag,
                    user_metadata.as_str(),
                )
                .await?,
            )
        }
        ActionRequest::TopicDelete(data) => single_stream(topic::delete(ctx, data.locator).await?),
        ActionRequest::TopicNotificationCreate(data) => single_stream(
            topic::notification_create(ctx, data.locator, data.notification_type, data.msg).await?,
        ),
        ActionRequest::TopicNotificationList(data) => {
            single_stream(topic::notification_list(ctx, data.locator).await?)
        }
        ActionRequest::TopicNotificationPurge(data) => {
            single_stream(topic::notification_purge(ctx, data.locator).await?)
        }
        ActionRequest::TopicFilterClusterize(data) => {
            topic::filter_clusterize(
                ctx,
                data.locator,
                data.clustering_dt_ns,
                data.ontology,
                data.timestamp_range,
            )
            .await
        }

        // /////
        // Query
        ActionRequest::Query(data) => single_stream(query_action::execute(ctx, data.query).await?),

        // ////
        // Api Key
        ActionRequest::ApiKeyCreate(data) => single_stream(
            auth::api_key_create(
                ctx,
                data.permissions,
                data.expires_at_ns.map(Into::into),
                data.description,
            )
            .await?,
        ),

        ActionRequest::ApiKeyStatus(data) => {
            single_stream(auth::api_key_status(ctx, data.api_key_fingerprint.as_str()).await?)
        }

        ActionRequest::ApiKeyRevoke(data) => {
            single_stream(auth::api_key_revoke(ctx, data.api_key_fingerprint.as_str()).await?)
        }

        // /////
        // Misc
        ActionRequest::Version(_) => single_stream(misc::version()?),
    }
}

/// Return true if the requested action matches the permissions, false otherwise
fn has_permissions(action: &ActionRequest, perm: &Permission) -> bool {
    match action {
        ActionRequest::SequenceCreate(_) => perm.can_write(),
        ActionRequest::SequenceNotificationCreate(_) => perm.can_write(),
        ActionRequest::TopicCreate(_) => perm.can_write(),
        ActionRequest::TopicNotificationCreate(_) => perm.can_write(),
        ActionRequest::SessionCreate(_) => perm.can_write(),
        ActionRequest::SessionFinalize(_) => perm.can_write(),

        ActionRequest::SequenceDelete(_) => perm.can_delete(),
        ActionRequest::SequenceNotificationPurge(_) => perm.can_delete(),
        ActionRequest::TopicDelete(_) => perm.can_delete(),
        ActionRequest::TopicNotificationPurge(_) => perm.can_delete(),
        ActionRequest::SessionDelete(_) => perm.can_delete(),

        ActionRequest::Query(_) => perm.can_read(),
        ActionRequest::SequenceNotificationList(_) => perm.can_read(),
        ActionRequest::TopicNotificationList(_) => perm.can_read(),
        ActionRequest::TopicFilterClusterize(_) => perm.can_read(),

        ActionRequest::ApiKeyCreate(_) => perm.can_manage(),
        ActionRequest::ApiKeyStatus(_) => perm.can_manage(),
        ActionRequest::ApiKeyRevoke(_) => perm.can_manage(),

        ActionRequest::Version(_) => true,
    }
}

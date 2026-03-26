//! Flight DoAction endpoint implementation.
//!
//! This module implements the main dispatcher for Flight DoAction requests,
//! delegating to specialized handler functions for each action category.

use super::{
    Context,
    actions::{layer, misc, query as query_action, sequence, session, topic},
};
use crate::endpoint::actions::auth;
use crate::errors::ServerError;
use mosaicod_core::types::auth::Permission;
use mosaicod_marshal::{ActionRequest, ActionResponse};

/// Dispatches a Flight action request to the appropriate handler.
///
/// This function serves as the main entry point for all Flight DoAction requests,
/// routing each action type to its specialized handler function.
pub async fn do_action(
    ctx: Context,
    action: ActionRequest,
    perm: &Permission,
) -> Result<ActionResponse, ServerError> {
    if !has_permissions(&action, perm) {
        return Err(ServerError::Unauthorized);
    }

    match action {
        // ////////
        // Sequence
        ActionRequest::SequenceCreate(data) => {
            let user_metadata = data.user_metadata()?;
            sequence::create(&ctx, data.locator, user_metadata.as_str()).await
        }
        ActionRequest::SequenceDelete(data) => sequence::delete(&ctx, data.locator).await,
        ActionRequest::SequenceNotificationCreate(data) => {
            sequence::notification_create(&ctx, data.locator, data.notification_type, data.msg)
                .await
        }
        ActionRequest::SequenceNotificationList(data) => {
            sequence::notification_list(&ctx, data.locator).await
        }
        ActionRequest::SequenceNotificationPurge(data) => {
            sequence::notification_purge(&ctx, data.locator).await
        }

        // ///////
        // Session
        ActionRequest::SessionCreate(data) => session::create(&ctx, data.locator).await,
        ActionRequest::SessionFinalize(data) => session::finalize(&ctx, data.session_uuid).await,
        ActionRequest::SessionDelete(data) => session::delete(&ctx, data.session_uuid).await,

        // /////
        // Topic
        ActionRequest::TopicCreate(data) => {
            let user_metadata = data.user_metadata()?;
            topic::create(
                &ctx,
                data.locator,
                data.session_uuid,
                data.serialization_format.into(),
                data.ontology_tag,
                user_metadata.as_str(),
            )
            .await
        }
        ActionRequest::TopicDelete(data) => topic::delete(&ctx, data.locator).await,
        ActionRequest::TopicNotificationCreate(data) => {
            topic::notification_create(&ctx, data.locator, data.notification_type, data.msg).await
        }
        ActionRequest::TopicNotificationList(data) => {
            topic::notification_list(&ctx, data.locator).await
        }
        ActionRequest::TopicNotificationPurge(data) => {
            topic::notification_purge(&ctx, data.locator).await
        }

        // /////
        // Layer
        ActionRequest::LayerCreate(data) => layer::create(&ctx, data.name, data.description).await,
        ActionRequest::LayerDelete(data) => layer::delete(&ctx, data.name).await,
        ActionRequest::LayerUpdate(data) => {
            layer::update(&ctx, data.prev_name, data.curr_name, data.curr_description).await
        }
        ActionRequest::LayerList(_) => layer::list(&ctx).await,

        // /////
        // Query
        ActionRequest::Query(data) => query_action::execute(&ctx, data.query).await,

        // ////
        // Api Key
        ActionRequest::ApiKeyCreate(data) => {
            auth::api_key_create(
                &ctx,
                data.permissions,
                data.expires_at_ns.map(Into::into),
                data.description,
            )
            .await
        }

        ActionRequest::ApiKeyStatus(data) => {
            auth::api_key_status(&ctx, data.api_key_fingerprint.as_str()).await
        }

        ActionRequest::ApiKeyRevoke(data) => {
            auth::api_key_revoke(&ctx, data.api_key_fingerprint.as_str()).await
        }

        // /////
        // Misc
        ActionRequest::Version(_) => misc::version(),
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
        ActionRequest::LayerCreate(_) => perm.can_write(),
        ActionRequest::LayerUpdate(_) => perm.can_write(),

        ActionRequest::SequenceDelete(_) => perm.can_delete(),
        ActionRequest::SequenceNotificationPurge(_) => perm.can_delete(),
        ActionRequest::TopicDelete(_) => perm.can_delete(),
        ActionRequest::TopicNotificationPurge(_) => perm.can_delete(),
        ActionRequest::SessionDelete(_) => perm.can_delete(),
        ActionRequest::LayerDelete(_) => perm.can_delete(),

        ActionRequest::Query(_) => perm.can_read(),
        ActionRequest::SequenceNotificationList(_) => perm.can_read(),
        ActionRequest::TopicNotificationList(_) => perm.can_read(),
        ActionRequest::LayerList(_) => perm.can_read(),

        ActionRequest::ApiKeyCreate(_) => perm.can_manage(),
        ActionRequest::ApiKeyStatus(_) => perm.can_manage(),
        ActionRequest::ApiKeyRevoke(_) => perm.can_manage(),

        ActionRequest::Version(_) => true,
    }
}

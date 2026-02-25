//! Flight DoAction endpoint implementation.
//!
//! This module implements the main dispatcher for Flight DoAction requests,
//! delegating to specialized handler functions for each action category.

use super::actions::{layer, query as query_action, sequence, session, topic};
use crate::{endpoints::Context, errors::ServerError};
use mosaicod_marshal::{ActionRequest, ActionResponse};

/// Dispatches a Flight action request to the appropriate handler.
///
/// This function serves as the main entry point for all Flight DoAction requests,
/// routing each action type to its specialized handler function.
pub async fn do_action(ctx: Context, action: ActionRequest) -> Result<ActionResponse, ServerError> {
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
        ActionRequest::SequenceSystemInfo(data) => sequence::system_info(&ctx, data.locator).await,

        // ///////
        // Session
        ActionRequest::SessionCreate(data) => session::create(&ctx, data.locator).await,
        ActionRequest::SessionFinalize(data) => session::finalize(&ctx, data.session_uuid).await,
        ActionRequest::SessionAbort(data) => session::abort(&ctx, data.session_uuid).await,

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
        ActionRequest::TopicSystemInfo(data) => topic::system_info(&ctx, data.locator).await,

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
    }
}

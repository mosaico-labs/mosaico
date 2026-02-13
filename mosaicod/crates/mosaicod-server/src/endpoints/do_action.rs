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
            sequence::create(&ctx, data.name, user_metadata.as_str()).await
        }
        ActionRequest::SequenceDelete(data) => sequence::delete(&ctx, data.name).await,
        ActionRequest::SequenceAbort(data) => sequence::abort(&ctx, data.name, data.key).await,
        ActionRequest::SequenceNotifyCreate(data) => {
            sequence::notify_create(&ctx, data.name, data.notify_type, data.msg).await
        }
        ActionRequest::SequenceNotifyList(data) => sequence::notify_list(&ctx, data.name).await,
        ActionRequest::SequenceNotifyPurge(data) => sequence::notify_purge(&ctx, data.name).await,
        ActionRequest::SequenceSystemInfo(data) => sequence::system_info(&ctx, data.name).await,

        // ///////
        // Session
        ActionRequest::SessionCreate(data) => session::create(&ctx, data.name).await,
        ActionRequest::SessionFinalize(data) => session::finalize(&ctx, data.key).await,

        // /////
        // Topic
        ActionRequest::TopicCreate(data) => {
            let user_metadata = data.user_metadata()?;
            topic::create(
                &ctx,
                data.name,
                data.sequence_key,
                data.serialization_format.into(),
                data.ontology_tag,
                user_metadata.as_str(),
            )
            .await
        }
        ActionRequest::TopicDelete(data) => topic::delete(&ctx, data.name).await,
        ActionRequest::TopicNotifyCreate(data) => {
            topic::notify_create(&ctx, data.name, data.notify_type, data.msg).await
        }
        ActionRequest::TopicNotifyList(data) => topic::notify_list(&ctx, data.name).await,
        ActionRequest::TopicNotifyPurge(data) => topic::notify_purge(&ctx, data.name).await,
        ActionRequest::TopicSystemInfo(data) => topic::system_info(&ctx, data.name).await,

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

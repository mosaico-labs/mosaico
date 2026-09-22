//! Flight DoAction endpoint implementation.
//!
//! This module implements the main dispatcher for Flight DoAction requests,
//! delegating to specialized handler functions for each action category.

use super::actions::{misc, query as query_action, sequence, session, topic};
use crate::flight::{DoActionStream, IntoStream};
use mosaicod_core::{self as core, types::auth::Permissions};
use mosaicod_facade as facade;
use mosaicod_grpc_common as grpc_common;
use mosaicod_marshal::{self as marshal, ActionRequest, requests, timestamp_range_from_proto};

/// Dispatches a Flight action request to the appropriate handler.
///
/// This function serves as the main entry point for all Flight DoAction requests,
/// routing each action type to its specialized handler function.
pub async fn do_action(
    ctx: &facade::Context,
    action: ActionRequest,
    perm: &Permissions,
) -> grpc_common::Result<DoActionStream> {
    if !has_permissions(&action, perm) {
        let err_msg = format!(
            "provided API key has not enough permissions to execute {} action.",
            action
        );
        Err(core::Error::unauthorized(err_msg))?;
    }

    match action {
        // Sequence
        ActionRequest::SequenceCreate(data) => {
            sequence::create(ctx, data.locator, &data.user_metadata)
                .await?
                .into_stream()
        }
        ActionRequest::SequenceDelete(data) => {
            sequence::delete(ctx, data.locator).await?.into_stream()
        }
        ActionRequest::SequenceNotificationCreate(data) => {
            sequence::notification_create(ctx, &data.locator, &data.notification_type, &data.msg)
                .await?
                .into_stream()
        }
        ActionRequest::SequenceNotificationList(data) => {
            sequence::notification_list(ctx, &data.locator)
                .await?
                .into_stream()
        }
        ActionRequest::SequenceNotificationPurge(data) => {
            sequence::notification_purge(ctx, &data.locator)
                .await?
                .into_stream()
        }

        // Session
        ActionRequest::SessionCreate(data) => {
            session::create(ctx, data.locator).await?.into_stream()
        }
        ActionRequest::SessionFinalize(data) => session::finalize(ctx, data.session_uuid)
            .await?
            .into_stream(),
        ActionRequest::SessionDelete(data) => {
            session::delete(ctx, data.locator).await?.into_stream()
        }

        // Topic
        ActionRequest::TopicCreate(data) => {
            let format = marshal::format_from_i32(data.serialization_format);
            topic::create(
                ctx,
                data.locator,
                data.session_uuid,
                format,
                data.ontology_tag,
                &data.user_metadata,
            )
            .await?
            .into_stream()
        }
        ActionRequest::TopicDelete(data) => topic::delete(ctx, data.locator).await?.into_stream(),
        ActionRequest::TopicNotificationCreate(data) => {
            topic::notification_create(ctx, &data.locator, &data.notification_type, &data.msg)
                .await?
                .into_stream()
        }
        ActionRequest::TopicNotificationList(data) => topic::notification_list(ctx, data.locator)
            .await?
            .into_stream(),
        ActionRequest::TopicNotificationPurge(data) => topic::notification_purge(ctx, data.locator)
            .await?
            .into_stream(),
        ActionRequest::TopicFilterClusterize(data) => {
            let ontology = requests::topic_clusterize_ontology(&data)?;
            topic::filter_clusterize(
                ctx,
                data.locator,
                data.clustering_dt_ns,
                ontology,
                data.timestamp_range
                    .map(|ts_range| timestamp_range_from_proto(&ts_range)),
            )
            .await
        }
        ActionRequest::TopicFilterIntersect(data) => {
            topic::filter_intersect(ctx, data.topics, data.intersect_dt_ns).await
        }

        // Query
        ActionRequest::Query(data) => query_action::execute(ctx, requests::query_filter(&data)?)
            .await?
            .into_stream(),

        // Misc
        ActionRequest::Info(_) => misc::info()?.into_stream(),
    }
}

/// Return true if the requested action matches the permissions, false otherwise
fn has_permissions(action: &ActionRequest, perm: &Permissions) -> bool {
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
        ActionRequest::TopicFilterIntersect(_) => perm.can_read(),

        ActionRequest::Info(_) => true,
    }
}

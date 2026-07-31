//! This module provides the high-level API for managing a persistent **Sequence**
//! entity within the application.

use super::{Context, session, topic};
use mosaicod_core::{self as core, error::PublicError, error::PublicResult as Result, types};
use mosaicod_db as db;
use mosaicod_marshal as marshal;
use std::path;
use tracing::trace;

/// Define sequence metadata type contaning json user metadata
pub type SequenceUserMetadata = marshal::JsonMetadataBlob;

pub type SequenceMetadata = types::SequenceMetadata<marshal::JsonMetadataBlob>;

pub struct SequenceInfo {
    pub metadata: SequenceMetadata,
    pub topics: Vec<topic::TopicInfo>,
}

pub(super) mod internal {
    use super::*;

    pub async fn metadata_write_to_store(
        context: &Context,
        path: &path::Path,
        metadata: SequenceUserMetadata,
    ) -> Result<()> {
        trace!("converting sequence metadata to bytes");
        let json_mdata = marshal::JsonSequenceMetadata {
            user_metadata: metadata,
        };
        let bytes: Vec<u8> = json_mdata.try_into()?;

        trace!("writing sequence metadata `{}` to store", path.display());

        context.store.write_bytes(&path, bytes).await?;

        Ok(())
    }

    /// Creates [`SequenceMetadata`] associated to the given sequence.
    pub async fn metadata(
        exe: &mut impl db::AsExec,
        sequence_record: &db::SequenceRecord,
    ) -> Result<SequenceMetadata> {
        let sessions = db::sequence_find_all_sessions(exe, sequence_record.sequence_id).await?;

        let mut sequence_metadata = SequenceMetadata {
            created_at: sequence_record.creation_timestamp(),
            resource_locator: sequence_record.locator(),
            sessions: vec![],
            user_metadata: sequence_record.user_metadata(),
        };

        for session_record in sessions {
            sequence_metadata
                .sessions
                .push(session::internal::metadata(exe, &session_record).await?);
        }

        Ok(sequence_metadata)
    }
}

/// Creates a new database entry for this sequence.
///
/// Once created the sequence is empty (it has only immutable user-defined metadata associated, if any).
/// Topics can be added later via uploading sessions.
///
/// If a record with the same locator already exists, the operation fails and
/// the database transaction is rolled back, restoring the previous state.
///
/// Returns the UUID of the newly created sequence.
pub async fn try_create(
    context: &Context,
    locator: &types::SequenceLocator,
    metadata: Option<SequenceUserMetadata>,
) -> Result<types::Uuid> {
    // Create a random name for the folder on Object Store.
    let path_in_store = types::SequencePathInStore::new();

    // 1. Create sequence in database.
    // Note: we want to prevent the newly created folder in the store from being marked as TO_DELETE by the cleanup routine.
    // That's why we create the DB record as first thing.
    let mut cx = context.db.connection();
    let record = db::sequence_create(
        &mut cx,
        locator,
        &path_in_store,
        metadata.clone().map(Into::into),
    )
    .await?;

    // 2. If metadata are present, save them to Store too.
    if let Some(mdata) = metadata {
        let res = internal::metadata_write_to_store(
            context,
            path_in_store.path_metadata().as_path(),
            mdata,
        )
        .await;

        // Rollback: remove the newly created sequence from the database.
        if let Err(e) = res {
            let mut cx = context.db.connection();
            let delete_res =
                db::sequence_delete_by_id(&mut cx, record.sequence_id, types::allow_data_loss())
                    .await;

            // If the sequence is not in the DB it means that somebody else did the job for us,
            // then there is no need to throw this specific error.
            if !matches!(delete_res, Err(db::Error::NotFound)) {
                delete_res?
            }

            return Err(e);
        }
    }

    Ok(record.uuid())
}

/// Returns a list of all available sequences.
///
/// This is primarily used for catalog discovery operations.
pub async fn all(context: &Context) -> Result<Vec<types::SequenceLocator>> {
    let mut cx = context.db.connection();
    Ok(db::sequence_find_all(&mut cx)
        .await?
        .into_iter()
        .map(|seq_record| seq_record.locator())
        .collect())
}

/// Add a notification to the sequence
pub async fn notify(
    context: &Context,
    locator: &types::SequenceLocator,
    ntype: types::NotificationType,
    msg: &str,
) -> Result<types::Notification<types::SequenceLocator>> {
    // Note: no need to check the sequence existence for it is already done internally
    // by the DB constraints checks on the foreign key.
    let mut cx = context.db.connection();
    let notification = db::sequence_notification_create(&mut cx, locator, ntype, msg)
        .await
        .map_err(|e| match &e {
            db::Error::NotFound | db::Error::ForeignKeyViolation => {
                core::Error::not_found(locator.to_string())
            }
            _ => e.error(),
        })?;

    Ok(notification.into_notification(locator.clone()))
}

/// Returns a list of all notifications for the sequence
pub async fn notification_list(
    context: &Context,
    locator: types::SequenceLocator,
) -> Result<Vec<types::Notification<types::SequenceLocator>>> {
    let mut cx = context.db.connection();

    let notifications = db::sequence_notifications_find_by_locator(&mut cx, &locator).await?;

    Ok(notifications
        .into_iter()
        .map(|n| n.into_notification(locator.clone()))
        .collect())
}

/// Deletes all the notifications associated with the sequence
pub async fn notification_purge(context: &Context, locator: &types::SequenceLocator) -> Result<()> {
    let mut cx = context.db.connection();
    db::sequence_notifications_purge(&mut cx, locator).await?;
    Ok(())
}

/// Retrieves info regarding the given sequence and its topics [`locator`]
pub async fn info(context: &Context, locator: &types::SequenceLocator) -> Result<SequenceInfo> {
    let mut cx = context.db.connection();

    let sequence_record = db::sequence_find_by_locator(&mut cx, locator)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => core::Error::not_found(locator.to_string()),
            _ => e.error(),
        })?;

    let mut res = SequenceInfo {
        metadata: internal::metadata(&mut cx, &sequence_record).await?,
        topics: vec![],
    };

    let topics = db::sequence_find_all_topics(&mut cx, sequence_record.sequence_id).await?;

    for topic_record in topics {
        res.topics.push(
            topic::internal::info(&mut cx, context.timeseries_querier.clone(), &topic_record)
                .await?,
        );
    }

    Ok(res)
}

/// Deletes a sequence and all its associated sessions and topics from the database.
///
/// The [`types::DataLossToken`] is required since this function will lead to data loss.
pub async fn delete(
    context: &Context,
    locator: &types::SequenceLocator,
    allow_data_loss: types::DataLossToken,
) -> Result<()> {
    let mut cx = context.db.connection();
    db::sequence_delete_by_locator(&mut cx, locator, allow_data_loss).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types::NotificationType;
    use mosaicod_query as query;
    use mosaicod_store as store;
    use std::sync::Arc;

    use types::MetadataBlob;

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new((*store).clone(), 0).unwrap());

        Context::new((*store).clone(), (*database).clone(), ts_gw)
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_sequence_create_and_delete(
        pool: sqlx::Pool<db::DatabaseType>,
    ) -> sqlx::Result<()> {
        let context = test_context(pool);

        let mdata = r#"{
            "driver" : "john",
            "weather": "sunny"
        }"#;
        dbg!(&mdata);
        let mdata = marshal::JsonMetadataBlob::try_from_str(mdata).unwrap();

        let seq_locator = "test_sequence".parse().unwrap();

        let seq_uuid = try_create(&context, &seq_locator, Some(mdata))
            .await
            .expect("Error creating sequence");

        assert!(seq_uuid.is_valid());

        // Check database user metadata
        let seq_record = db::sequence_find_by_locator(&mut context.db.connection(), &seq_locator)
            .await
            .unwrap();

        let user_mdata: serde_json::Value = seq_record
            .user_metadata()
            .expect("Unable to find user metadata in database record")
            .into();

        assert_eq!(user_mdata["driver"].as_str().unwrap(), "john");
        assert_eq!(user_mdata["weather"].as_str().unwrap(), "sunny");

        // Check sequence locator
        assert_eq!(seq_record.locator(), seq_locator);

        // Check path in store
        assert!(
            context
                .store
                .exists(seq_record.path_in_store().path_metadata())
                .await
                .unwrap()
        );

        let metadata = internal::metadata(&mut context.db.connection(), &seq_record)
            .await
            .unwrap();
        assert!(metadata.created_at.as_i64() > 0);
        assert!(metadata.user_metadata.is_some());
        assert!(metadata.sessions.is_empty());
        assert_eq!(metadata.resource_locator, seq_locator);

        // Root path in store must be a valid ULID (excluded the sq_ prefix)
        assert!(
            seq_record.path_in_store().root().to_str().unwrap()[3..]
                .parse::<ulid::Ulid>()
                .is_ok()
        );

        delete(&context, &seq_locator, types::allow_data_loss())
            .await
            .expect("Unable to delete the sequence");

        Ok(())
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn sequence_notify_and_notification_purge(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();

        try_create(&context, &seq_locator, None)
            .await
            .expect("Error creating sequence");

        let seq_record = db::sequence_find_by_locator(&mut context.db.connection(), &seq_locator)
            .await
            .unwrap();

        notify(
            &context,
            &seq_locator,
            NotificationType::Error,
            "test notification message",
        )
        .await
        .expect("Error creating notification");

        notify(
            &context,
            &seq_locator,
            NotificationType::Error,
            "test notification message 2",
        )
        .await
        .expect("Error creating notification");

        // Check if notifications were created on database.
        let mut cx = context.db.connection();
        let notifications = db::sequence_notifications_find_by_locator(&mut cx, &seq_locator)
            .await
            .unwrap();

        assert_eq!(notifications.len(), 2);

        let first_notification = notifications.first().unwrap();
        assert_eq!(
            first_notification.msg.as_ref().unwrap(),
            "test notification message"
        );
        assert!(first_notification.uuid().is_valid());
        assert_eq!(first_notification.sequence_id, seq_record.sequence_id);

        let second_notification = notifications.last().unwrap();
        assert_eq!(
            second_notification.msg.as_ref().unwrap(),
            "test notification message 2"
        );
        assert!(second_notification.uuid().is_valid());
        assert_eq!(second_notification.sequence_id, seq_record.sequence_id);

        notification_purge(&context, &seq_locator)
            .await
            .expect("Unable to purge notifications");

        // Check there are no more notifications on database.
        assert!(
            db::sequence_notifications_find_by_locator(&mut cx, &seq_locator)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

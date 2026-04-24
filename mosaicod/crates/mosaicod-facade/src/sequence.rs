//! This module provides the high-level API for managing a persistent **Sequence**
//! entity within the application.

use super::{Context, session, topic};
use log::trace;
use mosaicod_core::{
    error::PublicResult as Result,
    types::{self, SequencePathInStore},
};
use mosaicod_db as db;
use mosaicod_marshal as marshal;
use std::path;

/// Define sequence metadata type contaning json user metadata
type SequenceUserMetadata = marshal::JsonMetadataBlob;

type SequenceMetadata = types::SequenceMetadata<marshal::JsonMetadataBlob>;

/// Handle containing sequence identifiers.
/// It's used by all functions (except creation) in this module to indicate the sequence to operate on.
pub struct Handle {
    locator: types::SequenceLocator,
    id: i32,
    uuid: types::Uuid,
}

impl Handle {
    /// Try to obtain a handle from a sequence locator.
    /// Returns an error if the sequence does not exist.
    pub async fn try_from_locator(
        context: &Context,
        locator: types::SequenceLocator,
    ) -> Result<Handle> {
        let mut cx = context.db.connection();

        let db_sequence = db::sequence_find_by_locator(&mut cx, &locator).await?;

        Ok(Self {
            locator,
            id: db_sequence.sequence_id,
            uuid: db_sequence.uuid(),
        })
    }

    /// Try to obtain a handle from a sequence UUID.
    /// Returns an error if the sequence does not exist.
    pub async fn try_from_uuid(context: &Context, uuid: &types::Uuid) -> Result<Handle> {
        let mut cx = context.db.connection();

        let db_sequence = db::sequence_find_by_uuid(&mut cx, uuid).await?;

        Ok(Self {
            locator: db_sequence.locator(),
            id: db_sequence.sequence_id,
            uuid: db_sequence.uuid(),
        })
    }

    pub fn uuid(&self) -> &types::Uuid {
        &self.uuid
    }

    pub fn locator(&self) -> &types::SequenceLocator {
        &self.locator
    }

    pub(super) fn id(&self) -> i32 {
        self.id
    }
}

/// Creates a new database entry for this sequence.
///
/// Once created the sequence is empty (it has only immutable user-defined metadata associated, if any).
/// Topics can be added later via uploading sessions.
///
/// If a record with the same locator already exists, the operation fails and
/// the database transaction is rolled back, restoring the previous state.
pub async fn try_create(
    context: &Context,
    locator: types::SequenceLocator,
    metadata: Option<SequenceUserMetadata>,
) -> Result<Handle> {
    // 1. Creates a random name for the folder on Object Store and save metadata file (optional).
    let path_in_store = SequencePathInStore::new();

    if let Some(mdata) = &metadata {
        metadata_write_to_store(
            context,
            path_in_store.path_metadata().as_path(),
            mdata.clone(),
        )
        .await?;
    }

    // 2. Create sequence in database.
    let mut tx = context.db.transaction().await?;

    let mut record = db::SequenceRecord::new(locator.clone(), path_in_store.clone());

    if let Some(mdata) = metadata {
        record = record.with_user_metadata(mdata);
    }

    let record = db::sequence_create(&mut tx, &record).await?;

    tx.commit().await?;

    Ok(Handle {
        locator,
        id: record.sequence_id,
        uuid: record.uuid(),
    })
}

/// Retrieves all sequences from the database.
///
/// Returns a list of all available sequences as [`Handle`] objects.
/// This is primarily used for catalog discovery operations.
pub async fn all(context: &Context) -> Result<Vec<Handle>> {
    let mut cx = context.db.connection();
    let records = db::sequence_find_all(&mut cx).await?;

    Ok(records
        .into_iter()
        .map(|record| Handle {
            id: record.sequence_id,
            uuid: record.uuid(),
            locator: record.locator(),
        })
        .collect())
}

async fn metadata_write_to_store(
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

/// Add a notification to the sequence
pub async fn notify(
    context: &Context,
    handle: &Handle,
    ntype: types::NotificationType,
    msg: String,
) -> Result<types::Notification> {
    let mut tx = context.db.transaction().await?;

    // Note: no need to check the sequence existence for it is already done internally
    // by the DB constraints checks on the foreign key.
    let notification = db::SequenceNotificationRecord::new(handle.id(), ntype, Some(msg));
    let notification = db::sequence_notification_create(&mut tx, &notification).await?;

    tx.commit().await?;

    Ok(notification.into_notification(handle.locator.clone()))
}

/// Returns a list of all notifications for the sequence
pub async fn notification_list(
    context: &Context,
    handle: &Handle,
) -> Result<Vec<types::Notification>> {
    let mut trans = context.db.transaction().await?;
    let notifications =
        db::sequence_notifications_find_by_sequence_id(&mut trans, handle.id()).await?;
    trans.commit().await?;
    Ok(notifications
        .into_iter()
        .map(|n| n.into_notification(handle.locator.clone()))
        .collect())
}

/// Deletes all the notifications associated with the sequence
pub async fn notification_purge(context: &Context, handle: &Handle) -> Result<()> {
    let mut trans = context.db.transaction().await?;

    let notifications =
        db::sequence_notifications_find_by_sequence_id(&mut trans, handle.id()).await?;

    for notification in notifications {
        // Notification id is unwrapped since is retrieved from the database, and it has an id.
        db::sequence_notification_delete(&mut trans, notification.id().unwrap()).await?;
    }

    trans.commit().await?;
    Ok(())
}

/// Creates [`SequenceMetadata`] associated to the given session [`Handle`].
pub async fn metadata(context: &Context, handle: &Handle) -> Result<SequenceMetadata> {
    let mut cx = context.db.connection();

    let db_sequence = db::sequence_find_by_id(&mut cx, handle.id()).await?;

    let sessions = session_list(handle, &mut cx).await?;

    let mut sequence_metadata = SequenceMetadata {
        created_at: db_sequence.creation_timestamp(),
        resource_locator: handle.locator.clone(),
        sessions: vec![],
        user_metadata: db_sequence.user_metadata(),
    };

    for session_handle in sessions {
        sequence_metadata
            .sessions
            .push(session::metadata(context, &session_handle).await?);
    }

    Ok(sequence_metadata)
}

/// Returns the topic list for the given sequence
pub async fn topic_list(context: &Context, handle: &Handle) -> Result<Vec<topic::Handle>> {
    let mut cx = context.db.connection();

    Ok(db::sequence_find_all_topics(&mut cx, &handle.locator)
        .await?
        .into_iter()
        .map(|record| {
            topic::Handle::new(
                record.locator(),
                record.topic_id,
                record.uuid(),
                record.path_in_store(),
            )
        })
        .collect())
}

/// Returns the session list associated with this sequence as vector of session UUIDs
pub async fn session_list(
    handle: &Handle,
    exe: &mut impl db::AsExec,
) -> Result<Vec<session::Handle>> {
    Ok(db::sequence_find_all_sessions(exe, &handle.locator)
        .await?
        .into_iter()
        .map(|record| {
            session::Handle::new(handle.locator.clone(), record.session_id, record.uuid())
        })
        .collect())
}

/// Deletes a sequence and all its associated sessions and topics from the database.
///
/// The [`types::DataLossToken`] is required since this function will lead to data loss.
pub async fn delete(
    context: &Context,
    handle: Handle,
    allow_data_loss: types::DataLossToken,
) -> Result<()> {
    let mut cx = context.db.connection();
    db::sequence_delete_by_id(&mut cx, handle.id(), allow_data_loss).await?;
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

        let handle = try_create(&context, seq_locator, Some(mdata))
            .await
            .expect("Error creating sequence");

        // Check if sequence was correctly created on DB.
        let mut cx = context.db.connection();
        let sequence = db::sequence_find_by_locator(&mut cx, &handle.locator)
            .await
            .expect("Unable to find the created sequence");

        // Check database user metadata
        let user_mdata: serde_json::Value = sequence
            .user_metadata()
            .expect("Unable to find user metadata in database record")
            .into();

        assert_eq!(user_mdata["driver"].as_str().unwrap(), "john");
        assert_eq!(user_mdata["weather"].as_str().unwrap(), "sunny");

        // Check sequence locator
        assert_eq!(handle.locator, sequence.locator());

        // Check path in store
        assert!(
            context
                .store
                .exists(sequence.path_in_store().path_metadata())
                .await
                .unwrap()
        );

        let metadata = metadata(&context, &handle).await.unwrap();
        assert!(metadata.created_at.as_i64() > 0);
        assert!(metadata.user_metadata.is_some());
        assert!(metadata.sessions.is_empty());
        assert_eq!(metadata.resource_locator, handle.locator);

        // Root path in store must be a valid ULID (excluded the sq_ prefix)
        assert!(
            sequence.path_in_store().root().to_str().unwrap()[3..]
                .parse::<ulid::Ulid>()
                .is_ok()
        );

        delete(&context, handle, types::allow_data_loss())
            .await
            .expect("Unable to delete the sequence");

        Ok(())
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn sequence_notify_and_notification_purge(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();

        let handle = try_create(&context, seq_locator, None)
            .await
            .expect("Error creating sequence");

        // Check if sequence was created
        let handle = Handle::try_from_locator(&context, handle.locator)
            .await
            .expect("Unable to find the created sequence");

        notify(
            &context,
            &handle,
            NotificationType::Error,
            "test notification message".to_owned(),
        )
        .await
        .expect("Error creating notification");

        notify(
            &context,
            &handle,
            NotificationType::Error,
            "test notification message 2".to_owned(),
        )
        .await
        .expect("Error creating notification");

        // Check if notifications were created on database.
        let mut cx = context.db.connection();
        let notifications = db::sequence_notifications_find_by_name(&mut cx, &handle.locator)
            .await
            .unwrap();

        assert_eq!(notifications.len(), 2);

        let first_notification = notifications.first().unwrap();
        assert_eq!(
            first_notification.msg.as_ref().unwrap(),
            "test notification message"
        );
        assert!(first_notification.uuid().is_valid());
        assert_eq!(first_notification.sequence_id, handle.id());

        let second_notification = notifications.last().unwrap();
        assert_eq!(
            second_notification.msg.as_ref().unwrap(),
            "test notification message 2"
        );
        assert!(second_notification.uuid().is_valid());
        assert_eq!(second_notification.sequence_id, handle.id());

        notification_purge(&context, &handle)
            .await
            .expect("Unable to purge notifications");

        // Check there are no more notifications on database.
        assert!(
            db::sequence_notifications_find_by_name(&mut cx, &handle.locator)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

//! This module provides the high-level API for managing a persistent **Sequence**
//! entity within the application.

use super::{Context, Error, session};
use log::trace;
use mosaicod_core::types;
use mosaicod_core::types::Resource;
use mosaicod_db as db;
use mosaicod_marshal as marshal;

/// Define sequence metadata type contaning json user metadata
type SequenceMetadata = types::SequenceMetadata<marshal::JsonMetadataBlob>;

/// Handle containing sequence identifiers.
/// It's used by all functions (except creation) in this module to indicate the sequence to operate on.
pub struct Handle {
    pub(super) locator: types::SequenceResourceLocator,
    identifiers: types::Identifiers,
}

impl Handle {
    /// Try to obtain a handle from a sequence locator.
    /// Returns an error if the sequence does not exist.
    pub async fn try_from_locator(
        locator: &types::SequenceResourceLocator,
        context: &Context,
    ) -> Result<Handle, Error> {
        let mut cx = context.db.connection();

        let db_sequence = db::sequence_find_by_locator(&mut cx, locator).await?;

        Ok(Self {
            locator: locator.clone(),
            identifiers: db_sequence.identifiers(),
        })
    }

    /// Try to obtain a handle from a sequence UUID.
    /// Returns an error if the sequence does not exist.
    pub async fn try_from_uuid(uuid: &types::Uuid, context: &Context) -> Result<Handle, Error> {
        let mut cx = context.db.connection();

        let db_sequence = db::sequence_find_by_uuid(&mut cx, uuid).await?;

        Ok(Self {
            locator: types::SequenceResourceLocator::from(&db_sequence.locator_name),
            identifiers: db_sequence.identifiers(),
        })
    }

    pub(super) fn uuid(&self) -> &types::Uuid {
        &self.identifiers.uuid
    }

    pub(super) fn id(&self) -> i32 {
        self.identifiers.id
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
    locator: &types::SequenceResourceLocator,
    metadata: Option<SequenceMetadata>,
    context: &Context,
) -> Result<Handle, Error> {
    let mut tx = context.db.transaction().await?;

    let mut record = db::SequenceRecord::new(locator.locator());

    if let Some(mdata) = &metadata {
        record = record.with_user_metadata(mdata.user_metadata.clone());
    }

    let record = db::sequence_create(&mut tx, &record).await?;

    if let Some(mdata) = metadata {
        metadata_write_to_store(locator, mdata, context).await?;
    }

    tx.commit().await?;

    Ok(Handle {
        locator: locator.clone(),
        identifiers: record.into(),
    })
}

/// Retrieves all sequences from the database.
///
/// Returns a list of all available sequences as [`SequenceResourceLocator`] objects.
/// This is primarily used for catalog discovery operations.
pub async fn all(db: db::Database) -> Result<Vec<types::SequenceResourceLocator>, Error> {
    let mut cx = db.connection();
    let records = db::sequence_find_all(&mut cx).await?;

    Ok(records
        .into_iter()
        .map(|record| types::SequenceResourceLocator::from(record.locator_name))
        .collect())
}

async fn metadata_write_to_store(
    locator: &types::SequenceResourceLocator,
    metadata: SequenceMetadata,
    context: &Context,
) -> Result<(), Error> {
    let path = locator.path_metadata();

    trace!("converting sequence metadata to bytes");
    let json_mdata = marshal::JsonSequenceMetadata::from(metadata);
    let bytes: Vec<u8> = json_mdata.try_into()?;

    trace!(
        "writing sequence metadata `{}` to store",
        &path.to_string_lossy()
    );
    context.store.write_bytes(&path, bytes).await?;

    Ok(())
}

/// Add a notification to the sequence
pub async fn notify(
    handle: &Handle,
    ntype: types::NotificationType,
    msg: String,
    context: &Context,
) -> Result<types::Notification, Error> {
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
    handle: &Handle,
    context: &Context,
) -> Result<Vec<types::Notification>, Error> {
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
pub async fn notification_purge(handle: &Handle, context: &Context) -> Result<(), Error> {
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

/// Read the metadata from the store and returns an `HashMap` containing all the metadata
pub async fn metadata(handle: &Handle, context: &Context) -> Result<SequenceMetadata, Error> {
    let path = handle.locator.path_metadata();
    let bytes = context.store.read_bytes(&path).await?;

    let data: marshal::JsonSequenceMetadata = bytes.try_into()?;

    Ok(data.into())
}

/// Returns the topic list associated with this sequence and returns the list of topic names
pub async fn topic_list(
    handle: &Handle,
    context: &Context,
) -> Result<Vec<types::TopicResourceLocator>, Error> {
    let mut cx = context.db.connection();

    let topics = db::sequence_find_all_topic_locators(&mut cx, &handle.locator).await?;

    Ok(topics)
}

/// Returns the session list associated with this sequence as vector of session UUIDs
pub async fn session_list(handle: &Handle, context: &Context) -> Result<Vec<types::Uuid>, Error> {
    let mut cx = context.db.connection();
    let sessions = db::sequence_find_all_sessions(&mut cx, &handle.locator).await?;
    Ok(sessions)
}

/// Deletes a sequence and all its associated sessions and topics from the system.
///
/// Sequences, sessions and topics will be removed from the store and the database.
/// The [`types::DataLossToken`] is required since this function will lead to data loss.
pub async fn delete(
    handle: Handle,
    allow_data_loss: types::DataLossToken,
    context: &Context,
) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    // Retrieve sessions data and deletes it
    let sessions = session_list(&handle, context).await?;
    for session_uuid in sessions {
        let session_handle = session::Handle::try_from_uuid(&session_uuid, context).await?;
        session::delete(session_handle, false, allow_data_loss.clone(), context).await?;
    }

    // Delete sequence data
    db::sequence_delete_by_id(&mut tx, handle.id(), types::allow_data_loss()).await?;

    // Delete all remaining data
    context
        .store
        .delete_recursive(handle.locator.locator())
        .await?;

    tx.commit().await?;

    Ok(())
}

pub async fn manifest(
    handle: &Handle,
    context: &Context,
) -> Result<types::SequenceManifest, Error> {
    let mut cx = context.db.connection();

    // Check that sequence still exists on DB.
    let db_sequence = db::sequence_find_by_id(&mut cx, handle.id()).await?;

    let mut manifest = types::SequenceManifest {
        resource_locator: handle.locator.clone(),
        created_at: db_sequence.creation_timestamp(),
        sessions: Vec::new(),
    };

    let session_uuids = session_list(handle, context).await?;

    for session_uuid in session_uuids {
        let session_handle = session::Handle::try_from_uuid(&session_uuid, context).await?;
        let session_manifest = session::manifest(&session_handle, context).await?;
        manifest.sessions.push(session_manifest);
    }

    Ok(manifest)
}

pub fn uuid(handle: &Handle, _context: &Context) -> types::Uuid {
    handle.uuid().clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types::NotificationType;
    use mosaicod_query as query;
    use mosaicod_store as store;
    use std::sync::Arc;

    use types::{MetadataBlob, Resource};

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new(store.clone(), 0).unwrap());

        Context::new(store.clone(), database.clone(), ts_gw)
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn sequence_creation(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let context = test_context(pool);

        let mdata = r#"{
            "driver" : "john",
            "weather": "sunny"
        }"#;
        dbg!(&mdata);
        let mdata = SequenceMetadata::new(marshal::JsonMetadataBlob::try_from_str(mdata).unwrap());

        let seq_locator = types::SequenceResourceLocator::from("test_sequence".to_string());

        try_create(&seq_locator, Some(mdata), &context)
            .await
            .expect("Error creating sequence");

        // Check if sequence was correctly created on DB.
        let mut cx = context.db.connection();
        let sequence = db::sequence_find_by_locator(&mut cx, &seq_locator)
            .await
            .expect("Unable to find the created sequence");

        // Check database user metadata
        let user_mdata = sequence
            .user_metadata()
            .expect("Unable to find user metadata in database record");
        assert_eq!(user_mdata["driver"].as_str().unwrap(), "john");
        assert_eq!(user_mdata["weather"].as_str().unwrap(), "sunny");

        // Check sequence locator
        assert_eq!(seq_locator.locator(), sequence.locator_name);

        Ok(())
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn sequence_notify_and_notification_purge(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let seq_locator = types::SequenceResourceLocator::from("test_sequence".to_string());

        try_create(&seq_locator, None, &context)
            .await
            .expect("Error creating sequence");

        // Check if sequence was created
        let seq_handle = Handle::try_from_locator(&seq_locator, &context)
            .await
            .expect("Unable to find the created sequence");

        notify(
            &seq_handle,
            NotificationType::Error,
            "test notification message".to_owned(),
            &context,
        )
        .await
        .expect("Error creating notification");

        notify(
            &seq_handle,
            NotificationType::Error,
            "test notification message 2".to_owned(),
            &context,
        )
        .await
        .expect("Error creating notification");

        // Check if notifications were created on database.
        let mut cx = context.db.connection();
        let notifications = db::sequence_notifications_find_by_name(&mut cx, &seq_locator)
            .await
            .unwrap();

        assert_eq!(notifications.len(), 2);

        let first_notification = notifications.first().unwrap();
        assert_eq!(
            first_notification.msg.as_ref().unwrap(),
            "test notification message"
        );
        assert!(first_notification.uuid().is_valid());
        assert_eq!(first_notification.sequence_id, seq_handle.id());

        let second_notification = notifications.last().unwrap();
        assert_eq!(
            second_notification.msg.as_ref().unwrap(),
            "test notification message 2"
        );
        assert!(second_notification.uuid().is_valid());
        assert_eq!(second_notification.sequence_id, seq_handle.id());

        notification_purge(&seq_handle, &context)
            .await
            .expect("Unable to purge notifications");

        // Check there are no more notifications on database.
        assert!(
            db::sequence_notifications_find_by_name(&mut cx, &seq_locator)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

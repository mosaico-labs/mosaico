//! This module provides the high-level API for managing a persistent **Sequence**
//! entity within the application.
//!
//! The central type is the [`SequenceHandle`], which encapsulates the name of the
//! sequence and provides transactional methods for interacting with both the
//! database respository and the object store.

use super::{Error, Session};
use log::trace;
use mosaicod_core::types::{self, Resource};
use mosaicod_db as db;
use mosaicod_marshal as marshal;
use mosaicod_store as store;

/// Define sequence metadata type contaning json user metadata
type SequenceMetadata = types::SequenceMetadata<marshal::JsonMetadataBlob>;

pub struct Sequence {
    pub locator: types::SequenceResourceLocator,
    store: store::StoreRef,
    db: db::Database,
}

impl Sequence {
    pub fn new(name: String, store: store::StoreRef, db: db::Database) -> Sequence {
        Sequence {
            locator: types::SequenceResourceLocator::from(name),
            store,
            db,
        }
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

    /// Creates a new database entry for this sequence.
    ///
    /// The newly created sequence starts in an **unlocked** state, allowing
    /// additional topics to be added later. If the sequence contains user-defined
    /// metadata, all metadata fields are also persisted in the db.
    ///
    /// If a record with the same name already exists, the operation fails and
    /// the database transaction is rolled back, restoring the previous state.
    pub async fn create(
        &self,
        metadata: Option<SequenceMetadata>,
    ) -> Result<types::Identifiers, Error> {
        let mut tx = self.db.transaction().await?;

        let mut record = db::SequenceRecord::new(self.locator.name());

        if let Some(mdata) = &metadata {
            record = record.with_user_metadata(mdata.user_metadata.clone());
        }

        let record = db::sequence_create(&mut tx, &record).await?;

        if let Some(mdata) = metadata {
            self.metadata_write_to_store(mdata).await?;
        }

        tx.commit().await?;

        Ok(record.into())
    }

    /// Read the database record for this sequence. If no record is found an error is returned.
    pub async fn resource_id(&self) -> Result<types::Identifiers, Error> {
        let mut cx = self.db.connection();

        let record = db::sequence_find_by_locator(&mut cx, &self.locator).await?;

        Ok(record.into())
    }

    /// Add a notification to the sequence
    pub async fn notify(
        &self,
        ntype: types::NotificationType,
        msg: String,
    ) -> Result<types::Notification, Error> {
        let mut tx = self.db.transaction().await?;

        let record = db::sequence_find_by_locator(&mut tx, &self.locator).await?;
        let notification =
            db::SequenceNotificationRecord::new(record.sequence_id, ntype, Some(msg));
        let notification = db::sequence_notification_create(&mut tx, &notification).await?;

        tx.commit().await?;

        Ok(notification.into_notification(self.locator.clone()))
    }

    /// Returns a list of all notifications for the sequence
    pub async fn notification_list(&self) -> Result<Vec<types::Notification>, Error> {
        let mut trans = self.db.transaction().await?;
        let notifications =
            db::sequence_notifications_find_by_name(&mut trans, &self.locator).await?;
        trans.commit().await?;
        Ok(notifications
            .into_iter()
            .map(|n| n.into_notification(self.locator.clone()))
            .collect())
    }

    /// Deletes all the notifications associated with the sequence
    pub async fn notification_purge(&self) -> Result<(), Error> {
        let mut trans = self.db.transaction().await?;

        let notifications =
            db::sequence_notifications_find_by_name(&mut trans, &self.locator).await?;
        for notification in notifications {
            // Notification id is unwrapped since is retrieved from the database and
            // it has an id
            db::sequence_notification_delete(&mut trans, notification.id().unwrap()).await?;
        }
        trans.commit().await?;
        Ok(())
    }

    /// Creates a new update session for a sequence
    pub async fn session(&self) -> Result<types::Identifiers, Error> {
        let mut tx = self.db.transaction().await?;

        let sequence = db::sequence_lookup(
            &mut tx,
            &types::ResourceLookup::Locator(self.locator.to_string()),
        )
        .await?;

        let session = db::SessionRecord::new(sequence.sequence_id);
        let session = db::session_create(&mut tx, &session).await?;

        tx.commit().await?;

        Ok(session.into())
    }

    /// Read the metadata from the store and returns an `HashMap` containing all the metadata
    pub async fn metadata(&self) -> Result<SequenceMetadata, Error> {
        let path = self.locator.path_metadata();
        let bytes = self.store.read_bytes(&path).await?;

        let data: marshal::JsonSequenceMetadata = bytes.try_into()?;

        Ok(data.into())
    }

    async fn metadata_write_to_store(&self, metadata: SequenceMetadata) -> Result<(), Error> {
        let path = self.locator.path_metadata();

        trace!("converting sequence metadata to bytes");
        let json_mdata = marshal::JsonSequenceMetadata::from(metadata);
        let bytes: Vec<u8> = json_mdata.try_into()?;

        trace!(
            "writing sequence metadata `{}` to store",
            &path.to_string_lossy()
        );
        self.store.write_bytes(&path, bytes).await?;

        Ok(())
    }

    /// Returns the topic list associated with this sequence and returns the list of topic names
    pub async fn topic_list(&self) -> Result<Vec<types::TopicResourceLocator>, Error> {
        let mut cx = self.db.connection();

        let topics = db::sequence_find_all_topic_locators(&mut cx, &self.locator).await?;

        Ok(topics)
    }

    /// Returns the session list associated with this sequence as vector of session UUIDs
    pub async fn session_list(&self) -> Result<Vec<types::Uuid>, Error> {
        let mut cx = self.db.connection();

        let sessions = db::sequence_find_all_sessions(&mut cx, &self.locator).await?;

        Ok(sessions)
    }

    /// Deletes a sequence and all its associated sessions and topics from the system.
    ///
    /// Sequences, sessions and topics will be removed from the store and the database.
    /// The [`types::DataLossToken`] is required since this function will lead to data loss.
    pub async fn delete(self, allow_data_loss: types::DataLossToken) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        // Retrieve sessions data and deletes it
        let sessions = self.session_list().await?;
        for session_uuid in sessions {
            let session = Session::new(session_uuid, self.store.clone(), self.db.clone());
            session.delete(allow_data_loss.clone()).await?;
        }

        // Delete sequence data
        db::sequence_delete(&mut tx, &self.locator, types::allow_data_loss()).await?;

        // Delete all remaining data
        self.store.delete_recursive(self.locator.name()).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Computes system info for the sequence
    pub async fn system_info(&self) -> Result<types::SequenceSystemInfo, Error> {
        let mut cx = self.db.connection();
        let record = db::sequence_find_by_locator(&mut cx, &self.locator).await?;

        // Compute the sum of the size of all files in the sequence
        let files = self.store.list(&self.locator.name(), None).await?;
        let mut total_size = 0;
        for file in files {
            total_size += self.store.size(file).await?;
        }

        Ok(types::SequenceSystemInfo {
            total_size_bytes: total_size,
            created_datetime: record.creation_timestamp().into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types::NotificationType;

    use types::{MetadataBlob, Resource};

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn sequence_creation(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let fsequence = Sequence::new(
            "test_sequence".to_string(),
            (*store).clone(),
            (*database).clone(),
        );

        let mdata = r#"{
            "driver" : "john",
            "weather": "sunny"
        }"#;
        dbg!(&mdata);
        let mdata = SequenceMetadata::new(marshal::JsonMetadataBlob::try_from_str(mdata).unwrap());

        fsequence
            .create(Some(mdata)) // <-- testing this
            .await
            .expect("Error creating sequence");

        let _ = fsequence.session().await.unwrap();

        // Check if sequence was created
        let mut cx = database.connection();
        let sequence = db::sequence_find_by_locator(&mut cx, &fsequence.locator)
            .await
            .expect("Unable to find the created sequence");

        // Check database user metadata
        let user_mdata = sequence
            .user_metadata()
            .expect("Unable to find user metadata in database record");
        assert_eq!(user_mdata["driver"].as_str().unwrap(), "john");
        assert_eq!(user_mdata["weather"].as_str().unwrap(), "sunny");

        // Check sequence locator
        assert_eq!(
            fsequence.locator.path().to_string_lossy(),
            sequence.locator_name
        );

        Ok(())
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn sequence_notify_and_notification_purge(pool: sqlx::Pool<db::DatabaseType>) {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let fsequence = Sequence::new(
            "test_sequence".to_string(),
            (*store).clone(),
            (*database).clone(),
        );

        fsequence
            .create(None)
            .await
            .expect("Error creating sequence");

        // Check if sequence was created
        let mut cx = database.connection();
        let sequence = db::sequence_find_by_locator(&mut cx, &fsequence.locator)
            .await
            .expect("Unable to find the created sequence");

        fsequence
            .notify(
                NotificationType::Error,
                "test notification message".to_owned(),
            )
            .await
            .expect("Error creating notification");

        fsequence
            .notify(
                NotificationType::Error,
                "test notification message 2".to_owned(),
            )
            .await
            .expect("Error creating notification");

        // Check if notifications were created on database.
        let notifications = db::sequence_notifications_find_by_name(&mut cx, &fsequence.locator)
            .await
            .unwrap();

        assert_eq!(notifications.len(), 2);

        let first_notification = notifications.first().unwrap();
        assert_eq!(
            first_notification.msg.as_ref().unwrap(),
            "test notification message"
        );
        assert!(first_notification.uuid().is_valid());
        assert_eq!(first_notification.sequence_id, sequence.sequence_id);

        let second_notification = notifications.last().unwrap();
        assert_eq!(
            second_notification.msg.as_ref().unwrap(),
            "test notification message 2"
        );
        assert!(second_notification.uuid().is_valid());
        assert_eq!(second_notification.sequence_id, sequence.sequence_id);

        fsequence
            .notification_purge()
            .await
            .expect("Unable to purge notifications");

        // Check there are no more notifications on database.
        assert!(
            db::sequence_notifications_find_by_name(&mut cx, &fsequence.locator)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

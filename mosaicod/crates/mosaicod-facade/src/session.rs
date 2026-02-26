//! A Session represents a new update session for adding topics to a target
//! sequence. It serves as a container for these new topic uploads,
//! ensuring that topics from previous sessions within the same sequence are not modified.
//! This provides a mechanism for versioning or snapshotting data.
//!
//! Multiple sessions can occur in parallel for the same sequence. Once a session is
//! finalized, all data associated with it becomes immutable.

use crate::{Error, Sequence, Topic};
use log::trace;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_marshal as marshal;
use mosaicod_store as store;

/// A high-level facade for managing a session.
///
/// This struct provides a transactional API for creating and finalizing sessions,
/// coordinating operations between the metadata database and the object store.
pub struct Session {
    pub uuid: types::Uuid,

    /// A reference to the underlying object store.
    store: store::StoreRef,

    /// A reference to the metadata database.
    db: db::Database,
}

impl Session {
    /// Creates a new upload session for a given sequence.
    pub fn new(session_uuid: types::Uuid, store: store::StoreRef, db: db::Database) -> Self {
        Self {
            uuid: session_uuid,
            store,
            db,
        }
    }

    /// Finalizes the session, making it and all its associated data immutable.
    ///
    /// Once a session is finalized, no more topics can be added to it.
    pub async fn finalize(&self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        let session = db::session_find_by_uuid(&mut tx, &self.uuid).await?;

        // Collect all topics associated with this session
        let topics = db::session_find_all_topic_locators(&mut tx, &self.uuid).await?;

        let completion_timestamp = types::Timestamp::now();
        db::session_lock(&mut tx, &self.uuid, &completion_timestamp).await?;

        let manifest = types::SessionManifest {
            uuid: session.uuid(),
            topics,
            creation_timestamp: session.creation_timestamp(),
            completion_timestamp,
        };

        // Get sequence data in order to store the manifest file inside the sequence namespace/directory
        let sequence = db::sequence_find_by_id(&mut tx, session.sequence_id).await?;

        self.manifest_write_to_store(&sequence.resource_locator(), manifest)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Deletes all the topics associated with this session, deletes also the session manifest and
    /// the session record from the db.
    ///
    /// Since the session delets involves multiple deletes across the system, topics data and
    /// session manifest, if operation fails a notification will be created. The notification will
    /// enable the user to manually delete dangling resources if required.
    ///
    /// # Errors
    ///
    /// * [`Error::FailedAndNotified`]: if the error is correctly reported and notified.
    /// * [`Error::FailedAndUnableToNotify`]: if the notification creation faild.
    pub async fn delete(&self, allow_data_loss: types::DataLossToken) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        let error_report_msg = format!("Some error occured while deleting session `{}`", self.uuid);
        let mut error_report = types::ErrorReport::new(error_report_msg);

        // Deletes topic data
        let topics = self.topic_list().await?;
        for topic_loc in topics.clone() {
            let topic = Topic::new(
                topic_loc.clone().into(),
                self.store.clone(),
                self.db.clone(),
            );

            // We collect all the errors to build a sequence notification reporting all error if
            // something fails.
            if let Err(e) = topic.delete(allow_data_loss.clone()).await {
                error_report
                    .errors
                    .push(types::ErrorReportItem::new(topic_loc, e));
            }
        }

        let session = db::session_find_by_uuid(&mut tx, &self.uuid).await?;
        let sequence = db::sequence_find_by_id(&mut tx, session.sequence_id).await?;

        // Deletes the session manifest if session was previously locked (a unlocked
        // sessions has no manifest)
        if session.is_locked()
            && let Err(e) = self
                .store
                .delete(
                    sequence
                        .resource_locator()
                        .session_manifest(&session.uuid()),
                )
                .await
        {
            error_report.errors.push(types::ErrorReportItem::new(
                sequence.locator_name.clone(),
                e,
            ));
        }

        let error_occurs = error_report.has_errors();
        let mut notification = None;
        let mut msg = "".to_owned();

        // If some error occurs create a notification with all errors stacked otherwise
        // if no error occurs delete the session record
        if error_occurs {
            msg = error_report.into();
            let fsequence = Sequence::new(
                sequence.locator_name, //
                self.store.clone(),
                self.db.clone(),
            );
            notification = Some(
                fsequence
                    .notify(types::NotificationType::Error, msg.clone())
                    .await?,
            );
        } else {
            // This is done as last operation, otherwise multiple calls to this function will fail
            // since a session lookup is made above
            db::session_delete(&mut tx, &session.uuid(), allow_data_loss).await?;
        }

        tx.commit().await?;

        if error_occurs {
            return if let Some(notification) = notification {
                Err(Error::failed_and_notified(notification.uuid))
            } else {
                Err(Error::failed_and_unable_to_notify(msg))
            };
        }

        Ok(())
    }

    /// Returns the topic list associated with this session.
    pub async fn topic_list(&self) -> Result<Vec<types::TopicResourceLocator>, Error> {
        let mut cx = self.db.connection();

        let topics = db::session_find_all_topic_locators(&mut cx, &self.uuid).await?;

        Ok(topics)
    }

    async fn manifest_write_to_store(
        &self,
        locator: &types::SequenceResourceLocator,
        manifest: types::SessionManifest,
    ) -> Result<(), Error> {
        let path = locator.session_manifest(&manifest.uuid);

        trace!("converting session manifest to bytes");
        let json_manifest = marshal::SessionManifest::from(manifest);
        let bytes: Vec<u8> = json_manifest.try_into()?;

        trace!(
            "writing session manifest `{}` to store",
            &path.to_string_lossy()
        );
        self.store.write_bytes(&path, bytes).await?;

        Ok(())
    }
}

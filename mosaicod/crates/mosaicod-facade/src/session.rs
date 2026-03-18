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
    pub sequence_locator: types::SequenceResourceLocator,

    pub uuid: types::Uuid,

    /// A reference to the underlying object store.
    store: store::StoreRef,

    /// A reference to the metadata database.
    db: db::Database,
}

impl Session {
    /// Tries to retrieve a facade for the session with the given [`session_uuid`].
    /// Returns an error if something goes wrong.
    pub async fn try_new(
        session_uuid: types::Uuid,
        store: store::StoreRef,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut tx = db.transaction().await?;

        let db_session = db::session_find_by_uuid(&mut tx, &session_uuid).await?;
        let db_sequence = db::sequence_find_by_id(&mut tx, db_session.sequence_id).await?;

        tx.commit().await?;

        Ok(Self {
            sequence_locator: db_sequence.resource_locator(),
            uuid: session_uuid,
            store,
            db,
        })
    }

    /// Creates the session manifest and saves it on store
    /// TODO: find a better solution to create the manifest (without calling a method externally)
    pub async fn create_manifest(&self) -> Result<(), Error> {
        let mut cx = self.db.connection();

        let db_session = db::session_find_by_uuid(&mut cx, &self.uuid).await?;

        let manifest =
            types::SessionManifest::new(self.uuid.clone(), db_session.creation_timestamp());
        self.manifest_write_to_store(manifest).await?;

        Ok(())
    }

    /// Finalizes the session, making it and all its associated data immutable.
    ///
    /// Once a session is finalized, no more topics can be added to it.
    pub async fn finalize(&self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        // Collect all topics associated with this session
        let topics = db::session_find_all_topic_locators(&mut tx, &self.uuid).await?;

        // If the session does not contain any topic, return an error and leave the session unlocked.
        if topics.is_empty() {
            return Err(Error::SessionEmpty);
        }

        // If not all topics are locked, return an error and leave the session unlocked.
        let all_topics_locked = futures::future::join_all(topics.iter().map(async |topic_loc| {
            let topic =
                Topic::try_from_locator(topic_loc.clone(), self.store.clone(), self.db.clone())
                    .await?;

            topic.manifest().await
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .all(|v| v.properties.locked);

        if !all_topics_locked {
            return Err(Error::TopicUnlocked);
        }

        // Update manifest (store).
        let mut manifest = self.manifest().await?;
        manifest.locked = true;
        manifest.completed_at = Some(types::Timestamp::now());
        manifest.topics = topics;
        self.manifest_write_to_store(manifest).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Deletes all the topics associated with this session, deletes also the session manifest and
    /// the session record from the db.
    ///
    /// Since the session delete involves multiple deletes across the system, topics data and
    /// session manifest, if operation fails a notification will be created. The notification will
    /// enable the user to manually delete dangling resources if required.
    ///
    /// # Errors
    ///
    /// * [`Error::FailedAndNotified`]: if the error is correctly reported and notified.
    /// * [`Error::FailedAndUnableToNotify`]: if the notification creation failed.
    pub async fn delete(
        &self,
        only_if_unlocked: bool,
        allow_data_loss: types::DataLossToken,
    ) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        let error_report_msg =
            format!("Some error occurred while deleting session `{}`", self.uuid);
        let mut error_report = types::ErrorReport::new(error_report_msg);

        let session_locked = self.manifest().await?.locked;

        if only_if_unlocked && session_locked {
            return Err(Error::SessionLocked);
        }

        // Deletes topic data
        let topics = self.topic_list().await?;
        for topic_loc in topics.clone() {
            let topic =
                Topic::try_from_locator(topic_loc.clone(), self.store.clone(), self.db.clone())
                    .await?;

            // We collect all the errors to build a sequence notification reporting all error if
            // something fails.
            if let Err(e) = topic.delete(allow_data_loss.clone()).await {
                error_report
                    .errors
                    .push(types::ErrorReportItem::new(topic_loc, e));
            }
        }

        // Deletes the session manifest if session was previously locked (unlocked
        // sessions have no manifest)
        if session_locked
            && let Err(e) = self
                .store
                .delete(self.sequence_locator.session_manifest(&self.uuid))
                .await
        {
            error_report.errors.push(types::ErrorReportItem::new(
                self.sequence_locator.clone(),
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
                self.sequence_locator.clone().into(),
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
            db::session_delete(&mut tx, &self.uuid, allow_data_loss).await?;
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

    async fn manifest_write_to_store(&self, manifest: types::SessionManifest) -> Result<(), Error> {
        let path = self.sequence_locator.session_manifest(&manifest.uuid);

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

    pub async fn manifest(&self) -> Result<types::SessionManifest, Error> {
        let path = self.sequence_locator.session_manifest(&self.uuid);

        if !self.store.exists(&path).await? {
            return Err(Error::NotFound(format!(
                "missing manifest file for session `{}`",
                self.uuid
            )));
        }

        let bytes = self.store.read_bytes(path).await?;

        let data: marshal::SessionManifest = bytes.try_into()?;

        Ok(data.try_into()?)
    }
}

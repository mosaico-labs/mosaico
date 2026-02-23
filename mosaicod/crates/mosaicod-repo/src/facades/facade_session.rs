//! A Session represents a new update session for adding topics to a target
//! sequence. It serves as a container for these new topic uploads,
//! ensuring that topics from previous sessions within the same sequence are not modified.
//! This provides a mechanism for versioning or snapshotting data.
//!
//! Multiple sessions can occur in parallel for the same sequence. Once a session is
//! finalized, all data associated with it becomes immutable.

use crate::{self as repo, FacadeError, FacadeSequence, FacadeTopic};
use log::trace;
use mosaicod_core::types;
use mosaicod_marshal as marshal;
use mosaicod_store as store;

/// A high-level facade for managing a session.
///
/// This struct provides a transactional API for creating and finalizing sessions,
/// coordinating operations between the metadata repository and the object store.
pub struct FacadeSession {
    pub uuid: types::Uuid,

    /// A reference to the underlying object store.
    store: store::StoreRef,

    /// A reference to the metadata repository.
    repo: repo::Repository,
}

impl FacadeSession {
    /// Creates a new upload session for a given sequence.
    pub fn new(session_uuid: types::Uuid, store: store::StoreRef, repo: repo::Repository) -> Self {
        Self {
            uuid: session_uuid,
            store: store,
            repo: repo,
        }
    }

    /// Finalizes the session, making it and all its associated data immutable.
    ///
    /// Once a session is finalized, no more topics can be added to it.
    pub async fn finalize(&self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let session = repo::session_find_by_uuid(&mut tx, &self.uuid).await?;

        // Collect all topics associated with this session
        let topics = repo::session_find_all_topic_names(&mut tx, &self.uuid).await?;

        let completion_timestamp = types::Timestamp::now();
        repo::session_lock(&mut tx, &self.uuid, &completion_timestamp).await?;

        let manifest = types::SessionManifest {
            uuid: session.uuid(),
            topics,
            creation_timestamp: session.creation_timestamp(),
            completion_timestamp: completion_timestamp,
        };

        // Get sequence data in order to store the manifest file inside the sequence namespace/directory
        let sequence = repo::sequence_find_by_id(&mut tx, session.sequence_id).await?;

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
    /// * [`FacadeError::FailedAndNotified`]: if the error is correctly reported and notified.
    /// * [`FacadeError::FailedAndUnableToNotify`]: if the notification creation faild.
    pub async fn delete(&self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let session = repo::session_find_by_uuid(&mut tx, &self.uuid).await?;

        let error_report_msg = format!("Some error occured while deleting session `{}`", self.uuid);
        let mut error_report = types::ErrorReport::new(error_report_msg);

        // Deletes topic data
        let topics = self.topic_list().await?;
        for topic_loc in topics.clone() {
            let thandle = FacadeTopic::new(
                topic_loc.clone().into(),
                self.store.clone(),
                self.repo.clone(),
            );

            // For this special case we allow a data loss delete since the sequence is still unlocked (previous check).
            // This is because the system may be in a state where topics are partially uploaded:
            // some topics are fully uploaded and locked, while others are not.
            //
            // We collect all the errors to build a sequence notification reporting all error if
            // something fails.
            if let Err(e) = thandle.delete(types::allow_data_loss()).await {
                error_report
                    .errors
                    .push(types::ErrorReportItem::new(topic_loc, e));
            }
        }

        let sequence = repo::sequence_find_by_id(&mut tx, session.sequence_id).await?;

        // Deletes the session manifest if session was previously locked (an unlocked
        // sessions has no manifest)
        if session.is_locked() {
            if let Err(e) = self
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
        }

        let error_occurs = error_report.has_errors();
        let mut notify = None;
        let mut msg = "".to_owned();

        // If some error occurs create a notification with all errors stacked
        if error_occurs {
            msg = error_report.into();
            let fsequence = FacadeSequence::new(
                sequence.locator_name, //
                self.store.clone(),
                self.repo.clone(),
            );
            notify = Some(
                fsequence
                    .notify(types::NotifyType::Error, msg.clone())
                    .await?,
            );
        }

        tx.commit().await?;

        if error_occurs {
            if let Some(notify) = notify {
                return Err(FacadeError::failed_and_notified(notify.id));
            } else {
                return Err(FacadeError::failed_and_unable_to_notify(msg));
            }
        }

        Ok(())
    }

    /// Returns the topic list associated with this session.
    pub async fn topic_list(&self) -> Result<Vec<types::TopicResourceLocator>, FacadeError> {
        let mut cx = self.repo.connection();

        let topics = repo::session_find_all_topic_names(&mut cx, &self.uuid).await?;

        Ok(topics)
    }

    async fn manifest_write_to_store(
        &self,
        locator: &types::SequenceResourceLocator,
        manifest: types::SessionManifest,
    ) -> Result<(), FacadeError> {
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

//! This module provides the high-level API for managing a persistent **Sequence**
//! entity within the application.
//!
//! The central type is the [`SequenceHandle`], which encapsulates the name of the
//! sequence and provides transactional methods for interacting with both the
//! database respository and the object store.

use log::trace;

use crate::{
    marshal, repo, store,
    types::{self, Resource},
};

use super::{FacadeError, FacadeTopic};

/// Define sequence metadata type contaning json user metadata
type SequenceMetadata = types::SequenceMetadata<marshal::JsonMetadataBlob>;

pub struct FacadeSequence {
    pub locator: types::SequenceResourceLocator,
    store: store::StoreRef,
    repo: repo::Repository,
}

impl FacadeSequence {
    pub fn new(name: String, store: store::StoreRef, repo: repo::Repository) -> FacadeSequence {
        FacadeSequence {
            locator: types::SequenceResourceLocator::from(name),
            store,
            repo,
        }
    }

    /// Retrieves all sequences from the repository.
    ///
    /// Returns a list of all available sequences as [`SequenceResourceLocator`] objects.
    /// This is primarily used for catalog discovery operations.
    pub async fn all(
        repo: repo::Repository,
    ) -> Result<Vec<types::SequenceResourceLocator>, FacadeError> {
        let mut cx = repo.connection();
        let records = repo::sequence_find_all(&mut cx).await?;

        Ok(records
            .into_iter()
            .map(|record| types::SequenceResourceLocator::from(record.locator_name))
            .collect())
    }

    /// Creates a new repository entry for this sequence.
    ///
    /// The newly created sequence starts in an **unlocked** state, allowing
    /// additional topics to be added later. If the sequence contains user-defined
    /// metadata, all metadata fields are also persisted in the repo.
    ///
    /// If a record with the same name already exists, the operation fails and
    /// the repo transaction is rolled back, restoring the previous state.
    pub async fn create(
        &self,
        metadata: Option<SequenceMetadata>,
    ) -> Result<types::ResourceId, FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let mut record = repo::SequenceRecord::new(self.locator.name());

        if let Some(mdata) = &metadata {
            record = record.with_user_metadata(mdata.user_metadata.clone());
        }

        let record = repo::sequence_create(&mut tx, &record).await?;

        if let Some(mdata) = metadata {
            self.metadata_write_to_store(mdata).await?;
        }

        tx.commit().await?;

        Ok(record.into())
    }

    /// Read the repository record for this sequence. If no record is found an error is returned.
    pub async fn resource_id(&self) -> Result<types::ResourceId, FacadeError> {
        let mut cx = self.repo.connection();

        let record = repo::sequence_find_by_locator(&mut cx, &self.locator).await?;

        Ok(record.into())
    }

    pub async fn is_locked(&self) -> Result<bool, FacadeError> {
        let mut cx = self.repo.connection();

        let record = repo::sequence_find_by_locator(&mut cx, &self.locator).await?;

        Ok(record.is_locked())
    }

    /// Permanently locks the sequence, preventing any new topics from being added.
    ///
    /// Once a sequence is locked, it becomes immutable — no further topics can be
    /// appended or modified under it. This action is **irreversible**, as there is
    /// currently no API or mechanism to unlock a sequence.
    ///
    /// A sequence can be locked only if all the associated topics are locked, calling this
    /// function on a sequence with an unlocked topic returns an error.
    ///
    /// Calling lock on a locked sequence returns a [`HandleError::SequenceLocked`] error.
    pub async fn lock(&self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        // check that the sequence is currently unlocked
        let record = repo::sequence_find_by_locator(&mut tx, &self.locator).await?;
        if record.is_locked() {
            return Err(FacadeError::SequenceLocked);
        }

        // check if all associated topics are locked
        let topics = repo::sequence_find_all_topic_names(&mut tx, &self.locator).await?;
        for topic_loc in topics {
            let trecord = repo::topic_find_by_locator(&mut tx, &topic_loc).await?;
            if !trecord.is_locked() {
                return Err(FacadeError::TopicUnlocked);
            }
        }

        repo::sequence_lock(&mut tx, &self.locator).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Add a notification to the sequence
    pub async fn notify(
        &self,
        ntype: types::NotifyType,
        msg: String,
    ) -> Result<types::Notify, FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let record = repo::sequence_find_by_locator(&mut tx, &self.locator).await?;
        let notify = repo::SequenceNotify::new(record.sequence_id, ntype, Some(msg));
        let notify = repo::sequence_notify_create(&mut tx, &notify).await?;

        tx.commit().await?;

        Ok(notify.into_types(self.locator.clone()))
    }

    /// Returns a list of all notifications for the this sequence
    pub async fn notify_list(&self) -> Result<Vec<types::Notify>, FacadeError> {
        let mut trans = self.repo.transaction().await?;
        let notifies = repo::sequence_notifies_find_by_name(&mut trans, &self.locator).await?;
        trans.commit().await?;
        Ok(notifies
            .into_iter()
            .map(|n| n.into_types(self.locator.clone()))
            .collect())
    }

    /// Deletes all the notifications associated with the sequence
    pub async fn notify_purge(&self) -> Result<(), FacadeError> {
        let mut trans = self.repo.transaction().await?;

        let notifies = repo::sequence_notifies_find_by_name(&mut trans, &self.locator).await?;
        for notify in notifies {
            // Notify id is unwrapped since is retrieved from the database and
            // it has an id
            repo::sequence_notify_delete(&mut trans, notify.id().unwrap()).await?;
        }
        trans.commit().await?;
        Ok(())
    }

    /// Read the metadata from the store and returns an `HashMap` containing all the metadata
    pub async fn metadata(&self) -> Result<SequenceMetadata, FacadeError> {
        let path = self.locator.path_metadata();
        let bytes = self.store.read_bytes(&path).await?;

        let data: marshal::JsonSequenceMetadata = bytes.try_into()?;

        Ok(data.into())
    }

    async fn metadata_write_to_store(&self, metadata: SequenceMetadata) -> Result<(), FacadeError> {
        let path = self.locator.path_metadata();

        trace!("converting metadata to bytes");
        let json_mdata = marshal::JsonSequenceMetadata::from(metadata);
        let bytes: Vec<u8> = json_mdata.try_into()?;

        trace!("writing metadata to store");
        self.store.write_bytes(&path, bytes).await?;

        trace!("wiring metadata to database");
        // ...

        Ok(())
    }

    /// Returns the topic list associated with this sequence and returns the list of topic names
    pub async fn topic_list(&self) -> Result<Vec<types::TopicResourceLocator>, FacadeError> {
        let mut cx = self.repo.connection();

        let topics = repo::sequence_find_all_topic_names(&mut cx, &self.locator).await?;

        Ok(topics)
    }

    /// Deletes a sequence and all its associated topics from the system.
    ///
    /// Both the sequence and its topics will be removed from the store and the repository.
    ///
    /// This operation will only succeed if the sequence is locked.  
    /// If the sequence is not locked, the function returns a [`HandleError::SequenceLocked`] error.
    pub async fn delete(self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let srecord = repo::sequence_find_by_locator(&mut tx, &self.locator).await?;
        if srecord.is_locked() {
            return Err(FacadeError::SequenceLocked);
        }

        // Retrieve topics data and deletes it
        let topics = self.topic_list().await?;
        for topic_loc in topics {
            let thandle = FacadeTopic::new(topic_loc.into(), self.store.clone(), self.repo.clone());

            // For this special case we allow a data loss delete since the sequence is still unlocked (previous check).
            // This is because the system may be in a state where topics are partially uploaded:
            // some topics are fully uploaded and locked, while others are not.
            thandle.delete(types::allow_data_loss()).await?;
        }

        // Delete sequence data
        repo::sequence_delete_unlocked(&mut tx, &self.locator).await?;
        self.store.delete_recursive(self.locator.name()).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Computes system info for the sequence
    pub async fn system_info(&self) -> Result<types::SequenceSystemInfo, FacadeError> {
        let mut cx = self.repo.connection();
        let record = repo::sequence_find_by_locator(&mut cx, &self.locator).await?;

        // Compute the sum of the size of all files in the sequence
        let files = self.store.list(&self.locator.name(), None).await?;
        let mut total_size = 0;
        for file in files {
            total_size += self.store.size(file).await?;
        }

        Ok(types::SequenceSystemInfo {
            total_size_bytes: total_size,
            is_locked: record.is_locked(),
            created_datetime: record.creation_timestamp().into(),
        })
    }
}

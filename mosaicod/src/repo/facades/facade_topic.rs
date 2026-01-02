use super::FacadeError;
use crate::rw;
use crate::traits::AsExtension;
use crate::{
    marshal, repo, store,
    types::{self, Resource},
};
use arrow::datatypes::SchemaRef;
use log::trace;

/// Define topic metadata type contaning JSON user metadata
type TopicMetadata = types::TopicMetadata<marshal::JsonMetadataBlob>;

pub struct FacadeTopic {
    pub locator: types::TopicResourceLocator,
    store: store::StoreRef,
    repo: repo::Repository,
}

impl FacadeTopic {
    pub fn new(name: String, store: store::StoreRef, repo: repo::Repository) -> Self {
        Self {
            locator: types::TopicResourceLocator::from(name),
            store,
            repo,
        }
    }

    // Returns the path were the topic is located
    pub fn path(&self) -> &str {
        self.locator.name().as_str()
    }

    /// Creates a new repository entry for this topic.
    ///
    /// If a record with the same name already exists, the operation fails and
    /// the repository transaction is rolled back, restoring the previous state.
    pub async fn create(
        &self,
        sequence: &uuid::Uuid,
        metadata: Option<TopicMetadata>,
    ) -> Result<types::ResourceId, FacadeError> {
        let mut tx = self.repo.transaction().await?;

        // Ensure that a sequence with th provided id is available and is unlocked
        let srecord = repo::sequence_find_by_uuid(&mut tx, sequence).await?;
        if srecord.is_locked() {
            return Err(FacadeError::SequenceLocked);
        }

        let sloc = types::SequenceResourceLocator::from(&srecord.locator_name);

        // Ensure that this topic is child of the provided sequence, i.e. they are related with the same
        // name structure
        if !self.locator.is_sub_resource(&sloc) {
            return Err(FacadeError::Unauthorized);
        }

        let mut record = repo::TopicRecord::new(self.locator.name(), srecord.sequence_id);

        if let Some(metadata) = &metadata {
            record = record
                .with_user_metadata(metadata.user_metadata.clone())
                .with_ontology_tag(&metadata.properties.ontology_tag)
                .with_serialization_format(&metadata.properties.serialization_format.to_string());
        }

        let record = repo::topic_create(&mut tx, &record).await?;

        // This operation is done at the end to avoid deleting or reverting changes
        // to metadata file on store if some error causes a rollback on the repository
        if let Some(metadata) = metadata {
            self.metadata_write_to_store(metadata).await?;
        }

        tx.commit().await?;

        Ok(record.into())
    }

    pub async fn is_locked(&self) -> Result<bool, FacadeError> {
        let mut cx = self.repo.connection();

        let record = repo::topic_find_by_locator(&mut cx, &self.locator).await?;

        Ok(record.is_locked())
    }

    /// Updates the repository entry for this topic.
    ///
    /// If a record with the same name already exists, the operation fails and
    /// the repository transaction is rolled back, restoring the previous state.
    pub async fn update(&self, metadata: TopicMetadata) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        // find topic record to check that upload is not completed and is still prossible
        // to change data
        let record = repo::topic_find_by_locator(&mut tx, &self.locator).await?;
        if record.is_locked() {
            return Err(FacadeError::TopicLocked);
        }

        // check if parent sequenc is locked
        let sequence = repo::sequence_find_by_id(&mut tx, record.sequence_id).await?;
        if sequence.is_locked() {
            return Err(FacadeError::SequenceLocked);
        }

        repo::topic_update_user_metadata(
            &mut tx, //
            &self.locator,
            metadata.user_metadata.clone(),
        )
        .await?;
        repo::topic_update_ontology_tag(
            &mut tx, //
            &self.locator,
            &metadata.properties.ontology_tag,
        )
        .await?;
        // Save the last record for returning it
        let _ = repo::topic_update_serialization_format(
            &mut tx,
            &self.locator,
            &metadata.properties.serialization_format.to_string(),
        )
        .await?;

        self.metadata_write_to_store(metadata).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Read the repository record for this sequence. If no record is found an error is returned.
    pub async fn resource_id(&self) -> Result<types::ResourceId, FacadeError> {
        let mut cx = self.repo.connection();

        trace!("searching for `{}`", self.locator);
        let record = repo::topic_find_by_locator(&mut cx, &self.locator).await?;

        Ok(record.into())
    }

    pub async fn lock(&self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        trace!("locking `{}`", self.locator);
        repo::topic_lock(&mut tx, &self.locator).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Reads and deserializes the [`TopicMetadata`] associated with this topic.
    ///
    /// # Errors
    ///
    /// Returns [`HandleError::ReadError`] if reading or deserializing fails.
    pub async fn metadata(&self) -> Result<TopicMetadata, FacadeError> {
        let path = self.locator.metadata();
        let bytes = self.store.read_bytes(path).await?;

        let data: marshal::JsonTopicMetadata = bytes.try_into()?;

        Ok(data.into())
    }

    /// Returns the topic arrow schema.
    /// The serialization format is required to extract the schema, can be retrieved using [`TopicHandle::metadata`] function.
    pub async fn arrow_schema(&self, format: rw::Format) -> Result<SchemaRef, FacadeError> {
        // Get chunk 0 since this chunk needs to exist always
        let path = self.locator.datafile(0, &format);

        // Build a chunk reader reading in memory a file
        // (cabba) TODO: avoid reading the whole file, get from store only the header
        let buffer = self.store.read_bytes(path).await?;
        let reader = rw::ChunkReader::new(format, bytes::Bytes::from_owner(buffer))?;
        Ok(reader.schema())
    }

    /// Serializes and writes [`TopicMetadata`] to the object store.
    ///
    /// # Errors
    ///
    /// Returns [`HandleError::NotFound`] or [`HandleError::WriteError`] if serialization or writing fails.
    async fn metadata_write_to_store(&self, metadata: TopicMetadata) -> Result<(), FacadeError> {
        trace!("writing metadata to store to `{}`", self.locator);
        let path = self.locator.metadata();

        let json_mdata = marshal::JsonTopicMetadata::from(metadata);
        let bytes: Vec<u8> = json_mdata.try_into()?;

        self.store.write_bytes(&path, bytes).await?;

        Ok(())
    }

    pub fn writer(&self, format: rw::Format) -> rw::ChunkedWriter<'_, store::Store> {
        rw::ChunkedWriter::new(
            self.store.as_ref(),
            self.path(),
            format,
            |path, format, idx| types::TopicResourceLocator::from(path).datafile(idx, format),
        )
    }

    pub async fn delete(self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        // unsafe allowed since this function is unsafe itself
        repo::topic_delete_unlocked(&mut tx, &self.locator).await?;

        // Delete files
        self.store.delete_recursive(&self.path()).await?;

        tx.commit().await?;

        Ok(())
    }

    /// # Safety
    ///
    /// This function permanently deletes a topic and all its data, be caution
    pub async unsafe fn delete_unsafe(self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        // unsafe allowed since this function is unsafe itself
        unsafe {
            repo::topic_delete(&mut tx, &self.locator).await?;
        }

        // Delete files
        self.store.delete_recursive(&self.path()).await?;

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

        let record = repo::topic_find_by_locator(&mut tx, &self.locator).await?;
        let notify = repo::TopicNotify::new(record.topic_id, ntype, Some(msg));
        let notify = repo::topic_notify_create(&mut tx, &notify).await?;

        tx.commit().await?;

        Ok(notify.into_types(self.locator.clone()))
    }

    /// Returns a list of all notifications for the this topic
    pub async fn notify_list(&self) -> Result<Vec<types::Notify>, FacadeError> {
        let mut cx = self.repo.connection();
        let notifies = repo::topic_notifies_find_by_locator(&mut cx, &self.locator).await?;
        Ok(notifies
            .into_iter()
            .map(|e| e.into_types(self.locator.clone()))
            .collect())
    }

    /// Deletes all the notifications associated with the sequence
    pub async fn notify_purge(&self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let notifies = repo::topic_notifies_find_by_locator(&mut tx, &self.locator).await?;
        for notify in notifies {
            // Notify id is unwrapped since is retrieved from the database and
            // it has an id
            repo::topic_notify_delete(&mut tx, notify.id().unwrap()).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Returns the statistics about topic's chunks
    pub async fn chunks_stats(&self) -> Result<types::TopicChunksStats, FacadeError> {
        let mut cx = self.repo.connection();
        let stats = repo::topic_get_stats(&mut cx, &self.locator).await?;
        Ok(stats)
    }

    /// Computes system info for the topic
    pub async fn system_info(&self) -> Result<types::TopicSystemInfo, FacadeError> {
        let mut cx = self.repo.connection();
        let record = repo::topic_find_by_locator(&mut cx, &self.locator).await?;

        let format = record
            .serialization_format()
            .ok_or_else(|| FacadeError::MissingMetadataField("serialization_format".to_owned()))?;

        let datafiles = self
            .store
            .list(&self.locator.name(), Some(&format.as_extension()))
            .await?;

        let mut total_size = 0;
        for file in &datafiles {
            total_size += self.store.size(file).await?;
        }

        Ok(types::TopicSystemInfo {
            chunks_number: datafiles.len(),
            is_locked: record.is_locked(),
            total_size_bytes: total_size,
            created_datetime: record.creation_timestamp().into(),
        })
    }
}

// Batch Reader needs to implement Stream trait

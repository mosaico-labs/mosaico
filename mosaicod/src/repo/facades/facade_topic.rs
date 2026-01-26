use super::FacadeError;
use crate::rw;
use crate::traits::AsExtension;
use crate::{
    marshal, params, query, repo, store,
    types::{self, Resource},
};
use arrow::datatypes::SchemaRef;
use log::trace;
use std::sync::Arc;

/// Define topic metadata type contaning JSON user metadata
type TopicMetadata = types::TopicMetadata<marshal::JsonMetadataBlob>;

pub struct FacadeTopic {
    pub locator: types::TopicResourceLocator,
    store: store::StoreRef,
    repo: repo::Repository,
}

impl FacadeTopic {
    /// Create a new facade using a topic name
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

    /// Lock the topic
    pub async fn lock(&self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        trace!("locking `{}`", self.locator);
        repo::topic_lock(&mut tx, &self.locator).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Finalize the write procedure of the topic. The topic is locked and additional data are
    /// consolidated (e.g. manifest, timestamp bounds). This function is intended to be called by
    /// [`FacadeTopicWriterGuard`] to finilize the writing process.
    async fn finalize(
        &mut self,
        timeseries_querier: query::TimeseriesRef,
        format: rw::Format,
    ) -> Result<(), FacadeError> {
        let res = timeseries_querier
            .read(self.locator.path(), format, None)
            .await?;

        let ts_range = res.timestamp_range().await?;

        let manifest = types::TopicManifest::new()
            .with_timestamp(types::TopicManifestTimestamp::new(ts_range));

        self.manifest_write_to_store(manifest).await?;

        self.lock().await?;

        Ok(())
    }

    /// Reads [`TopicMetadata`] associated with this topic.
    ///
    /// # Errors
    ///
    /// Returns [`HandleError::ReadError`] if reading or deserializing fails.
    pub async fn metadata(&self) -> Result<TopicMetadata, FacadeError> {
        let path = self.locator.path_metadata();
        let bytes = self.store.read_bytes(path).await?;

        let data: marshal::JsonTopicMetadata = bytes.try_into()?;

        Ok(data.into())
    }

    /// Reads [`TopicManifest`] associated with this topic.
    ///
    /// If manifest can't be found a [`FacadeError::NotFound`] error is returned.
    pub async fn manifest(&self) -> Result<types::TopicManifest, FacadeError> {
        let path = self.locator.path_manifest();

        if !self.store.exists(&path).await? {
            return Err(FacadeError::not_found(path.to_string_lossy().to_string()));
        }

        let bytes = self.store.read_bytes(path).await?;

        let data: marshal::TopicManifest = bytes.try_into()?;

        Ok(data.into())
    }

    /// Returns the topic arrow schema.
    /// The serialization format is required to extract the schema, can be retrieved using [`TopicHandle::metadata`] function.
    ///
    /// If no arrow_schema is found a [`FacadeError::NotFound`] error is returned
    pub async fn arrow_schema(&self, format: rw::Format) -> Result<SchemaRef, FacadeError> {
        // Get chunk 0 since this chunk needs to exist always
        let path = self.locator.path_data(0, &format);

        if !self.store.exists(&path).await? {
            return Err(FacadeError::NotFound(path.to_string_lossy().to_string()));
        }

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
        let path = self.locator.path_metadata();

        let json_mdata = marshal::JsonTopicMetadata::from(metadata);
        let bytes: Vec<u8> = json_mdata.try_into()?;

        self.store.write_bytes(&path, bytes).await?;

        Ok(())
    }

    /// Write timestamp data (for quick access without performing queries) into the store
    async fn manifest_write_to_store(
        &self,
        manifest: types::TopicManifest,
    ) -> Result<(), FacadeError> {
        trace!("writing manifest to store to `{}`", self.locator);
        let path = self.locator.path_manifest();

        let json_manifest: marshal::TopicManifest = manifest.into();
        let bytes: Vec<u8> = json_manifest.try_into()?;

        self.store.write_bytes(&path, bytes).await?;

        Ok(())
    }

    /// Returns a writer used to write chunked record batches using a specified serialization
    /// format `format`.
    pub fn writer(
        &mut self,
        querier: query::TimeseriesRef,
        format: rw::Format,
    ) -> FacadeTopicWriterGuard<'_> {
        let max_chunk_size = {
            let config_value = params::configurables().max_chunk_size_in_bytes;
            if config_value == 0 {
                None // 0 means unlimited (no automatic splitting)
            } else {
                Some(config_value)
            }
        };

        let cw = rw::ChunkedWriter::new(
            self.store.clone(),
            self.path(),
            format,
            |path, format, idx| types::TopicResourceLocator::from(path).path_data(idx, format),
        )
        .with_max_chunk_size(max_chunk_size);

        FacadeTopicWriterGuard {
            facade: self,
            querier,
            format,
            writer: cw,
        }
    }

    /// Deletes this topic, if unlocked
    pub async fn delete_unlocked(self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        repo::topic_delete_unlocked(&mut tx, &self.locator).await?;

        // Delete files
        self.store.delete_recursive(&self.path()).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Permanently deletes a topic and all its data, be caution
    pub async fn delete(self, allowed_data_loss: types::DataLossToken) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        // allowed since this function is unsafe itself
        repo::topic_delete(&mut tx, &self.locator, allowed_data_loss).await?;

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

    /// Computes the optimal batch size based on topic statistics from the database.
    ///
    /// Returns `Some(batch_size)` if statistics are available, `None` otherwise
    /// (e.g., for empty topics).
    pub async fn compute_optimal_batch_size(&self) -> Result<usize, FacadeError> {
        let stats = self.chunks_stats().await?;

        if stats.total_size_bytes == 0 || stats.total_row_count == 0 {
            return Err(FacadeError::missing_data(
                "unable to compute optimal batch size".to_owned(),
            ));
        }

        let target_size = params::configurables().target_message_size_in_bytes;
        let batch_size = (target_size as i64 * stats.total_row_count) / stats.total_size_bytes;

        Ok(batch_size as usize)
    }
}

/// A guard ensuring exclusive write access to a [`FacadeTopic`].
///
/// While this struct exists, the underlying topic is mutably borrowed, preventing
/// any other operations (such as locking or concurrent reads) until [`FacadeTopicWriterGuard::finalize`] is called.
pub struct FacadeTopicWriterGuard<'a> {
    /// Anchors the exclusive borrow of the facade, strictly tying the writer's lifetime
    /// to the topic's availability.
    facade: &'a mut FacadeTopic,

    /// Query engine for timeseries data used to finalize topic data at the end of write process
    querier: query::TimeseriesRef,

    /// Serialization format used to write
    format: rw::Format,

    /// The underlying writer handling the actual data operations.
    writer: rw::ChunkedWriter<Arc<store::Store>>,
}

impl<'a> FacadeTopicWriterGuard<'a> {
    /// Performs all the operations required to finilize the writing stream, consolidate topic data
    /// and lock the topic
    pub async fn finalize(self) -> Result<(), FacadeError> {
        trace!("internal writer finalized");
        let summary = self.writer.finalize().await?;

        if summary.number_of_chunks_created > 0 {
            trace!("consolidating topic manifest");
            self.facade.finalize(self.querier, self.format).await?;
        } else {
            trace!("finalizing topic without data");
            self.facade.lock().await?;
            trace!("topic has been locked");
        }

        Ok(())
    }
}

impl<'a> std::ops::Deref for FacadeTopicWriterGuard<'a> {
    type Target = rw::ChunkedWriter<Arc<store::Store>>;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl<'a> std::ops::DerefMut for FacadeTopicWriterGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        trace!("dereferencing writer");
        &mut self.writer
    }
}

use super::{Error, Session};
use arrow::datatypes::SchemaRef;
use log::trace;
use mosaicod_core::{
    params,
    types::{self, Resource},
};
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use mosaicod_query as query;
use mosaicod_rw::{self as rw, ToProperties};
use mosaicod_store as store;
use std::sync::Arc;

/// Define topic manifest type containing JSON user metadata
type TopicManifest = types::TopicManifest<marshal::JsonMetadataBlob>;
type TopicOntologyMetadata = types::TopicOntologyMetadata<marshal::JsonMetadataBlob>;

pub struct Topic {
    pub locator: types::TopicResourceLocator,

    identifiers: types::Identifiers,

    store: store::StoreRef,
    db: db::Database,
}

impl Topic {
    /// Create a new facade using a topic locator
    pub async fn try_from_locator(
        locator: types::TopicResourceLocator,
        store: store::StoreRef,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut cx = db.connection();

        let record = db::topic_find_by_locator(&mut cx, &locator).await?;

        Ok(Self {
            locator,
            identifiers: record.identifiers(),
            store,
            db,
        })
    }

    /// Creates a new database entry for this topic.
    ///
    /// If a record with the same name already exists an error [`Error::TopicAlreadyExists`] is returned.
    ///
    /// Also additional checks about the scope of the topic are performed. If the topic locator is
    /// not a child of the related sequence locator an error [`Error::Unauthorized`] is returned.
    pub async fn create(
        locator: types::TopicResourceLocator,
        session: types::Uuid,
        ontology_metadata: TopicOntologyMetadata,
        store: store::StoreRef,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut tx = db.transaction().await?;

        // Check that there are not other topics with the same locator
        if db::topic_find_by_locator(&mut tx, &locator).await.is_ok() {
            return Err(Error::topic_already_exists(locator));
        }

        // Ensure that uuid points to an unlocked session
        let session_facade = Session::try_new(session.clone(), store.clone(), db.clone()).await?;
        if session_facade.manifest().await?.locked {
            return Err(Error::SessionLocked);
        }

        // Find parent sequence and ensure that this topic is child of the provided
        // sequence, i.e. they are related with the same name structure
        let ses_rec = db::session_find_by_uuid(&mut tx, &session).await?;
        let seq_rec = db::sequence_find_by_id(&mut tx, ses_rec.sequence_id).await?;
        let seq_loc = types::SequenceResourceLocator::from(&seq_rec.locator_name);
        if !locator.is_sub_resource(&seq_loc) {
            return Err(Error::Unauthorized);
        }

        let mut record = db::TopicRecord::new(
            locator.locator(), //
            seq_rec.sequence_id,
            ses_rec.session_id,
            &ontology_metadata.properties.ontology_tag,
            &ontology_metadata
                .properties
                .serialization_format
                .to_string(),
        );

        if let Some(user_metadata) = &ontology_metadata.user_metadata {
            record = record.with_user_metadata(user_metadata.clone());
        }

        let record = db::topic_create(&mut tx, &record).await?;

        let ftopic = Self {
            locator: record.locator(),
            identifiers: record.identifiers(),
            store,
            db: db.clone(),
        };

        let manifest = types::TopicManifest::new(
            types::TopicProperties::new_with_created_at(
                ftopic.locator.clone(),
                session,
                record.creation_timestamp(),
            ),
            ontology_metadata,
        );

        // This operation is done at the end to avoid deleting or reverting changes
        // to manifest file on store if some error causes a rollback on the database
        ftopic.manifest_write_to_store(manifest).await?;

        tx.commit().await?;

        Ok(ftopic)
    }

    /// Return the inner locator and consumes the facade
    pub fn into_inner(self) -> types::TopicResourceLocator {
        self.locator
    }

    /// Return the topic internal uuid
    pub fn uuid(&self) -> &types::Uuid {
        &self.identifiers.uuid
    }

    /// Read the database record for this sequence. If no record is found an error is returned.
    pub async fn resource_id(&self) -> Result<types::Identifiers, Error> {
        let mut cx = self.db.connection();

        trace!("searching for `{}`", self.locator);
        let record = db::topic_find_by_locator(&mut cx, &self.locator).await?;

        Ok(record.into())
    }

    /// Finalize the write procedure of the topic. The topic is locked and additional data are
    /// consolidated (e.g. manifest, timestamp bounds). This function is intended to be called by
    /// [`TopicWriterGuard`] to finalize the writing process.
    async fn finalize(
        &mut self,
        timeseries_querier: query::TimeseriesRef,
        format: types::Format,
    ) -> Result<(), Error> {
        let info = self.compute_data_info(timeseries_querier, format).await?;
        self.data_info_write_to_db(info).await?;

        // Update manifest
        let mut manifest = self.manifest().await?;

        // Check if topic is already locked.
        if manifest.properties.locked {
            return Err(Error::TopicLocked);
        }

        manifest.properties.completed_at = Some(types::Timestamp::now());
        manifest.properties.locked = true;
        self.manifest_write_to_store(manifest).await?;

        Ok(())
    }

    /// Reads [`TopicManifest`] associated with this topic.
    ///
    /// # Errors
    ///
    /// Returns [`HandleError::ReadError`] if reading or deserializing fails.
    /// Returns an error if manifest file does not exist.
    pub async fn manifest(&self) -> Result<TopicManifest, Error> {
        let path = self.locator.path_manifest();

        if !self.store.exists(&path).await? {
            return Err(Error::NotFound(format!(
                "missing manifest file for topic {}",
                self.locator
            )));
        }

        let bytes = self.store.read_bytes(path).await?;

        let data: marshal::JsonTopicManifest = bytes.try_into()?;

        Ok(data.try_into()?)
    }

    /// Returns the topic arrow schema.
    /// The serialization format is required to extract the schema.
    /// It can be retrieved using [`Topic::manifest`] function.
    ///
    /// If no arrow_schema is found a [`Error::NotFound`] error is returned
    pub async fn arrow_schema(&self, format: types::Format) -> Result<SchemaRef, Error> {
        // Get chunk 0 since this chunk needs to exist always
        let path =
            self.locator
                .path_data(&self.identifiers.uuid, 0, format.to_properties().as_ref());

        if !self.store.exists(&path).await? {
            return Err(Error::NotFound(path.to_string_lossy().to_string()));
        }

        // Build a parquet reader reading in memory a file
        let mut parquet_reader = self.store.parquet_reader(path);
        let schema = ext::arrow::schema_from_parquet_reader(&mut parquet_reader).await?;

        Ok(schema)
    }

    /// Serializes and writes [`TopicManifest`] to the object store.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] or [`Error::WriteError`] if serialization or writing fails.
    async fn manifest_write_to_store(&self, manifest: TopicManifest) -> Result<(), Error> {
        trace!("writing manifest to store to `{}`", self.locator);
        let path = self.locator.path_manifest();

        let json_manifest = marshal::JsonTopicManifest::from(manifest);
        let bytes: Vec<u8> = json_manifest.try_into()?;

        self.store.write_bytes(&path, bytes).await?;

        Ok(())
    }

    /// Returns a writer used to write chunked record batches using a specified serialization
    /// format `format`.
    pub fn writer(
        &mut self,
        querier: query::TimeseriesRef,
        format: types::Format,
    ) -> TopicWriterGuard<'_> {
        let max_chunk_size = {
            let config_value = params::params().max_chunk_size_in_bytes;
            if config_value == 0 {
                None // 0 means unlimited (no automatic splitting)
            } else {
                Some(config_value)
            }
        };

        let data_folder = self.locator.path_data_folder(self.uuid());

        let cw = rw::ChunkedWriter::new(self.store.clone(), format, move |chunk_number| {
            data_folder.join(types::TopicResourceLocator::data_file(
                chunk_number,
                format.to_properties().as_ref(),
            ))
        })
        .with_max_chunk_size(max_chunk_size);

        TopicWriterGuard {
            facade: self,
            querier,
            format,
            writer: cw,
        }
    }

    /// Deletes this topic, if unlocked
    pub async fn delete_unlocked(self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        if self.manifest().await?.properties.locked {
            return Err(Error::TopicLocked);
        }

        db::topic_delete(&mut tx, &self.locator, types::allow_data_loss()).await?;

        // Delete files
        self.store.delete_recursive(&self.locator.path()).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Permanently deletes a topic and all its data, be caution
    ///
    /// A [`types::DataLossToken`] is required since this call will lead to data losses.
    pub async fn delete(self, allowed_data_loss: types::DataLossToken) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        // Delete the record from DB first, then from the store. Order matters (think in case of rollback).
        db::topic_delete(&mut tx, &self.locator, allowed_data_loss).await?;
        self.store.delete_recursive(&self.locator.path()).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Add a notification to the sequence
    pub async fn notify(
        &self,
        ntype: types::NotificationType,
        msg: String,
    ) -> Result<types::Notification, Error> {
        let mut tx = self.db.transaction().await?;

        let record = db::topic_find_by_locator(&mut tx, &self.locator).await?;
        let notification = db::TopicNotificationRecord::new(record.topic_id, ntype, Some(msg));
        let notification = db::topic_notification_create(&mut tx, &notification).await?;

        tx.commit().await?;

        Ok(notification.into_notification(self.locator.clone()))
    }

    /// Returns a list of all notifications for the this topic
    pub async fn notification_list(&self) -> Result<Vec<types::Notification>, Error> {
        let mut cx = self.db.connection();
        let notifications = db::topic_notifications_find_by_locator(&mut cx, &self.locator).await?;
        Ok(notifications
            .into_iter()
            .map(|e| e.into_notification(self.locator.clone()))
            .collect())
    }

    /// Deletes all the notifications associated with the sequence
    pub async fn notification_purge(&self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        let notifications = db::topic_notifications_find_by_locator(&mut tx, &self.locator).await?;
        for notification in notifications {
            // Notification id is unwrapped since is retrieved from the database and
            // it has an id
            db::topic_notification_delete(&mut tx, notification.id().unwrap()).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Returns the statistics about topic's chunks
    pub async fn chunks_stats(&self) -> Result<types::TopicChunksStats, Error> {
        let mut cx = self.db.connection();
        let stats = db::topic_get_stats(&mut cx, &self.locator).await?;
        Ok(stats)
    }

    /// Computes metrics about topic's stored data
    /// (e.g. total size in bytes, first and last timestamps recorded in the topic)
    async fn compute_data_info(
        &self,
        timeseries_querier: query::TimeseriesRef,
        format: types::Format,
    ) -> Result<types::TopicDataInfo, Error> {
        let timeseries_res = timeseries_querier
            .read(self.locator.path_data_folder(self.uuid()), format, None)
            .await;

        let timestamp_range = match timeseries_res {
            Ok(res) => {
                let ts_range = res.timestamp_range().await;
                ts_range.unwrap_or(types::TimestampRange::unbounded())
            }
            Err(_) => types::TimestampRange::unbounded(),
        };

        let mut cx = self.db.connection();
        let record = db::topic_find_by_locator(&mut cx, &self.locator).await?;

        let format = record
            .serialization_format()
            .ok_or_else(|| Error::MissingMetadataField("serialization_format".to_owned()))?;

        let datafiles = self
            .store
            .list(
                &self.locator.locator(),
                Some(&format.to_properties().as_extension()),
            )
            .await?;

        let mut total_bytes = 0;
        for file in &datafiles {
            total_bytes += self.store.size(file).await? as u64;
        }

        Ok(types::TopicDataInfo {
            chunks_number: datafiles.len() as u64,
            total_bytes,
            timestamp_range,
        })
    }

    /// Caches metrics about topic's data.
    ///
    /// Since they can be recalculated at any time, it's enough to save them in the DB.
    async fn data_info_write_to_db(&self, system_info: types::TopicDataInfo) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;
        db::topic_update_system_info(&mut tx, &self.locator, &system_info).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Retrieves system info for the topic from db. Returns an error if not present.
    pub async fn data_info(&self) -> Result<types::TopicDataInfo, Error> {
        let mut cx = self.db.connection();
        let record = db::topic_find_by_locator(&mut cx, &self.locator).await?;
        let topic_info = record.info();
        topic_info.ok_or(Error::MissingData(format!(
            "missing info on DB for topic {}",
            self.locator
        )))
    }

    /// Computes the optimal batch size based on topic statistics from the database.
    /// Batch size is the minimum between the computed batch size and
    /// [`params::ConfigurablesParams::max_batch_size`].
    ///
    /// Returns `Some(batch_size)` if statistics are available, `None` otherwise
    /// (e.g., for empty topics).
    pub async fn compute_optimal_batch_size(&self) -> Result<usize, Error> {
        let stats = self.chunks_stats().await?;

        if stats.total_size_bytes == 0 || stats.total_row_count == 0 {
            return Err(Error::missing_data(
                "unable to compute optimal batch size".to_owned(),
            ));
        }

        let params = params::params();

        let target_size = params.target_message_size_in_bytes;
        let batch_size = (target_size as i64 * stats.total_row_count) / stats.total_size_bytes;

        Ok((batch_size as usize).min(params.max_batch_size))
    }
}

/// A guard ensuring exclusive write access to a [`Topic`].
///
/// While this struct exists, the underlying topic is mutably borrowed, preventing
/// any other operations (such as locking or concurrent reads) until [`TopicWriterGuard::finalize`] is called.
pub struct TopicWriterGuard<'a> {
    /// Anchors the exclusive borrow of the facade, strictly tying the writer's lifetime
    /// to the topic's availability.
    facade: &'a mut Topic,

    /// Query engine for timeseries data used to finalize topic data at the end of write process
    querier: query::TimeseriesRef,

    /// Serialization format used to write
    format: types::Format,

    /// The underlying writer handling the actual data operations.
    writer: rw::ChunkedWriter<Arc<store::Store>>,
}

impl<'a> TopicWriterGuard<'a> {
    /// Performs all the operations required to finalize the writing stream, consolidate topic data
    /// and lock the topic
    pub async fn finalize(self) -> Result<(), Error> {
        trace!("internal writer finalized");
        let _ = self.writer.finalize().await?;
        self.facade.finalize(self.querier, self.format).await?;
        Ok(())
    }
}

impl<'a> std::ops::Deref for TopicWriterGuard<'a> {
    type Target = rw::ChunkedWriter<Arc<store::Store>>;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl<'a> std::ops::DerefMut for TopicWriterGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        trace!("dereferencing writer");
        &mut self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sequence;
    use mosaicod_core::types::NotificationType;
    use types::Resource;

    fn dummy_ontology_metadata() -> TopicOntologyMetadata {
        types::TopicOntologyMetadata::new(
            types::TopicOntologyProperties {
                ontology_tag: "dummy".to_owned(),
                serialization_format: types::Format::Default,
            },
            None,
        )
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) {
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

        // Check sequence locator
        assert_eq!(fsequence.locator.locator(), sequence.locator_name);

        let session = fsequence.session().await.unwrap();
        assert!(session.uuid.is_valid());

        let topic_locator: types::TopicResourceLocator = "test_sequence/test_topic".into();

        let ftopic = Topic::create(
            topic_locator,
            session.uuid,
            dummy_ontology_metadata(),
            (*store).clone(),
            (*database).clone(),
        )
        .await
        .expect("Unable to create topic");

        // Check if topic was created
        let mut cx = database.connection();
        let topic = db::topic_find_by_locator(&mut cx, &ftopic.locator)
            .await
            .expect("Unable to find the created topic");

        // Check topic locator.
        assert_eq!(ftopic.locator.locator(), topic.locator().to_string());

        // Check topic deletion.
        ftopic.delete(types::allow_data_loss()).await.unwrap();

        assert!(
            db::topic_find_by_locator(&mut cx, &"test_sequence/test_topic".into())
                .await
                .is_err()
        );
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_notify_and_notify_purge(pool: sqlx::Pool<db::DatabaseType>) {
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

        // Check sequence locator
        assert_eq!(fsequence.locator.locator(), sequence.locator_name);

        let session = fsequence.session().await.unwrap();
        assert!(session.uuid.is_valid());

        let topic_locator: types::TopicResourceLocator = "test_sequence/test_topic".into();

        let ftopic = Topic::create(
            topic_locator,
            session.uuid,
            dummy_ontology_metadata(),
            (*store).clone(),
            (*database).clone(),
        )
        .await
        .expect("Unable to create topic");

        ftopic
            .notify(
                NotificationType::Error,
                "test notification message".to_owned(),
            )
            .await
            .expect("Error creating notification message");

        ftopic
            .notify(
                NotificationType::Error,
                "test notification message 2".to_owned(),
            )
            .await
            .expect("Error creating notification message");

        let topic = db::topic_find_by_locator(&mut cx, &ftopic.locator)
            .await
            .expect("Unable to find the created topic");

        // Check if notifications were created on database.
        let notifications = db::topic_notifications_find_by_locator(&mut cx, &ftopic.locator)
            .await
            .unwrap();

        assert_eq!(notifications.len(), 2);

        let first_notification = notifications.first().unwrap();
        assert_eq!(
            first_notification.msg.as_ref().unwrap(),
            "test notification message"
        );
        assert!(first_notification.uuid().is_valid());
        assert_eq!(first_notification.topic_id, topic.topic_id);

        let second_notification = notifications.last().unwrap();
        assert_eq!(
            second_notification.msg.as_ref().unwrap(),
            "test notification message 2"
        );
        assert!(second_notification.uuid().is_valid());
        assert_eq!(second_notification.topic_id, topic.topic_id);

        ftopic
            .notification_purge()
            .await
            .expect("Unable to purge notifications");

        // Check there are no more notifications on database.
        assert!(
            db::topic_notifications_find_by_locator(&mut cx, &ftopic.locator)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

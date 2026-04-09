use super::{Context, Error, session};
use arrow::datatypes::SchemaRef;
use log::trace;
use mosaicod_core::{
    params,
    types::{self, Resource},
};
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use mosaicod_rw::{self as rw, ToProperties};
use mosaicod_store as store;
use std::sync::Arc;

/// Define topic manifest type containing JSON user metadata
type TopicManifest = types::TopicManifest<marshal::JsonMetadataBlob>;
type TopicOntologyMetadata = types::TopicOntologyMetadata<marshal::JsonMetadataBlob>;

/// Handle containing topic identifiers.
/// It's used by all functions (except creation) in this module to indicate the topic to operate on.
pub struct Handle {
    locator: types::TopicResourceLocator,
    identifiers: types::Identifiers,
}

impl Handle {
    pub(super) fn new(
        locator: types::TopicResourceLocator,
        identifiers: types::Identifiers,
    ) -> Self {
        Self {
            locator,
            identifiers,
        }
    }

    /// Try to obtain a handle from a topic locator.
    /// Returns an error if the topic does not exist.
    pub async fn try_from_locator(
        context: &Context,
        locator: types::TopicResourceLocator,
    ) -> Result<Self, Error> {
        let mut cx = context.db.connection();

        let db_topic = db::topic_find_by_locator(&mut cx, &locator).await?;

        Ok(Self {
            locator,
            identifiers: db_topic.identifiers(),
        })
    }

    /// Try to obtain a handle from a topic UUID.
    /// Returns an error if the topic does not exist.
    pub async fn try_from_uuid(context: &Context, uuid: &types::Uuid) -> Result<Self, Error> {
        let mut cx = context.db.connection();

        let db_topic = db::topic_find_by_uuid(&mut cx, uuid).await?;

        Ok(Self {
            locator: db_topic.locator(),
            identifiers: db_topic.identifiers(),
        })
    }

    pub fn uuid(&self) -> &types::Uuid {
        &self.identifiers.uuid
    }

    pub fn locator(&self) -> &types::TopicResourceLocator {
        &self.locator
    }

    pub(super) fn id(&self) -> i32 {
        self.identifiers.id
    }
}

/// Creates a new database entry for this topic.
///
/// If a record with the same name already exists an error [`Error::TopicAlreadyExists`] is returned.
///
/// Additional checks about the scope of the topic are performed. If the topic locator is
/// not a child of the related sequence locator an error [`Error::Unauthorized`] is returned.
pub async fn try_create(
    context: &Context,
    locator: types::TopicResourceLocator,
    session_handle: &session::Handle,
    ontology_metadata: TopicOntologyMetadata,
) -> Result<Handle, Error> {
    let mut tx = context.db.transaction().await?;

    // Check that there are not other topics with the same locator
    if db::topic_find_by_locator(&mut tx, &locator).await.is_ok() {
        return Err(Error::topic_already_exists(locator));
    }

    // Ensure that uuid points to an unlocked session
    if session::manifest(context, session_handle).await?.locked {
        return Err(Error::SessionLocked);
    }

    // Find parent sequence and ensure that this topic is child of the provided
    // sequence, i.e. they are related with the same name structure
    let seq_rec = db::sequence_find_by_locator(&mut tx, session_handle.sequence_locator()).await?;

    if !locator.is_sub_resource(session_handle.sequence_locator()) {
        return Err(Error::Unauthorized);
    }

    let mut record = db::TopicRecord::new(
        locator.locator(),
        seq_rec.sequence_id,
        session_handle.id(),
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

    let topic_handle = Handle {
        locator: locator.clone(),
        identifiers: types::Identifiers {
            id: record.topic_id,
            uuid: record.uuid(),
        },
    };

    let manifest = types::TopicManifest::new(
        types::TopicProperties::new_with_created_at(
            locator,
            session_handle.uuid().clone(),
            record.creation_timestamp(),
        ),
        ontology_metadata,
    );

    // This operation is done at the end to avoid deleting or reverting changes
    // to manifest file on store if some error causes a rollback on the database
    manifest_write_to_store(context, &topic_handle, manifest).await?;

    tx.commit().await?;

    Ok(topic_handle)
}

/// Finalize the write procedure of the topic. The topic is locked and additional data are
/// consolidated (e.g. manifest, timestamp bounds). This function is intended to be called by
/// [`TopicWriterGuard`] to finalize the writing process.
async fn finalize(context: &Context, handle: &Handle, format: types::Format) -> Result<(), Error> {
    let info = compute_data_info(context, handle, format).await?;
    data_info_write_to_db(context, handle, info).await?;

    // Update manifest
    let mut manifest = manifest(context, handle).await?;

    // Check if topic is already locked.
    if manifest.properties.locked {
        return Err(Error::TopicLocked);
    }

    manifest.properties.completed_at = Some(types::Timestamp::now());
    manifest.properties.locked = true;
    manifest_write_to_store(context, handle, manifest).await?;

    Ok(())
}

/// Reads [`TopicManifest`] associated to the given topic [`Handle`].
///
/// # Errors
///
/// Returns [`HandleError::ReadError`] if reading or deserializing fails.
/// Returns an error if manifest file does not exist.
pub async fn manifest(context: &Context, handle: &Handle) -> Result<TopicManifest, Error> {
    let path = handle.locator.path_manifest();

    if !context.store.exists(&path).await? {
        return Err(Error::NotFound(format!(
            "missing manifest file for topic {}",
            handle.locator
        )));
    }

    let bytes = context.store.read_bytes(path).await?;

    let data: marshal::JsonTopicManifest = bytes.try_into()?;

    Ok(data.try_into()?)
}

/// Returns the topic arrow schema.
/// The serialization format is required to extract the schema.
/// It can be retrieved using [`Topic::manifest`] function.
///
/// If no arrow_schema is found a [`Error::NotFound`] error is returned
pub async fn arrow_schema(
    context: &Context,
    handle: &Handle,
    format: types::Format,
) -> Result<SchemaRef, Error> {
    // Get chunk 0 since this chunk needs to exist always
    let path = handle
        .locator
        .path_data(handle.uuid(), 0, format.to_properties().as_ref());

    if !context.store.exists(&path).await? {
        return Err(Error::NotFound(path.to_string_lossy().to_string()));
    }

    // Build a parquet reader reading in memory a file
    let mut parquet_reader = context.store.parquet_reader(path);
    let schema = ext::arrow::schema_from_parquet_reader(&mut parquet_reader).await?;

    Ok(schema)
}

/// Serializes and writes [`TopicManifest`] to the object store.
///
/// # Errors
///
/// Returns [`Error::NotFound`] or [`Error::WriteError`] if serialization or writing fails.
async fn manifest_write_to_store(
    context: &Context,
    handle: &Handle,
    manifest: TopicManifest,
) -> Result<(), Error> {
    trace!("writing manifest to store to `{}`", handle.locator);
    let path = handle.locator.path_manifest();

    let json_manifest = marshal::JsonTopicManifest::from(manifest);
    let bytes: Vec<u8> = json_manifest.try_into()?;

    context.store.write_bytes(&path, bytes).await?;

    Ok(())
}

/// Returns a writer used to write chunked record batches using a specified serialization
/// format `format`.
pub fn writer(
    context: Context,
    handle: Handle,
    format: types::Format,
    schema: SchemaRef,
) -> HandleWriter {
    let data_folder = handle.locator.path_data_folder(handle.uuid());

    let writer = rw::ChunkWriter::new(
        context.store.clone(),
        format,
        schema.clone(),
        move |chunk_number| {
            data_folder.join(types::TopicResourceLocator::data_file(
                chunk_number,
                format.to_properties().as_ref(),
            ))
        },
    );

    HandleWriter {
        handle,
        format,
        writer,
        context,
    }
}

/// Deletes this topic, if unlocked
pub async fn delete_unlocked(context: &Context, handle: Handle) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    if manifest(context, &handle).await?.properties.locked {
        return Err(Error::TopicLocked);
    }

    db::topic_delete(&mut tx, &handle.locator, types::allow_data_loss()).await?;

    // Delete files
    context
        .store
        .delete_recursive(&handle.locator.path())
        .await?;

    tx.commit().await?;

    Ok(())
}

/// Permanently deletes a topic and all its data, be caution
///
/// A [`types::DataLossToken`] is required since this call will lead to data losses.
pub async fn delete(
    context: &Context,
    handle: Handle,
    allowed_data_loss: types::DataLossToken,
) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    // Delete the record from DB first, then from the store. Order matters (think in case of rollback).
    db::topic_delete(&mut tx, &handle.locator, allowed_data_loss).await?;
    context
        .store
        .delete_recursive(&handle.locator.path())
        .await?;

    tx.commit().await?;

    Ok(())
}

/// Add a notification to the sequence
pub async fn notify(
    context: &Context,
    handle: &Handle,
    ntype: types::NotificationType,
    msg: String,
) -> Result<types::Notification, Error> {
    let mut tx = context.db.transaction().await?;

    let record = db::topic_find_by_locator(&mut tx, &handle.locator).await?;
    let notification = db::TopicNotificationRecord::new(record.topic_id, ntype, Some(msg));
    let notification = db::topic_notification_create(&mut tx, &notification).await?;

    tx.commit().await?;

    Ok(notification.into_notification(handle.locator.clone()))
}

/// Returns a list of all notifications for the this topic
pub async fn notification_list(
    context: &Context,
    handle: &Handle,
) -> Result<Vec<types::Notification>, Error> {
    let mut cx = context.db.connection();
    let notifications = db::topic_notifications_find_by_locator(&mut cx, &handle.locator).await?;
    Ok(notifications
        .into_iter()
        .map(|e| e.into_notification(handle.locator.clone()))
        .collect())
}

/// Deletes all the notifications associated with the sequence
pub async fn notification_purge(context: &Context, handle: &Handle) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    let notifications = db::topic_notifications_find_by_locator(&mut tx, &handle.locator).await?;
    for notification in notifications {
        // Notification id is unwrapped since is retrieved from the database and
        // it has an id
        db::topic_notification_delete(&mut tx, notification.id().unwrap()).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Returns the statistics about topic's chunks
pub async fn chunks_stats(
    context: &Context,
    handle: &Handle,
) -> Result<types::TopicChunksStats, Error> {
    let mut cx = context.db.connection();
    let stats = db::topic_get_stats(&mut cx, &handle.locator).await?;
    Ok(stats)
}

/// Computes metrics about topic's stored data
/// (e.g. total size in bytes, first and last timestamps recorded in the topic)
async fn compute_data_info(
    context: &Context,
    handle: &Handle,
    format: types::Format,
) -> Result<types::TopicDataInfo, Error> {
    let timeseries_res = context
        .timeseries_querier
        .read(handle.locator.path_data_folder(handle.uuid()), format, None)
        .await;

    let timestamp_range = match timeseries_res {
        Ok(res) => {
            let ts_range = res.timestamp_range().await;
            ts_range.unwrap_or(types::TimestampRange::unbounded())
        }
        Err(_) => types::TimestampRange::unbounded(),
    };

    let mut cx = context.db.connection();
    let record = db::topic_find_by_locator(&mut cx, &handle.locator).await?;

    let format = record
        .serialization_format()
        .ok_or_else(|| Error::MissingMetadataField("serialization_format".to_owned()))?;

    let datafiles = context
        .store
        .list(
            &handle.locator.locator(),
            Some(&format.to_properties().as_extension()),
        )
        .await?;

    let mut total_bytes = 0;
    for file in &datafiles {
        total_bytes += context.store.size(file).await? as u64;
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
async fn data_info_write_to_db(
    context: &Context,
    handle: &Handle,
    system_info: types::TopicDataInfo,
) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;
    db::topic_update_system_info(&mut tx, &handle.locator, &system_info).await?;
    tx.commit().await?;
    Ok(())
}

/// Retrieves system info for the topic from db. Returns an error if not present.
pub async fn data_info(context: &Context, handle: &Handle) -> Result<types::TopicDataInfo, Error> {
    let mut cx = context.db.connection();
    let record = db::topic_find_by_locator(&mut cx, &handle.locator).await?;
    let topic_info = record.info();
    topic_info.ok_or(Error::MissingData(format!(
        "missing info on DB for topic {}",
        handle.locator
    )))
}

/// Computes the optimal batch size based on topic statistics from the database.
/// Batch size is the minimum between the computed batch size and
/// [`params::ConfigurablesParams::max_batch_size`].
///
/// Returns `Some(batch_size)` if statistics are available, `None` otherwise
/// (e.g., for empty topics).
pub async fn compute_optimal_batch_size(
    context: &Context,
    handle: &Handle,
) -> Result<usize, Error> {
    let stats = chunks_stats(context, handle).await?;

    if stats.total_size_bytes == 0 || stats.total_row_count == 0 {
        return Err(Error::missing_data(
            "unable to compute optimal batch size".to_owned(),
        ));
    }

    let params = params::params();

    let target_size = params.target_message_size;
    let batch_size = (target_size as i64 * stats.total_row_count) / stats.total_size_bytes;

    Ok((batch_size as usize).min(params.max_batch_size))
}

/// A guard ensuring exclusive write access to [`Handle`].
///
/// While this struct exists, the underlying topic is mutably borrowed, preventing
/// any other operations (such as locking or concurrent reads) until [`TopicWriter::finalize`] is called.
pub struct HandleWriter {
    /// Anchors the exclusive borrow of the handle, strictly tying the writer's lifetime
    /// to the topic's availability.
    handle: Handle,

    /// Serialization format used to write
    format: types::Format,

    /// The underlying writer handling the actual data operations.
    writer: rw::ChunkWriter<Arc<store::Store>>,

    /// Context containing query engine for timeseries data used to finalize topic data at the end of write process
    context: Context,
}

impl HandleWriter {
    /// Performs all the operations required to finalize the writing stream, consolidate topic data
    /// and lock the topic
    pub async fn finalize(self) -> Result<(), Error> {
        finalize(&self.context, &self.handle, self.format).await?;
        Ok(())
    }
}

impl std::ops::Deref for HandleWriter {
    type Target = rw::ChunkWriter<Arc<store::Store>>;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl std::ops::DerefMut for HandleWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequence;
    use mosaicod_core::types::NotificationType;
    use mosaicod_query as query;
    use types::Resource;

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new(store.clone(), 0).unwrap());

        Context::new(store.clone(), database.clone(), ts_gw)
    }

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
        let context = test_context(pool);

        let seq_locator = types::SequenceResourceLocator::from("test_sequence");

        let seq_handle = sequence::try_create(&context, seq_locator, None)
            .await
            .expect("Error creating sequence");

        // Check if sequence was created
        let mut cx = context.db.connection();
        let sequence = db::sequence_find_by_locator(&mut cx, seq_handle.locator())
            .await
            .expect("Unable to find the created sequence");

        // Check sequence locator
        assert_eq!(seq_handle.locator().locator(), sequence.locator_name);

        let session_handle = session::try_create(&context, seq_handle.locator().clone())
            .await
            .unwrap();

        let topic_locator = types::TopicResourceLocator::from("test_sequence/test_topic");

        let topic_handle = try_create(
            &context,
            topic_locator,
            &session_handle,
            dummy_ontology_metadata(),
        )
        .await
        .expect("Unable to create topic");

        // Check if topic was created
        let mut cx = context.db.connection();
        let topic = db::topic_find_by_locator(&mut cx, topic_handle.locator())
            .await
            .expect("Unable to find the created topic");

        // Check topic locator.
        assert_eq!(
            topic_handle.locator().locator(),
            topic.locator().to_string()
        );

        // Check topic deletion.
        delete(&context, topic_handle, types::allow_data_loss())
            .await
            .unwrap();

        assert!(
            db::topic_find_by_locator(&mut cx, &"test_sequence/test_topic".into())
                .await
                .is_err()
        );
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_notify_and_notify_purge(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let seq_locator = types::SequenceResourceLocator::from("test_sequence");

        let seq_handle = sequence::try_create(&context, seq_locator, None)
            .await
            .expect("Unable to create sequence");

        // Check if sequence was created
        let mut cx = context.db.connection();

        let sequence = db::sequence_find_by_locator(&mut cx, seq_handle.locator())
            .await
            .expect("Unable to find the created sequence");

        // Check sequence locator
        assert_eq!(seq_handle.locator().locator(), sequence.locator_name);

        let session_handle = session::try_create(&context, seq_handle.locator().clone())
            .await
            .expect("Unable to create session");
        assert!(session_handle.uuid().is_valid());

        let topic_locator: types::TopicResourceLocator = "test_sequence/test_topic".into();

        let topic_handle = try_create(
            &context,
            topic_locator,
            &session_handle,
            dummy_ontology_metadata(),
        )
        .await
        .expect("Unable to create topic");

        notify(
            &context,
            &topic_handle,
            NotificationType::Error,
            "test notification message".to_owned(),
        )
        .await
        .expect("Error creating notification message");

        notify(
            &context,
            &topic_handle,
            NotificationType::Error,
            "test notification message 2".to_owned(),
        )
        .await
        .expect("Error creating notification message");

        let topic = db::topic_find_by_locator(&mut cx, topic_handle.locator())
            .await
            .expect("Unable to find the created topic");

        // Check if notifications were created on database.
        let notifications =
            db::topic_notifications_find_by_locator(&mut cx, topic_handle.locator())
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

        notification_purge(&context, &topic_handle)
            .await
            .expect("Unable to purge notifications");

        // Check there are no more notifications on database.
        assert!(
            db::topic_notifications_find_by_locator(&mut cx, topic_handle.locator())
                .await
                .unwrap()
                .is_empty()
        );
    }
}

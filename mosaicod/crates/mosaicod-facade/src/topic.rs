use super::{Context, Error, session};
use arrow::datatypes::SchemaRef;
use log::{trace, warn};
use mosaicod_core::types::TopicMetadataProperties;
use mosaicod_core::{self as core, error::PublicResult as Result, params, types};
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use mosaicod_rw::{self as rw, ToProperties};
use mosaicod_store as store;
use std::path;
use std::sync::Arc;

/// Define topic metadata type containing JSON user metadata
type TopicMetadata = types::TopicMetadata<marshal::JsonMetadataBlob>;
type TopicOntologyMetadata = types::TopicOntologyMetadata<marshal::JsonMetadataBlob>;

#[derive(PartialEq)]
pub enum Status {
    /// The topic has just been created. Still no data has been uploaded.
    Empty,
    /// The topic is uploading data.
    Uploading,
    /// The topic has been completely uploaded and finalized.
    Finalized,
}

/// Handle containing topic identifiers.
/// It's used by all functions (except creation) in this module to indicate the topic to operate on.
pub struct Handle {
    id: i32,
    uuid: types::Uuid,
    locator: types::TopicLocator,
    path_in_store: Option<types::TopicPathInStore>,
}

impl Handle {
    pub(super) fn new(
        locator: types::TopicLocator,
        id: i32,
        uuid: types::Uuid,
        path_in_store: Option<types::TopicPathInStore>,
    ) -> Self {
        Self {
            locator,
            id,
            uuid,
            path_in_store,
        }
    }

    /// Try to obtain a handle from a topic locator.
    /// Returns an error if the topic does not exist.
    pub async fn try_from_locator(context: &Context, locator: types::TopicLocator) -> Result<Self> {
        let mut cx = context.db.connection();

        let db_topic = db::topic_find_by_locator(&mut cx, &locator).await?;

        Ok(Self {
            locator,
            id: db_topic.topic_id,
            uuid: db_topic.uuid(),
            path_in_store: db_topic.path_in_store(),
        })
    }

    /// Try to obtain a handle from a topic UUID.
    /// Returns an error if the topic does not exist.
    pub async fn try_from_uuid(context: &Context, uuid: &types::Uuid) -> Result<Self> {
        let mut cx = context.db.connection();

        let db_topic = db::topic_find_by_uuid(&mut cx, uuid).await?;

        Ok(Self {
            locator: db_topic.locator(),
            id: db_topic.topic_id,
            uuid: db_topic.uuid(),
            path_in_store: db_topic.path_in_store(),
        })
    }

    pub fn uuid(&self) -> &types::Uuid {
        &self.uuid
    }

    pub fn locator(&self) -> &types::TopicLocator {
        &self.locator
    }

    pub(super) fn id(&self) -> i32 {
        self.id
    }

    pub fn path_in_store(&self) -> Option<&types::TopicPathInStore> {
        self.path_in_store.as_ref()
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
    locator: types::TopicLocator,
    session_handle: &session::Handle,
    ontology_metadata: TopicOntologyMetadata,
) -> Result<Handle> {
    let mut tx = context.db.transaction().await?;

    // Session must be unlocked (not finalized)
    let session_locked = db::session_locked(&mut tx, session_handle.id()).await?;

    if session_locked {
        // (cabba) NOTE: Now I'm returning the uuid as session identifier
        // we need to substitute this with the session "locator" when implemented
        Err(core::Error::session_already_finalized(
            session_handle.uuid().to_string(),
        ))?;
    }

    // Find parent sequence and ensure that this topic is child of the provided
    // sequence, i.e. they are related with the same name structure
    let seq_rec = db::sequence_find_by_locator(&mut tx, session_handle.sequence_locator()).await?;

    if !locator.is_sub_locator(session_handle.sequence_locator()) {
        Err(core::Error::unauthorized())?;
    }

    let mut record = db::TopicRecord::new(
        locator.clone(),
        seq_rec.sequence_id,
        session_handle.id(),
        &ontology_metadata.properties.ontology_tag,
        &ontology_metadata
            .properties
            .serialization_format
            .to_string(),
        None,
    );

    if let Some(user_metadata) = &ontology_metadata.user_metadata {
        record = record.with_user_metadata(user_metadata.clone());
    }

    let record = db::topic_create(&mut tx, &record).await?;

    tx.commit().await?;

    let topic_handle = Handle {
        locator: locator.clone(),
        id: record.topic_id,
        uuid: record.uuid(),
        path_in_store: None,
    };

    Ok(topic_handle)
}

/// Private method to tell the topic status (just created, uploading data, finalized).
///
/// Note: please use this function instead of [`status`] if you need to call it internally
/// (from another function in this module that already has an active transaction)
async fn impl_status(handle: &Handle, exe: &mut impl db::AsExec) -> Result<Status> {
    let db_topic = db::topic_find_by_id(exe, handle.id()).await?;

    if db_topic.path_in_store().is_none() {
        debug_assert!(db_topic.completion_timestamp().is_none());
        return Ok(Status::Empty);
    } else if db_topic.completion_timestamp().is_none() {
        return Ok(Status::Uploading);
    }

    debug_assert!(db_topic.path_in_store().is_some() && db_topic.completion_timestamp().is_some());
    Ok(Status::Finalized)
}

/// Tells the topic status (just created, uploading data, finalized).
///
/// Note: if you need to call this method internally (from another function in this module that
/// already has an active transaction), please use [`impl_status`]
pub async fn status(context: &Context, handle: &Handle) -> Result<Status> {
    let mut cx = context.db.connection();
    impl_status(handle, &mut cx).await
}

/// Creates [`TopicMetadata`] associated to the given topic [`Handle`].
pub async fn metadata(context: &Context, handle: &Handle) -> Result<TopicMetadata> {
    let mut cx = context.db.connection();

    let db_topic = db::topic_find_by_id(&mut cx, handle.id()).await?;
    let session_uuid = db::session_find_by_id(&mut cx, db_topic.session_id)
        .await?
        .uuid();

    Ok(TopicMetadata {
        properties: TopicMetadataProperties {
            created_at: db_topic.creation_timestamp(),
            completed_at: db_topic.completion_timestamp(),
            session_uuid,
            resource_locator: handle.locator.clone(),
        },
        ontology_metadata: TopicOntologyMetadata {
            properties: types::TopicOntologyProperties {
                serialization_format: db_topic
                    .serialization_format()
                    .ok_or_else(|| Error::MissingDbData("serialization_format".to_owned()))?,
                ontology_tag: db_topic.ontology_tag.clone(),
            },
            user_metadata: db_topic.user_metadata(),
        },
    })
}

/// Returns the topic arrow schema.
/// The serialization format is required to extract the schema.
/// It can be retrieved using [`metadata`] function.
///
/// If no arrow_schema is found an empty one is returned.
pub async fn arrow_schema(
    context: &Context,
    handle: &Handle,
    format: types::Format,
) -> Result<SchemaRef> {
    let Some(path_in_store) = &handle.path_in_store else {
        return Ok(mosaicod_ext::arrow::empty_schema_ref());
    };

    // Get chunk 0 since this chunk needs to exist always
    let path = path_in_store.path_data(0, format.to_properties().as_ref());

    if !context.store.exists(&path).await? {
        return Ok(mosaicod_ext::arrow::empty_schema_ref());
    }

    // Build a parquet reader reading in memory a file
    let mut parquet_reader = context.store.parquet_reader(path);
    let schema = ext::arrow::schema_from_parquet_reader(&mut parquet_reader).await?;

    Ok(schema)
}

/// Serializes and writes [`TopicMetadata`] to the object store.
///
/// # Errors
///
/// Returns [`Error::NotFound`] or [`Error::WriteError`] if serialization or writing fails.
async fn metadata_write_to_store(
    context: &Context,
    path: &path::Path,
    metadata: TopicMetadata,
) -> Result<()> {
    trace!("writing topic metadata `{}` to store", path.display());

    let json_manifest = marshal::JsonTopicMetadata::from(metadata);
    let bytes: Vec<u8> = json_manifest.try_into()?;

    context.store.write_bytes(path, bytes).await?;

    Ok(())
}

/// Returns a writer used to write chunked record batches using a specified serialization
/// format `format`.
pub async fn writer(
    context: Context,
    mut handle: Handle,
    schema: SchemaRef,
) -> Result<HandleWriter> {
    // Precondition: check if topic has already been finalized or if someone else is already uploading data.
    let topic_status = status(&context, &handle).await?;
    match topic_status {
        Status::Empty => (),
        Status::Uploading => Err(core::Error::topic_upload_in_progress(
            handle.locator.to_string(),
        ))?,
        Status::Finalized => Err(core::Error::topic_already_finalized(
            handle.locator.to_string(),
        ))?,
    }

    let mdata = metadata(&context, &handle).await?;

    // Set up the callback that will be used to create the database record for the data catalog
    // and prepare variables that will be moved in the closure
    let ontology_tag = mdata.ontology_metadata.properties.ontology_tag.clone();
    let format = mdata.ontology_metadata.properties.serialization_format;

    // 1. Create folder in Store and save metadata.
    let path_in_store = types::TopicPathInStore::new();

    metadata_write_to_store(&context, path_in_store.path_metadata().as_path(), mdata).await?;

    let data_folder = path_in_store.data_folder_path();

    // 2. Save path_in_store on DB.
    let mut cx = context.db.connection();
    db::topic_update_path_in_store(&mut cx, handle.id, path_in_store.clone()).await?;

    let writer = rw::ChunkWriter::new(
        context.store.clone(),
        format,
        schema.clone(),
        move |chunk_number| {
            data_folder.join(types::TopicPathInStore::data_file(
                chunk_number,
                format.to_properties().as_ref(),
            ))
        },
    );

    handle.path_in_store = Some(path_in_store);

    Ok(HandleWriter {
        handle,
        format,
        ontology_tag,
        writer,
        context,
    })
}

/// Permanently deletes a topic and all its data, be caution
///
/// A [`types::DataLossToken`] is required since this call will lead to data losses.
pub async fn delete(
    context: &Context,
    handle: Handle,
    allowed_data_loss: types::DataLossToken,
) -> Result<()> {
    warn!("(data loss) deleting topic '{}'", handle.locator);
    let mut cx = context.db.connection();
    db::topic_delete(&mut cx, handle.id, allowed_data_loss).await?;
    Ok(())
}

/// Add a notification to the sequence
pub async fn notify(
    context: &Context,
    handle: &Handle,
    ntype: types::NotificationType,
    msg: String,
) -> Result<types::Notification> {
    let mut tx = context.db.transaction().await?;

    let record = db::topic_find_by_locator(&mut tx, &handle.locator).await?;
    let notification = db::TopicNotificationRecord::new(record.topic_id, ntype, Some(msg));
    let notification = db::topic_notification_create(&mut tx, &notification).await?;

    tx.commit().await?;

    Ok(notification.into_notification(handle.locator.clone()))
}

/// Returns a list of all notifications for the topic
pub async fn notification_list(
    context: &Context,
    handle: &Handle,
) -> Result<Vec<types::Notification>> {
    let mut cx = context.db.connection();
    let notifications = db::topic_notifications_find_by_locator(&mut cx, &handle.locator).await?;
    Ok(notifications
        .into_iter()
        .map(|e| e.into_notification(handle.locator.clone()))
        .collect())
}

/// Deletes all the notifications associated with the sequence
pub async fn notification_purge(context: &Context, handle: &Handle) -> Result<()> {
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
pub async fn chunks_stats(context: &Context, handle: &Handle) -> Result<types::TopicChunksStats> {
    let mut cx = context.db.connection();
    let stats = db::topic_get_stats(&mut cx, &handle.locator).await?;
    Ok(stats)
}

/// Computes metrics about the topic's stored data
/// (e.g. total size in bytes, first and last timestamps recorded in the topic)
async fn compute_data_info(
    context: &Context,
    handle: &Handle,
    exe: &mut impl db::AsExec,
    format: types::Format,
) -> Result<types::TopicDataInfo> {
    let path_in_store = handle
        .path_in_store
        .clone()
        .ok_or(Error::MissingDbData(format!(
            "No path in store set for topic {}",
            handle.locator
        )))?;

    let timeseries_res = context
        .timeseries_querier
        .read(path_in_store.data_folder_path(), format, None)
        .await;

    let timestamp_range = match timeseries_res {
        Ok(res) => {
            let ts_range = res.timestamp_range().await;
            ts_range.unwrap_or(types::TimestampRange::unbounded())
        }
        Err(_) => types::TimestampRange::unbounded(),
    };

    let record = db::topic_find_by_locator(exe, &handle.locator).await?;

    let format = record
        .serialization_format()
        .ok_or_else(|| Error::MissingDbData("serialization_format".to_owned()))?;

    let datafiles = context
        .store
        .list(
            path_in_store.root(),
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

/// Retrieves system info for the topic from db. Returns an error if not present.
pub async fn data_info(context: &Context, handle: &Handle) -> Result<types::TopicDataInfo> {
    let mut cx = context.db.connection();
    let record = db::topic_find_by_locator(&mut cx, &handle.locator).await?;
    let topic_info = record.info();
    topic_info.ok_or(
        Error::MissingDbData(format!("missing info on DB for topic {}", handle.locator)).into(),
    )
}

/// Computes the optimal batch size based on topic statistics from the database.
/// Batch size is the minimum between the computed batch size and
/// [`params::ConfigurablesParams::max_batch_size`].
///
/// Returns `Some(batch_size)` if statistics are available, `None` otherwise
/// (e.g., for empty topics).
pub async fn compute_optimal_batch_size(context: &Context, handle: &Handle) -> Result<usize> {
    let stats = chunks_stats(context, handle).await?;

    if stats.total_size_bytes == 0 || stats.total_row_count == 0 {
        Err(Error::MissingDbData(
            "unable to compute optimal batch size".to_owned(),
        ))?;
    }

    let params = params::params();

    let target_size = params.target_message_size.value;
    let batch_size = (target_size as i64 * stats.total_row_count) / stats.total_size_bytes;

    Ok((batch_size as usize).min(params.max_batch_size.value))
}

/// A guard ensuring exclusive write access to [`Handle`].
///
/// While this struct exists, the underlying topic is mutably borrowed, preventing
/// any other operations (such as locking or concurrent reads) until [`HandleWriter::finalize`] is called.
pub struct HandleWriter {
    /// Anchors the exclusive borrow of the handle, strictly tying the writer's lifetime
    /// to the topic's availability.
    handle: Handle,

    /// Serialization format used to write
    format: types::Format,

    ontology_tag: String,

    /// The underlying writer handling the actual data operations.
    writer: rw::ChunkWriter<Arc<store::Store>>,

    /// Context containing query engine for timeseries data used to finalize topic data at the end of write process
    context: Context,
}

impl HandleWriter {
    pub fn ontology_tag(&self) -> &str {
        &self.ontology_tag
    }

    /// Finalize the write procedure of the topic. The topic is locked and additional data are
    /// consolidated (e.g. metadata, timestamp bounds).
    pub async fn finalize(self) -> Result<()> {
        // 1. Update topic record in database.
        let mut tx = self.context.db.transaction().await?;

        let info = compute_data_info(&self.context, &self.handle, &mut tx, self.format).await?;
        db::topic_update_system_info(&mut tx, &self.handle.locator, &info).await?;

        // Check if topic has already been uploaded and finalized.
        if let Status::Finalized = impl_status(&self.handle, &mut tx).await? {
            return Err(core::Error::topic_already_finalized(
                self.handle.locator().to_string(),
            ))?;
        }

        // Update completion timestamp
        db::topic_update_completion_tstamp(
            &mut tx,
            self.handle.id(),
            types::Timestamp::now().as_i64(),
        )
        .await?;

        tx.commit().await?;

        // 2. Update metadata in Store (read entirely from DB and save to Store).
        let metadata = metadata(&self.context, &self.handle).await?;

        // Path in store is expected to be set inside handle while creating the HandleWriter.
        // Here it should be safe to unwrap it.
        let Some(path_in_store) = &self.handle.path_in_store else {
            panic!("No path in store set for topic {}", self.handle.locator);
        };

        metadata_write_to_store(
            &self.context,
            path_in_store.path_metadata().as_path(),
            metadata,
        )
        .await?;

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

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new((*store).clone(), 0).unwrap());

        Context::new((*store).clone(), (*database).clone(), ts_gw)
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

        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();

        let seq_handle = sequence::try_create(&context, seq_locator, None)
            .await
            .expect("Error creating sequence");

        // Check if sequence was created
        let mut cx = context.db.connection();
        let sequence = db::sequence_find_by_locator(&mut cx, seq_handle.locator())
            .await
            .expect("Unable to find the created sequence");

        // Check sequence locator
        assert_eq!(*seq_handle.locator(), sequence.locator());

        let session_handle = session::try_create(&context, seq_handle.locator().clone())
            .await
            .unwrap();

        let topic_locator = "test_sequence/test_topic"
            .parse::<types::TopicLocator>()
            .unwrap();

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
        assert_eq!(*topic_handle.locator(), topic.locator());

        // Check path in store
        assert!(topic.path_in_store().is_none());

        // Check topic deletion.
        delete(&context, topic_handle, types::allow_data_loss())
            .await
            .unwrap();

        assert!(
            db::topic_find_by_locator(&mut cx, &"test_sequence/test_topic".parse().unwrap())
                .await
                .is_err()
        );
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_notify_and_notify_purge(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();

        let seq_handle = sequence::try_create(&context, seq_locator, None)
            .await
            .expect("Unable to create sequence");

        // Check if sequence was created
        let mut cx = context.db.connection();

        let sequence = db::sequence_find_by_locator(&mut cx, seq_handle.locator())
            .await
            .expect("Unable to find the created sequence");

        // Check sequence locator
        assert_eq!(*seq_handle.locator(), sequence.locator());

        let session_handle = session::try_create(&context, seq_handle.locator().clone())
            .await
            .expect("Unable to create session");
        assert!(session_handle.uuid().is_valid());

        let topic_locator: types::TopicLocator = "test_sequence/test_topic".parse().unwrap();

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

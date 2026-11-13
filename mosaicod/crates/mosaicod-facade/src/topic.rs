use super::{Context, Error};
use arrow::datatypes::SchemaRef;

use mosaicod_config::params;
use mosaicod_core::error::PublicError;
use mosaicod_core::types::TopicMetadataProperties;
use mosaicod_core::{self as core, error::PublicResult as Result, types};
use mosaicod_db as db;
use mosaicod_marshal as marshal;
use mosaicod_query as query;
use mosaicod_rw::{self as rw, ToProperties};
use mosaicod_store as store;
use std::path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{trace, warn};

/// Define topic metadata type containing JSON user metadata.
pub type TopicMetadata = types::TopicMetadata<marshal::JsonMetadataBlob>;
pub type TopicOntologyMetadata = types::TopicOntologyMetadata<marshal::JsonMetadataBlob>;
pub type TopicInfo = types::TopicInfo<marshal::JsonMetadataBlob>;

pub struct TopicStreamingReadParams {
    pub metadata: TopicMetadata,
    pub optimal_batch_size: usize,
    pub data_folder_path: path::PathBuf,
}

pub(super) mod internal {
    use super::*;
    use mosaicod_core::error::PublicError;
    use mosaicod_core::types::TopicTimeWindowInfo;

    /// Creates [`TopicMetadata`] associated to the given `topic_record`.
    pub async fn metadata(
        exe: &mut impl db::AsExec,
        topic_record: &db::TopicRecord,
    ) -> Result<TopicMetadata> {
        let session_locator =
            db::session_find_by_id(exe, topic_record.session_id, db::RowLocking::None)
                .await?
                .locator();

        Ok(TopicMetadata {
            properties: TopicMetadataProperties {
                created_at: topic_record.creation_timestamp(),
                completed_at: topic_record.completion_timestamp(),
                session_locator,
                locator: topic_record.locator(),
            },
            ontology_metadata: TopicOntologyMetadata {
                serialization_format: topic_record
                    .serialization_format()
                    .ok_or_else(|| Error::MissingDbData("serialization_format".to_owned()))?,
                ontology_tag: topic_record.ontology_tag.clone(),
                user_metadata: topic_record.user_metadata(),
            },
        })
    }

    /// Private method to tell the topic status (just created, uploading data, finalized).
    ///
    /// Note: please use this function instead of [`status`] if you need to call it internally
    /// (from another function in this module that already has an active transaction).
    pub async fn status(topic_record: &db::TopicRecord) -> Result<Status> {
        if topic_record.path_in_store().is_none() {
            debug_assert!(topic_record.completion_timestamp().is_none());
            return Ok(Status::Empty);
        } else if topic_record.completion_timestamp().is_none() {
            return Ok(Status::Uploading);
        }

        debug_assert!(
            topic_record.path_in_store().is_some() && topic_record.completion_timestamp().is_some()
        );
        Ok(Status::Finalized)
    }

    /// Number of attempts made by [`finalize_timestamp_range`] to read the timestamp range of a
    /// topic whose chunks are recorded in the database.
    const FINALIZE_RANGE_ATTEMPTS: u32 = 5;

    /// Delay before the second attempt of [`finalize_timestamp_range`], doubled after every
    /// further attempt (100 + 200 + 400 + 800 ms in total).
    const FINALIZE_RANGE_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

    /// Computes first and last timestamps recorded in the topic data files.
    ///
    /// Returns `None` if the data files contain no rows. Errors of the timeseries engine
    /// (e.g. a failing object store or an unreadable data file) are returned to the caller.
    pub async fn compute_timestamp_range(
        ts_engine: query::TimeseriesEngineRef,
        topic_record: &db::TopicRecord,
    ) -> Result<Option<types::TimestampRange>> {
        let path_in_store = topic_record
            .path_in_store()
            .ok_or(Error::MissingDbData(format!(
                "No path in store set for topic {}",
                topic_record.locator()
            )))?;

        let format = topic_record
            .serialization_format()
            .ok_or_else(|| Error::MissingDbData("serialization_format".to_owned()))?;

        let timestamp_range = ts_engine
            .read(path_in_store.data_folder_path(), format, None)
            .await?
            .timestamp_range()
            .await?;

        Ok(timestamp_range)
    }

    /// Computes the timestamp range that is stored when the topic is finalized.
    ///
    /// A topic without any chunk recorded in the database (only empty batches have been
    /// sent) gets an unbounded range. If chunks are recorded, the range must be readable from
    /// the data files: the computation is retried a few times with a growing backoff, since
    /// a just-written file can be missing from an object store listing for a short time, and
    /// an error is returned if no range can be computed. A topic with data must never be
    /// finalized with an unbounded range, since readers would treat it as empty.
    pub async fn finalize_timestamp_range(
        exe: &mut impl db::AsExec,
        ts_engine: &query::TimeseriesEngineRef,
        topic_record: &db::TopicRecord,
    ) -> Result<types::TimestampRange> {
        let stats = db::topic_get_stats(exe, topic_record.topic_id).await?;
        if stats.chunks_count == 0 {
            return Ok(types::TimestampRange::unbounded());
        }

        let locator = topic_record.locator();
        let mut backoff = FINALIZE_RANGE_INITIAL_BACKOFF;
        let mut last_failure = String::new();
        for attempt in 1..=FINALIZE_RANGE_ATTEMPTS {
            last_failure = match compute_timestamp_range(ts_engine.clone(), topic_record).await {
                Ok(Some(range)) => return Ok(range),
                Ok(None) => "no data found in store".to_owned(),
                Err(e) => e.to_string(),
            };
            warn!(
                "unable to compute timestamp range for topic `{}` with {} chunk(s) recorded (attempt {}/{}): {}",
                locator, stats.chunks_count, attempt, FINALIZE_RANGE_ATTEMPTS, last_failure
            );
            if attempt < FINALIZE_RANGE_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }

        Err(core::Error::internal(Some(format!(
            "unable to finalize topic {}: {} chunk(s) recorded but no timestamp range readable \
            after {} attempts: {}",
            locator, stats.chunks_count, FINALIZE_RANGE_ATTEMPTS, last_failure
        )))
        .to_public_error())
    }

    async fn time_window_info(
        ts_engine: &query::TimeseriesEngineRef,
        topic_record: &db::TopicRecord,
        time_window: types::TimestampRange,
    ) -> Result<types::TopicTimeWindowInfo> {
        let Some(path_in_store) = &topic_record.path_in_store() else {
            return Ok(types::TopicTimeWindowInfo {
                row_count: 0,
                timestamp_range: types::TimestampRange::unbounded(),
            });
        };

        let format = topic_record
            .serialization_format()
            .ok_or_else(|| Error::MissingDbData("serialization_format".to_owned()))?;

        let path = path_in_store.data_folder_path();

        let mut query_result = ts_engine.read(&path, format, None).await?;

        query_result = query_result.filter_by_timestamp_range(time_window)?;

        // Timestamp_range can be None only if there is no data uploaded for the topic yet.
        // In that case the entire app metadata is left empty.
        let (row_count, timestamp_range) = query_result.clone().count_and_timestamp_range().await?;

        Ok(TopicTimeWindowInfo {
            row_count: row_count as u64,
            timestamp_range: timestamp_range.unwrap_or(types::TimestampRange::unbounded()),
        })
    }

    /// Creates [`TopicInfo`] associated to the given `topic_record`.
    pub async fn info(
        exe: &mut impl db::AsExec,
        ts_engine: &query::TimeseriesEngineRef,
        topic_record: &db::TopicRecord,
        time_window: Option<types::TimestampRange>,
    ) -> Result<TopicInfo> {
        let stats = db::topic_get_stats(exe, topic_record.topic_id).await?;

        let ts_range = if stats.chunks_count == 0 {
            types::TimestampRange::unbounded()
        } else {
            // Return an unbounded range instead of throwing an error regarding missing data in DB,
            // because a get_flight_info read could be performed when some chunk has already been
            // stored, but the topic is not finalized yet.
            topic_record
                .timestamp_range()
                .unwrap_or(types::TimestampRange::unbounded())
        };

        let data_info = types::TopicDataInfo {
            total_chunks: stats.chunks_count,
            total_bytes: stats.total_size_bytes,
            timestamp_range: ts_range,
            total_row_count: stats.total_row_count,
        };

        let time_window_info = if let Some(time_window) = time_window {
            Some(time_window_info(ts_engine, topic_record, time_window).await?)
        } else {
            None
        };

        Ok(TopicInfo {
            metadata: metadata(exe, topic_record).await?,
            data_info,
            time_window_info,
        })
    }

    /// Returns the topic arrow schema.
    /// The serialization format is required to extract the schema.
    /// It can be retrieved using [`metadata`] function.
    #[allow(dead_code)]
    pub async fn arrow_schema(
        ts_engine: query::TimeseriesEngineRef,
        topic_record: &db::TopicRecord,
    ) -> Result<SchemaRef> {
        let Some(path_in_store) = &topic_record.path_in_store() else {
            return Ok(mosaicod_ext::arrow::empty_schema_ref());
        };

        let format = topic_record
            .serialization_format()
            .ok_or_else(|| Error::MissingDbData("serialization_format".to_owned()))?;

        // Get chunk 0 since this chunk needs to exist always.
        // Here we use a single file and not the directory path since will improve performance.
        // Timeseries engine backend (datafusion) needs to scan only a single file avoiding to
        // read metadata about all files in the directory.
        let path = path_in_store.path_data(0, format.to_properties().as_ref());

        // If there is an error retrieving the schema return an empty
        let Ok(schema) = ts_engine.schema(path, format).await else {
            return Ok(mosaicod_ext::arrow::empty_schema_ref());
        };

        Ok(schema)
    }

    /// Computes the optimal batch size based on topic statistics from the database.
    /// The computed batch size is clamped between 1 and
    /// [`params::Params::max_batch_size`], so topics whose rows are larger
    /// than [`params::Params::grpc_target_encode_message_size`] still stream at least
    /// one row per batch instead of a degenerate batch size of 0.
    ///
    /// Returns `Some(batch_size)` if statistics are available, `None` otherwise
    /// (e.g., for empty topics).
    pub async fn compute_optimal_batch_size(
        exe: &mut impl db::AsExec,
        topic_record: &db::TopicRecord,
    ) -> Result<usize> {
        let stats = db::topic_get_stats(exe, topic_record.topic_id)
            .await
            .map_err(|e| match e {
                db::Error::NotFound => core::Error::not_found(topic_record.locator().to_string()),
                _ => e.error(),
            })?;

        if stats.total_size_bytes == 0 || stats.total_row_count == 0 {
            Err(Error::MissingDbData(
                "unable to compute optimal batch size".to_owned(),
            ))?;
        }

        let params = params::params();

        // Guard in case of average 0 to avoid panic
        let avg_bytes_per_row = (stats.avg_bytes_per_row as usize).max(1);
        let batch_size = params.grpc_target_encode_message_size.value / avg_bytes_per_row;

        Ok(batch_size.clamp(1, params.max_batch_size.value.get()))
    }
}

#[derive(PartialEq)]
pub enum Status {
    /// The topic has just been created. Still no data has been uploaded.
    Empty,
    /// The topic is uploading data.
    Uploading,
    /// The topic has been completely uploaded and finalized.
    Finalized,
}

/// Creates a new database entry for this topic.
///
/// If a record with the same name already exists, an error is returned.
///
/// Additional checks about the scope of the topic are performed. If the topic locator is
/// not a child of the related sequence locator, an unauthorized error is returned.
///
/// Returns the UUID of the newly created topic.
pub async fn try_create(
    context: &Context,
    locator: &types::TopicLocator,
    session_uuid: &types::Uuid,
    ontology_metadata: TopicOntologyMetadata,
) -> Result<types::Uuid> {
    let mut tx = context.db.transaction().await?;

    // Session must not be already finalized.
    // A shared lock is used to avoid a possible concurrent session finalize.
    // No topic can be created nor deleted during a session finalization.
    let session_record = db::session_find_by_uuid(&mut tx, session_uuid, db::RowLocking::Shared)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => {
                core::Error::not_found(format!("session with UUID {}", session_uuid))
            }
            _ => e.error(),
        })?;

    if session_record.completion_timestamp().is_some() {
        Err(core::Error::session_already_finalized(
            session_record.locator().to_string(),
        ))?;
    }

    if locator.sequence != session_record.locator().sequence {
        Err(core::Error::unauthorized(
            "provided topic locator and session do not share the same sequence".to_string(),
        ))?;
    }

    let record = db::topic_create(
        &mut tx,
        locator,
        session_uuid.clone(),
        &ontology_metadata.ontology_tag,
        &ontology_metadata.serialization_format.to_string(),
        None,
        ontology_metadata.user_metadata.map(Into::into),
    )
    .await
    .map_err(|e| match e {
        db::Error::NotFound => core::Error::not_found(locator.sequence.to_string()),
        db::Error::ForeignKeyViolation => core::Error::not_found(format!(
            "sequence {} or session with UUID {}",
            locator.sequence, session_uuid
        )),
        _ => e.error(),
    })?;

    tx.commit().await?;

    Ok(record.uuid())
}

/// Creates [`TopicInfo`] associated to the given topic `locator`.
pub async fn info(
    context: &Context,
    locator: &types::TopicLocator,
    time_window: Option<types::TimestampRange>,
) -> Result<TopicInfo> {
    let mut cx = context.db.connection();
    let topic_record = db::topic_find_by_locator(&mut cx, locator)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => core::Error::not_found(locator.to_string()),
            _ => e.error(),
        })?;
    internal::info(
        &mut cx,
        &context.timeseries_querier,
        &topic_record,
        time_window,
    )
    .await
}

/// Returns the arrow schema of the topic identified by `locator`.
pub async fn schema(context: &Context, locator: &types::TopicLocator) -> Result<SchemaRef> {
    let mut cx = context.db.connection();
    let topic_record = db::topic_find_by_locator(&mut cx, locator)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => core::Error::not_found(locator.to_string()),
            _ => e.error(),
        })?;
    internal::arrow_schema(context.timeseries_querier.clone(), &topic_record).await
}

/// Serializes and writes [`TopicMetadata`] to the object store.
///
/// # Errors
///
/// Returns an error if serialization or writing fails.
async fn metadata_write_to_store(
    context: &Context,
    path: &path::Path,
    metadata: TopicMetadata,
) -> Result<()> {
    trace!("writing topic metadata `{}` to store", path.display());

    let json_topic_metadata = marshal::JsonTopicMetadata::from(metadata);
    let bytes: Vec<u8> = json_topic_metadata.try_into()?;

    context.store.write_bytes(path, bytes).await?;

    Ok(())
}

/// Returns a writer used to write chunked record batches using a specified serialization
/// format `format`.
pub async fn writer(
    context: Context,
    locator: types::TopicLocator,
    topic_uuid: &types::Uuid,
    schema: SchemaRef,
) -> Result<HandleWriter> {
    let mut cx = context.db.connection();

    let topic_record = db::topic_find_by_locator(&mut cx, &locator)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => core::Error::not_found(locator.to_string()),
            _ => e.error(),
        })?;

    // Precondition: check if provided topic UUID matches the one corresponding to the provided locator.
    if *topic_uuid != topic_record.uuid() {
        Err(core::Error::unauthorized(
            "received uuid does not match topic uuid.".to_string(),
        ))?
    }

    // Precondition: check if topic has already been finalized or if someone else is already uploading data.
    let topic_status = internal::status(&topic_record).await?;
    match topic_status {
        Status::Empty => (),
        Status::Uploading => Err(core::Error::topic_upload_in_progress(locator.to_string()))?,
        Status::Finalized => Err(core::Error::topic_already_finalized(locator.to_string()))?,
    }

    let mdata = internal::metadata(&mut cx, &topic_record).await?;

    // Set up the callback that will be used to create the database record for the data catalog
    // and prepare variables that will be moved in the closure
    let ontology_tag = mdata.ontology_metadata.ontology_tag.clone();
    let format = mdata.ontology_metadata.serialization_format;

    // Create random folder for the Store.
    let path_in_store = types::TopicPathInStore::new();

    // 1. Save path_in_store on DB.
    //
    // Note1: Updating it only if NULL is mainly intended as a barrier, preventing other writers
    // to start while one is already uploading data.
    //
    // Note2: we want to prevent the newly created folder in the store from being marked as TO_DELETE by the cleanup routine.
    // That's why we update the DB record as first thing.
    let topic_updated = db::topic_update_path_in_store_if_null(
        &mut cx,
        topic_record.topic_id,
        path_in_store.clone(),
    )
    .await?;

    // If the path_in_store update fails, it can be that the topic has been deleted by a concurrent request,
    // or another do_put has already started. In this case refresh the topic record and check its status.
    if !topic_updated {
        let topic_record =
            db::topic_find_by_locator(&mut cx, &locator)
                .await
                .map_err(|e| match e {
                    db::Error::NotFound => core::Error::not_found(locator.to_string()),
                    _ => e.error(),
                })?;
        let topic_status = internal::status(&topic_record).await?;
        match topic_status {
            Status::Empty => Err(core::Error::internal(Some(format!(
                "can't set path_in_store for topic {}.\
            This may be due to a concurrent do_put that fails during initialization",
                locator
            ))))?,
            Status::Uploading => Err(core::Error::topic_upload_in_progress(locator.to_string()))?,
            Status::Finalized => Err(core::Error::topic_already_finalized(locator.to_string()))?,
        }
    }

    // 2. Save metadata to Store.
    let res =
        metadata_write_to_store(&context, path_in_store.path_metadata().as_path(), mdata).await;

    // Rollback: remove path_in_store from the topic db entry.
    if let Err(e) = res {
        trace!(
            "Rollback: remove path_in_store from topic {}",
            topic_record.locator()
        );
        let deleted = db::topic_delete_path_in_store(&mut cx, topic_record.topic_id).await?;
        if !deleted {
            Err(core::Error::not_found(topic_record.locator().to_string()))?;
        }
        return Err(e);
    }

    // 3. Create ChunkWriter.
    let data_folder = path_in_store.data_folder_path();
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

    Ok(HandleWriter {
        topic_id: topic_record.topic_id,
        topic_locator: locator,
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
    locator: &types::TopicLocator,
    allowed_data_loss: types::DataLossToken,
) -> Result<()> {
    warn!("(data loss) deleting topic '{}'", locator);
    let mut tx = context.db.transaction().await?;

    // Lock the session record to prevent races with session finalize.
    let topic_record = db::topic_find_by_locator(&mut tx, locator).await?;
    db::session_find_by_id(&mut tx, topic_record.session_id, db::RowLocking::Shared)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => {
                core::Error::not_found(format!("session with id {}", topic_record.session_id))
            }
            _ => e.error(),
        })?;

    db::topic_delete(&mut tx, locator, allowed_data_loss).await?;

    tx.commit().await?;

    Ok(())
}

/// Add a notification to the sequence.
pub async fn notify(
    context: &Context,
    locator: &types::TopicLocator,
    ntype: types::NotificationType,
    msg: &str,
) -> Result<types::Notification<types::TopicLocator>> {
    let mut cx = context.db.connection();

    let notification_record = db::topic_notification_create(&mut cx, locator, ntype, msg)
        .await
        .map_err(|e| match e {
            db::Error::NotFound | db::Error::ForeignKeyViolation => {
                core::Error::not_found(locator.to_string())
            }
            _ => core::Error::internal(None),
        })?;

    Ok(notification_record.into_notification(locator.clone()))
}

/// Returns a list of all notifications for the topic.
pub async fn notification_list(
    context: &Context,
    locator: &types::TopicLocator,
) -> Result<Vec<types::Notification<types::TopicLocator>>> {
    let mut cx = context.db.connection();
    let notifications = db::topic_notifications_find_by_locator(&mut cx, locator).await?;
    Ok(notifications
        .into_iter()
        .map(|e| e.into_notification(locator.clone()))
        .collect())
}

/// Deletes all the notifications associated with the sequence.
pub async fn notification_purge(context: &Context, locator: &types::TopicLocator) -> Result<()> {
    let mut cx = context.db.connection();
    db::topic_notifications_purge(&mut cx, locator).await?;
    Ok(())
}

pub async fn streaming_read_prepare(
    context: &Context,
    locator: &types::TopicLocator,
) -> Result<TopicStreamingReadParams> {
    let mut cx = context.db.connection();

    let topic_record = db::topic_find_by_locator(&mut cx, locator)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => core::Error::not_found(locator.to_string()),
            _ => e.error(),
        })?;

    // If topic is empty (no data has been loaded yet), do_get must fail.
    let topic_status = internal::status(&topic_record).await?;

    if topic_status == Status::Empty {
        Err(core::Error::missing_doput(locator.to_string()))?
    }

    // Here path_in_store should be already set and available,
    // otherwise the check on the topic status should have failed.
    // That's why an internal error is returned.
    let path_in_store = topic_record
        .path_in_store()
        .ok_or(core::error::Error::internal(Some(format!(
            "Path in store not set for topic {}",
            locator
        ))))?;

    Ok(TopicStreamingReadParams {
        metadata: internal::metadata(&mut cx, &topic_record).await?,
        optimal_batch_size: internal::compute_optimal_batch_size(&mut cx, &topic_record).await?,
        data_folder_path: path_in_store.data_folder_path(),
    })
}

/// A guard ensuring exclusive write access to the topic.
///
/// While this struct exists, the underlying topic is mutably borrowed, preventing
/// any other operations (such as locking or concurrent reads) until [`HandleWriter::finalize`] is called.
pub struct HandleWriter {
    topic_id: i32,

    topic_locator: types::TopicLocator,

    ontology_tag: String,

    /// The underlying writer handling the actual data operations.
    writer: rw::ChunkWriter<Arc<store::Store>>,

    /// Context containing query engine for timeseries data used to finalize topic data at the end of write process.
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

        let topic_record =
            db::topic_find_by_id(&mut tx, self.topic_id)
                .await
                .map_err(|e| match e {
                    db::Error::NotFound => core::Error::not_found(self.topic_locator.to_string()),
                    _ => core::Error::internal(Some(format!("an error occurred during topic finalization while trying to access topic record {}", self.topic_locator))),
                })?;

        let topic_locator = topic_record.locator();

        // ts_range is unbounded only if no chunk has been recorded (only empty batches have
        // been sent). Errors are returned, so that a topic with data is never finalized as empty.
        let ts_range = internal::finalize_timestamp_range(
            &mut tx,
            &self.context.timeseries_querier,
            &topic_record,
        )
        .await?;

        db::topic_update_index_timestamp_range(&mut tx, &topic_locator, ts_range).await?;

        // Check if topic has already been uploaded and finalized.
        if let Status::Finalized = internal::status(&topic_record).await? {
            Err(core::Error::topic_already_finalized(
                topic_locator.to_string(),
            ))?;
        }

        // Update completion timestamp
        let completed_at = types::Timestamp::now();
        db::topic_update_completion_tstamp(&mut tx, self.topic_id, completed_at.as_i64()).await?;

        tx.commit().await?;

        // 2. Update metadata in Store.
        let mut cx = self.context.db.connection();
        let mut metadata = internal::metadata(&mut cx, &topic_record).await?;
        metadata.properties.completed_at = Some(completed_at);

        let path_in_store = topic_record
            .path_in_store()
            .ok_or(Error::MissingDbData(format!(
                "No path in store set for topic {}",
                topic_locator
            )))?;

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
    use crate::{sequence, session};
    use mosaicod_core::{self as core, types::NotificationType};
    use mosaicod_query as query;

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new((*store).clone(), 0).unwrap());

        Context::new((*store).clone(), (*database).clone(), ts_gw)
    }

    fn dummy_ontology_metadata() -> TopicOntologyMetadata {
        types::TopicOntologyMetadata::new("dummy".to_owned(), types::Format::Default, None)
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();

        sequence::try_create(&context, &seq_locator, None)
            .await
            .expect("Error creating sequence");

        let seq_record = db::sequence_find_by_locator(&mut context.db.connection(), &seq_locator)
            .await
            .unwrap();

        // Check sequence locator
        assert_eq!(*seq_record.locator(), *seq_locator);

        let (_, session_uuid) = session::try_create(&context, seq_record.locator().clone())
            .await
            .unwrap();

        let topic_locator = "test_sequence/test_topic"
            .parse::<types::TopicLocator>()
            .unwrap();

        let topic_uuid = try_create(
            &context,
            &topic_locator,
            &session_uuid,
            dummy_ontology_metadata(),
        )
        .await
        .expect("Unable to create topic");

        assert!(topic_uuid.is_valid());

        let topic_record = db::topic_find_by_locator(&mut context.db.connection(), &topic_locator)
            .await
            .unwrap();

        // Check topic locator.
        assert_eq!(topic_record.topic_id, 1);
        assert_eq!(topic_record.locator(), topic_locator);

        // Check path in store
        assert!(topic_record.path_in_store().is_none());

        // Check topic deletion.
        delete(&context, &topic_record.locator(), types::allow_data_loss())
            .await
            .unwrap();

        assert!(
            db::topic_find_by_locator(
                &mut context.db.connection(),
                &"test_sequence/test_topic".parse().unwrap()
            )
            .await
            .is_err()
        );
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_notify_for_non_existent_topic(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let topic_locator = "test_sequence/ghost_topic"
            .parse::<types::TopicLocator>()
            .unwrap();

        let res = notify(
            &context,
            &topic_locator,
            NotificationType::Error,
            "test notification message",
        )
        .await
        .unwrap_err()
        .error();

        assert!(matches!(res.kind(), core::ErrorKind::NotFound(_)));
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_notify_and_notify_purge(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();

        sequence::try_create(&context, &seq_locator, None)
            .await
            .expect("Unable to create sequence");

        // Check if sequence was created
        let mut cx = context.db.connection();

        let sequence = db::sequence_find_by_locator(&mut cx, &seq_locator)
            .await
            .expect("Unable to find the created sequence");

        // Check sequence locator
        assert_eq!(*seq_locator, *sequence.locator());

        let (_, session_uuid) = session::try_create(&context, seq_locator.clone())
            .await
            .expect("Unable to create session");
        assert!(session_uuid.is_valid());

        // Create 2 topics and add notifications to the second one because it has an ID different from the sequence's one.

        let topic_locator: types::TopicLocator = "test_sequence/test_topic".parse().unwrap();

        try_create(
            &context,
            &topic_locator,
            &session_uuid,
            dummy_ontology_metadata(),
        )
        .await
        .expect("Unable to create topic");

        let topic_locator2: types::TopicLocator = "test_sequence/test_topic2".parse().unwrap();

        try_create(
            &context,
            &topic_locator2,
            &session_uuid,
            dummy_ontology_metadata(),
        )
        .await
        .expect("Unable to create topic");

        notify(
            &context,
            &topic_locator2,
            NotificationType::Error,
            "test notification message",
        )
        .await
        .expect("Error creating notification message");

        notify(
            &context,
            &topic_locator2,
            NotificationType::Error,
            "test notification message 2",
        )
        .await
        .expect("Error creating notification message");

        let topic = db::topic_find_by_locator(&mut cx, &topic_locator2)
            .await
            .expect("Unable to find the created topic");

        // Check if notifications were created on database.
        let notifications = db::topic_notifications_find_by_locator(&mut cx, &topic_locator2)
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

        notification_purge(&context, &topic_locator2)
            .await
            .expect("Unable to purge notifications");

        // Check there are no more notifications on database.
        assert!(
            db::topic_notifications_find_by_locator(&mut cx, &topic_locator2)
                .await
                .unwrap()
                .is_empty()
        );
    }

    // --- Finalization and timestamp range (issue #1102) ---

    /// Like [`test_context`], but also returns the temporary store, which must be kept alive
    /// by the test, so the test can manipulate the data files on disk.
    fn test_context_with_store(
        pool: sqlx::Pool<db::DatabaseType>,
    ) -> (Context, store::testing::Store) {
        // Chunk encoding reads the daemon parameters.
        params::load_params(params::ParamsLoadOptions::testing()).unwrap();

        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new((*store).clone(), 0).unwrap());

        (
            Context::new((*store).clone(), (*database).clone(), ts_gw),
            store,
        )
    }

    fn test_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                core::constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
                arrow::datatypes::DataType::Int64,
                false,
            ),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
        ]))
    }

    fn test_batch(timestamps: &[i64]) -> arrow::array::RecordBatch {
        let values: Vec<f64> = timestamps.iter().map(|t| *t as f64).collect();
        arrow::array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(arrow::array::Int64Array::from(timestamps.to_vec())),
                Arc::new(arrow::array::Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    /// Creates sequence, session and topic, and returns a writer for the topic.
    async fn create_topic_writer(
        context: &Context,
    ) -> (types::TopicLocator, types::Uuid, HandleWriter) {
        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();
        sequence::try_create(context, &seq_locator, None)
            .await
            .unwrap();
        let (_, session_uuid) = session::try_create(context, seq_locator).await.unwrap();

        let topic_locator = "test_sequence/test_topic"
            .parse::<types::TopicLocator>()
            .unwrap();
        let topic_uuid = try_create(
            context,
            &topic_locator,
            &session_uuid,
            dummy_ontology_metadata(),
        )
        .await
        .unwrap();

        let writer = writer(
            context.clone(),
            topic_locator.clone(),
            &topic_uuid,
            test_schema(),
        )
        .await
        .unwrap();

        (topic_locator, topic_uuid, writer)
    }

    /// Writes a chunk to the store and records it in the database, as `do_put` does.
    /// Returns the path of the chunk file in the store.
    async fn write_chunk(
        context: &Context,
        writer: &mut HandleWriter,
        topic_uuid: &types::Uuid,
        timestamps: &[i64],
    ) -> path::PathBuf {
        let chunk = writer.write(test_batch(timestamps)).await.unwrap();
        let ontology_tag = writer.ontology_tag().to_owned();

        let mut tx = context.db.transaction().await.unwrap();
        crate::update_chunk_stats(
            &mut tx,
            topic_uuid,
            &chunk.path,
            chunk.metadata,
            &ontology_tag,
            chunk.ontology_stats,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        chunk.path
    }

    async fn find_topic(context: &Context, locator: &types::TopicLocator) -> db::TopicRecord {
        db::topic_find_by_locator(&mut context.db.connection(), locator)
            .await
            .unwrap()
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_finalize_stores_range_of_written_data(pool: sqlx::Pool<db::DatabaseType>) {
        let (context, _store) = test_context_with_store(pool);
        let (locator, uuid, mut writer) = create_topic_writer(&context).await;
        write_chunk(&context, &mut writer, &uuid, &[10, 20, 30]).await;

        writer.finalize().await.unwrap();

        let record = find_topic(&context, &locator).await;
        assert!(record.completion_timestamp().is_some());
        let range = record.timestamp_range().unwrap();
        assert_eq!(range.start.as_i64(), 10);
        assert_eq!(range.end.as_i64(), 30);
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_finalize_without_chunks_stores_unbounded_range(
        pool: sqlx::Pool<db::DatabaseType>,
    ) {
        let (context, _store) = test_context_with_store(pool);
        let (locator, _, writer) = create_topic_writer(&context).await;

        // Only empty batches sent: no chunk is written nor recorded.
        writer.finalize().await.unwrap();

        let record = find_topic(&context, &locator).await;
        assert!(record.completion_timestamp().is_some());
        assert!(record.timestamp_range().unwrap().is_unbounded());
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_finalize_fails_if_range_query_fails(pool: sqlx::Pool<db::DatabaseType>) {
        let (context, store) = test_context_with_store(pool);
        let (locator, uuid, mut writer) = create_topic_writer(&context).await;
        let chunk_path = write_chunk(&context, &mut writer, &uuid, &[10, 20, 30]).await;

        // The data file can not be read by the timeseries engine.
        std::fs::write(store.root.join(&chunk_path), b"not a parquet file").unwrap();

        let err = writer.finalize().await.unwrap_err().to_string();
        assert!(
            err.contains("1 chunk(s) recorded"),
            "unexpected error: {err}"
        );

        let record = find_topic(&context, &locator).await;
        assert!(record.completion_timestamp().is_none());
        assert!(record.timestamp_range().is_none());
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_finalize_fails_if_recorded_chunks_are_not_in_store(
        pool: sqlx::Pool<db::DatabaseType>,
    ) {
        let (context, store) = test_context_with_store(pool);
        let (locator, uuid, mut writer) = create_topic_writer(&context).await;
        let chunk_path = write_chunk(&context, &mut writer, &uuid, &[10, 20, 30]).await;

        // The chunk is recorded in the database but its file is not visible in the store.
        std::fs::remove_file(store.root.join(&chunk_path)).unwrap();

        let err = writer.finalize().await.unwrap_err().to_string();
        assert!(
            err.contains("1 chunk(s) recorded"),
            "unexpected error: {err}"
        );

        let record = find_topic(&context, &locator).await;
        assert!(record.completion_timestamp().is_none());
        assert!(record.timestamp_range().is_none());
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn topic_finalize_retries_until_chunk_is_visible(pool: sqlx::Pool<db::DatabaseType>) {
        let (context, store) = test_context_with_store(pool);
        let (locator, uuid, mut writer) = create_topic_writer(&context).await;
        let chunk_path = write_chunk(&context, &mut writer, &uuid, &[10, 20, 30]).await;

        // Hide the chunk file and make it visible again after a delay shorter than the
        // retry window, as an object store listing that lags behind a completed write.
        let visible = store.root.join(&chunk_path);
        let hidden = store.root.join("hidden-chunk");
        std::fs::rename(&visible, &hidden).unwrap();
        let reappear = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            std::fs::rename(&hidden, &visible).unwrap();
        });

        writer.finalize().await.unwrap();
        reappear.await.unwrap();

        let record = find_topic(&context, &locator).await;
        assert!(record.completion_timestamp().is_some());
        let range = record.timestamp_range().unwrap();
        assert_eq!(range.start.as_i64(), 10);
        assert_eq!(range.end.as_i64(), 30);
    }
}

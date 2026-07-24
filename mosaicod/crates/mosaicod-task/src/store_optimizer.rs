//! This module provides the optimization routine that improves the efficiency of the files in the object store,
//! merging small files into bigger ones.

use datafusion as df;
use futures::StreamExt;
use mosaicod_core::{
    self as core, error::PublicResult as Result, params, traits::AsyncWriteToPath, types,
};
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_rw::{self as rw, SerializedChunk, format::ToProperties};
use mosaicod_store as store;
use std::cmp::max;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const DEFAULT_TIME_INTERVAL: u32 = 86400;

// Default max size for merged files.
pub const DEFAULT_MAX_OUTPUT_FILE_SIZE: usize = 256 * 1024 * 1024; // Bytes

const OUTPUT_FILE_FILLING_PERCENTAGE: f32 = 0.10;

const MAX_LEASE_NS: i64 = 7 * 86400 * 1_000_000_000; // 7 days (in nanoseconds)

fn df_to_internal_error(err: df::error::DataFusionError) -> core::Error {
    let err_msg = format!("datafusion error: {}", err);
    core::Error::internal(Some(err_msg))
}

pub struct StoreOptimizer {
    db: db::Database,
    store: store::StoreRef,
    time_interval: types::Duration,
    // Soft threshold to split the output files.
    // The value should stay between 128MB and 512MB for best performance with object store.
    max_file_size: usize,
}

impl StoreOptimizer {
    /// Creates a new optimizer routine with default [`time_interval`].
    pub fn new(db: db::Database, store: store::StoreRef) -> Self {
        Self {
            db,
            store,
            time_interval: types::Duration::seconds(DEFAULT_TIME_INTERVAL),
            max_file_size: DEFAULT_MAX_OUTPUT_FILE_SIZE,
        }
    }

    pub fn with_time_interval(mut self, time_interval: types::Duration) -> Self {
        self.time_interval = time_interval;
        self
    }

    pub fn with_max_file_size(mut self, max_file_size: usize) -> Self {
        self.max_file_size = max_file_size;
        self
    }

    /// Gets the max row size between every chunk for the given topic.
    pub async fn max_row_size(&self, topic_record: &db::TopicRecord) -> Result<u32> {
        let mut cx = self.db.connection();

        let max_row_size = db::topic_chunk_max_row_size(&mut cx, topic_record.topic_id)
            .await
            .map_err(|e| {
                let err_msg = format!(
                    "database error while accessing max row size for topic {}: {}",
                    topic_record.locator(),
                    e
                );
                core::Error::internal(Some(err_msg))
            })?
            .ok_or_else(|| {
                let err_msg = format!(
                    "missing chunk stats in DB for topic {}",
                    topic_record.locator()
                );
                core::Error::internal(Some(err_msg))
            })?;

        Ok(max_row_size as u32)
    }

    /// Flushes the buffer on disk composing the path with [`path_in_store`] and [`data_file`].
    async fn flush_chunk(
        &self,
        buffer: Vec<u8>,
        path_in_store: &types::TopicPathInStore,
        data_file: &std::path::Path,
    ) -> Result<std::path::PathBuf> {
        let path = path_in_store.data_folder_path().join(data_file);

        self.store
            .write_to_path(&path, buffer)
            .await
            .map_err(|e| core::Error::internal(Some(e.to_string())))?;

        Ok(path)
    }

    /// Replaces old chunk stats with new ones.
    async fn update_chunk_stats(
        &self,
        tx: &mut db::Tx<'_>,
        topic: &db::TopicRecord,
        chunk_stats: Vec<SerializedChunk>,
    ) -> Result<()> {
        // Remove old stats.
        db::chunk_delete_by_topic_id(tx, topic.topic_id, types::allow_data_loss()).await?;

        // Save new stats.
        for stats in chunk_stats {
            facade::update_chunk_stats(
                tx,
                &topic.uuid(),
                &stats.path,
                stats.metadata.size_bytes as i64,
                stats.metadata.row_count as i64,
                &topic.ontology_tag,
                stats.ontology_stats,
            )
            .await?;
        }

        Ok(())
    }

    /// Optimizes data for the given topic.
    pub async fn optimize_topic(
        &self,
        topic_record: &db::TopicRecord,
        output_path_in_store: &types::TopicPathInStore,
    ) -> Result<Vec<SerializedChunk>> {
        debug!(
            "Store optimization for topic {} started",
            topic_record.locator()
        );

        // Estimates the batch size considering to fill iteratively the target output file size by a percentage.
        // If the size of a single row is greater than max_file_size, set batcb_size to 1.
        let batch_size = max(
            (self.max_file_size as f32 * OUTPUT_FILE_FILLING_PERCENTAGE) as usize
                / self.max_row_size(topic_record).await? as usize,
            1,
        );

        // Configure the session settings for file compaction
        let config = df::execution::config::SessionConfig::new().with_batch_size(batch_size);

        let runtime = Arc::new(
            df::execution::runtime_env::RuntimeEnvBuilder::new()
                .with_object_store_registry(self.store.registry())
                .build()
                .map_err(df_to_internal_error)?,
        );

        let session_ctx =
            df::execution::context::SessionContext::new_with_config_rt(config, runtime);

        let Some(pis) = topic_record.path_in_store() else {
            let err_msg = format!("path in store not set for topic {}", topic_record.locator());
            Err(core::Error::internal(Some(err_msg)))?
        };

        // Copy metadata.json into new path in store before switch.
        let metadata = self.store.read_bytes(pis.path_metadata()).await?;
        self.store
            .write_bytes(output_path_in_store.path_metadata(), metadata)
            .await?;

        let input_folder = self
            .store
            .url_schema
            .join(&(pis.data_folder_path().to_string_lossy() + "/"))
            .map_err(|e| {
                let err_msg = format!(
                    "error composing input directory path for topic {}: {}",
                    topic_record.locator(),
                    e
                );
                core::Error::internal(Some(err_msg))
            })?;

        let df = session_ctx
            .read_parquet(
                input_folder.as_str(),
                df::datasource::file_format::options::ParquetReadOptions::default(),
            )
            .await
            .map_err(df_to_internal_error)?;

        // Preserve and ensure global chronological sort order.
        // Because DataFusion scans files via multiple parallel threads, sorting explicitly
        // guarantees that the new larger files are neatly segmented by time.
        let sorted_df = df
            .sort(vec![
                df::logical_expr::col(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)
                    .sort(true, true),
            ])
            .map_err(df_to_internal_error)?;

        let schema = Arc::new(sorted_df.schema().as_arrow().clone());

        let mut batch_stream = sorted_df
            .execute_stream()
            .await
            .map_err(df_to_internal_error)?;

        let mut chunk_idx: usize = 0;

        let Some(format) = topic_record.serialization_format() else {
            let err_msg = format!(
                "missing serialization format in DB for topic {}",
                topic_record.locator()
            );
            Err(core::Error::internal(Some(err_msg)))?
        };

        let mut chunk_stats: Vec<SerializedChunk> = vec![];

        let mut writer = rw::InMemoryChunkEncoder::try_new(schema.clone(), format)?;

        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result.map_err(df_to_internal_error)?;

            // Offload CPU-intensive parquet encoding/compression to blocking thread pool
            writer = tokio::task::spawn_blocking(move || -> Result<_> {
                writer.write(&batch)?;
                Ok(writer)
            })
            .await
            .map_err(|e| core::Error::internal(Some(e.to_string())))??;

            let estimated_size = writer.bytes_written() + writer.in_progress_size();

            if estimated_size >= self.max_file_size {
                let (buffer, stats, chunk_metadata) = writer.finalize()?;

                let chunk_path = self
                    .flush_chunk(
                        buffer,
                        output_path_in_store,
                        types::TopicPathInStore::data_file(
                            chunk_idx,
                            format.to_properties().as_ref(),
                        )
                        .as_ref(),
                    )
                    .await?;

                chunk_stats.push(SerializedChunk {
                    path: chunk_path,
                    metadata: chunk_metadata,
                    ontology_stats: stats,
                });

                chunk_idx += 1;

                writer = rw::InMemoryChunkEncoder::try_new(schema.clone(), format)?;
            }
        }

        // Finalize the last writer.
        if writer.bytes_written() + writer.in_progress_size() > 0 {
            let (buffer, stats, chunk_metadata) = writer.finalize()?;

            let chunk_path = self
                .flush_chunk(
                    buffer,
                    output_path_in_store,
                    types::TopicPathInStore::data_file(chunk_idx, format.to_properties().as_ref())
                        .as_ref(),
                )
                .await?;

            chunk_stats.push(SerializedChunk {
                path: chunk_path,
                metadata: chunk_metadata,
                ontology_stats: stats,
            });
        }

        Ok(chunk_stats)
    }

    async fn optimize(&self) -> Result<u32> {
        // Check for stale topics (if the optimization has started too long ago, then we can assume that something went wrong).
        // In this case remove the topic from the list (it will be re-added later (see below).
        let stale_deleted = db::topic_optimization_delete_stale(
            &mut self.db.connection(),
            (types::Timestamp::now().as_i64() - MAX_LEASE_NS).into(),
        )
        .await?;

        debug!(
            "stale topics deleted from optimization table: {}",
            stale_deleted
        );

        // Scans the database to search for topics not yet optimized and to put them inside topic optimization table.
        let inserted_topics = db::topic_update_optimization_list(&mut self.db.connection()).await?;

        let mut optimized_topics = 0;

        loop {
            let mut tx = self.db.transaction().await?;

            if let Some(topic_to_optimize_record) = db::topic_next_to_be_optimized(&mut tx).await? {
                // Update start_unix_tstamp and opt_path_in_store for the retrieved topic_to_optimize_record.
                let opt_path_in_store = types::TopicPathInStore::new();

                db::topic_start_optimization(
                    &mut tx,
                    topic_to_optimize_record.topic_id,
                    types::Timestamp::now(),
                    opt_path_in_store.clone(),
                )
                .await?;

                tx.commit().await?;

                let topic_record = db::topic_find_by_id(
                    &mut self.db.connection(),
                    topic_to_optimize_record.topic_id,
                )
                .await?;

                let chunk_stats = self
                    .optimize_topic(&topic_record, &opt_path_in_store)
                    .await?;

                // Remove topic from optimization list once processed.
                db::topic_optimization_delete(
                    &mut self.db.connection(),
                    topic_to_optimize_record.topic_id,
                    types::allow_data_loss(),
                )
                .await?;

                let mut tx = self.db.transaction().await?;

                db::topic_optimization_complete(
                    &mut tx,
                    topic_to_optimize_record.topic_id,
                    types::Timestamp::now().as_i64(),
                    opt_path_in_store,
                )
                .await?;

                self.update_chunk_stats(&mut tx, &topic_record, chunk_stats)
                    .await?;

                tx.commit().await?;

                optimized_topics += 1;
            } else {
                break;
            }
        }

        Ok(optimized_topics)
    }

    /// Starts the optimization routine every [`time_interval`].
    pub async fn run(self, shutdown_notifier: CancellationToken) {
        loop {
            info!("Store optimization routine started");

            match self.optimize().await {
                Ok(optimized_topics) => info!(
                    "Store optimization routine completed: {} topics optimized",
                    optimized_topics
                ),
                Err(e) => error!("Store optimization routine failed: {}", e),
            }

            // If time interval is set to 0, exit after the first run.
            if self.time_interval.is_zero() {
                return;
            }

            tokio::select! {
                // Here we can call .unwrap() safely because duration is non-negative by construction.
                _ = tokio::time::sleep(self.time_interval.to_std().unwrap()) => {
                }
                _ = shutdown_notifier.cancelled() => {
                    info!("Exiting store optimization background routine. Shutdown received.");
                    break; // Exit the loop immediately
                }
            }
        }
    }
}

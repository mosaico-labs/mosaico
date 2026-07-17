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
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const DEFAULT_TIME_INTERVAL: u32 = 86400;

// Default max size for merged files.
pub const DEFAULT_MAX_OUTPUT_FILE_SIZE: i64 = 512 * 1024 * 1024; // Bytes

fn df_to_internal_error(err: df::error::DataFusionError) -> core::Error {
    let err_msg = format!("datafusion error: {}", err);
    core::Error::internal(Some(err_msg))
}

pub struct StoreOptimizer {
    db: db::Database,
    store: store::StoreRef,
    time_interval: types::Duration,
    max_file_size: i64,
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

    pub fn with_max_file_size(mut self, max_file_size: i64) -> Self {
        self.max_file_size = max_file_size;
        self
    }

    pub async fn max_rows_per_output_file(&self, topic_record: &db::TopicRecord) -> Result<u32> {
        let mut cx = self.db.connection();

        let stats = db::topic_get_stats(&mut cx, topic_record.topic_id)
            .await
            .map_err(|e| match e {
                db::Error::NotFound => core::Error::not_found(topic_record.locator().to_string()),
                _ => {
                    let err_msg = format!(
                        "database error while accessing stats for topic {}: {}",
                        topic_record.locator(),
                        e
                    );
                    core::Error::internal(Some(err_msg))
                }
            })?;

        if stats.total_size_bytes == 0 || stats.total_row_count == 0 {
            let err_msg = format!("missing stats in DB for topic {}", topic_record.locator());
            Err(core::Error::internal(Some(err_msg)))?;
        }

        let max_rows_per_output_file =
            self.max_file_size / (stats.total_size_bytes / stats.total_row_count);

        Ok(max_rows_per_output_file as u32)
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

        let max_rows = self.max_rows_per_output_file(topic_record).await? as usize;

        // Configure the session settings for file compaction
        let config = df::execution::config::SessionConfig::new().with_batch_size(max_rows);

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

        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result.map_err(df_to_internal_error)?;

            let mut writer = rw::InMemoryChunkEncoder::try_new(schema.clone(), format)?;

            // Offload CPU-intensive parquet encoding/compression to blocking thread pool
            writer = tokio::task::spawn_blocking(move || -> Result<_> {
                writer.write(&batch)?;
                Ok(writer)
            })
            .await
            .map_err(|e| core::Error::internal(Some(e.to_string())))??;

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

            chunk_idx += 1;
        }

        Ok(chunk_stats)
    }

    async fn optimize(&self) -> Result<()> {
        // Scans the database to search for topics not yet optimized and to put them inside topic optimization table.
        db::topic_update_optimization_list(&mut self.db.connection()).await?;

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
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Starts the optimization routine every [`time_interval`].
    pub async fn run(self, shutdown_notifier: CancellationToken) {
        info!("Launching store optimization background routine");

        loop {
            if let Err(e) = self.optimize().await {
                error!("Store optimization failed: {}", e);
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

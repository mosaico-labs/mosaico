//! This module provides the optimization routine that improves the efficiency of the files in the
//! object store, merging small files into bigger ones.
//!
//! # Overview
//!
//! [`StoreOptimizer::run`] loops forever (or once, if `time_interval` is zero), sleeping for
//! `time_interval` between runs unless a shutdown is requested. Each iteration ([`StoreOptimizer::optimize`]):
//!
//! 1. Reclaims topics whose optimization lease is older than [`MAX_LEASE_NS`] (a previous run
//!    likely crashed or was killed mid-optimization) and re-scans the database for topics that
//!    need optimizing, queuing them in the `topic_optimization` table.
//! 2. Acquires and processes queued topics one at a time. Acquiring a topic
//!    ([`StoreOptimizer::acquire_next_topic`]) leases it (so concurrent optimizer instances don't
//!    race on the same topic) and allocates a fresh [`types::TopicPathInStore`] that the rewritten
//!    data will be written to, leaving the topic's current files untouched until the new ones are
//!    ready.
//! 3. A topic is optimized ([`StoreOptimizer::optimize_topic`]) by reading all of its existing
//!    Parquet chunks through DataFusion, sorting the rows chronologically, and re-encoding the
//!    stream into new chunks. Chunks are flushed once they reach roughly `max_file_size` bytes,
//!    with the DataFusion batch size tuned so the underlying encoder fills each output file
//!    efficiently. Schema and field-level metadata (e.g. anything a client attached at `do_put`
//!    time) is preserved verbatim across the rewrite.
//! 4. Once all chunks are written, the topic's DB record is atomically updated in a single
//!    transaction: old chunk stats are replaced with the new ones, the topic's `path_in_store` is
//!    switched to the freshly written data, and the topic is removed from the optimization queue.
//!
//! If optimizing a topic fails, it is simply dropped from the optimization queue (rather than
//! left leased) so it gets picked up again on a future run; its original data is never modified,
//! so a failure is always safe to retry.

use datafusion as df;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
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
use tracing::{debug, error, info, warn};

const DEFAULT_TIME_INTERVAL: u32 = 86400;

// Default max size for merged files.
pub const DEFAULT_MAX_OUTPUT_FILE_SIZE: usize = 256 * 1024 * 1024; // Bytes

const OUTPUT_FILE_FILLING_PERCENTAGE: f32 = 0.10;

const MAX_LEASE_NS: i64 = 7 * 86400 * 1_000_000_000; // 7 days (in nanoseconds)

fn df_to_internal_error(err: df::error::DataFusionError) -> core::Error {
    let err_msg = format!("datafusion error: {}", err);
    core::Error::internal(Some(err_msg))
}

#[derive(Default, Debug)]
struct OptimizationResult {
    completed: Vec<types::TopicLocator>,
    // String in the tuple represents the error.
    failed: Vec<(types::TopicLocator, String)>,
}

struct AcquiredTopic {
    topic_record: db::TopicRecord,
    opt_path_in_store: types::TopicPathInStore,
}

pub struct StoreOptimizer {
    db: db::Database,
    store: store::StoreRef,
    time_interval: types::Duration,
    // Soft threshold to split the output files.
    // The value should stay between 128MB and 512MB for best performance with object store.
    max_file_size: usize,
    result: OptimizationResult,
}

impl StoreOptimizer {
    /// Creates a new optimizer routine with default [`time_interval`].
    pub fn new(db: db::Database, store: store::StoreRef) -> Self {
        Self {
            db,
            store,
            time_interval: types::Duration::seconds(DEFAULT_TIME_INTERVAL),
            max_file_size: DEFAULT_MAX_OUTPUT_FILE_SIZE,
            result: OptimizationResult::default(),
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

    /// Gets the max average row size between every chunk for the given topic.
    async fn max_avg_row_size(&self, topic_record: &db::TopicRecord) -> Result<u32> {
        let mut cx = self.db.connection();

        let max_avg_row_size = db::topic_chunk_max_avg_row_size(&mut cx, topic_record.topic_id)
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

        Ok(max_avg_row_size as u32)
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
                stats.metadata,
                &topic.ontology_tag,
                stats.ontology_stats,
            )
            .await?;
        }

        Ok(())
    }

    /// Optimizes data for the given topic.
    async fn optimize_topic(&self, acquired_topic: &AcquiredTopic) -> Result<()> {
        debug!(
            "Store optimization for topic {} started",
            acquired_topic.topic_record.locator()
        );

        // Estimates the batch size considering to fill iteratively the target output file size by a percentage.
        // If max average row size is zero or the size of a single row is greater than max_file_size, set batch_size to 1.
        let max_avg_row_size = self.max_avg_row_size(&acquired_topic.topic_record).await?;
        let batch_size = if max_avg_row_size == 0 {
            warn!(
                "max avg row size for topic {} is 0, clamping batch size to 1 to avoid division by zero",
                acquired_topic.topic_record.locator()
            );
            1
        } else {
            max(
                (self.max_file_size as f32 * OUTPUT_FILE_FILLING_PERCENTAGE) as usize
                    / max_avg_row_size as usize,
                1,
            )
        };

        // Configure the session settings for file compaction
        let config = df::execution::config::SessionConfig::new().with_batch_size(batch_size);

        let mut builder =
            RuntimeEnvBuilder::new().with_object_store_registry(self.store.registry());

        let memory_pool_size = params::params().store_optimizer_memory_pool_size.value;
        if memory_pool_size != 0 {
            builder = builder
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(memory_pool_size)))
                .with_disk_manager_builder(DiskManagerBuilder::default());
        };

        let runtime = Arc::new(builder.build().map_err(df_to_internal_error)?);

        let session_ctx =
            df::execution::context::SessionContext::new_with_config_rt(config, runtime);

        let Some(pis) = acquired_topic.topic_record.path_in_store() else {
            let err_msg = format!(
                "path in store not set for topic {}",
                acquired_topic.topic_record.locator()
            );
            Err(core::Error::internal(Some(err_msg)))?
        };

        // Copy metadata.json into new path in store before switch.
        let metadata = self.store.read_bytes(pis.path_metadata()).await?;
        self.store
            .write_bytes(acquired_topic.opt_path_in_store.path_metadata(), metadata)
            .await?;

        let input_folder = self
            .store
            .url_schema
            .join(&(pis.data_folder_path().to_string_lossy() + "/"))
            .map_err(|e| {
                let err_msg = format!(
                    "error composing input directory path for topic {}: {}",
                    acquired_topic.topic_record.locator(),
                    e
                );
                core::Error::internal(Some(err_msg))
            })?;

        let df = session_ctx
            .read_parquet(
                input_folder.as_str(),
                // Keep schema/field-level metadata (e.g. client-supplied metadata written
                // at `do_put` time) instead of having DataFusion strip it during schema
                // inference, which would otherwise be silently and permanently dropped
                // from the optimized/compacted output.
                // Note: having all parquet files of a topic the same schema+metadata this operation can be done safely.
                df::datasource::file_format::options::ParquetReadOptions::default()
                    .skip_metadata(false),
            )
            .await
            .map_err(df_to_internal_error)?;

        // Preserve and ensure global chronological sort order.
        // Because DataFusion scans files via multiple parallel threads, sorting explicitly
        // guarantees that the new larger files are neatly segmented by time.
        let sorted_df = df
            .sort(vec![
                df::logical_expr::col(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)
                    .sort(true, false),
            ])
            .map_err(df_to_internal_error)?;

        let schema = Arc::new(sorted_df.schema().as_arrow().clone());

        let mut batch_stream = sorted_df
            .execute_stream()
            .await
            .map_err(df_to_internal_error)?;

        let mut chunk_idx: usize = 0;

        let Some(format) = acquired_topic.topic_record.serialization_format() else {
            let err_msg = format!(
                "missing serialization format in DB for topic {}",
                acquired_topic.topic_record.locator()
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
                        &acquired_topic.opt_path_in_store,
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
                    &acquired_topic.opt_path_in_store,
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

        let mut tx = self.db.transaction().await?;

        // Remove topic from optimization list once processed and update path_in_store inside topic record.
        // These operations are done within the same transaction to prevent a cleanup routine scan in between.
        db::topic_optimization_delete(
            &mut tx,
            acquired_topic.topic_record.topic_id,
            types::allow_data_loss(),
        )
        .await?;

        db::topic_optimization_complete(
            &mut tx,
            acquired_topic.topic_record.topic_id,
            types::Timestamp::now().as_i64(),
            acquired_topic.opt_path_in_store.clone(),
        )
        .await?;

        self.update_chunk_stats(&mut tx, &acquired_topic.topic_record, chunk_stats)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn acquire_next_topic(&self) -> Result<Option<AcquiredTopic>> {
        let mut tx = self.db.transaction().await?;

        Ok(
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

                let topic_record =
                    db::topic_find_by_id(&mut tx, topic_to_optimize_record.topic_id).await?;

                tx.commit().await?;

                Some(AcquiredTopic {
                    topic_record,
                    opt_path_in_store,
                })
            } else {
                None
            },
        )
    }

    async fn optimize(&mut self, shutdown_notifier: &CancellationToken) -> Result<()> {
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
        db::topic_update_optimization_list(&mut self.db.connection()).await?;

        loop {
            if shutdown_notifier.is_cancelled() {
                info!("Shutdown received. Interrupting store optimization run early.");
                break;
            }

            let Some(acquired_topic) = self
                .acquire_next_topic()
                .await
                .inspect_err(|_| error!("failed to acquire next topic to optimize"))?
            else {
                break;
            };

            let topic_res = self.optimize_topic(&acquired_topic).await;

            match topic_res {
                Ok(_) => {
                    self.result
                        .completed
                        .push(acquired_topic.topic_record.locator());
                }
                Err(error) => {
                    warn!(
                        "failed to optimize topic {}. It will be removed from optimization list waiting for the next run.",
                        acquired_topic.topic_record.locator()
                    );

                    // If the topic optimization fails, remove it from the optimization list anyway.
                    // It will be re-added at the next execution.
                    // If even the deletion from the list fails, we can only wait until lease time expires.
                    db::topic_optimization_delete(
                            &mut self.db.connection(),
                            acquired_topic.topic_record.topic_id,
                            types::allow_data_loss(),
                        )
                            .await.unwrap_or_else(|_| {
                            warn!("failed to delete topic {} from optimization list. Let's wait until its lease time expires.", acquired_topic.topic_record.locator());
                        });

                    self.result
                        .failed
                        .push((acquired_topic.topic_record.locator(), error.to_string()));
                }
            };
        }

        Ok(())
    }

    /// Starts the optimization routine every [`time_interval`].
    pub async fn run(mut self, shutdown_notifier: CancellationToken) {
        loop {
            info!("Store optimization routine started");

            match self.optimize(&shutdown_notifier).await {
                Ok(_) => {
                    info!(
                        "Store optimization routine completed: {} topics successful, {} topics failed:",
                        self.result.completed.len(),
                        self.result.failed.len()
                    );

                    let errors_list = self
                        .result
                        .failed
                        .iter()
                        .map(|(locator, err)| format!("\t{}: {}", locator, err))
                        .collect::<Vec<_>>()
                        .join("\n");

                    info!(errors_list);
                }
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

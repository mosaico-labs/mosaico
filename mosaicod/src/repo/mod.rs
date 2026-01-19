pub mod core;
pub use core::{AsExec, Config, Cx, Database, Repository, Tx, UNREGISTERED};

mod facades;
pub use facades::*;

// Exported queries
//
// We expose a minimal set of queries to ensure that database logic and
// operations remain encapsulated within the facade layer.
pub use sql_models::{get_resource_locator_from_name, layer_bootstrap, sequence_find_all};

// Chunk-related exports for do_put.rs transactional outbox pattern
pub use sql_models::{
    Chunk, ColumnChunkLiteral, ColumnChunkNumeric, chunk_create, column_chunk_literal_create_batch,
    column_chunk_numeric_create_batch, column_get_or_create,
};

// Outbox-related exports for the background worker
pub use sql_models::{
    ChunkOutboxEntry, outbox_cleanup_processed, outbox_count_pending, outbox_create,
    outbox_fetch_pending, outbox_mark_failed, outbox_mark_processed,
};

mod error;
pub use error::Error;

#[cfg(test)]
pub use core::testing;

// Private to module exports

mod sql_models;
use sql_models::*;

// Test-only exports for outbox edge-case tests
#[cfg(test)]
pub use sql_models::{SequenceRecord, TopicRecord, sequence_create, topic_create};

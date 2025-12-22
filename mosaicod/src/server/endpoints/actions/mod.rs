//! Action handlers for Flight DoAction requests.
//!
//! This module implements the Command pattern for handling Flight actions,
//! separating each action type into its own handler for better maintainability
//! and testability.

mod layer;
mod query;
mod sequence;
mod topic;

pub use layer::LayerActionHandler;
pub use query::QueryActionHandler;
pub use sequence::SequenceActionHandler;
pub use topic::TopicActionHandler;

use crate::{query as ts_query, repo, store};

/// Shared context for all action handlers.
///
/// Contains references to the store, repository, and timeseries engine
/// that handlers need to perform their operations.
pub struct ActionContext {
    pub store: store::StoreRef,
    pub repo: repo::Repository,
    pub ts_engine: ts_query::TimeseriesGwRef,
}

impl ActionContext {
    pub fn new(
        store: store::StoreRef,
        repo: repo::Repository,
        ts_engine: ts_query::TimeseriesGwRef,
    ) -> Self {
        Self {
            store,
            repo,
            ts_engine,
        }
    }
}

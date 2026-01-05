//! Action handlers for Flight DoAction requests.
//!
//! This module contains free functions for handling Flight actions,
//! organized by resource type (sequence, topic, layer, query).

pub mod layer;
pub mod query;
pub mod sequence;
pub mod topic;

use crate::{query as ts_query, repo, store};

/// Shared context for all action handlers.
///
/// Contains references to the store, repository, and timeseries engine
/// that handlers need to perform their operations.
pub struct ActionContext {
    pub store: store::StoreRef,
    pub repo: repo::Repository,
    pub ts_gw: ts_query::TimeseriesGatewayRef,
}

impl ActionContext {
    pub fn new(
        store: store::StoreRef,
        repo: repo::Repository,
        ts_gw: ts_query::TimeseriesGatewayRef,
    ) -> Self {
        Self { store, repo, ts_gw }
    }
}

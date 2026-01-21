use crate::{query, repo, store};

/// Shared context for all endpoint handlers.
///
/// Contains references to the store, repository, and timeseries engine
/// that handlers require to perform their operations.
pub struct Context {
    pub store: store::StoreRef,
    pub repo: repo::Repository,
    pub ts_gw: query::TimeseriesGatewayRef,
}

impl Context {
    pub fn new(
        store: store::StoreRef,
        repo: repo::Repository,
        ts_gw: query::TimeseriesGatewayRef,
    ) -> Self {
        Self { store, repo, ts_gw }
    }
}

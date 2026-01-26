use mosaicod_query as query;
use mosaicod_repo as repo;
use mosaicod_store as store;

/// Shared context for all endpoint handlers.
///
/// Contains references to the store, repository, and timeseries engine
/// that handlers require to perform their operations.
#[derive(Clone)]
pub struct Context {
    pub store: store::StoreRef,
    pub repo: repo::Repository,
    pub timeseries_querier: query::TimeseriesRef,
}

impl Context {
    pub fn new(
        store: store::StoreRef,
        repo: repo::Repository,
        ts_gw: query::TimeseriesRef,
    ) -> Self {
        Self {
            store,
            repo,
            timeseries_querier: ts_gw,
        }
    }
}

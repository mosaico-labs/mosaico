use mosaicod_query as query;
use mosaicod_db as db;
use mosaicod_store as store;

/// Shared context for all endpoint handlers.
///
/// Contains references to the store, database, and timeseries engine
/// that handlers require to perform their operations.
#[derive(Clone)]
pub struct Context {
    pub store: store::StoreRef,
    pub db: db::Database,
    pub timeseries_querier: query::TimeseriesRef,
}

impl Context {
    pub fn new(
        store: store::StoreRef,
        db: db::Database,
        ts_gw: query::TimeseriesRef,
    ) -> Self {
        Self {
            store,
            db,
            timeseries_querier: ts_gw,
        }
    }
}

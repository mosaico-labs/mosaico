use mosaicod_db as db;
use mosaicod_query as query;
use mosaicod_store as store;

/// Shared context for all facade functions.
///
/// Contains references to the store, database, and timeseries engine
/// that facade functions require to perform their operations.
#[derive(Clone)]
pub struct Context {
    pub store: store::StoreRef,
    pub db: db::Database,
    pub timeseries_querier: query::TimeseriesEngineRef,
}

impl Context {
    pub fn new(
        store: store::StoreRef,
        db: db::Database,
        ts_gw: query::TimeseriesEngineRef,
    ) -> Self {
        Self {
            store,
            db,
            timeseries_querier: ts_gw,
        }
    }
}

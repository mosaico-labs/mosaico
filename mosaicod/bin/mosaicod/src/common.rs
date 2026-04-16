//! Common functions shared between multiple commands

use mosaicod_core::{self as core, error::PublicResult as Result, params};
use mosaicod_db as db;
use mosaicod_store as store;
use std::sync::Arc;
use std::sync::OnceLock;
use tracing::{debug, info};

/// Stores startup time
static STARTUP_TIME: OnceLock<std::time::Instant> = OnceLock::new();

pub fn pin_startup_time() {
    STARTUP_TIME
        .set(std::time::Instant::now())
        .expect("Startup time already set");
}

pub fn startup_time() -> &'static std::time::Instant {
    STARTUP_TIME.get().expect(
        "Startup time not initialized, plase call `common::pin_startup_time()` before accessing.",
    )
}

pub fn init_db(rt: &tokio::runtime::Runtime, config: &db::Config) -> Result<db::Database> {
    let database = rt.block_on(async {
        let database = db::Database::try_new(config).await?;
        Ok::<db::Database, mosaicod_core::error::BoxPublicError>(database)
    })?;

    Ok(database)
}

pub fn init_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| core::Error::internal(Some("event loop startup failure".to_owned())))?)
}

pub fn init_local_store(path: impl AsRef<std::path::Path>) -> Result<store::StoreRef> {
    Ok(Arc::new(store::Store::try_from_filesystem(path)?))
}

pub fn init_s3_store() -> Result<store::StoreRef> {
    let params = params::params();

    let config = store::S3Config {
        endpoint: params.store_endpoint.value.clone(),
        bucket: params.store_bucket.value.clone(),
        secret_key: params.store_secret_key.value.clone(),
        access_key: params.store_access_key.value.clone(),
    };

    // This will return and error if the s3 confuration has some problems
    config.validate()?;

    debug!("{:#?}", config);

    Ok(Arc::new(store::Store::try_from_s3_store(config)?))
}

/// Load the defined env variables from the system.
pub fn load_env_variables() -> Result<()> {
    info!("loading environment variables");
    dotenv::dotenv().ok();

    params::load_params_from_env(params::ParamsLoadOptions::default())?;

    debug!("{:#?}", params::params());

    Ok(())
}

//! Common functions shared between multiple commands

use mosaicod_core::params;
use mosaicod_db as db;
use mosaicod_store as store;
use std::sync::Arc;
use std::sync::OnceLock;
use tracing::{debug, info};

pub type Error = Box<dyn std::error::Error>;

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

pub fn init_db(rt: &tokio::runtime::Runtime, config: &db::Config) -> Result<db::Database, Error> {
    let database = rt.block_on(async {
        let database = db::Database::try_new(config).await?;

        let mut tx = database.transaction().await?;
        db::layer_bootstrap(&mut tx).await?;
        tx.commit().await?;

        Ok::<db::Database, Box<dyn std::error::Error>>(database)
    })?;

    Ok(database)
}

pub fn init_runtime() -> Result<tokio::runtime::Runtime, Error> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

pub fn init_local_store(path: impl AsRef<std::path::Path>) -> Result<store::StoreRef, Error> {
    Ok(Arc::new(store::Store::try_from_filesystem(path)?))
}

pub fn init_s3_store() -> Result<store::StoreRef, Error> {
    let params = params::params();

    let config = store::S3Config {
        endpoint: params.store_endpoint.clone(),
        bucket: params.store_bucket.clone(),
        secret_key: params.store_secret_key.clone(),
        access_key: params.store_access_key.clone(),
    };

    // This will return and error if the s3 confuration has some problems
    config.validate()?;

    debug!("{:#?}", config);

    Ok(Arc::new(store::Store::try_from_s3_store(config)?))
}

/// Load the defined env variables from the system.
pub fn load_env_variables() -> Result<(), Error> {
    info!("loading environment variables");
    dotenv::dotenv().ok();

    params::load_params_from_env(params::ParamsLoadOptions::default())?;

    debug!("{:#?}", params::params());

    Ok(())
}

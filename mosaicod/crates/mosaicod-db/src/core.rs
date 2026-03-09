//! This module provides the core data access layer for the application, managing connections
//! to the database and offering utility functions for database operations.
//!
//! The central component is the [`Database`] struct, which holds the connection pool and
//! methods for interacting with the database. Error handling is unified through the
//! [`DatabaseError`] enum.

use log::debug;
use sqlx::Pool;
use url::Url;

use super::Error;
use mosaicod_core::params;

/// The concrete database type used throughout this module.
pub type DatabaseType = sqlx::Postgres;

/// If the layer has this id is not registered in the database
pub const UNREGISTERED: i32 = -1;

/// A trait for types that can provide a [`sqlx::Executor`].
///
/// This trait establishes a generic contract, allowing functions to operate
/// on any type that can supply the necessary execution interface.
pub trait AsExec {
    /// Returns a reference to the underlying execution interface.
    fn as_exec(&mut self) -> impl sqlx::Executor<'_, Database = DatabaseType>;
}

/// A wrapper struct for a database **Transaction**.
///
/// This structure provides control over the transaction lifecycle—allowing
/// for atomic operations that can either be permanently saved [`commit`]
/// or discarded [`rollback`]
pub struct Tx<'a> {
    inner: sqlx::Transaction<'a, DatabaseType>,
}

impl<'a> Tx<'a> {
    pub async fn commit(self) -> Result<(), Error> {
        self.inner.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), Error> {
        self.inner.rollback().await?;
        Ok(())
    }
}

impl<'a> AsExec for Tx<'a> {
    fn as_exec(&mut self) -> impl sqlx::Executor<'_, Database = DatabaseType> {
        &mut *self.inner
    }
}

/// The **Connection** truct, designed to hold a reference to a core resource pool.
pub struct Cx<'a> {
    inner: &'a Pool<DatabaseType>,
}

impl<'a> AsExec for Cx<'a> {
    /// Returns a reference to the inner resource pool as the execution interface.
    ///
    /// Since the inner `Pool` itself typically fulfills the `Executor` contract,
    /// this method simply returns the reference to the internal resource.
    fn as_exec(&mut self) -> impl sqlx::Executor<'_, Database = DatabaseType> {
        self.inner
    }
}

/// Configuration structure for initializing the [`Database`].
pub struct Config {
    pub db_url: Url,
}

#[derive(Clone)]
pub struct Database {
    pub(super) pool: Pool<DatabaseType>,
}

impl Database {
    pub async fn try_new(config: &Config) -> Result<Self, Error> {
        debug!("creating database connection pool");
        let max_connections = params::params().max_db_connections;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(config.db_url.as_str())
            .await?;

        debug!("running migrations");
        sqlx::migrate!().run(&pool).await?;

        Ok(Self { pool })
    }

    /// Builds a transaction.
    ///
    /// This call should be used when performing **write** operations on the
    /// database.
    pub async fn transaction(&self) -> Result<Tx<'_>, Error> {
        Ok(Tx {
            inner: self.pool.begin().await?,
        })
    }

    /// Returns a connection to perform operations on the database.
    ///
    /// This call should be used when performing **read-only** operations on the database.
    pub fn connection(&self) -> Cx<'_> {
        Cx { inner: &self.pool }
    }
}

/// Testing utilities for the database module.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

    use std::ops::Deref;

    /// A wrapper around the [`Database`] struct for testing purposes.
    pub struct Database {
        inner: super::Database,
    }

    impl Database {
        /// Creates a new [`Database`] instance for testing using the provided database pool.
        pub fn new(pool: sqlx::Pool<super::DatabaseType>) -> Self {
            Self {
                inner: super::Database { pool },
            }
        }

        /// Provides access to the inner database pool.
        pub fn pool(&self) -> &sqlx::Pool<super::DatabaseType> {
            &self.inner.pool
        }
    }

    /// Deref implementation to allow easy access to the inner [`Database`].
    impl Deref for Database {
        type Target = super::Database;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }
}

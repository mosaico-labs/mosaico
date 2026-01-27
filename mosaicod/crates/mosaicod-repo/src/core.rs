//! This module provides the core data access layer for the application, managing connections
//! to the database and offering utility functions for database operations.
//!
//! The central component is the [`Repository`] struct, which holds the connection pool and
//! methods for interacting with the database. Error handling is unified through the
//! [`RepositoryError`] enum.

use log::debug;
use sqlx::Pool;
use url::Url;

use super::Error;
use mosaicod_core::params;

/// The concrete database type used throughout this module.
pub type Database = sqlx::Postgres;

/// If the layer has this id is not registered in the repository
pub const UNREGISTERED: i32 = -1;

/// A trait for types that can provide a [`sqlx::Executor`].
///
/// This trait establishes a generic contract, allowing functions to operate
/// on any type that can supply the necessary execution interface.
pub trait AsExec {
    /// Returns a reference to the underlying execution interface.
    fn as_exec(&mut self) -> impl sqlx::Executor<'_, Database = Database>;
}

/// A wrapper struct for a database **Transaction**.
///
/// This structure provides control over the transaction lifecycle—allowing
/// for atomic operations that can either be permanently saved [`commit`]
/// or discarded [`rollback`]
pub struct Tx<'a> {
    inner: sqlx::Transaction<'a, Database>,
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
    fn as_exec(&mut self) -> impl sqlx::Executor<'_, Database = Database> {
        &mut *self.inner
    }
}

/// The **Connection** truct, designed to hold a reference to a core resource pool.
pub struct Cx<'a> {
    inner: &'a Pool<Database>,
}

impl<'a> AsExec for Cx<'a> {
    /// Returns a reference to the inner resource pool as the execution interface.
    ///
    /// Since the inner `Pool` itself typically fulfills the `Executor` contract,
    /// this method simply returns the reference to the internal resource.
    fn as_exec(&mut self) -> impl sqlx::Executor<'_, Database = Database> {
        self.inner
    }
}

/// Configuration structure for initializing the [`Repository`].
pub struct Config {
    pub db_url: Url,
}

#[derive(Clone)]
pub struct Repository {
    pub(super) pool: Pool<Database>,
}

impl Repository {
    pub async fn try_new(config: &Config) -> Result<Self, Error> {
        debug!("creating database connection pool");
        let max_connections = params::configurables().max_db_connections;
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
    /// repository.
    pub async fn transaction(&self) -> Result<Tx<'_>, Error> {
        Ok(Tx {
            inner: self.pool.begin().await?,
        })
    }

    /// Returns a connection to perform operations on the repository.
    ///
    /// This call should be used when performing **read-only** operations on the repository.
    pub fn connection(&self) -> Cx<'_> {
        Cx { inner: &self.pool }
    }
}

/// Testing utilities for the repository module.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

    use std::ops::Deref;

    /// A wrapper around the [`Repository`] struct for testing purposes.
    pub struct Repository {
        inner: super::Repository,
    }

    impl Repository {
        /// Creates a new [`Repository`] instance for testing using the provided database pool.
        pub fn new(pool: sqlx::Pool<super::Database>) -> Self {
            Self {
                inner: super::Repository { pool },
            }
        }

        /// Provides access to the inner database pool.
        pub fn pool(&self) -> &sqlx::Pool<super::Database> {
            &self.inner.pool
        }
    }

    /// Deref implementation to allow easy access to the inner [`Repository`].
    impl Deref for Repository {
        type Target = super::Repository;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }
}

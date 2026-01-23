//! This module provides the [`Store`], the application's core client for interacting
//! with S3-compatible object storage services providing
//! essential CRUD (Create, Read, Update, Delete) methods for byte-level data access.

use futures::stream::TryStreamExt;
use std::sync::Arc;

use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use log::trace;
use object_store::{ObjectStore, PutPayload, aws::AmazonS3Builder, local::LocalFileSystem};
use thiserror::Error;
use url::Url;

use crate::{params, traits};

/// Converts a filesystem path to an object_store Path.
#[inline]
fn to_object_path(path: impl AsRef<std::path::Path>) -> object_store::path::Path {
    object_store::path::Path::from(path.as_ref().to_string_lossy().into_owned())
}

#[derive(Debug, Clone)]
pub struct S3Config {
    /// Bucket name.
    pub bucket: String,
    /// Endpoint name
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: params::Hidden,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("storage backend error: {0}")]
    BackendError(#[from] object_store::Error),
    #[error("bad url: {0}")]
    BadUrl(#[from] url::ParseError),
    #[error("io error :: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub enum StoreTarget {
    Filesystem(String),
    S3Compatible(String),
}

/// Implements the object storage client for the application.
///
/// It provides methods to read, write, list, and delete byte-level data
/// from S3-compatible object storage services or local filesystem.
#[derive(Debug, Clone)]
pub struct Store {
    pub url_schema: Url,
    target: StoreTarget,
    driver: Arc<dyn ObjectStore>,
    registry: Arc<dyn ObjectStoreRegistry>,
}

pub type StoreRef = Arc<Store>;

impl Store {
    pub fn try_from_filesystem(root: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        // Create the directory structure if not existing
        std::fs::create_dir_all(&root)?;

        let target = root.as_ref().to_string_lossy().to_string();

        let storage = Arc::new(LocalFileSystem::new_with_prefix(root)?);

        let bucket_url = Url::parse("file://")?;

        // Create object store registry (for datafusion support)
        let registry = Arc::new(DefaultObjectStoreRegistry::default());
        registry.register_store(&bucket_url, storage.clone());

        Ok(Self {
            url_schema: bucket_url,
            target: StoreTarget::Filesystem(target),
            driver: storage.clone(),
            registry,
        })
    }

    pub fn try_from_s3_store(config: S3Config) -> Result<Self, Error> {
        trace!(
            "creating object driver for a s3 compatible store, endpoint: {}",
            config.endpoint
        );

        let bucket_url = Url::parse(&format!("s3://{}", config.bucket))?;

        // Setup connection with object storage service
        // (cabba) TODO: add region support
        let storage = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(&config.endpoint)
                .with_bucket_name(&config.bucket)
                .with_access_key_id(config.access_key)
                .with_secret_access_key(config.secret_key.take())
                .with_allow_http(true)
                .build()?,
        );

        // Create object store registry (for datafusion support)
        let registry = Arc::new(DefaultObjectStoreRegistry::default());
        registry.register_store(&bucket_url, storage.clone());

        Ok(Self {
            url_schema: bucket_url,
            target: StoreTarget::S3Compatible(config.bucket),
            driver: storage.clone(),
            registry: registry.clone(),
        })
    }

    pub fn registry(&self) -> Arc<dyn ObjectStoreRegistry> {
        self.registry.clone()
    }

    pub fn target(&self) -> &StoreTarget {
        &self.target
    }

    pub async fn read_bytes(&self, path: impl AsRef<std::path::Path>) -> Result<Vec<u8>, Error> {
        trace!("reading bytes from {}", path.as_ref().display());
        Ok(self
            .driver
            .get(&to_object_path(&path))
            .await?
            .bytes()
            .await?
            .into())
    }

    pub async fn write_bytes(
        &self,
        path: impl AsRef<std::path::Path>,
        bytes: impl Into<bytes::Bytes>,
    ) -> Result<(), Error> {
        trace!("writing bytes to {}", path.as_ref().display());

        self.driver
            .put(&to_object_path(&path), PutPayload::from_bytes(bytes.into()))
            .await?;

        Ok(())
    }

    /// Returns a list of elements located at the given `path`.
    ///
    /// If an extension is provided, the results will be filtered to include only
    /// the elements whose extension matches exactly.es extacly
    pub async fn list(
        &self,
        path: impl AsRef<std::path::Path>,
        extension: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let mut list_stream = self.driver.list(Some(&to_object_path(&path)));

        let mut locations = Vec::new();
        while let Some(elem) = list_stream.try_next().await? {
            let location = &elem.location;
            // If some extension is provided:
            // - check if current element has an extension, if has no extension
            //   should the excluded
            // - if has an extension but is different from the one provided shoukd
            //   be excluded
            if let Some(ext) = extension {
                if let Some(path_ext) = location.extension() {
                    if path_ext != ext {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            locations.push(location.to_string());
        }

        Ok(locations)
    }

    pub async fn exists(&self, path: impl AsRef<std::path::Path>) -> Result<bool, Error> {
        match self.driver.head(&to_object_path(&path)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn size(&self, path: impl AsRef<std::path::Path>) -> Result<usize, Error> {
        let head = self.driver.head(&to_object_path(&path)).await?;

        Ok(head.size as usize)
    }

    pub async fn delete(&self, path: impl AsRef<std::path::Path>) -> Result<(), Error> {
        Ok(self.driver.delete(&to_object_path(&path)).await?)
    }

    /// Deletes recursively all objects under a given path
    pub async fn delete_recursive(&self, path: impl AsRef<std::path::Path>) -> Result<(), Error> {
        let mut list_stream = self.driver.list(Some(&to_object_path(&path)));

        while let Some(e) = list_stream.try_next().await? {
            self.driver.delete(&e.location).await?;
        }

        Ok(())
    }
}

impl traits::AsyncWriteToPath for Store {
    #[expect(
        clippy::manual_async_fn,
        reason = "trait requires impl Future return type"
    )]
    fn write_to_path(
        &self,
        path: impl AsRef<std::path::Path>,
        buf: impl Into<bytes::Bytes>,
    ) -> impl Future<Output = std::io::Result<()>> {
        async move {
            self.write_bytes(&path, buf).await.map_err(|e| {
                std::io::Error::other(format!(
                    "unable to write data to store on path {}: {}",
                    path.as_ref().display(),
                    e
                ))
            })
        }
    }
}

/// Provides a temporary store wrapper for testing.
///
/// This module contains a [`Store`] struct which wraps a `super::StoreRef` and manages
/// a temporary directory on the filesystem. When the [`Store`] struct is dropped,
/// it automatically deletes the directory it was created with, cleaning up all resources.
/// This is useful for integration tests that need a real store instance.
#[cfg(test)]
pub mod testing {
    use super::*;
    use std::ops::Deref;

    pub struct Store {
        inner: super::StoreRef,
        root: std::path::PathBuf,
    }

    impl Store {
        /// Creates a new temporary [`Store`] at the specified root path.
        ///
        /// The path **must not** exist, as it will be created by this function
        /// and recursively deleted when the returned [`Store`] is dropped.
        pub fn new(root: impl AsRef<std::path::Path>) -> Result<Self, Box<dyn std::error::Error>> {
            if root.as_ref().exists() {
                Err(format!(
                    "directory {:?} already exist, can't be used as temporary store since at the end will be deleted",
                    root.as_ref()
                ))?;
            }

            Ok(Self {
                root: root.as_ref().to_path_buf(),
                inner: Arc::new(super::Store::try_from_filesystem(root)?),
            })
        }

        /// Creates a new temporary [`Store`] in a randomly named directory inside `/tmp`.
        ///
        /// The store's directory will be automatically deleted when the [`Store`] is dropped.
        /// The directory name is based on the current timestamp.
        pub fn new_random_on_tmp() -> Result<Self, Box<dyn std::error::Error>> {
            let random_location = format!("/tmp/{}", crate::utils::random::random_string(10));
            Self::new(random_location)
        }
    }

    impl Drop for Store {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    impl Deref for Store {
        type Target = super::StoreRef;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }
}

#[cfg(test)]
mod test {

    use crate::{traits::AsyncWriteToPath, types};

    use super::*;

    /// Checks that filesystem store works, writing and reading data to `/tmp`` directory
    ///
    /// To avoid to delete system files the test directories are created in `/tmp` and are not removed automatically
    #[tokio::test]
    async fn test_filesystem_store() {
        let bucket_name = types::DateTime::now().fmt_to_ms();
        let path = format!("/tmp/{}", bucket_name);
        let store = Store::try_from_filesystem(path).unwrap();

        let sample = r#"
            Some example text
        "#;
        let buffer = sample.as_bytes();
        let target = "write_text";

        store.write_to_path(&target, buffer).await.unwrap();

        let read_buffer = store.read_bytes(&target).await.unwrap();

        assert_eq!(buffer, read_buffer);
    }
}

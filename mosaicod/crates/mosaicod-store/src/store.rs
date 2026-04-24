//! This module provides the [`Store`], the application's core client for interacting
//! with S3-compatible object storage services providing
//! essential CRUD (Create, Read, Update, Delete) methods for byte-level data access.

use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use futures::stream::TryStreamExt;
use log::trace;
use mosaicod_core::traits;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder, local::LocalFileSystem,
};
use parquet::arrow::async_reader::ParquetObjectReader;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub enum Error {
    #[error("storage backend error")]
    BackendError(#[from] object_store::Error),
    #[error("{0}")]
    MissingCredentials(String),
    #[error("the provided bucket `{0}` is not valid")]
    InvalidBucket(String),
    #[error("the provided endpoint `{0}` is not valid")]
    InvalidEndpoint(String),
    #[error("unable to create directory `{0}`: {1}")]
    DirCreationFailed(String, std::io::Error),
}

impl mosaicod_core::error::PublicError for Error {
    fn error(&self) -> mosaicod_core::Error {
        use mosaicod_core::Error;

        match self {
            Self::MissingCredentials(_) => Error::invalid_configuration(
                "object store credentials".to_owned(),
                self.to_string(),
            ),
            Self::InvalidEndpoint(_) => {
                Error::invalid_configuration("object store endpoint".to_owned(), self.to_string())
            }
            Self::InvalidBucket(_) => {
                Error::invalid_configuration("object store bucket".to_owned(), self.to_string())
            }
            Self::DirCreationFailed(_, _) => Error::invalid_configuration(
                "object store local directory".to_owned(),
                self.to_string(),
            ),
            _ => Error::internal(Some("store failed".to_owned())),
        }
    }
}

/// Converts a filesystem path to an object_store Path.
#[inline]
fn to_object_path(path: impl AsRef<std::path::Path>) -> object_store::path::Path {
    object_store::path::Path::from(path.as_ref().to_string_lossy().into_owned())
}

#[inline]
/// Check if a given bucket name is valid
fn is_valid_bucket_name(bucket: &str) -> bool {
    bucket
        .chars()
        .filter(|c| !c.is_alphabetic() && !c.is_numeric() && !matches!(c, '.' | '-'))
        .count()
        == 0
}

/// A configuration builder for initializing a storage backend.
#[derive(Debug, Clone)]
pub struct Builder {
    /// The base URL of the storage service.
    ///
    /// To configure the store to use local filesystem use `file:///`
    pub endpoint: url::Url,

    /// The name of the specific bucket to access.
    pub bucket: String,

    /// The access key for the storage bucket
    ///
    /// This field is **required** to work with remote object store.
    pub access_key: Option<String>,

    /// The secret key of the storage bucket
    ///
    /// This field is **required** to work with remote object store.
    pub secret_key: Option<String>,
}

impl Builder {
    pub fn new(endpoint: url::Url, bucket: String) -> Self {
        Self {
            endpoint,
            bucket,
            access_key: None,
            secret_key: None,
        }
    }

    /// Configure credentials to access the object store
    pub fn with_credentials(mut self, key: String, secret: String) -> Self {
        self.access_key = Some(key);
        self.secret_key = Some(secret);
        self
    }

    /// Create a new store backend
    pub fn build(self) -> Result<Store, Error> {
        if !is_valid_bucket_name(&self.bucket) {
            return Err(Error::InvalidBucket(self.bucket));
        }

        if self.endpoint.scheme() == "file" {
            // If the user provided a `file:///some/local/path` the
            // url will contain a domain == "some"
            if self.endpoint.domain().is_some() {
                return Err(Error::InvalidEndpoint(
                    "relative path are not supported, please provide an absolute path using the `file:///` URI scheme."
                        .to_owned(),
                ));
            }
            // Merge the endpoint path and the bucket in a unique path.
            // For example if the endpoint is `file:///tmp` and the bucket
            // is `mosaico` file will be saved into /tmp/mosaico
            let path = self
                .endpoint
                .to_file_path()
                .map_err(|_| Error::InvalidEndpoint(self.endpoint.to_string()))?;

            let path = path.join(self.bucket);
            return Store::try_from_filesystem(&path);
        }

        let Some(access_key) = self.access_key else {
            return Err(Error::MissingCredentials("access key".to_owned()));
        };

        let Some(secret_key) = self.secret_key else {
            return Err(Error::MissingCredentials("secret key".to_owned()));
        };

        Store::try_from_s3_store(self.endpoint, self.bucket, access_key, secret_key)
    }
}

#[derive(Debug, Clone)]
pub enum Target {
    Filesystem(std::path::PathBuf),
    S3Compatible(url::Url),
}

/// Implements the object storage client for the application.
///
/// It provides methods to read, write, list, and delete byte-level data
/// from S3-compatible object storage services or local filesystem.
#[derive(Debug, Clone)]
pub struct Store {
    pub url_schema: Url,

    target: Target,

    driver: Arc<dyn ObjectStore>,
    registry: Arc<dyn ObjectStoreRegistry>,
}

pub type StoreRef = Arc<Store>;

impl Store {
    /// Create a new store configured to work with the local filesystem
    pub fn try_from_filesystem(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        // Create the directory structure if not existing
        std::fs::create_dir_all(&path).map_err(|e| {
            Error::DirCreationFailed(path.as_ref().to_string_lossy().to_string(), e)
        })?;

        let storage = Arc::new(LocalFileSystem::new_with_prefix(path.as_ref())?);

        // Here we use unwrap since `file://` IS a valid url
        let bucket_url = Url::parse("file://").unwrap();

        // Create object store registry (for datafusion support)
        let registry = Arc::new(DefaultObjectStoreRegistry::default());
        registry.register_store(&bucket_url, storage.clone());

        Ok(Self {
            url_schema: bucket_url,
            target: Target::Filesystem(path.as_ref().to_owned()),
            driver: storage.clone(),
            registry,
        })
    }

    /// Create a new store configured to work with an s3-compatible system
    pub fn try_from_s3_store(
        endpoint: url::Url,
        bucket: String,
        access_key: String,
        secret_key: String,
    ) -> Result<Self, Error> {
        trace!(
            "creating object driver for a s3 compatible store, endpoint: {}",
            endpoint
        );

        // We map a url parse error into a bad bucket since `s3://` is a valid url
        // and only problem we can get from this is a bucket name non url safe
        let bucket_url = Url::parse(&format!("s3://{}", bucket))
            .map_err(|_| Error::InvalidBucket("non URL safe string".to_owned()))?;

        // Setup connection with object storage service
        // (cabba) TODO: add region support (??)
        let storage = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(endpoint.to_string())
                .with_bucket_name(&bucket)
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .with_allow_http(true)
                .build()?,
        );

        // Create object store registry (for datafusion support)
        let registry = Arc::new(DefaultObjectStoreRegistry::default());
        registry.register_store(&bucket_url, storage.clone());

        Ok(Self {
            url_schema: bucket_url,
            target: Target::S3Compatible(endpoint),
            driver: storage.clone(),
            registry: registry.clone(),
        })
    }

    pub fn registry(&self) -> Arc<dyn ObjectStoreRegistry> {
        self.registry.clone()
    }

    pub fn target(&self) -> &Target {
        &self.target
    }

    pub async fn read_bytes(&self, path: impl AsRef<std::path::Path>) -> Result<Vec<u8>, Error> {
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
        self.driver
            .put(&to_object_path(&path), PutPayload::from_bytes(bytes.into()))
            .await?;

        Ok(())
    }

    /// Returns a list of elements located at the given `path`.
    ///
    /// If an extension is provided, the results will be filtered to include only
    /// the elements whose extension matches exactly.es exactly
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

    pub fn parquet_reader(&self, path: impl AsRef<std::path::Path>) -> ParquetObjectReader {
        ParquetObjectReader::new(self.driver.clone(), to_object_path(path))
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
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use super::*;
    use std::ops::Deref;

    pub struct Store {
        inner: super::StoreRef,
        pub root: std::path::PathBuf,
    }

    impl Store {
        /// Creates a new temporary [`Store`] at the specified root path.
        ///
        /// The path **must not** exist, as it will be created by this function
        /// and recursively deleted when the returned [`Store`] is dropped.
        pub fn new(path: impl AsRef<std::path::Path>) -> Result<Self, Box<dyn std::error::Error>> {
            if path.as_ref().exists() {
                Err(format!(
                    "directory {:?} already exist, can't be used as temporary store since at the end will be deleted",
                    path.as_ref()
                ))?;
            }

            let store = super::Store::try_from_filesystem(path.as_ref())?;

            Ok(Self {
                root: path.as_ref().to_path_buf(),
                inner: Arc::new(store),
            })
        }

        /// Creates a new temporary [`Store`] in a randomly named directory inside `/tmp`.
        ///
        /// The store's directory will be automatically deleted when the [`Store`] is dropped.
        /// The directory name is based on the current timestamp.
        pub fn new_random_on_tmp() -> Result<Self, Box<dyn std::error::Error>> {
            let random_location = mosaicod_core::random::alphabetic(10);
            let path: std::path::PathBuf = format!("/tmp/{}", random_location).parse()?;
            Self::new(path)
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

    use mosaicod_core::{traits::AsyncWriteToPath, types};

    use super::*;

    /// Checks that filesystem store works, writing and reading data to `/tmp`` directory
    ///
    /// To avoid to delete system files the test directories are created in `/tmp` and are not removed automatically
    #[tokio::test]
    async fn test_filesystem_store() {
        let bucket = types::DateTime::now().fmt_to_ms();
        let endpoint = "file:///tmp".parse().unwrap();

        let store = Builder::new(endpoint, bucket).build().unwrap();

        let sample = r#"Some example text"#;
        let buffer = sample.as_bytes();

        let target = "write_text";
        store.write_to_path(&target, buffer).await.unwrap();
        let read_buffer = store.read_bytes(&target).await.unwrap();
        assert_eq!(buffer, read_buffer);

        let target = "test_dir/write_text";
        store.write_to_path(&target, buffer).await.unwrap();
        let read_buffer = store.read_bytes(&target).await.unwrap();
        assert_eq!(buffer, read_buffer);

        assert_eq!(store.list("", None).await.unwrap().len(), 2);
    }

    #[test]
    fn test_filesystem_store_endpoint_fs_relative() {
        let bucket = types::DateTime::now().fmt_to_ms();
        // Now we are testing a file:// non RFC8089 filesystem
        // url that should throw an error
        let endpoint = "file://tmp".parse().unwrap();

        let res = Builder::new(endpoint, bucket).build();

        dbg!(&res);
        assert!(matches!(res, Err(Error::InvalidEndpoint(_))));
    }

    #[test]
    fn test_filesystem_store_endpoint_fs_bad_bucket() {
        // We are testing a bad bucket name contaning additonal slashes
        let bucket = "bad/bucket".to_owned();

        let endpoint = "file://tmp".parse().unwrap();

        let res = Builder::new(endpoint, bucket).build();

        dbg!(&res);
        assert!(matches!(res, Err(Error::InvalidBucket(_))));
    }

    #[test]
    fn test_filesystem_store_remote_missing_credentials() {
        let bucket = "my-bucket".to_owned();

        let endpoint = "dummy://fake.url".parse().unwrap();

        let res = Builder::new(endpoint, bucket).build();

        dbg!(&res);

        // A non `file:///` store should require credentials
        assert!(matches!(res, Err(Error::MissingCredentials(_))));
    }

    #[test]
    fn test_filesystem_store_remote() {
        let bucket = "my-bucket".to_owned();

        let endpoint = "dummy://fake.url".parse().unwrap();

        let res = Builder::new(endpoint, bucket)
            .with_credentials("access-key".to_owned(), "secret".to_owned())
            .build();

        dbg!(&res);
        assert!(res.is_ok());
    }
}

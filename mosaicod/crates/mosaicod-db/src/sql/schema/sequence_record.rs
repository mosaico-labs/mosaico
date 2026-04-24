//! This module provides the data access and minimal business logics for managing
//! **Sequences** within the application database.
//!
//! A Sequence represents a named, persistent entity in the system, often associated
//! with a physical data stream or a collection of topics. It includes methods for
//! creating, retrieving, and managing the state (e.g., locking) of these records.
//!
//! All database operations accept a type that implements `sqlx::Executor`, allowing
//! them to be executed directly against a connection pool or within a transaction.

use crate as db;
use mosaicod_core::types;
use mosaicod_marshal as marshal;

#[derive(Debug, PartialEq)]
pub struct SequenceRecord {
    pub sequence_id: i32,
    pub(crate) sequence_uuid: uuid::Uuid,
    pub(crate) locator_name: String,

    /// This metadata field is only for database query access and
    /// should not be exposed
    pub(crate) user_metadata: Option<serde_json::Value>,

    /// UNIX timestamp in milliseconds from the creation
    pub(crate) creation_unix_tstamp: i64,

    /// Path inside Object store where to find backup files and other sequence info.
    pub(crate) path_in_store: String,
}

impl SequenceRecord {
    /// Creates a new sequence record.
    ///
    /// The new sequence is created in an **unlocked** state. To lock it,
    /// you must call the [`sequence_lock`] method.
    ///
    /// **Note**: This function only creates a local instance. The record will not be present
    /// in the database until [`sequence_create`] is called.
    pub fn new(
        locator_name: types::SequenceLocator,
        path_in_store: types::SequencePathInStore,
    ) -> Self {
        Self {
            sequence_id: db::UNREGISTERED,
            sequence_uuid: uuid::Uuid::new_v4(),
            locator_name: locator_name.into(),
            creation_unix_tstamp: types::Timestamp::now().into(),
            user_metadata: None,
            path_in_store: path_in_store.into(),
        }
    }

    pub fn with_user_metadata(mut self, user_metadata: marshal::JsonMetadataBlob) -> Self {
        self.user_metadata = Some(user_metadata.into());
        self
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }

    pub fn user_metadata(&self) -> Option<marshal::JsonMetadataBlob> {
        self.user_metadata.clone().map(Into::into)
    }

    /// Returns the resource locator for this sequence.
    ///
    /// Because a [`SequenceRecord`] should only be created using [`SequenceRecord::new`], that requires a [`types::SequenceLocator`],
    /// we can assume the locator value inside the DB is always valid. It should panic only if somebody
    /// changed it manually directly inside the database.
    pub fn locator(&self) -> types::SequenceLocator {
        self.locator_name
            .parse()
            .unwrap_or_else(|_| panic!("Invalid sequence locator in DB {}", self.locator_name))
    }

    pub fn uuid(&self) -> types::Uuid {
        self.sequence_uuid.into()
    }

    pub fn path_in_store(&self) -> types::SequencePathInStore {
        self.path_in_store.to_owned().into()
    }
}

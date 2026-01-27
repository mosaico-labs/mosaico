//! This module provides the data access and minimal business logics for managing
//! **Sequences** within the application repository.
//!
//! A Sequence represents a named, persistent entity in the system, often associated
//! with a physical data stream or a collection of topics. It includes methods for
//! creating, retrieving, and managing the state (e.g., locking) of these records.
//!
//! All database operations accept a type that implements `sqlx::Executor`, allowing
//! them to be executed directly against a connection pool or within a transaction.

use crate as repo;
use mosaicod_core::types;
use mosaicod_marshal as marshal;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SequenceRecord {
    pub sequence_id: i32,
    pub sequence_uuid: uuid::Uuid,
    pub locator_name: String,

    pub(super) locked: bool,

    /// This metadata field is only for database query access and
    /// should not be exposed
    pub(super) user_metadata: Option<serde_json::Value>,

    /// UNIX timestamp in milliseconds from the creation
    pub(super) creation_unix_tstamp: i64,
}

impl From<SequenceRecord> for types::ResourceId {
    fn from(value: SequenceRecord) -> Self {
        Self {
            id: value.sequence_id,
            uuid: value.sequence_uuid,
        }
    }
}

impl SequenceRecord {
    /// Creates a new sequence record.
    ///
    /// The new sequence is created in an **unlocked** state. To lock it,
    /// you must call the [`sequence_lock`] method.
    ///
    /// **Note**: This function only creates a local instance. The record will not be present
    /// in the repository until [`sequence_create`] is called.
    pub fn new(name: &str) -> Self {
        Self {
            sequence_id: repo::UNREGISTERED,
            sequence_uuid: uuid::Uuid::new_v4(),
            locator_name: name.to_owned(),
            locked: false,
            creation_unix_tstamp: types::Timestamp::now().into(),
            user_metadata: None,
        }
    }

    pub fn with_user_metadata(mut self, user_metadata: marshal::JsonMetadataBlob) -> Self {
        self.user_metadata = Some(user_metadata.into());
        self
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }
}

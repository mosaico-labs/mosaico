//! This module provides the data access and business logic for managing sessions
//! within the application repository.

use crate as repo;
use mosaicod_core::types;

/// Represents a session record in the database.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SessionRecord {
    /// The unique identifier for the session.
    pub session_id: i32,
    /// The unique UUID for the session.
    pub session_uuid: uuid::Uuid,
    /// The ID of the sequence this session belongs to.
    pub sequence_id: i32,

    /// Specifies if the session is completed. If completed, all data within this
    /// session is considered immutable.
    pub(super) locked: bool,

    /// UNIX timestamp in milliseconds since the creation
    pub(super) creation_unix_tstamp: i64,

    /// UNIX timestamp in milliseconds since the completion
    pub(super) completion_unix_tstamp: Option<i64>,
}

impl From<SessionRecord> for types::ResourceId {
    fn from(value: SessionRecord) -> Self {
        Self {
            id: value.session_id,
            uuid: value.session_uuid.into(),
        }
    }
}

impl SessionRecord {
    /// Creates a new `SessionRecord` for a given sequence.
    ///
    /// The new session is created in an unlocked state.
    /// The record is not persisted until an explicit database operation is called.
    pub fn new(sequence_id: i32) -> Self {
        Self {
            session_id: repo::UNREGISTERED,
            session_uuid: types::Uuid::new().into(),
            sequence_id,
            locked: false,
            creation_unix_tstamp: types::Timestamp::now().into(),
            completion_unix_tstamp: None,
        }
    }

    /// Checks if the session is locked (completed).
    pub fn is_locked(&self) -> bool {
        self.locked
    }

    /// Returns the creation timestamp of the session.
    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }

    /// Returns the completion timestamp of the session, if it has been completed.
    pub fn completion_timestamp(&self) -> Option<types::Timestamp> {
        self.completion_unix_tstamp
            .map(|t| types::Timestamp::from(t))
    }
}

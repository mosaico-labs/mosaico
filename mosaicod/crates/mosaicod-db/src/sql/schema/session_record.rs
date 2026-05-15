//! This module provides the data access and business logic for managing sessions
//! within the application database.

use crate as db;
use mosaicod_core::types;

/// Represents a session record in the database.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SessionRecord {
    /// The unique identifier for the session.
    pub session_id: i32,
    /// The ID of the sequence this session belongs to.
    pub sequence_id: i32,

    pub(crate) locator_name: String,

    /// The unique UUID for the session.
    pub(crate) session_uuid: uuid::Uuid,

    /// UNIX timestamp in milliseconds since the creation
    pub(crate) creation_unix_tstamp: i64,

    /// UNIX timestamp in milliseconds since the completion
    pub(crate) completion_unix_tstamp: Option<i64>,
}

impl SessionRecord {
    /// Creates a new `SessionRecord` for a given sequence.
    ///
    /// The record is not persisted until an explicit database operation is called.
    pub fn new(locator: types::SessionLocator, sequence_id: i32) -> Self {
        Self {
            session_id: db::UNREGISTERED,
            session_uuid: types::Uuid::new().into(),
            sequence_id,
            locator_name: locator.to_string(),
            creation_unix_tstamp: types::Timestamp::now().into(),
            completion_unix_tstamp: None,
        }
    }

    /// Returns the resource locator for this session.
    ///
    /// Because a [`SessionRecord`] should only be created using [`SessionRecord::new`], that requires a [`types::SessionLocator`],
    /// we can assume the locator value inside the DB is always valid. It should panic only if somebody
    /// changed it manually directly inside the database.
    pub fn locator(&self) -> types::SessionLocator {
        self.locator_name
            .parse()
            .unwrap_or_else(|_| panic!("Invalid session locator in DB {}", self.locator_name))
    }

    /// Returns the creation timestamp of the session.
    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }

    /// Returns the completion timestamp of the session, if it has been completed.
    pub fn completion_timestamp(&self) -> Option<types::Timestamp> {
        self.completion_unix_tstamp.map(types::Timestamp::from)
    }

    pub fn uuid(&self) -> types::Uuid {
        self.session_uuid.into()
    }
}

//! This module provides the access to the cleanup routine logs stored in the database.
//!
//! All database operations accept a type that implements `sqlx::Executor`, allowing
//! them to be executed directly against a connection pool or within a transaction.

use crate as db;

#[derive(Debug, PartialEq)]
pub struct CleanupLogRecord {
    pub cleanup_id: i32,
    pub(crate) start_unix_tstamp_secs: i64,
    pub(crate) end_unix_tstamp_secs: Option<i64>,
}

impl Default for CleanupLogRecord {
    fn default() -> Self {
        Self {
            cleanup_id: db::UNREGISTERED,
            start_unix_tstamp_secs: chrono::Utc::now().timestamp(),
            end_unix_tstamp_secs: None,
        }
    }
}

impl CleanupLogRecord {
    pub fn start_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(self.start_unix_tstamp_secs, 0).unwrap_or_else(|| {
            panic!(
                "Error converting cleanup log start UNIX timestamp {} to DateTime",
                self.start_unix_tstamp_secs
            )
        })
    }

    pub fn end_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.end_unix_tstamp_secs.map(|t| {
            chrono::DateTime::from_timestamp(t, 0).unwrap_or_else(|| {
                panic!(
                    "Error converting cleanup log end UNIX timestamp {} to DateTime",
                    t
                )
            })
        })
    }
}

//! This module provides the access to the cleanup routine logs stored in the database.
//!
//! All database operations accept a type that implements `sqlx::Executor`, allowing
//! them to be executed directly against a connection pool or within a transaction.

#[derive(Debug, PartialEq)]
pub struct CleanupLogRecord {
    pub cleanup_id: i32,
    pub(crate) start_unix_tstamp_secs: i64,
    pub(crate) end_unix_tstamp_secs: Option<i64>,

    pub(crate) marked_folders: serde_json::Value,
    pub(crate) deleted_folders: serde_json::Value,
    pub(crate) failed_folders: serde_json::Value,

    /// The instance (see `instance_registry_t`) that performed this run, or `None` if the
    /// originating instance has been garbage collected from the registry.
    pub instance_id: Option<i32>,
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

    pub fn marked_folders(&self) -> Vec<String> {
        serde_json::from_value(self.marked_folders.clone()).unwrap_or_else(|e| {
            panic!(
                "Error deserializing marked folders in cleanup log {}: {}",
                self.cleanup_id, e
            )
        })
    }

    pub fn deleted_folders(&self) -> Vec<String> {
        serde_json::from_value(self.deleted_folders.clone()).unwrap_or_else(|e| {
            panic!(
                "Error deserializing deleted folders in cleanup log {}: {}",
                self.cleanup_id, e
            )
        })
    }

    pub fn failed_folders(&self) -> Vec<(String, String)> {
        serde_json::from_value(self.failed_folders.clone()).unwrap_or_else(|e| {
            panic!(
                "Error deserializing failed folders in cleanup log {}: {}",
                self.cleanup_id, e
            )
        })
    }
}

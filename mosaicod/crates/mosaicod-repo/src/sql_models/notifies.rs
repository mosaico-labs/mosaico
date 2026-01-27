use std::str::FromStr;

use crate as repo;
use mosaicod_core::types;

#[derive(Debug)]
pub struct SequenceNotify {
    pub(super) sequence_notify_id: i32,
    pub sequence_id: i32,
    /// Field containing the string representation of
    /// the underlying [`NotifyType`], this filed is stored as a raw String
    /// since the sqlx driver cannot interact directly with enums.
    pub notify_type: String,
    pub msg: Option<String>,
    /// UNIX timestamp in milliseconds from the creation
    pub(super) creation_unix_tstamp: i64,
}

impl SequenceNotify {
    /// Creates a new sequence notify.
    ///
    /// **Note**: This function only creates a local instance. The record will not be present
    /// in the repository until [`topic_notify_create`] is called.
    pub fn new(sequence_id: i32, notify_type: types::NotifyType, msg: Option<String>) -> Self {
        Self {
            sequence_notify_id: repo::UNREGISTERED,
            sequence_id,
            notify_type: notify_type.to_string(),
            msg,
            creation_unix_tstamp: types::Timestamp::now().into(),
        }
    }

    pub fn into_types(self, loc: types::SequenceResourceLocator) -> types::Notify {
        types::Notify {
            id: self.sequence_notify_id,
            target: Box::new(loc),
            notify_type: self.notify_type(),
            msg: self.msg,
            created_at: types::Timestamp::from(self.creation_unix_tstamp).into(),
        }
    }

    /// Returns the id of the persistent notification record.
    ///
    /// Returns **`None`** if this entity has not yet been persisted to the repository.
    pub fn id(&self) -> Option<i32> {
        if self.sequence_notify_id == repo::UNREGISTERED {
            None
        } else {
            Some(self.sequence_notify_id)
        }
    }

    pub fn notify_type(&self) -> types::NotifyType {
        // Here we use unwrap since we assume that if `SequenceNotify` is
        // constructed correctly there is alwais a conversion from str to `NotifyType`
        types::NotifyType::from_str(&self.notify_type).unwrap()
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }
}

#[derive(Debug)]
pub struct TopicNotify {
    pub(super) topic_notify_id: i32,
    pub topic_id: i32,
    /// Field containing the string representation of
    /// the underlying [`NotifyType`], this filed is stored as a raw String
    /// since the sqlx driver cannot interact directly with enums.
    pub notify_type: String,
    pub msg: Option<String>,
    /// UNIX timestamp in milliseconds from the creation
    pub(super) creation_unix_tstamp: i64,
}

impl TopicNotify {
    /// Creates a new topic notify.
    ///
    /// **Note**: This function only creates a local instance. The record will not be present
    /// in the repository until [`topic_notify_create`] is called.
    pub fn new(topic_id: i32, notify_type: types::NotifyType, msg: Option<String>) -> Self {
        Self {
            topic_notify_id: repo::UNREGISTERED,
            topic_id,
            notify_type: notify_type.to_string(),
            msg,
            creation_unix_tstamp: types::Timestamp::now().into(),
        }
    }

    pub fn into_types(self, loc: types::TopicResourceLocator) -> types::Notify {
        types::Notify {
            id: self.topic_notify_id,
            target: Box::new(loc),
            notify_type: self.notify_type(),
            msg: self.msg,
            created_at: types::Timestamp::from(self.creation_unix_tstamp).into(),
        }
    }

    /// Returns the notify id.
    ///
    /// Returns [`None`] if this entity has not yet been persisted to the repository.
    pub fn id(&self) -> Option<i32> {
        if self.topic_notify_id == repo::UNREGISTERED {
            None
        } else {
            Some(self.topic_notify_id)
        }
    }

    pub fn notify_type(&self) -> types::NotifyType {
        types::NotifyType::from_str(&self.notify_type).unwrap()
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }
}

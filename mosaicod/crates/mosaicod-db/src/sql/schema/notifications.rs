use std::str::FromStr;

use crate as db;
use mosaicod_core::types;

#[derive(Debug)]
pub struct SequenceNotificationRecord {
    pub sequence_notification_id: i32,
    pub sequence_id: i32,
    pub(crate) sequence_notification_uuid: uuid::Uuid,
    /// Field containing the string representation of
    /// the underlying [`NotificationType`], this filed is stored as a raw String
    /// since the sqlx driver cannot interact directly with enums.
    pub(crate) notification_type: String,
    pub msg: Option<String>,
    /// UNIX timestamp in milliseconds from the creation
    pub(crate) creation_unix_tstamp: i64,
}

impl SequenceNotificationRecord {
    /// Creates a new sequence notification.
    ///
    /// **Note**: This function only creates a local instance. The record will not be present
    /// in the database until [`sequence_notification_create`] is called.
    pub fn new(
        sequence_id: i32,
        notification_type: types::NotificationType,
        msg: Option<String>,
    ) -> Self {
        Self {
            sequence_notification_id: db::UNREGISTERED,
            sequence_notification_uuid: types::Uuid::new().into(),
            sequence_id,
            notification_type: notification_type.to_string(),
            msg,
            creation_unix_tstamp: types::Timestamp::now().into(),
        }
    }

    pub fn into_notification(self, loc: types::SequenceResourceLocator) -> types::Notification {
        types::Notification {
            uuid: self.sequence_notification_uuid.into(),
            target: Box::new(loc),
            notification_type: self.notification_type(),
            msg: self.msg,
            created_at: types::Timestamp::from(self.creation_unix_tstamp).into(),
        }
    }

    /// Returns the id of the persistent notification record.
    ///
    /// Returns **`None`** if this entity has not yet been persisted to the database.
    pub fn id(&self) -> Option<i32> {
        if self.sequence_notification_id == db::UNREGISTERED {
            None
        } else {
            Some(self.sequence_notification_id)
        }
    }

    pub fn notification_type(&self) -> types::NotificationType {
        // Here we use unwrap since we assume that if `SequenceNotification` is
        // constructed correctly there is always a conversion from str to `NotificationType`
        types::NotificationType::from_str(&self.notification_type).unwrap()
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }

    pub fn uuid(&self) -> types::Uuid {
        self.sequence_notification_uuid.into()
    }
}

#[derive(Debug)]
pub struct TopicNotificationRecord {
    pub topic_notification_id: i32,
    pub topic_id: i32,
    pub(crate) topic_notification_uuid: uuid::Uuid,
    /// Field containing the string representation of
    /// the underlying [`NotificationType`], this filed is stored as a raw String
    /// since the sqlx driver cannot interact directly with enums.
    pub(crate) notification_type: String,
    pub msg: Option<String>,
    /// UNIX timestamp in milliseconds from the creation
    pub(crate) creation_unix_tstamp: i64,
}

impl TopicNotificationRecord {
    /// Creates a new topic notification.
    ///
    /// **Note**: This function only creates a local instance. The record will not be present
    /// in the database until [`topic_notification_create`] is called.
    pub fn new(
        topic_id: i32,
        notification_type: types::NotificationType,
        msg: Option<String>,
    ) -> Self {
        Self {
            topic_notification_id: db::UNREGISTERED,
            topic_notification_uuid: types::Uuid::new().into(),
            topic_id,
            notification_type: notification_type.to_string(),
            msg,
            creation_unix_tstamp: types::Timestamp::now().into(),
        }
    }

    pub fn into_notification(self, loc: types::TopicResourceLocator) -> types::Notification {
        types::Notification {
            uuid: self.topic_notification_uuid.into(),
            target: Box::new(loc),
            notification_type: self.notification_type(),
            msg: self.msg,
            created_at: types::Timestamp::from(self.creation_unix_tstamp).into(),
        }
    }

    /// Returns the notification id.
    ///
    /// Returns [`None`] if this entity has not yet been persisted to the database.
    pub fn id(&self) -> Option<i32> {
        if self.topic_notification_id == db::UNREGISTERED {
            None
        } else {
            Some(self.topic_notification_id)
        }
    }

    pub fn notification_type(&self) -> types::NotificationType {
        types::NotificationType::from_str(&self.notification_type).unwrap()
    }

    pub fn creation_timestamp(&self) -> types::Timestamp {
        types::Timestamp::from(self.creation_unix_tstamp)
    }

    pub fn uuid(&self) -> types::Uuid {
        self.topic_notification_uuid.into()
    }
}

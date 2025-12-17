use serde::Serialize;

use crate::types::{self, Resource};

/// Generic response message used to provide to clients the key
/// of a resource
#[derive(Serialize, Debug)]
pub struct ResourceKey {
    pub key: String,
}

impl From<types::ResourceId> for ResourceKey {
    fn from(value: types::ResourceId) -> Self {
        Self {
            key: value.uuid.to_string(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct TopicSystemInfo {
    /// Number of chunks in the topic
    pub chunks_number: usize,
    /// Total size in bytes of the data.
    /// Metadata and other system files are excluded in the count.
    pub total_size_bytes: usize,
    /// True if topic is locked
    pub is_locked: bool,
    /// Datetime of the topic creation
    pub created_datetime: String,
}

impl From<types::TopicSystemInfo> for TopicSystemInfo {
    fn from(value: types::TopicSystemInfo) -> Self {
        Self {
            chunks_number: value.chunks_number,
            total_size_bytes: value.total_size_bytes,
            is_locked: value.is_locked,
            created_datetime: value.created_datetime.to_string(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct SequenceSystemInfo {
    /// Total size in bytes of the data.
    /// This values includes additional system files.
    pub total_size_bytes: usize,
    /// True if sequence is locked
    pub is_locked: bool,
    /// Datetime of the sequence creation
    pub created_datetime: String,
}

impl From<types::SequenceSystemInfo> for SequenceSystemInfo {
    fn from(value: types::SequenceSystemInfo) -> Self {
        Self {
            total_size_bytes: value.total_size_bytes,
            is_locked: value.is_locked,
            created_datetime: value.created_datetime.to_string(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ResponseNotifyItem {
    pub name: String,
    pub notify_type: String,
    pub msg: String,
    pub created_datetime: String,
}

impl From<types::Notify> for ResponseNotifyItem {
    fn from(value: types::Notify) -> Self {
        Self {
            name: value.target.name().to_string(),
            notify_type: value.notify_type.to_string(),
            msg: value.msg.unwrap_or_default(),
            created_datetime: value.created_at.to_string(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct NotifyList {
    pub notifies: Vec<ResponseNotifyItem>,
}

impl From<Vec<types::Notify>> for NotifyList {
    fn from(value: Vec<types::Notify>) -> Self {
        Self {
            notifies: value.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ResponseLayerItem {
    pub name: String,
    pub description: String,
}

impl From<types::Layer> for ResponseLayerItem {
    fn from(value: types::Layer) -> Self {
        Self {
            name: value.locator.name().to_owned(),
            description: value.description,
        }
    }
}

#[derive(Serialize, Debug)]
pub struct LayerList {
    pub layers: Vec<ResponseLayerItem>,
}

impl From<Vec<types::Layer>> for LayerList {
    fn from(v: Vec<types::Layer>) -> Self {
        Self {
            layers: v.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ResponseQueryItem {
    pub sequence: String,
    pub topics: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct Query {
    pub items: Vec<ResponseQueryItem>,
}

impl From<types::SequenceTopicGroup> for ResponseQueryItem {
    fn from(value: types::SequenceTopicGroup) -> Self {
        Self {
            sequence: value.sequence.name().to_string(),
            topics: value
                .topics
                .into_iter()
                .map(|t| t.name().to_string())
                .collect(),
        }
    }
}

impl From<types::SequenceTopicGroups> for Query {
    fn from(value: types::SequenceTopicGroups) -> Self {
        let vec: Vec<types::SequenceTopicGroup> = value.into();
        Self {
            items: vec.into_iter().map(Into::into).collect(),
        }
    }
}

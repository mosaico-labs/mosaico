//! This module defines the formatting structure for
//! responses.
use serde::Serialize;

use mosaicod_core::types::{self, Resource};

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

// ########
// Notifies
// ########

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

// ######
// Layers
// ######

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

// #####
// Query
// #####

#[derive(Serialize, Debug)]
pub struct Query {
    pub items: Vec<ResponseQueryItem>,
}

/// Holds topic data: locator and optional timestamp.
#[derive(Serialize, Debug)]
pub struct ResponseQueryItemTopic {
    pub locator: String,
    /// Timestamp range will be omitted from the output if it is None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_range: Option<(i64, i64)>,
}

impl From<types::TopicResourceLocator> for ResponseQueryItemTopic {
    fn from(value: types::TopicResourceLocator) -> Self {
        Self {
            locator: value.name().to_owned(),
            timestamp_range: value
                .timestamp_range
                .map(|e| (e.start.into(), e.end.into())),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ResponseQueryItem {
    pub sequence: String,
    pub topics: Vec<ResponseQueryItemTopic>,
}

impl From<types::SequenceTopicGroup> for ResponseQueryItem {
    fn from(value: types::SequenceTopicGroup) -> Self {
        Self {
            sequence: value.sequence.name().to_string(),
            topics: value.topics.into_iter().map(Into::into).collect(),
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn response_query_item() {
        use types::{
            SequenceResourceLocator, SequenceTopicGroup, TimestampRange, TopicResourceLocator,
        };

        let sequence = SequenceResourceLocator::from("/my_sequence");
        let topics = vec![
            TopicResourceLocator::from("/my_sequence/topic1/subtopic").with_timestamp_range(
                TimestampRange {
                    start: 1000.into(),
                    end: 1001.into(),
                },
            ),
            TopicResourceLocator::from("/my_sequence/topic2/subtopic"),
        ];

        let group = SequenceTopicGroup::new(sequence, topics);
        let response: ResponseQueryItem = group.into();

        let body = serde_json::to_string(&response).unwrap();

        dbg!(body.to_string());

        let response_raw = r#"{"sequence":"my_sequence","topics":[{"locator":"my_sequence/topic1/subtopic","timestamp_range":[1000,1001]},{"locator":"my_sequence/topic2/subtopic"}]}"#;

        let body_serialized = body.to_string();

        if body_serialized != response_raw {
            panic!("wrong response\nexpecting:\n{response_raw}\ngot\n{body_serialized}");
        }
    }
}

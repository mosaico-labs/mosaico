//! This module defines the formatting structure for
//! responses.

use mosaicod_core::types::{self, Locator};
use semver;
use serde::{Deserialize, Serialize};

/// Generic response message used to provide to clients the a unique key
/// of a resource
#[derive(Serialize, Deserialize, Debug)]
pub struct ResourceUuid {
    pub uuid: String,
}

impl From<types::Uuid> for ResourceUuid {
    fn from(value: types::Uuid) -> Self {
        Self {
            uuid: value.to_string(),
        }
    }
}

// ########
// Session
// ########

#[derive(Serialize, Deserialize, Debug)]
pub struct SessionCreate {
    pub uuid: String,
    pub locator: String,
}

// ########
// Notifications
// ########

#[derive(Serialize, Debug)]
pub struct ResponseNotificationItem {
    pub name: String,
    pub notification_type: String,
    pub msg: String,
    pub created_datetime: String,
}

impl<L: Locator> From<types::Notification<L>> for ResponseNotificationItem {
    fn from(value: types::Notification<L>) -> Self {
        Self {
            name: value.target.to_string(),
            notification_type: value.notification_type.to_string(),
            msg: value.msg.unwrap_or_default(),
            created_datetime: value.created_at.to_string(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct NotificationList {
    pub notifications: Vec<ResponseNotificationItem>,
}

impl<L: Locator> From<Vec<types::Notification<L>>> for NotificationList {
    fn from(value: Vec<types::Notification<L>>) -> Self {
        Self {
            notifications: value.into_iter().map(Into::into).collect(),
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
    pub ontology_tag: String,
}

impl From<(types::TopicLocator, String)> for ResponseQueryItemTopic {
    fn from(value: (types::TopicLocator, String)) -> Self {
        Self {
            locator: value.0.to_string(),
            ontology_tag: value.1,
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
            sequence: value.sequence.to_string(),
            topics: value.topics.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<types::SequenceTopicGroupSet> for Query {
    fn from(value: types::SequenceTopicGroupSet) -> Self {
        let vec: Vec<types::SequenceTopicGroup> = value.into();
        Self {
            items: vec.into_iter().map(Into::into).collect(),
        }
    }
}

// ####
// Clustering
// ####

/// Timestamp range of a cluster, in nanoseconds.
#[derive(Serialize, Debug)]
pub struct ClusterTimestampRange {
    pub start_ns: u64,
    pub end_ns: u64,
}

/// Single JSONL record emitted as response to a TopicFilterClusterize request:
/// one cluster per line, identified by a progressive `id` and bounded by ts.
#[derive(Serialize, Debug)]
pub struct TopicFilterClusterize {
    pub ts: ClusterTimestampRange,
    pub id: u64,
}

// ####
// Misc
// ####
#[derive(Serialize, Debug)]
pub struct SemVerItem {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub pre: String,
}

/// Server-configured limits that clients should respect (e.g. when sizing requests).
#[derive(Serialize, Debug)]
pub struct ServerConfig {
    /// Maximum message size (in bytes) accepted/emitted by the gRPC protocol.
    pub max_grpc_message_size: usize,
    /// Target message size (in bytes) the server aims for when streaming data.
    pub target_message_size: usize,
}

#[derive(Serialize, Debug)]
pub struct ServerInfo {
    pub version: String,
    pub semver: SemVerItem,
    pub config: ServerConfig,
}

impl ServerInfo {
    pub fn new(version: &str, config: ServerConfig) -> Result<Self, semver::Error> {
        let parsed = semver::Version::parse(version)?;

        Ok(Self {
            version: version.to_owned(),
            semver: SemVerItem {
                major: parsed.major,
                minor: parsed.minor,
                patch: parsed.patch,
                pre: if !parsed.pre.is_empty() {
                    parsed.pre.to_string()
                } else {
                    String::new()
                },
            },
            config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_query_item() {
        let sequence = "my_sequence".parse().unwrap();
        let topics = vec![
            (
                "my_sequence/topic1/subtopic"
                    .parse::<types::TopicLocator>()
                    .unwrap(),
                "dummy_ontology".to_owned(),
            ),
            (
                "my_sequence/topic2/subtopic"
                    .parse::<types::TopicLocator>()
                    .unwrap(),
                "dummy_ontology".to_owned(),
            ),
        ];

        let group = types::SequenceTopicGroup::new(sequence, topics);
        let response: ResponseQueryItem = group.into();

        let body = serde_json::to_string(&response).unwrap();

        dbg!(body.to_string());

        let response_raw = r#"{"sequence":"my_sequence","topics":[{"locator":"my_sequence/topic1/subtopic","ontology_tag":"dummy_ontology"},{"locator":"my_sequence/topic2/subtopic","ontology_tag":"dummy_ontology"}]}"#;

        let body_serialized = body.to_string();

        assert_eq!(
            body_serialized, response_raw,
            "wrong response\nexpecting:\n{response_raw}\ngot\n{body_serialized}"
        );
    }
}

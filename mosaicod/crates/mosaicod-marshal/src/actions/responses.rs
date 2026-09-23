//! This module defines the formatting structure for responses.
//!
//! Most types here are generated from `.proto` schemas in the repo-root
//! `proto/` directory (see `mosaicod-proto`).
//! Wire format is raw protobuf binary.

use mosaicod_core::types::{self, Locator};

/// Generic response message used to provide to clients the unique key of a resource.
pub use mosaicod_proto::v1::responses::ResourceUuid;

// ########
// Session
// ########

pub use mosaicod_proto::v1::responses::SessionCreate;

// ########
// Notifications
// ########

pub use mosaicod_proto::v1::responses::NotificationItem as ResponseNotificationItem;

pub fn notification_item<L: Locator>(value: types::Notification<L>) -> ResponseNotificationItem {
    ResponseNotificationItem {
        name: value.target.to_string(),
        notification_type: value.notification_type.to_string(),
        msg: value.msg.unwrap_or_default(),
        created_datetime: value.created_at.to_string(),
    }
}

pub use mosaicod_proto::v1::responses::NotificationList;

pub fn notification_list<L: Locator>(value: Vec<types::Notification<L>>) -> NotificationList {
    NotificationList {
        notifications: value.into_iter().map(notification_item).collect(),
    }
}

// #####
// Query
// #####

pub use mosaicod_proto::v1::responses::Query;

/// Holds topic data: locator and optional timestamp.
pub use mosaicod_proto::v1::responses::QueryItemTopic as ResponseQueryItemTopic;

pub fn query_item_topic(value: (types::TopicLocator, String)) -> ResponseQueryItemTopic {
    ResponseQueryItemTopic {
        locator: value.0.to_string(),
        ontology_tag: value.1,
    }
}

pub use mosaicod_proto::v1::responses::QueryItem as ResponseQueryItem;

pub fn query_item(value: types::SequenceTopicGroup) -> ResponseQueryItem {
    ResponseQueryItem {
        sequence: value.sequence.to_string(),
        topics: value.topics.into_iter().map(query_item_topic).collect(),
    }
}

pub fn query_response(value: types::SequenceTopicGroupSet) -> Query {
    let vec: Vec<types::SequenceTopicGroup> = value.into();
    Query {
        items: vec.into_iter().map(query_item).collect(),
    }
}

// ####
// Clustering
// ####

/// Single JSONL record emitted as response to a TopicFilterClusterize request:
/// one cluster per line, identified by a progressive `id` and bounded by ts.
pub use mosaicod_proto::v1::responses::TopicFilterClusterize;

// ####
// Misc
// ####

pub use mosaicod_proto::v1::responses::SemVerItem;

/// Server-configured limits that clients should respect (e.g. when sizing requests).
pub use mosaicod_proto::v1::responses::ServerConfig;

pub use mosaicod_proto::v1::responses::ServerInfo;

pub fn server_info(version: &str, config: ServerConfig) -> Result<ServerInfo, semver::Error> {
    let parsed = semver::Version::parse(version)?;

    Ok(ServerInfo {
        version: version.to_owned(),
        semver: Some(SemVerItem {
            major: parsed.major,
            minor: parsed.minor,
            patch: parsed.patch,
            pre: if !parsed.pre.is_empty() {
                parsed.pre.to_string()
            } else {
                String::new()
            },
        }),
        config: Some(config),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

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
        let response = query_item(group);

        let bytes = response.encode_to_vec();
        let decoded = ResponseQueryItem::decode(bytes.as_slice()).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn resource_uuid_wire_shape() {
        let response = ResourceUuid {
            uuid: "11111111-1111-1111-1111-111111111111".to_string(),
        };

        let bytes = response.encode_to_vec();
        let decoded = ResourceUuid::decode(bytes.as_slice()).unwrap();

        assert_eq!(decoded, response);
    }
}

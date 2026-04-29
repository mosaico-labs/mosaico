//! This module defines the formatting structure for
//! responses.

use mosaicod_core::types::{self, Locator, auth};
use semver;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

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
    /// Timestamp range will be omitted from the output if it is None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_range: Option<(i64, i64)>,
}

impl From<types::TopicLocator> for ResponseQueryItemTopic {
    fn from(value: types::TopicLocator) -> Self {
        Self {
            locator: value.to_string(),
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
// Api Key
// ####

#[derive(Serialize, Debug)]
pub struct ApiKeyToken {
    pub api_key_token: String,
}

impl From<auth::Token> for ApiKeyToken {
    fn from(value: auth::Token) -> Self {
        Self {
            api_key_token: value.to_string(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ApiKeyStatus {
    pub api_key_fingerprint: String,
    pub description: String,
    pub created_at_ns: i64,
    pub expires_at_ns: Option<i64>,
}

impl From<&auth::ApiKey> for ApiKeyStatus {
    fn from(value: &auth::ApiKey) -> Self {
        Self {
            api_key_fingerprint: value.token().fingerprint().to_string(),
            description: value.description.clone(),
            created_at_ns: value.created_at.as_i64(),
            expires_at_ns: value.expires_at.map(Into::into),
        }
    }
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

#[derive(Serialize, Debug)]
pub struct ServerVersion {
    pub version: String,
    pub semver: SemVerItem,
}

impl FromStr for ServerVersion {
    type Err = semver::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let version = semver::Version::parse(s)?;

        Ok(Self {
            version: s.to_owned(),
            semver: SemVerItem {
                major: version.major,
                minor: version.minor,
                patch: version.patch,
                pre: if !version.pre.is_empty() {
                    version.pre.to_string()
                } else {
                    String::new()
                },
            },
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
            "my_sequence/topic1/subtopic"
                .parse::<types::TopicLocator>()
                .unwrap()
                .with_timestamp_range(types::TimestampRange {
                    start: 1000.into(),
                    end: 1001.into(),
                }),
            "my_sequence/topic2/subtopic"
                .parse::<types::TopicLocator>()
                .unwrap(),
        ];

        let group = types::SequenceTopicGroup::new(sequence, topics);
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

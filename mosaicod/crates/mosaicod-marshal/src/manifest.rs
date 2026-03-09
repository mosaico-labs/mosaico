use mosaicod_core::types;
use serde::{Deserialize, Serialize};

// /////////////////////////////////////////////////////////////////////////////
// Topic Manifest
// /////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicManifest {
    timestamp: Option<TopicManifestTimestamp>,
}

impl From<types::TopicManifest> for TopicManifest {
    fn from(value: types::TopicManifest) -> Self {
        Self {
            timestamp: value.timestamp.map(|v| v.into()),
        }
    }
}

impl From<TopicManifest> for types::TopicManifest {
    fn from(value: TopicManifest) -> Self {
        Self {
            timestamp: value.timestamp.map(|v| v.into()),
        }
    }
}

impl TryInto<Vec<u8>> for TopicManifest {
    type Error = super::Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err(|e| super::Error::DeserializationError(e.to_string()))
    }
}

impl TryInto<TopicManifest> for Vec<u8> {
    type Error = super::Error;
    fn try_into(self) -> Result<TopicManifest, Self::Error> {
        serde_json::from_slice(&self).map_err(|e| super::Error::SerializationError(e.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicManifestTimestamp {
    min: i64,
    max: i64,
}

impl From<types::TopicManifestTimestamp> for TopicManifestTimestamp {
    fn from(value: types::TopicManifestTimestamp) -> Self {
        Self {
            min: value.range.start.as_i64(),
            max: value.range.end.as_i64(),
        }
    }
}

impl From<TopicManifestTimestamp> for types::TopicManifestTimestamp {
    fn from(value: TopicManifestTimestamp) -> Self {
        Self {
            range: types::TimestampRange::between(value.min.into(), value.max.into()),
        }
    }
}

// /////////////////////////////////////////////////////////////////////////////
// Session Manifest
// /////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionManifest {
    uuid: String,
    topics: Vec<String>,
    creation_unix_tstamp: i64,
    completion_unix_tstamp: i64,
}

impl From<types::SessionManifest> for SessionManifest {
    fn from(value: types::SessionManifest) -> Self {
        Self {
            uuid: value.uuid.to_string(),
            topics: value.topics.into_iter().map(|t| t.into()).collect(),
            creation_unix_tstamp: value.creation_timestamp.into(),
            completion_unix_tstamp: value.completion_timestamp.into(),
        }
    }
}

impl TryFrom<SessionManifest> for types::SessionManifest {
    type Error = super::Error;
    fn try_from(value: SessionManifest) -> Result<Self, Self::Error> {
        Ok(Self {
            uuid: value
                .uuid
                .parse()
                .map_err(|e: types::UuidError| super::Error::DeserializationError(e.to_string()))?,
            topics: value.topics.into_iter().map(Into::into).collect(),
            creation_timestamp: value.creation_unix_tstamp.into(),
            completion_timestamp: value.completion_unix_tstamp.into(),
        })
    }
}

impl TryInto<Vec<u8>> for SessionManifest {
    type Error = super::Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err(|e| super::Error::DeserializationError(e.to_string()))
    }
}

impl TryInto<SessionManifest> for Vec<u8> {
    type Error = super::Error;
    fn try_into(self) -> Result<SessionManifest, Self::Error> {
        serde_json::from_slice(&self).map_err(|e| super::Error::SerializationError(e.to_string()))
    }
}

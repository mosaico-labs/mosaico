use mosaicod_core::types;
use serde::{Deserialize, Serialize};

// /////////////////////////////////////////////////////////////////////////////
// Topic Manifest
// /////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicManifestInfo {
    chunks_number: usize,
    is_locked: bool,
    total_size_bytes: usize,
    created_timestamp: i64,
}

impl From<types::TopicInfo> for TopicManifestInfo {
    fn from(info: types::TopicInfo) -> TopicManifestInfo {
        Self {
            chunks_number: info.chunks_number,
            is_locked: info.is_locked,
            total_size_bytes: info.total_size_bytes,
            created_timestamp: info.created_timestamp.as_i64(),
        }
    }
}

impl From<TopicManifestInfo> for types::TopicInfo {
    fn from(info: TopicManifestInfo) -> types::TopicInfo {
        Self {
            chunks_number: info.chunks_number,
            is_locked: info.is_locked,
            total_size_bytes: info.total_size_bytes,
            created_timestamp: info.created_timestamp.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicManifest {
    timestamp: TopicManifestTimestamp,
    info: TopicManifestInfo,
}

impl From<types::TopicManifest> for TopicManifest {
    fn from(value: types::TopicManifest) -> Self {
        Self {
            timestamp: value.timestamp.into(),
            info: value.info.into(),
        }
    }
}

impl From<TopicManifest> for types::TopicManifest {
    fn from(value: TopicManifest) -> Self {
        Self {
            timestamp: value.timestamp.into(),
            info: value.info.into(),
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
    created_unix_tstamp: i64,
    completed_unix_tstamp: i64,
}

impl From<types::SessionManifest> for SessionManifest {
    fn from(value: types::SessionManifest) -> Self {
        Self {
            uuid: value.uuid.to_string(),
            topics: value.topics.into_iter().map(|t| t.into()).collect(),
            created_unix_tstamp: value.created_timestamp.into(),
            completed_unix_tstamp: value.completed_timestamp.into(),
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
            created_timestamp: value.created_unix_tstamp.into(),
            completed_timestamp: value.completed_unix_tstamp.into(),
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

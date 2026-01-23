use crate::types;
use serde::{Deserialize, Serialize};

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

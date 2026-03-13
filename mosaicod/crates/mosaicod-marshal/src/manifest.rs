use mosaicod_core::types;
use serde::{Deserialize, Serialize};

// /////////////////////////////////////////////////////////////////////////////
// Session Manifest
// /////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionManifest {
    uuid: String,
    topics: Vec<String>,
    created_at: i64,
    completed_at: Option<i64>,
    locked: bool,
}

impl From<types::SessionManifest> for SessionManifest {
    fn from(value: types::SessionManifest) -> Self {
        Self {
            uuid: value.uuid.to_string(),
            topics: value.topics.into_iter().map(|t| t.into()).collect(),
            created_at: value.created_at.into(),
            completed_at: value.completed_at.map(Into::into),
            locked: value.locked,
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
            created_at: value.created_at.into(),
            completed_at: value.completed_at.map(Into::into),
            locked: value.locked,
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

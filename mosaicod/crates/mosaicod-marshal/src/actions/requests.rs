use super::ActionError;
use crate::Format;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Empty {}

// ////////////////////////////////////////////////////////////////////////////
// Sequence
// ////////////////////////////////////////////////////////////////////////////

/// Specialized message used to create a new sequence in the platform
#[derive(Deserialize, Debug)]
pub struct SequenceCreate {
    pub locator: String,
    user_metadata: serde_json::Value,
}

impl SequenceCreate {
    pub fn user_metadata(&self) -> Result<String, ActionError> {
        Ok(serde_json::to_string(&self.user_metadata)?)
    }
}

// ////////////////////////////////////////////////////////////////////////////
// Topic
// ////////////////////////////////////////////////////////////////////////////

/// Specialized message used to create a new sequence in the platform
#[derive(Deserialize, Debug)]
pub struct TopicCreate {
    pub locator: String,
    pub session_uuid: String,
    pub serialization_format: Format,
    pub ontology_tag: String,

    user_metadata: serde_json::Value,
}

impl TopicCreate {
    pub fn user_metadata(&self) -> Result<String, ActionError> {
        Ok(serde_json::to_string(&self.user_metadata)?)
    }
}

// ////////////////////////////////////////////////////////////////////////////
// Locate & Upload
// ////////////////////////////////////////////////////////////////////////////

/// Request used to locate a specific resource by name.
#[derive(Deserialize, Debug)]
pub struct ResourceLocator {
    pub locator: String,
}

// ////////////////////////////////////////////////////////////////////////////
// Session
// ////////////////////////////////////////////////////////////////////////////

/// Request used to identify a session with its uuid.
#[derive(Deserialize, Debug)]
pub struct SessionUuid {
    pub session_uuid: String,
}

// ////////////////////////////////////////////////////////////////////////////
// Notifications
// ////////////////////////////////////////////////////////////////////////////

/// Generic request message used to create notifications
#[derive(Deserialize, Debug)]
pub struct NotificationCreate {
    pub locator: String,
    pub notification_type: String,
    pub msg: String,
}

// ////////////////////////////////////////////////////////////////////////////
// Query
// ////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Debug)]
pub struct Query {
    #[serde(flatten)]
    /// Query filter used to find matches in the system
    pub query: serde_json::Value,
}

// ////////////////////////////////////////////////////////////////////////////
// Api Key
// ////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Debug)]
pub struct ApiKeyCreate {
    pub permissions: String,
    pub expires_at_ns: Option<i64>,
    pub description: String,
}

#[derive(Deserialize, Debug)]
pub struct ApiKeyFingerprint {
    pub api_key_fingerprint: String,
}

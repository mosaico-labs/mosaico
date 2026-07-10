use super::ActionError;
use crate::Format;
use crate::Ontology;
use crate::flight::FilterTimestampRange;
use serde::Deserialize;
use serde::Serialize;

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

/// Parameters for filtering a single topic by ontology and timestamp range,
/// then clustering matching timestamps by a time-gap threshold.
#[derive(Serialize, Deserialize, Debug)]
pub struct TopicClusterizeParams {
    pub locator: String,
    pub clustering_dt_ns: u64,
    pub ontology: Ontology,
    pub timestamp_range: Option<FilterTimestampRange>,
}

/// Filters a topic by ontology and timestamp range,
/// then clusters matching timestamps by a time-gap threshold.
#[derive(Serialize, Deserialize, Debug)]
pub struct TopicFilterClusterize {
    #[serde(flatten)]
    pub params: TopicClusterizeParams,
}

/// Receives multiple topic filters (each with its own clustering configuration)
/// and intersects their clustered timestamp sets, retaining only timestamps
/// that fall within intersect_dt_ns nanoseconds of each other across all topics
#[derive(Deserialize, Debug)]
pub struct TopicFilterIntersect {
    pub topics: Vec<TopicClusterizeParams>,
    pub intersect_dt_ns: u64,
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

use super::ActionError;
use crate::Ontology;

// Empty request.
pub use mosaicod_proto::v1::requests::Empty;

/// Request used to locate a specific resource by name.
pub use mosaicod_proto::v1::requests::ResourceLocator;

// ////////////////////////////////////////////////////////////////////////////
// Session
// ////////////////////////////////////////////////////////////////////////////

/// Request used to identify a session with its uuid.
pub use mosaicod_proto::v1::requests::SessionUuid;

// ////////////////////////////////////////////////////////////////////////////
// Notifications
// ////////////////////////////////////////////////////////////////////////////

/// Generic request message used to create notifications.
pub use mosaicod_proto::v1::requests::NotificationCreate;

// ////////////////////////////////////////////////////////////////////////////
// Sequence
// ////////////////////////////////////////////////////////////////////////////

/// Specialized message used to create a new sequence in the platform.
pub use mosaicod_proto::v1::requests::SequenceCreate;

// ////////////////////////////////////////////////////////////////////////////
// Topic
// ////////////////////////////////////////////////////////////////////////////

pub use mosaicod_proto::v1::requests::TopicCreate;

/// Parameters for filtering a single topic by ontology and timestamp range,
/// then clustering matching timestamps by a time-gap threshold. Reused
/// (nested) inside [`TopicFilterIntersect::topics`], and directly as the
/// `topic_filter_clusterize` action's own request payload.
pub use mosaicod_proto::v1::requests::TopicClusterizeParams;

/// `TopicClusterizeParams.ontology` carries the query filter DSL's arbitrary,
/// user-supplied shape as raw JSON bytes (see the note on it in
/// `proto/mosaicod/v1/requests.proto`). Bridges it back to [`crate::Ontology`].
pub fn topic_clusterize_ontology(value: &TopicClusterizeParams) -> Result<Ontology, ActionError> {
    let bytes: &[u8] = if value.ontology.is_empty() {
        b"{}"
    } else {
        &value.ontology
    };

    Ok(serde_json::from_slice(bytes)?)
}

pub use mosaicod_proto::v1::requests::TopicFilterIntersect;

// ////////////////////////////////////////////////////////////////////////////
// Query
// ////////////////////////////////////////////////////////////////////////////

/// The query filter DSL (see [`Ontology`]) has an arbitrary, user-supplied
/// shape with no fixed field set.
pub use mosaicod_proto::v1::requests::Query;

pub fn query_filter(value: &Query) -> Result<serde_json::Value, ActionError> {
    let bytes: &[u8] = if value.query.is_empty() {
        b"{}"
    } else {
        &value.query
    };

    Ok(serde_json::from_slice(bytes)?)
}

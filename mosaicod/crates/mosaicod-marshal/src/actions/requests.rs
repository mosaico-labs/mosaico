// Empty request.
pub use mosaicod_proto::v1::core::Empty;
// ////////////////////////////////////////////////////////////////////////////
// Notifications
// ////////////////////////////////////////////////////////////////////////////
/// Generic request message used to create notifications.
pub use mosaicod_proto::v1::requests::NotificationCreate;
/// Request used to locate a specific resource by name.
pub use mosaicod_proto::v1::requests::ResourceLocator;
// ////////////////////////////////////////////////////////////////////////////
// Sequence
// ////////////////////////////////////////////////////////////////////////////
/// Specialized message used to create a new sequence in the platform.
pub use mosaicod_proto::v1::requests::SequenceCreate;
// ////////////////////////////////////////////////////////////////////////////
// Session
// ////////////////////////////////////////////////////////////////////////////
/// Request used to identify a session with its uuid.
pub use mosaicod_proto::v1::requests::SessionUuid;
/// Parameters for filtering a single topic by ontology and timestamp range,
/// then clustering matching timestamps by a time-gap threshold. Reused
/// (nested) inside [`TopicFilterIntersect::topics`], and directly as the
/// `topic_filter_clusterize` action's own request payload.
pub use mosaicod_proto::v1::requests::TopicClusterizeParams;
// ////////////////////////////////////////////////////////////////////////////
// Topic
// ////////////////////////////////////////////////////////////////////////////
pub use mosaicod_proto::v1::requests::TopicCreate;
use mosaicod_query as query;

/// Converts `TopicClusterizeParams.ontology` into a [`query::OntologyFilter`], an
/// absent filter is treated as an empty one.
pub fn topic_clusterize_ontology(
    value: &TopicClusterizeParams,
) -> Result<query::OntologyFilter, crate::Error> {
    crate::ontology_filter_from_proto(value.ontology.clone().unwrap_or_default())
}

/// Typed query filter carried by [`Query`].
pub use mosaicod_proto::v1::query::Filter as QueryFilter;
// ////////////////////////////////////////////////////////////////////////////
// Query
// ////////////////////////////////////////////////////////////////////////////
/// `query` action's request payload, carrying a typed query filter.
pub use mosaicod_proto::v1::requests::Query;
pub use mosaicod_proto::v1::requests::TopicFilterIntersect;

/// Extracts the query filter, an absent filter is treated as an empty one.
pub fn query_filter(value: Query) -> QueryFilter {
    value.filter.unwrap_or_default()
}

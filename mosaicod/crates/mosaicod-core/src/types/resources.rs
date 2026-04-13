use super::{Format, TimestampRange};
use super::{SessionMetadata, Uuid};
use crate::{params, traits, types};
use std::path;
use thiserror::Error;

// ////////////////////////////////////////////////////////////////////////////
// RESOURCE
// ////////////////////////////////////////////////////////////////////////////

/// Represents the unique identifiers of a record.
pub struct Identifiers {
    /// The internal, numeric ID of the resource (e.g., a database primary key).
    pub id: i32,
    /// The universally unique identifier (UUID) for the resource.
    pub uuid: Uuid,
}

pub enum IdLookup {
    Id(i32),
    Uuid(Uuid),
}

impl std::fmt::Display for IdLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id(id) => write!(f, "id:{}", id),
            Self::Uuid(uuid) => write!(f, "uuid:{}", uuid),
        }
    }
}

/// Defines the different ways a resource (topic, sequence and sessions) can be looked up.
pub enum ResourceLookup {
    /// Lookup by the internal numeric ID.
    Id(i32),
    /// Lookup by its unique string locator (e.g., `my/sequence/my/topic`).
    Locator(String),
    /// Lookup by its universally unique identifier.
    Uuid(Uuid),
}

impl std::fmt::Display for ResourceLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id(id) => write!(f, "id:{}", id),
            Self::Uuid(uuid) => write!(f, "uuid:{}", uuid),
            Self::Locator(locator) => write!(f, "locator:{}", locator),
        }
    }
}

/// Enumerates the types of resources available in Mosaico.
pub enum ResourceType {
    /// A resource that represents a collection of related topics.
    Sequence,
    /// A resource that represents a stream of data.
    Topic,
}

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("error encoding resource to url")]
    UrlError(#[from] url::ParseError),
}

pub trait Resource: std::fmt::Display + Send + Sync {
    fn locator(&self) -> &str;

    fn resource_type(&self) -> ResourceType;

    /// Return the URL representing the resource
    /// For now the URL is without authority.
    ///
    /// # Example
    /// `mosaico:/sequence_name/topic/subtopic/sensor`
    fn url(&self) -> Result<url::Url, ResourceError> {
        let schema = params::MOSAICO_URL_SCHEMA;
        let path = self.locator();
        Ok(url::Url::parse(&format!("{schema}:/{path}"))?)
    }

    fn is_sub_resource(&self, parent: &dyn Resource) -> bool {
        self.locator().starts_with(parent.locator())
    }

    /// Return the resource path
    ///
    /// # Example
    /// ```txt, ignore
    /// sequence/my/topic
    /// ```
    /// or
    /// ```txt, ignore
    /// sequence
    /// ```
    fn path(&self) -> &path::Path {
        path::Path::new(self.locator())
    }
}

// ////////////////////////////////////////////////////////////////////////////
// TOPIC
// ////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a topic resource and an optional time-based filter.
///
/// This locator combines a string-based path (`locator`) with an optional
/// [`TimestampRange`] to specify a subset of data within the topic.
#[derive(Default, Debug, Clone)]
pub struct TopicResourceLocator {
    /// The unique string identifier for the topic (e.g., `my/sequence/my/topic`).
    locator: String,
    /// An optional time range to filter data within the topic.
    pub timestamp_range: Option<TimestampRange>,
}

impl TopicResourceLocator {
    pub fn with_timestamp_range(mut self, ts: TimestampRange) -> Self {
        self.timestamp_range = Some(ts);
        self
    }

    /// Returns the filename of the data file.
    ///
    /// The data file is composed as follows:
    /// ```txt,ignore
    /// data-[chunk_number].[extension]
    /// ```
    pub fn data_file(chunk_number: usize, extension: &dyn traits::AsExtension) -> String {
        format!(
            "data-{chunk_number:05}.{ext}",
            ext = extension.as_extension()
        )
    }

    pub fn into_parts(self) -> (String, Option<TimestampRange>) {
        (self.locator, self.timestamp_range)
    }

    /// Returns the complete path of a specific data file.
    ///
    /// # Example
    /// ```txt, ignore
    /// sequence/my/topic/2sr5g/data-0000.parquet
    /// ```
    pub fn path_data(
        &self,
        uuid: &Uuid,
        chunk_number: usize,
        extension: &dyn traits::AsExtension,
    ) -> path::PathBuf {
        let filename = Self::data_file(chunk_number, extension);
        self.path_data_folder(uuid).join(filename)
    }

    /// Return the complete path of the folder contianing all data
    ///
    /// # Example
    /// ```txt, ignore
    /// sequence/my/topic/2sr5g
    /// ```
    pub fn path_data_folder(&self, uuid: &Uuid) -> path::PathBuf {
        let cropped_uuid: String = uuid.non_hyphened_string().chars().take(5).collect();

        self.path().join(format!("data:{cropped_uuid}"))
    }

    /// Return the full path of the metadata file
    pub fn path_metadata(&self) -> path::PathBuf {
        path::Path::new(self.locator()).join("metadata.json")
    }
}

impl Resource for TopicResourceLocator {
    fn locator(&self) -> &str {
        &self.locator
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::Topic
    }
}

impl<T> From<T> for TopicResourceLocator
where
    T: AsRef<path::Path>,
{
    fn from(value: T) -> Self {
        Self {
            locator: sanitize_name(&value.as_ref().to_string_lossy()),
            ..Default::default()
        }
    }
}

impl std::fmt::Display for TopicResourceLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.locator)
    }
}

impl From<TopicResourceLocator> for String {
    fn from(value: TopicResourceLocator) -> Self {
        value.locator
    }
}

impl AsRef<str> for TopicResourceLocator {
    fn as_ref(&self) -> &str {
        self.locator.as_ref()
    }
}

#[derive(Debug)]
pub struct TopicOntologyProperties {
    pub serialization_format: Format,
    pub ontology_tag: String,
}

/// Properties defining the data semantic and encoding for a topic.
#[derive(Debug)]
pub struct TopicOntologyMetadata<M> {
    pub properties: TopicOntologyProperties,
    pub user_metadata: Option<M>,
}

impl<M> TopicOntologyMetadata<M> {
    pub fn new(props: TopicOntologyProperties, user_metadata: Option<M>) -> Self
    where
        M: super::MetadataBlob,
    {
        Self {
            properties: props,
            user_metadata,
        }
    }
}

#[derive(Debug)]
pub struct TopicMetadata<M> {
    pub properties: TopicMetadataProperties,
    pub ontology_metadata: TopicOntologyMetadata<M>,
}

impl<M> TopicMetadata<M> {
    pub fn new(
        properties: TopicMetadataProperties,
        ontology_metadata: TopicOntologyMetadata<M>,
    ) -> Self
    where
        M: super::MetadataBlob,
    {
        Self {
            properties,
            ontology_metadata,
        }
    }
}

/// Aggregated statistics for a topic's chunks.
#[derive(Debug, Clone, Default)]
pub struct TopicChunksStats {
    pub total_size_bytes: i64,
    pub total_row_count: i64,
}

/// Metadata properties associated to a topic.
#[derive(Debug)]
pub struct TopicMetadataProperties {
    pub created_at: types::Timestamp,
    pub completed_at: Option<types::Timestamp>,
    pub session_uuid: Uuid,
    pub resource_locator: TopicResourceLocator,
}

impl TopicMetadataProperties {
    pub fn new(resource_locator: TopicResourceLocator, session_uuid: Uuid) -> Self {
        Self::new_with_created_at(resource_locator, session_uuid, types::Timestamp::now())
    }

    pub fn new_with_created_at(
        resource_locator: TopicResourceLocator,
        session_uuid: Uuid,
        created_at: types::Timestamp,
    ) -> Self {
        Self {
            resource_locator,
            created_at,
            completed_at: None,
            session_uuid,
        }
    }
}

/// Represents system-level metadata and statistical information for a specific topic.
///
/// This struct provides a snapshot of the topic's physical state on disk, including
/// its size, structure, and lifecycle status.
#[derive(Debug)]
pub struct TopicDataInfo {
    /// Number of chunks in the topic
    pub chunks_number: u64,
    /// Total size in bytes of the data.
    /// Metadata and other system files are excluded in the count.
    pub total_bytes: u64,
    /// First and last timestamps present in the topic data.
    pub timestamp_range: TimestampRange,
}

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE
// ////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a sequence resource.
///
/// A sequence acts as a container for a collection of related topics. This locator
/// is a sanitized, path-like string (e.g., `my/sequence`) that provides a
/// human-readable and stable identifier for the sequence.
#[derive(Debug, Clone)]
pub struct SequenceResourceLocator(String);

impl SequenceResourceLocator {
    /// Returns the location of the metadata file associated with the sequence.
    ///
    /// The metadata file may or may not exists, no check if performed by this function.
    pub fn path_metadata(&self) -> path::PathBuf {
        let mut path = path::Path::new(self.locator()).join("metadata");
        path.set_extension(params::ext::JSON);
        path
    }
}

impl Resource for SequenceResourceLocator {
    fn locator(&self) -> &str {
        &self.0
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::Sequence
    }
}

impl<T> From<T> for SequenceResourceLocator
where
    T: AsRef<path::Path>,
{
    fn from(value: T) -> Self {
        Self(sanitize_name(&value.as_ref().to_string_lossy()))
    }
}

impl std::fmt::Display for SequenceResourceLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SequenceResourceLocator> for String {
    fn from(value: SequenceResourceLocator) -> String {
        value.0
    }
}

impl AsRef<str> for SequenceResourceLocator {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

pub struct SequenceMetadata<M> {
    /// Timestamp of the sequence creation
    pub created_at: super::Timestamp,
    pub resource_locator: SequenceResourceLocator,
    pub sessions: Vec<SessionMetadata>,
    pub user_metadata: Option<M>,
}

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE TOPIC GROUP
// ////////////////////////////////////////////////////////////////////////////

/// Groups a specific sequence with its associated topics and an optional time filter.
///
/// This structure acts as a container to link a [`SequenceResourceLocator`] with multiple [`TopicResourceLocator`]s.
#[derive(Debug)]
pub struct SequenceTopicGroup {
    pub sequence: SequenceResourceLocator,
    pub topics: Vec<TopicResourceLocator>,
}

impl SequenceTopicGroup {
    pub fn new(sequence: SequenceResourceLocator, topics: Vec<TopicResourceLocator>) -> Self {
        Self { sequence, topics }
    }

    pub fn into_parts(self) -> (SequenceResourceLocator, Vec<TopicResourceLocator>) {
        (self.sequence, self.topics)
    }
}

/// A collection of [`SequenceTopicGroup`] items, providing utilities for
/// set-based operations like merging and intersection.
///
/// This wrapper facilitates grouped management of topics associated with specific
/// sequences, ensuring data consistency during complex merge operations.
#[derive(Debug)]
pub struct SequenceTopicGroupSet(Vec<SequenceTopicGroup>);

impl SequenceTopicGroupSet {
    pub fn new(groups: Vec<SequenceTopicGroup>) -> Self {
        Self(groups)
    }

    /// Returns and empty group set
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Merges two sets of groups by intersecting sequences and joining their topics.
    ///
    /// Only groups present in both `self` and `groups` are retained. Topics within
    /// matched groups are combined, deduplicated, and sorted by name.
    /// # Example
    ///
    /// ```
    /// # use mosaicod_core::types::{SequenceTopicGroupSet, SequenceTopicGroup};
    /// # // Assuming SequenceTopicGroup and relevant types are in scope
    /// let set_a = SequenceTopicGroupSet::new(vec![/* ... */]);
    /// let set_b = SequenceTopicGroupSet::new(vec![/* ... */]);
    ///
    /// let merged = set_a.merge(set_b);
    /// ```
    pub fn merge(self, mut groups: Self) -> Self {
        let max_capacity = groups.0.len().max(self.0.len());
        let mut result = Vec::with_capacity(max_capacity);

        groups
            .0
            .sort_unstable_by(|a, b| a.sequence.locator().cmp(b.sequence.locator()));

        for mut self_grp in self.0 {
            let found = groups.0.binary_search_by(|grp_aux| {
                grp_aux.sequence.locator().cmp(self_grp.sequence.locator())
            });

            if let Ok(found) = found {
                self_grp.topics.extend(groups.0[found].topics.clone());

                // Sort and remove duplicates
                self_grp
                    .topics
                    .sort_unstable_by(|a, b| a.locator().cmp(b.locator()));
                self_grp.topics.dedup_by(|a, b| a.locator() == b.locator());

                result.push(self_grp);
            }
        }

        Self(result)
    }
}

impl Default for SequenceTopicGroupSet {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<SequenceTopicGroup>> for SequenceTopicGroupSet {
    fn from(value: Vec<SequenceTopicGroup>) -> Self {
        Self::new(value)
    }
}

impl From<SequenceTopicGroupSet> for Vec<SequenceTopicGroup> {
    fn from(value: SequenceTopicGroupSet) -> Self {
        value.0
    }
}

/// Builds a sanitized resource name
///
/// Sanitized resource names have the following requirements:
/// - remove any space
/// - remove any leading `/`
/// - any non-alphanumeric char as first element is removed
/// - these symbol `! " ' * £ $ % &` are removed
/// - any non-ASCII char is replaced with a `?`
fn sanitize_name(name: &str) -> String {
    let chars_to_replace = vec!["!", "\"", "'", "*", "£", "$", "%", "&", "."];

    let mut sanitized: String = name
        .replace(" ", "")
        .trim()
        .trim_start_matches('/')
        .to_owned();

    sanitized = sanitized
        .chars()
        .map(|c| if c.is_ascii() { c } else { '?' })
        .collect();

    for c in chars_to_replace {
        sanitized = sanitized.replace(c, "");
    }

    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_name() {
        let target = "my/resource/name";
        let san = sanitize_name("/my/resource/name");
        assert_eq!(san, target);

        let san = sanitize_name("    my/resource/name   ");
        assert_eq!(san, target);

        let san = sanitize_name("//my/resource/name");
        assert_eq!(san, target);

        let san = sanitize_name("/ /my/resource/name");
        assert_eq!(san, target);

        let san = sanitize_name("/ //my/resource/name");
        assert_eq!(san, target);

        let san = sanitize_name("/!\"my/resource/name");
        assert_eq!(san, target);

        let san = sanitize_name("/my/resource/na.me");
        assert_eq!(san, target);

        let san = sanitize_name("/èmy/resource/name");
        assert_eq!(san, "?my/resource/name");

        let san = sanitize_name("my/resourcè/name");
        assert_eq!(san, "my/resourc?/name");
    }

    #[test]
    fn merge_sequence_topic_groups() {
        let groups1 = SequenceTopicGroupSet::new(vec![
            SequenceTopicGroup::new(
                SequenceResourceLocator::from("sequence_1"),
                vec![
                    TopicResourceLocator::from("topic_1"),
                    TopicResourceLocator::from("topic_2"),
                ],
            ),
            SequenceTopicGroup::new(
                SequenceResourceLocator::from("sequence_2"),
                vec![TopicResourceLocator::from("topic_1")],
            ),
        ]);

        let groups2 = SequenceTopicGroupSet::new(vec![
            SequenceTopicGroup::new(
                SequenceResourceLocator::from("sequence_1"),
                vec![
                    TopicResourceLocator::from("topic_1"),
                    TopicResourceLocator::from("topic_3"),
                ],
            ),
            SequenceTopicGroup::new(
                SequenceResourceLocator::from("sequence_3"),
                vec![TopicResourceLocator::from("topic_1")],
            ),
        ]);

        let merged: Vec<SequenceTopicGroup> = groups1.merge(groups2).into();

        dbg!(&merged);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].sequence.locator(), "sequence_1");
        assert_eq!(merged[0].topics.len(), 3);
    }
}

use super::{Format, TimestampRange};
use crate::{params, traits};
use std::path;
use thiserror::Error;

// ////////////////////////////////////////////////////////////////////////////
// RESOURCE
// ////////////////////////////////////////////////////////////////////////////

pub struct ResourceId {
    pub id: i32,
    pub uuid: uuid::Uuid,
}

pub enum ResourceType {
    Sequence,
    Topic,
}

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("error encoding resource to url :: {0}")]
    UrlError(#[from] url::ParseError),
}

pub trait Resource: std::fmt::Display + Send + Sync {
    fn name(&self) -> &String;

    fn resource_type(&self) -> ResourceType;

    /// Returns the location of the metadata file associated with the resource.
    ///
    /// The metadata file may or may not exists, no check if performed by this function.
    fn path_metadata(&self) -> path::PathBuf {
        let mut path = path::Path::new(self.name()).join("metadata");
        path.set_extension(params::ext::JSON);
        path
    }

    /// Return the URL representing the resource
    /// For now the URL is without authority.
    ///
    /// # Example
    /// `mosaico:/sequence_name/topic/subtopic/sensor`
    fn url(&self) -> Result<url::Url, ResourceError> {
        let schema = params::MOSAICO_URL_SCHEMA;
        let path = self.name();
        Ok(url::Url::parse(&format!("{schema}:/{path}"))?)
    }

    /// Return the path of the resource
    fn path(&self) -> &path::Path {
        path::Path::new(self.name())
    }

    fn is_sub_resource(&self, parent: &dyn Resource) -> bool {
        self.name().starts_with(parent.name())
    }
}

// ////////////////////////////////////////////////////////////////////////////
// TOPIC
// ////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone)]
pub struct TopicResourceLocator {
    locator: String,
    pub timestamp_range: Option<TimestampRange>,
}

impl TopicResourceLocator {
    pub fn with_timestamp_range(mut self, ts: TimestampRange) -> Self {
        self.timestamp_range = Some(ts);
        self
    }

    pub fn into_parts(self) -> (String, Option<TimestampRange>) {
        (self.locator, self.timestamp_range)
    }

    pub fn path_data(
        &self,
        chunk_number: usize,
        extension: &dyn traits::AsExtension,
    ) -> path::PathBuf {
        let filename = format!("data-{:05}", chunk_number);
        let mut path = path::Path::new(self.name()).join(filename);

        path.set_extension(extension.as_extension());

        path
    }

    /// Return the full path of the manifest file
    pub fn path_manifest(&self) -> path::PathBuf {
        path::Path::new(self.name()).join("manifest.json")
    }
}

impl Resource for TopicResourceLocator {
    fn name(&self) -> &String {
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
        if let Some(ts) = &self.timestamp_range {
            write!(f, "[topic|{}|{}]", self.locator, ts)
        } else {
            write!(f, "[topic|{}]", self.locator)
        }
    }
}

impl From<TopicResourceLocator> for String {
    fn from(value: TopicResourceLocator) -> Self {
        value.locator
    }
}

#[derive(Debug)]
pub struct TopicMetadata<M> {
    pub properties: TopicProperties,
    pub user_metadata: M,
}

impl<M> TopicMetadata<M> {
    pub fn new(props: TopicProperties, user_metadata: M) -> Self
    where
        M: super::MetadataBlob,
    {
        Self {
            properties: props,
            user_metadata,
        }
    }
}

/// Aggregated statistics for a topic's chunks.
#[derive(Debug, Clone, Default)]
pub struct TopicChunksStats {
    pub total_size_bytes: i64,
    pub total_row_count: i64,
}

/// Configuration properties defining the data semantic and encoding for a topic.
#[derive(Debug)]
pub struct TopicProperties {
    pub serialization_format: Format,
    pub ontology_tag: String,
}

impl TopicProperties {
    pub fn new(serialization_format: Format, ontology_tag: String) -> Self {
        Self {
            serialization_format,
            ontology_tag,
        }
    }
}

/// Represents system-level metadata and statistical information for a specific topic.
///
/// This struct provides a snapshot of the topic's physical state on disk, including
/// its size, structure, and lifecycle status.
pub struct TopicSystemInfo {
    /// Number of chunks in the topic
    pub chunks_number: usize,
    /// True is the topic is currently locked, a topic is locked if
    /// some data was uploaded and the connection was closed gracefully
    ///
    /// # Note
    /// (cabba) TODO: evaluate move this into a separate function since is not strictly related to system info
    pub is_locked: bool,
    /// Total size in bytes of the data.
    /// Metadata and other system files are excluded in the count.
    pub total_size_bytes: usize,
    /// Datetime of the topic creation
    pub created_datetime: super::DateTime,
}

/// Metadata generated during topic consolidation.
///
/// This manifest aggregates all topic details once the write process is finalized.
#[derive(Default)]
pub struct TopicManifest {
    pub timestamp: Option<TopicManifestTimestamp>,
}

impl TopicManifest {
    /// Generates an empty topic manifest
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn with_timestamp(mut self, timestamp: TopicManifestTimestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// Timestamp statistics for the topic index.
pub struct TopicManifestTimestamp {
    /// Timestamp range observed (min and max) in this topic
    pub range: super::TimestampRange,
}

impl TopicManifestTimestamp {
    pub fn new(range: super::TimestampRange) -> Self {
        Self { range }
    }
}

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE
// ////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SequenceResourceLocator(String);

impl Resource for SequenceResourceLocator {
    fn name(&self) -> &String {
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
        write!(f, "[sequence|{}]", self.0)
    }
}

impl From<SequenceResourceLocator> for String {
    fn from(value: SequenceResourceLocator) -> String {
        value.0
    }
}

pub struct SequenceMetadata<M>
where
    M: super::MetadataBlob,
{
    pub user_metadata: M,
}

impl<M> SequenceMetadata<M>
where
    M: super::MetadataBlob,
{
    pub fn new(user_metadata: M) -> Self {
        Self { user_metadata }
    }
}

pub struct SequenceSystemInfo {
    /// Total size in bytes of the data.
    /// This values includes additional system files.
    pub total_size_bytes: usize,
    /// True is the sequence is locked, a sequence is locked if
    /// all its topics are locked and the `sequence_finalize` action
    /// was called.
    pub is_locked: bool,
    /// Datetime of the sequence creation
    pub created_datetime: super::DateTime,
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

#[derive(Debug)]
pub struct SequenceTopicGroups(Vec<SequenceTopicGroup>);

impl SequenceTopicGroups {
    pub fn new(groups: Vec<SequenceTopicGroup>) -> Self {
        Self(groups)
    }

    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Consumes the current group and a provided group to produce a new group in which
    /// the sequences are intersected while the topics are joined
    pub fn merge(self, group: Self) -> Self {
        let mut result = Self::empty();
        for mut grp1 in self.0 {
            let found = group
                .0
                .iter()
                .find(|grp2| grp1.sequence.name() == grp2.sequence.name());

            if let Some(found) = found {
                grp1.topics.extend(found.topics.clone());
                result
                    .0
                    .push(SequenceTopicGroup::new(found.sequence.clone(), grp1.topics));
            }
        }

        result
    }
}

impl Default for SequenceTopicGroups {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<SequenceTopicGroup>> for SequenceTopicGroups {
    fn from(value: Vec<SequenceTopicGroup>) -> Self {
        Self::new(value)
    }
}

impl From<SequenceTopicGroups> for Vec<SequenceTopicGroup> {
    fn from(value: SequenceTopicGroups) -> Self {
        value.0
    }
}

/// Builds a sanitized resource name
///
/// Sanitized resource names have the following requirements:
/// - remove any leading `/`
/// - any non-alphanumeric char as first element is removed
/// - these symbol `! " ' * £ $ % &` are removed
/// - any non-ASCII char is replaced with a `?`
fn sanitize_name(name: &str) -> String {
    let chars_to_replace = vec!["!", "\"", "'", "*", "£", "$", "%", "&"];

    let mut sanitized: String = name.trim().trim_start_matches('/').to_owned();

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

        let san = sanitize_name("/!\"my/resource/name");
        assert_eq!(san, target);

        let san = sanitize_name("/èmy/resource/name");
        assert_eq!(san, "?my/resource/name");

        let san = sanitize_name("my/resourcè/name");
        assert_eq!(san, "my/resourc?/name");
    }

    #[test]
    fn merge_sequence_topic_groups() {}
}

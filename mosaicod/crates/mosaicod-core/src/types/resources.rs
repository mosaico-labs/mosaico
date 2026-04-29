use super::{Format, SessionMetadata, TimestampRange, Uuid};
use crate::{Error, params, traits, types};
use std::cmp::PartialEq;
use std::ops::Deref;
use std::path;
use std::str::FromStr;

// ////////////////////////////////////////////////////////////////////////////
// RESOURCE LOCATOR
// ////////////////////////////////////////////////////////////////////////////

/// List of invalid symbols in a locator name.
static INVALID_CHARS: &[char] = &['!', '\"', '\'', '*', '£', '$', '%', '&', '.', ' '];

/// Enumerates the types of resources available in Mosaico.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    /// A resource that represents a collection of sessions and topics.
    Sequence,
    /// A resource that represents a group of topics uploaded together.
    Session,
    /// A resource that represents a stream of data.
    Topic,
}

impl std::fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let kind = match self {
            ResourceKind::Sequence => "Sequence",
            ResourceKind::Session => "Session",
            ResourceKind::Topic => "Topic",
        };
        write!(f, "{}", kind)
    }
}

pub trait Locator: std::fmt::Display {
    fn kind() -> ResourceKind;
}

/// Checks if value has symbols not admitted for a locator.
///
/// The following criteria must be met:
/// - non-ASCII chars are not allowed
/// - special symbols `! " ' * £ $ % &` are not allowed
fn has_invalid_symbols(value: &str, others: Option<&[char]>) -> bool {
    value.chars().any(|c| {
        !c.is_ascii() || INVALID_CHARS.contains(&c) || others.is_some_and(|o| o.contains(&c))
    })
}

// ////////////////////////////////////////////////////////////////////////////
// TOPIC
// ////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a topic resource and an optional time-based filter.
///
/// This locator combines a string-based path (`locator`) with an optional
/// [`TimestampRange`] to specify a subset of data within the topic.
#[derive(Debug, Clone)]
pub struct TopicLocator {
    /// The unique string identifier for the sequence (e.g., `my_sequence`).
    pub sequence: SequenceLocator,

    /// Topic name (it does not contain the sequence nor the '/' separator, e.g. my/topic).
    name: String,

    /// An optional time range to filter data within the topic.
    pub timestamp_range: Option<TimestampRange>,
}

impl TopicLocator {
    pub fn with_timestamp_range(mut self, ts: TimestampRange) -> Self {
        self.timestamp_range = Some(ts);
        self
    }
}

impl Locator for TopicLocator {
    fn kind() -> ResourceKind {
        ResourceKind::Topic
    }
}

/// Checks whether the given string is a valid topic part or not.
fn is_invalid_topic(topic: &str) -> bool {
    topic.is_empty()
        || topic.starts_with('/')
        || topic.ends_with('/')
        || topic.contains("//")
        || has_invalid_symbols(topic, Some(&[':']))
}

impl FromStr for TopicLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let (sequence_part, topic_part) = s
            .split_once('/')
            .ok_or_else(|| Error::bad_locator(s.to_owned()))?;

        let sequence = SequenceLocator::from_str(sequence_part)?;

        if is_invalid_topic(topic_part) {
            return Err(Error::bad_locator(s.to_owned()));
        }

        Ok(Self {
            sequence,
            name: topic_part.to_owned(),
            timestamp_range: None,
        })
    }
}

impl PartialEq for TopicLocator {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence && self.name == other.name
    }
}

impl Eq for TopicLocator {}

impl PartialOrd for TopicLocator {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopicLocator {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sequence
            .cmp(&other.sequence)
            .then_with(|| self.name.cmp(&other.name))
    }
}

impl std::fmt::Display for TopicLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.sequence, self.name)
    }
}

/// Path inside object store of the topic's root folder.
#[derive(Debug, Clone)]
pub struct TopicPathInStore(String);

impl TopicPathInStore {
    fn generate_random_folder_name() -> String {
        let id = ulid::Ulid::new();
        format!("tp_{}", id)
    }

    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Self::generate_random_folder_name())
    }

    pub fn root(&self) -> &path::Path {
        path::Path::new(&self.0)
    }

    /// Returns the filename of the data file.
    ///
    /// The data file is composed as follows:
    /// ```txt,ignore
    /// [chunk_number].[extension]
    /// ```
    pub fn data_file(chunk_number: usize, extension: &dyn traits::AsExtension) -> String {
        format!("{chunk_number:05}.{ext}", ext = extension.as_extension())
    }

    /// Returns the complete path of a specific data file.
    ///
    /// # Example
    /// ```txt, ignore
    /// sequence/my/topic/data/0000.parquet
    /// ```
    pub fn path_data(
        &self,
        chunk_number: usize,
        extension: &dyn traits::AsExtension,
    ) -> path::PathBuf {
        let filename = Self::data_file(chunk_number, extension);
        self.data_folder_path().join(filename)
    }

    /// Return the complete path of the folder containing all data
    ///
    /// # Example
    /// ```txt, ignore
    /// sequence/my/topic/data
    /// ```
    pub fn data_folder_path(&self) -> path::PathBuf {
        self.root().join("data")
    }

    /// Return the full path of the metadata file
    pub fn path_metadata(&self) -> path::PathBuf {
        self.root().join("metadata.json")
    }
}

impl From<String> for TopicPathInStore {
    /// WARNING: No checks performed on the input string.
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<TopicPathInStore> for String {
    fn from(s: TopicPathInStore) -> Self {
        s.0
    }
}

impl std::fmt::Display for TopicPathInStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
    pub resource_locator: TopicLocator,
}

impl TopicMetadataProperties {
    pub fn new(resource_locator: TopicLocator, session_uuid: Uuid) -> Self {
        Self::new_with_created_at(resource_locator, session_uuid, types::Timestamp::now())
    }

    pub fn new_with_created_at(
        resource_locator: TopicLocator,
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
// SESSION
// ////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a session resource.
///
/// A session is a collection of topics uploaded together.
/// The accepted locator format is my_sequence:<session>,
/// where <session> is a ULID generated by the server (e.g. `my_sequence:<session-ULID>`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionLocator {
    /// Parent sequence locator.
    pub sequence: SequenceLocator,
    /// Name of the session (it does not contain the sequence prefix, but only the session ULID).
    name: String,
}

impl SessionLocator {
    /// Creates a new session locator for the given parent sequence.
    pub fn new(parent: SequenceLocator) -> Self {
        Self {
            name: ulid::Ulid::new().to_string(),
            sequence: parent,
        }
    }
}

impl Locator for SessionLocator {
    fn kind() -> ResourceKind {
        ResourceKind::Session
    }
}

impl FromStr for SessionLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let mut split = s.split(':');

        let (Some(sequence_part), Some(session_part), None) =
            (split.next(), split.next(), split.next())
        else {
            return Err(Error::bad_locator(s.to_owned()));
        };

        let sequence = SequenceLocator::from_str(sequence_part)?;

        if session_part.is_empty() || has_invalid_symbols(session_part, Some(&['/'])) {
            return Err(Error::bad_locator(s.to_owned()));
        }

        Ok(Self {
            sequence,
            name: session_part.to_owned(),
        })
    }
}

impl std::fmt::Display for SessionLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.sequence, self.name)
    }
}

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE
// ////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a sequence resource.
///
/// A sequence acts as a container for a collection of related topics. This locator
/// wraps a string (e.g., `my_sequence`) that provides a human-readable and stable identifier for the sequence.
/// No '/' chars are accepted in this string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceLocator {
    /// The unique string identifier for the sequence (e.g., `my_sequence`).
    name: String,
}

impl Locator for SequenceLocator {
    fn kind() -> ResourceKind {
        ResourceKind::Sequence
    }
}

impl Deref for SequenceLocator {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.name
    }
}

impl FromStr for SequenceLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        if s.is_empty() || has_invalid_symbols(s, None) {
            return Err(Error::bad_locator(s.to_owned()));
        }

        let slash_colon_count = s.chars().filter(|c| c == &':' || c == &'/').count();

        if slash_colon_count != 0 {
            return Err(Error::bad_locator(s.to_owned()));
        }

        Ok(Self { name: s.to_owned() })
    }
}

impl From<SequenceLocator> for String {
    fn from(locator: SequenceLocator) -> Self {
        locator.name
    }
}

impl PartialEq<&str> for SequenceLocator {
    fn eq(&self, other: &&str) -> bool {
        self.name == *other
    }
}

impl PartialEq<SequenceLocator> for &str {
    fn eq(&self, other: &SequenceLocator) -> bool {
        self == &other.name
    }
}

impl std::fmt::Display for SequenceLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Path inside object store of the sequence's root folder.
#[derive(Debug, Clone)]
pub struct SequencePathInStore(String);

impl SequencePathInStore {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Self::generate_random_folder_name())
    }

    pub fn root(&self) -> &path::Path {
        path::Path::new(&self.0)
    }

    /// Returns the location of the metadata file associated with the sequence.
    ///
    /// The metadata file may or may not exist, no check performed by this function.
    pub fn path_metadata(&self) -> path::PathBuf {
        let mut path = self.root().join("metadata");
        path.set_extension(params::ext::JSON);
        path
    }

    fn generate_random_folder_name() -> String {
        let id = ulid::Ulid::new();
        format!("sq_{}", id)
    }
}

impl From<String> for SequencePathInStore {
    /// WARNING: No checks performed on the input string.
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<SequencePathInStore> for String {
    fn from(s: SequencePathInStore) -> Self {
        s.0
    }
}

impl std::fmt::Display for SequencePathInStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct SequenceMetadata<M> {
    /// Timestamp of the sequence creation
    pub created_at: super::Timestamp,
    pub resource_locator: SequenceLocator,
    pub sessions: Vec<SessionMetadata>,
    pub user_metadata: Option<M>,
}

// ////////////////////////////////////////////////////////////////////////////
// SEQUENCE TOPIC GROUP
// ////////////////////////////////////////////////////////////////////////////

/// Groups a specific sequence with its associated topics and an optional time filter.
///
/// This structure acts as a container to link a [`SequenceLocator`] with multiple [`TopicLocator`]s.
#[derive(Debug)]
pub struct SequenceTopicGroup {
    pub sequence: SequenceLocator,
    pub topics: Vec<TopicLocator>,
}

impl SequenceTopicGroup {
    pub fn new(sequence: SequenceLocator, topics: Vec<TopicLocator>) -> Self {
        Self { sequence, topics }
    }

    pub fn into_parts(self) -> (SequenceLocator, Vec<TopicLocator>) {
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
            .sort_unstable_by(|a, b| a.sequence.cmp(&b.sequence));

        for mut self_grp in self.0 {
            let found = groups
                .0
                .binary_search_by(|grp_aux| grp_aux.sequence.cmp(&self_grp.sequence));

            if let Ok(found) = found {
                self_grp.topics.extend(groups.0[found].topics.clone());

                // Sort and remove duplicates
                self_grp.topics.sort_unstable();
                self_grp.topics.dedup_by(|a, b| a == b);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_invalid_symbols() {
        assert!(has_invalid_symbols("/!\"my/resource/name", None));

        assert!(has_invalid_symbols("/my/resource/na.me", None));

        assert!(has_invalid_symbols("/èmy/resource/name", None));

        assert!(has_invalid_symbols("my/resourcè/name", None));

        assert!(has_invalid_symbols("my/resource/name", Some(&['/'])));

        assert!(has_invalid_symbols("my/resource:name", Some(&[':'])));

        assert!(!has_invalid_symbols("my/resource:name/", None));

        assert!(!has_invalid_symbols("my/resource_name", None));
    }

    #[test]
    fn test_merge_sequence_topic_groups() {
        let groups1 = SequenceTopicGroupSet::new(vec![
            SequenceTopicGroup::new(
                SequenceLocator::from_str("sequence_1").unwrap(),
                vec![
                    TopicLocator::from_str("sequence_1/topic_1").unwrap(),
                    TopicLocator::from_str("sequence_1/topic_2").unwrap(),
                ],
            ),
            SequenceTopicGroup::new(
                SequenceLocator::from_str("sequence_2").unwrap(),
                vec![TopicLocator::from_str("sequence_2/topic_1").unwrap()],
            ),
        ]);

        let groups2 = SequenceTopicGroupSet::new(vec![
            SequenceTopicGroup::new(
                SequenceLocator::from_str("sequence_1").unwrap(),
                vec![
                    TopicLocator::from_str("sequence_1/topic_1").unwrap(),
                    TopicLocator::from_str("sequence_1/topic_3").unwrap(),
                ],
            ),
            SequenceTopicGroup::new(
                SequenceLocator::from_str("sequence_3").unwrap(),
                vec![TopicLocator::from_str("sequence_3/topic_1").unwrap()],
            ),
        ]);

        let merged: Vec<SequenceTopicGroup> = groups1.merge(groups2).into();

        dbg!(&merged);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].sequence, "sequence_1");
        assert_eq!(merged[0].topics.len(), 3);
    }

    #[test]
    fn test_sequence_locator() {
        assert!("".parse::<SequenceLocator>().is_err());
        assert!("/my/wrong/sequence".parse::<SequenceLocator>().is_err());
        assert!("/my_wrong_sequence".parse::<SequenceLocator>().is_err());
        assert!("my_wrong/sequence".parse::<SequenceLocator>().is_err());
        assert!(" /my_wrong_sequence".parse::<SequenceLocator>().is_err());
        assert!("my_wrong_sequence ".parse::<SequenceLocator>().is_err());
        assert!("my wrong_sequence".parse::<SequenceLocator>().is_err());
        assert!("my wrong sequence".parse::<SequenceLocator>().is_err());
        assert!("/ wrong/sequence".parse::<SequenceLocator>().is_err());
        assert!(
            "/another:wrong_sequence"
                .parse::<SequenceLocator>()
                .is_err()
        );
        assert!("another:wrong_sequence".parse::<SequenceLocator>().is_err());

        let loc = "my_sequence".parse::<SequenceLocator>().unwrap();
        assert_eq!(loc, "my_sequence");
    }

    #[test]
    fn test_session_locator() {
        assert!(":wrong_session".parse::<SessionLocator>().is_err());
        assert!("/wrong_session".parse::<SessionLocator>().is_err());
        assert!("/sequence:wrong session".parse::<SessionLocator>().is_err());
        assert!("/sequence:wrong_session".parse::<SessionLocator>().is_err());
        assert!("sequence:wrong/session".parse::<SessionLocator>().is_err());
        assert!("sequence:wrong:session".parse::<SessionLocator>().is_err());
        assert!("sequence:wrong/session".parse::<SessionLocator>().is_err());
        assert!("sequence:wrong_session:".parse::<SessionLocator>().is_err());
        assert!(
            "  sequence:wrong_session  "
                .parse::<SessionLocator>()
                .is_err()
        );

        let loc = "my_sequence:my_session".parse::<SessionLocator>().unwrap();
        assert_eq!(loc.to_string(), "my_sequence:my_session");
    }

    #[test]
    fn test_topic_locator() {
        assert!("/wrong_topic".parse::<TopicLocator>().is_err());
        assert!("/wrong topic".parse::<TopicLocator>().is_err());
        assert!("sequence/ wrong topic".parse::<TopicLocator>().is_err());
        assert!("sequence/ wrong/topic".parse::<TopicLocator>().is_err());
        assert!("sequence/wrong topic".parse::<TopicLocator>().is_err());
        assert!("sequence/wrong/ topic".parse::<TopicLocator>().is_err());
        assert!("sequence/wrong / topic".parse::<TopicLocator>().is_err());
        assert!("sequence/wrong /topic".parse::<TopicLocator>().is_err());
        assert!("/another:wrong_topic".parse::<TopicLocator>().is_err());
        assert!("yet_another/wrong_topic/".parse::<TopicLocator>().is_err());
        assert!("yet_another/wrong/topic/".parse::<TopicLocator>().is_err());
        assert!("/my_sequence/my_topic".parse::<TopicLocator>().is_err());
        assert!(" my_sequence/my_topic ".parse::<TopicLocator>().is_err());
        assert!("my_sequence//my_topic".parse::<TopicLocator>().is_err());
        assert!(
            "my_sequence/my_topic//subtopic"
                .parse::<TopicLocator>()
                .is_err()
        );

        let loc = "my_sequence/my_topic".parse::<TopicLocator>().unwrap();
        assert_eq!(loc.to_string(), "my_sequence/my_topic");

        let loc = "my_sequence/my_topic/my_subtopic"
            .parse::<TopicLocator>()
            .unwrap();
        assert_eq!(loc.to_string(), "my_sequence/my_topic/my_subtopic");
    }

    #[test]
    fn test_sequence_path_in_store() {
        let rand_dir = SequencePathInStore::generate_random_folder_name();
        assert_eq!(rand_dir.len(), 29);
        assert!(rand_dir.starts_with("sq_"));

        let pis = SequencePathInStore::new();
        assert!(!pis.root().has_root());
        let metadata = pis.path_metadata();
        assert!(metadata.starts_with(pis.root()));
        assert_eq!(metadata.extension().unwrap(), params::ext::JSON);
        assert!(metadata.ends_with("metadata.json"));
    }

    #[test]
    fn test_topic_path_in_store() {
        let rand_dir = TopicPathInStore::generate_random_folder_name();
        assert_eq!(rand_dir.len(), 29);
        assert!(rand_dir.starts_with("tp_"));

        let pis = TopicPathInStore::new();
        assert!(!pis.root().has_root());
        let metadata = pis.path_metadata();
        assert!(metadata.starts_with(pis.root()));
        assert_eq!(metadata.extension().unwrap(), params::ext::JSON);
        assert!(metadata.ends_with("metadata.json"));

        let data_folder = pis.data_folder_path();
        assert!(&data_folder.starts_with(pis.root()));
        assert!(&data_folder.ends_with("data"));
    }
}

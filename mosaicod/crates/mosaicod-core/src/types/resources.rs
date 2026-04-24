use super::{Format, SessionMetadata, TimestampRange, Uuid};
use crate::{Error, error::PublicError, params, traits, types};
use std::cmp::PartialEq;
use std::ops::Deref;
use std::path;
use std::str::FromStr;
use thiserror::Error;

// ////////////////////////////////////////////////////////////////////////////
// RESOURCE LOCATOR
// ////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("{0} is not a valid locator")]
    InvalidLocator(String),
    #[error("{1} is not a valid {0} locator")]
    LocatorKindMismatch(ResourceKind, String),
}

impl PublicError for ResourceError {
    fn error(&self) -> Error {
        match self {
            ResourceError::InvalidLocator(locator) => Error::bad_locator(locator.clone()),
            ResourceError::LocatorKindMismatch(kind, locator) => {
                Error::locator_kind_mismatch(locator.clone(), kind.to_string())
            }
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Locator {
    inner: String,
    pub kind: ResourceKind,
}

impl Locator {
    pub fn is_sub_locator(&self, parent: &Locator) -> bool {
        self.starts_with(&parent.inner)
    }

    /// Checks if value is a valid locator.
    ///
    /// The following criteria must be met:
    /// - string must be non-empty
    /// - non-ASCII chars are not allowed
    /// - special symbols `! " ' * £ $ % &` are not allowed
    fn is_valid_locator(value: &str) -> bool {
        if value.is_empty() {
            return false;
        }
        let invalid_chars = vec!['!', '\"', '\'', '*', '£', '$', '%', '&', '.', ' '];
        !value
            .chars()
            .any(|c| !c.is_ascii() || invalid_chars.contains(&c))
    }

    /// Builds a sanitized resource locator.
    ///
    /// Sanitized resource locators have the following requirements:
    /// - no leading and trailing spaces
    /// - no leading `/`
    fn sanitize(value: &str) -> String {
        value.trim().trim_start_matches('/').to_owned()
    }
}

impl FromStr for Locator {
    type Err = ResourceError;

    /// Performs checks on the input string and tries to recognize its [`ResourceKind`].
    /// Returns a [`ResourceError::InvalidLocator`] in case of failure.
    fn from_str(s: &str) -> Result<Self, ResourceError> {
        let sanitized_name = Self::sanitize(s);

        if !Self::is_valid_locator(&sanitized_name) {
            return Err(ResourceError::InvalidLocator(s.to_owned()));
        }

        let colon_count = sanitized_name.chars().filter(|c| c == &':').count();
        let slash_count = sanitized_name.chars().filter(|c| c == &'/').count();

        if colon_count == 0 {
            return if slash_count == 0 {
                Ok(Self {
                    inner: sanitized_name,
                    kind: ResourceKind::Sequence,
                })
            } else {
                Ok(Self {
                    inner: sanitized_name,
                    kind: ResourceKind::Topic,
                })
            };
        } else if colon_count == 1 && slash_count == 0 {
            return Ok(Self {
                inner: sanitized_name,
                kind: ResourceKind::Session,
            });
        }

        Err(ResourceError::InvalidLocator(s.to_owned()))
    }
}

impl From<Locator> for String {
    fn from(locator: Locator) -> String {
        locator.inner
    }
}

impl Deref for Locator {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::fmt::Display for Locator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl PartialEq<&str> for Locator {
    fn eq(&self, other: &&str) -> bool {
        self.inner == *other
    }
}

impl PartialEq<Locator> for &str {
    fn eq(&self, other: &Locator) -> bool {
        self == &other.inner
    }
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
    /// The unique string identifier for the topic (e.g., `my_sequence/my/topic`).
    inner: Locator,
    /// An optional time range to filter data within the topic.
    pub timestamp_range: Option<TimestampRange>,
}

impl TopicLocator {
    pub fn with_timestamp_range(mut self, ts: TimestampRange) -> Self {
        self.timestamp_range = Some(ts);
        self
    }
}

impl FromStr for TopicLocator {
    type Err = ResourceError;
    fn from_str(s: &str) -> Result<Self, ResourceError> {
        let locator = Locator::from_str(s)?;

        if locator.kind != ResourceKind::Topic {
            return Err(ResourceError::LocatorKindMismatch(
                ResourceKind::Topic,
                locator.into(),
            ));
        }

        Ok(Self {
            inner: locator,
            timestamp_range: None,
        })
    }
}

impl From<TopicLocator> for String {
    fn from(locator: TopicLocator) -> Self {
        locator.inner.into()
    }
}

impl From<TopicLocator> for Locator {
    fn from(locator: TopicLocator) -> Self {
        locator.inner
    }
}

impl From<Locator> for TopicLocator {
    fn from(locator: Locator) -> Self {
        Self {
            inner: locator,
            timestamp_range: None,
        }
    }
}

impl Deref for TopicLocator {
    type Target = Locator;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PartialEq for TopicLocator {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl PartialEq<&str> for TopicLocator {
    fn eq(&self, other: &&str) -> bool {
        self.inner == *other
    }
}

impl PartialEq<TopicLocator> for &str {
    fn eq(&self, other: &TopicLocator) -> bool {
        self == &other.inner
    }
}

impl std::fmt::Display for TopicLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
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
// SEQUENCE
// ////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a sequence resource.
///
/// A sequence acts as a container for a collection of related topics. This locator
/// is a sanitized, path-like string (e.g., `my_sequence`) that provides a
/// human-readable and stable identifier for the sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceLocator {
    /// The unique string identifier for the sequence (e.g., `my_sequence`).
    inner: Locator,
}

impl Deref for SequenceLocator {
    type Target = Locator;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<SequenceLocator> for Locator {
    fn from(locator: SequenceLocator) -> Self {
        locator.inner
    }
}

impl From<Locator> for SequenceLocator {
    fn from(locator: Locator) -> Self {
        Self { inner: locator }
    }
}

impl FromStr for SequenceLocator {
    type Err = ResourceError;

    fn from_str(s: &str) -> Result<Self, ResourceError> {
        let locator = Locator::from_str(s)?;

        if locator.kind != ResourceKind::Sequence {
            return Err(ResourceError::LocatorKindMismatch(
                ResourceKind::Sequence,
                locator.into(),
            ));
        }

        Ok(Self { inner: locator })
    }
}

impl From<SequenceLocator> for String {
    fn from(locator: SequenceLocator) -> Self {
        locator.inner.into()
    }
}

impl PartialEq<&str> for SequenceLocator {
    fn eq(&self, other: &&str) -> bool {
        &self.inner == other
    }
}

impl PartialEq<SequenceLocator> for &str {
    fn eq(&self, other: &SequenceLocator) -> bool {
        self == &other.inner
    }
}

impl std::fmt::Display for SequenceLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
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
// SESSION
// ////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a session resource.
///
/// A session is a collection of topics uploaded all together. This locator
/// is a sanitized, path-like string (e.g., `my_sequence:my_session`) that provides a
/// human-readable and stable identifier for the sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLocator {
    /// The unique string identifier for the session (e.g., `my_sequence:my_session`).
    inner: Locator,
}

impl Deref for SessionLocator {
    type Target = Locator;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<SessionLocator> for Locator {
    fn from(locator: SessionLocator) -> Self {
        locator.inner
    }
}

impl From<Locator> for SessionLocator {
    fn from(locator: Locator) -> Self {
        Self { inner: locator }
    }
}

impl FromStr for SessionLocator {
    type Err = ResourceError;

    fn from_str(s: &str) -> Result<Self, ResourceError> {
        let locator = Locator::from_str(s)?;

        if locator.kind != ResourceKind::Session {
            return Err(ResourceError::LocatorKindMismatch(
                ResourceKind::Session,
                locator.into(),
            ));
        }

        Ok(Self { inner: locator })
    }
}

impl From<SessionLocator> for String {
    fn from(locator: SessionLocator) -> Self {
        locator.inner.into()
    }
}

impl PartialEq<&str> for SessionLocator {
    fn eq(&self, other: &&str) -> bool {
        &self.inner == other
    }
}

impl PartialEq<SessionLocator> for &str {
    fn eq(&self, other: &SessionLocator) -> bool {
        self == &other.inner
    }
}

impl std::fmt::Display for SessionLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
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
            .sort_unstable_by(|a, b| a.sequence.cmp(b.sequence.as_ref()));

        for mut self_grp in self.0 {
            let found = groups
                .0
                .binary_search_by(|grp_aux| grp_aux.sequence.cmp(self_grp.sequence.as_ref()));

            if let Ok(found) = found {
                self_grp.topics.extend(groups.0[found].topics.clone());

                // Sort and remove duplicates
                self_grp.topics.sort_unstable_by(|a, b| a.cmp(b.as_ref()));
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
    fn test_resource_name() {
        let target = "my/resource/name";

        assert_eq!(Locator::sanitize("/my/resource/name"), target);

        assert_eq!(Locator::sanitize("    my/resource/name   "), target);

        assert_eq!(Locator::sanitize("    /my/resource/name   "), target);

        assert_eq!(Locator::sanitize("//my/resource/name"), target);

        assert_ne!(Locator::sanitize("/ /my/resource/name"), target);

        assert_ne!(Locator::sanitize("/ //my/resource/name"), target);

        assert!(!Locator::is_valid_locator("/!\"my/resource/name"));

        assert!(!Locator::is_valid_locator("/my/resource/na.me"));

        assert!(!Locator::is_valid_locator("/èmy/resource/name"));

        assert!(!Locator::is_valid_locator("my/resourcè/name"));
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
    fn test_str_to_locator_conversion() {
        let t1 = TopicLocator::from_str("my_sequence/topic_1").unwrap();
        assert!(Locator::is_valid_locator(&t1))
    }

    #[test]
    fn test_sequence_locator() {
        assert!("/my/wrong/sequence".parse::<SequenceLocator>().is_err());
        assert!("/ wrong/sequence".parse::<SequenceLocator>().is_err());
        assert!(
            "/another:wrong_sequence"
                .parse::<SequenceLocator>()
                .is_err()
        );

        let loc = "my_sequence".parse::<SequenceLocator>().unwrap();
        assert_eq!(loc, "my_sequence");

        let loc = "/my_sequence".parse::<SequenceLocator>().unwrap();
        assert_eq!(loc, "my_sequence");

        let loc = "  my_sequence  ".parse::<SequenceLocator>().unwrap();
        assert_eq!(loc, "my_sequence");
    }

    #[test]
    fn test_session_locator() {
        assert!("/wrong_session".parse::<SessionLocator>().is_err());
        assert!("/sequence:wrong session".parse::<SessionLocator>().is_err());
        assert!("sequence:wrong/session".parse::<SessionLocator>().is_err());
        assert!("sequence:wrong:session".parse::<SessionLocator>().is_err());
        assert!("sequence:wrong/session".parse::<SessionLocator>().is_err());

        let loc = "my_sequence:my_session".parse::<SessionLocator>().unwrap();
        assert_eq!(loc, "my_sequence:my_session");

        let loc = "/my_sequence:my_session".parse::<SessionLocator>().unwrap();
        assert_eq!(loc, "my_sequence:my_session");

        let loc = "  my_sequence:my_session  "
            .parse::<SessionLocator>()
            .unwrap();
        assert_eq!(loc, "my_sequence:my_session");
    }

    #[test]
    fn test_topic_locator() {
        assert!("/wrong_topic".parse::<TopicLocator>().is_err());
        assert!("/wrong topic".parse::<TopicLocator>().is_err());
        assert!("sequence/wrong topic".parse::<TopicLocator>().is_err());
        assert!("/another:wrong_topic".parse::<TopicLocator>().is_err());

        let loc = "my_sequence/my_topic".parse::<TopicLocator>().unwrap();
        assert_eq!(loc, "my_sequence/my_topic");

        let loc = "/my_sequence/my_topic".parse::<TopicLocator>().unwrap();
        assert_eq!(loc, "my_sequence/my_topic");

        let loc = "  my_sequence/my_topic  ".parse::<TopicLocator>().unwrap();
        assert_eq!(loc, "my_sequence/my_topic");
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

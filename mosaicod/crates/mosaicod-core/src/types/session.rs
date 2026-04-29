#[derive(Clone)]
pub struct SessionMetadata {
    pub locator: super::SessionLocator,
    pub topics: Vec<super::TopicLocator>,
    pub created_at: super::Timestamp,
    pub completed_at: Option<super::Timestamp>,
}

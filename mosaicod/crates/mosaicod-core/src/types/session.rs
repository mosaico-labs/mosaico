#[derive(Clone)]
pub struct SessionMetadata {
    pub uuid: super::Uuid,
    pub topics: Vec<super::TopicResourceLocator>,
    pub created_at: super::Timestamp,
    pub completed_at: Option<super::Timestamp>,
}

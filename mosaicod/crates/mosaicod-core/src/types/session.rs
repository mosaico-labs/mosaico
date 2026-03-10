#[derive(Clone)]
pub struct SessionManifest {
    pub uuid: super::Uuid,
    pub topics: Vec<super::TopicResourceLocator>,
    pub created_timestamp: super::Timestamp,
    pub completed_timestamp: super::Timestamp,
}

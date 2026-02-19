pub struct SessionManifest {
    pub uuid: super::Uuid,
    pub topics: Vec<super::TopicResourceLocator>,
    pub creation_timestamp: super::Timestamp,
    pub completion_timestamp: super::Timestamp,
}

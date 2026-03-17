use crate::types;

#[derive(Clone)]
pub struct SessionManifest {
    pub uuid: super::Uuid,
    pub topics: Vec<super::TopicResourceLocator>,
    pub created_at: super::Timestamp,
    pub completed_at: Option<super::Timestamp>,
    pub locked: bool,
}

impl SessionManifest {
    pub fn new(uuid: super::Uuid, created_at: types::Timestamp) -> Self {
        Self {
            uuid,
            created_at,
            completed_at: None,
            locked: false,
            topics: vec![],
        }
    }
}

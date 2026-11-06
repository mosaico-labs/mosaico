use super::{SessionMetadata, Timestamp, TopicMetadata};

/// An HTTP endpoint that mosaicod sends an event to when a topic is uploaded or a session is
/// finalized. See `mosaicod hook`.
#[derive(Debug, Clone, PartialEq)]
pub struct Hook {
    pub id: i32,
    /// Normalized URL, as stored.
    pub url: String,
    pub created_at: Timestamp,
}

/// Something hooks are told about. `M` is the type of the topic's user metadata.
#[derive(Clone)]
pub enum HookEvent<M> {
    /// A topic's upload finished and the topic is finalized.
    TopicUploaded(TopicMetadata<M>),
    /// A session was finalized.
    SessionUploaded(SessionMetadata),
}

impl<M> HookEvent<M> {
    /// The event's name, as sent to hooks.
    pub fn name(&self) -> &'static str {
        match self {
            Self::TopicUploaded(_) => "topic_uploaded",
            Self::SessionUploaded(_) => "session_uploaded",
        }
    }
}

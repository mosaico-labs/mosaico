use super::Timestamp;

/// An HTTP endpoint that mosaicod sends an event to when a topic is uploaded or a session is
/// finalized. See `mosaicod hook`.
#[derive(Debug, Clone, PartialEq)]
pub struct Hook {
    pub id: i32,
    /// Normalized URL, as stored.
    pub url: String,
    pub created_at: Timestamp,
}

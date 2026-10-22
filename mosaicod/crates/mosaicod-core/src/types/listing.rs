//! Types used to list sequences page by page (keyset pagination).
//!
//! A page is identified by a [`SequenceCursor`]: the sort key of the last
//! sequence the client received. The next page starts strictly *after* that
//! key, so no server-side state is needed and any instance can serve any page.

use super::{SequenceLocator, Timestamp};
use std::num::NonZeroU32;

/// Sort order used when listing sequences. Ordering is always ascending.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SequenceOrder {
    /// Order by creation timestamp, ties broken by the sequence id.
    #[default]
    CreatedAt,
    /// Order by locator name, in byte order.
    Name,
}

/// Maximum number of sequences returned by a listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ListLimit {
    /// Every sequence after the cursor.
    #[default]
    Unlimited,
    /// At most this many sequences.
    Bounded(NonZeroU32),
}

impl From<u32> for ListLimit {
    fn from(value: u32) -> Self {
        NonZeroU32::new(value).map_or(Self::Unlimited, Self::Bounded)
    }
}

/// Position in a sequence listing after which the next page starts.
///
/// The cursor only stores a sort key: the sequence it was built from does
/// not need to exist anymore for the cursor to remain valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceCursor {
    /// Key for [`SequenceOrder::CreatedAt`]. The `sequence_id` makes the key
    /// unique when several sequences share the same creation timestamp.
    CreatedAt {
        created_at: Timestamp,
        sequence_id: i32,
    },
    Name(SequenceLocator),
}

impl SequenceCursor {
    /// Returns the sort order this cursor was built for.
    pub fn order(&self) -> SequenceOrder {
        match self {
            Self::CreatedAt { .. } => SequenceOrder::CreatedAt,
            Self::Name(_) => SequenceOrder::Name,
        }
    }
}

/// A sequence returned by a paginated listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceListEntry {
    pub locator: SequenceLocator,
    /// Cursor pointing just past this sequence: using it as the request
    /// cursor resumes the listing right after this entry.
    pub cursor: SequenceCursor,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_from_u32() {
        assert_eq!(ListLimit::from(0), ListLimit::Unlimited);
        assert_eq!(
            ListLimit::from(10),
            ListLimit::Bounded(NonZeroU32::new(10).unwrap())
        );
    }
}

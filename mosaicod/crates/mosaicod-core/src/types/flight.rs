use crate::types;
use crate::types::TimestampRange;

/// Message used to initiate the flight communication to upload a new datastream.
pub struct DoPutCmd {
    pub locator: String,
    pub key: String,
}

/// Request info on a mosaico resource (topic or sequence).
pub struct GetFlightInfoCmd {
    pub locator: String,
    pub timestamp_range: Option<TimestampRange>,
    /// What to retrieve of the image sequence topics: carried in their tickets.
    pub image_sequence_view: ImageSequenceView,
}

/// What to retrieve of a topic of the `ImageSequence` serialization format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImageSequenceView {
    /// The images, as they were uploaded: decoded from the topic's video once it is optimized.
    #[default]
    Images,
    /// The topic's video as MPEG-DASH, a segment per second of video, of the seconds of
    /// `interval` (all of them if `None`): only once it is optimized.
    Video { interval: Option<VideoInterval> },
}

impl ImageSequenceView {
    /// Checks that a view of the video has no time range: its interval is its seconds.
    pub fn check_time_range(self, range: Option<TimestampRange>) -> Result<(), crate::Error> {
        match (self, range) {
            (Self::Video { .. }, Some(_)) => Err(crate::Error::bad_request(
                "the video takes an interval of seconds, not a time range".to_owned(),
            )),
            _ => Ok(()),
        }
    }
}

/// Seconds of video `[start_s, end_s)`, second 0 starting at the first frame: `start_s < end_s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoInterval {
    pub start_s: u32,
    pub end_s: u32,
}

/// Request the schema of a mosaico topic.
pub struct GetSchemaCmd {
    pub locator: String,
}

/// Request a page of sequences.
pub struct ListFlightsCmd {
    pub start: types::ListStart,
    pub limit: types::ListLimit,
}

pub struct TicketTopic {
    /// Locator for the topic.
    pub locator: types::TopicLocator,
    /// Optional timestamp range used to limit the data stream.
    pub timestamp_range: Option<TimestampRange>,
    /// What to retrieve of an image sequence topic: [`ImageSequenceView::Images`] for any other.
    pub image_sequence_view: ImageSequenceView,
}

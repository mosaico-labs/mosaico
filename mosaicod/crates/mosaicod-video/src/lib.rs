//! Encoding [`Image`]s into a lossless FFV1 video in a fragmented MP4 file,
//! and decoding it back, with FFmpeg: its own FFV1 encoder and decoder, and
//! its MP4 muxer and demuxer.
//!
//! The encoder and the decoder need FFmpeg, so they are behind the `ffmpeg`
//! feature; without it, only the layout of a video is read ([`mp4_layout`]).
//!
//! # The video
//!
//! - FFV1 version 3, in 16 slices, each with a CRC: lossless, so every frame
//!   decodes back to exactly the image it was made from, with its channels,
//!   alpha included, and its bit depth;
//! - the pixels stored as the image holds them, with no color conversion:
//!
//!   | Image | Frame |
//!   |---|---|
//!   | gray, 8 or 16 bits | `gray`, `gray16le` |
//!   | gray with alpha, 8 bits | `ya8` |
//!   | gray with alpha, 16 bits | `yuva444p16le`, U and V neutral |
//!   | RGB, 8 or 16 bits | `bgr0`, `gbrp16le` |
//!   | RGBA, 8 or 16 bits | `bgra`, `gbrap16le` |
//!
//! - one frame per image pushed, in order: none dropped or repeated;
//! - every frame a keyframe, which decodes on its own, in a fragment of its
//!   own (`moof` and `mdat`) with its start time (`tfdt`), after the header
//!   (`ftyp` and `moov`): the header followed by any of the fragments, in
//!   order, is a video of those frames, at their time in the whole video, as
//!   MPEG-DASH segments are;
//! - each frame at its own timestamp, in microseconds, which must increase
//!   (a variable frame rate). Every frame lasts until the next one, for at
//!   most [`MAX_FRAME_DURATION_US`]; the last one as long as the gap before
//!   it.
//!
//! FFV1 has no registered MP4 sample entry: FFmpeg writes it as `FFV1`, with
//! its configuration in a `glbl` box, which FFmpeg reads back.

#[cfg(feature = "ffmpeg")]
mod codec;
mod mp4;
#[cfg(feature = "ffmpeg")]
pub use codec::{FfmpegError, VideoEncoder, check_ffmpeg, decode};
pub use mp4::{Mp4Layout, SEGMENT_TYPE_BOX, mp4_layout};

use mosaicod_image::Image;

/// An error encoding or decoding a video.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[cfg(feature = "ffmpeg")]
    #[error("{context}: {source}")]
    Ffmpeg {
        context: String,
        source: codec::FfmpegError,
    },
    /// FFmpeg lacks the FFV1 encoder or decoder, or the MP4 muxer or demuxer.
    #[error("{0}")]
    Codec(String),
    #[error("{0}")]
    Frame(String),
    #[error(transparent)]
    Image(#[from] mosaicod_image::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

#[cfg(feature = "ffmpeg")]
type Result<T> = std::result::Result<T, Error>;

/// How long the only frame of a video lasts, in microseconds: a frame lasts
/// until the next one, so a single frame has no duration of its own.
pub const SINGLE_FRAME_DURATION_US: i64 = 33_333;

/// The longest a frame lasts, in microseconds (~35.8 minutes, the longest
/// duration the MP4 muxer takes): a frame before a longer gap lasts this long,
/// and the next one still starts at its own timestamp.
pub const MAX_FRAME_DURATION_US: i64 = i32::MAX as i64;

/// A frame decoded from a video.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFrame {
    /// The frame's number in the file decoded, from 0.
    pub frame: u64,
    /// The image the frame was made from, exactly.
    pub image: Image,
    /// The frame's timestamp, in microseconds.
    pub timestamp_us: i64,
}

#[cfg(test)]
mod tests {
    // Used by the tests of the encoder and the decoder only.
    #[cfg(not(feature = "ffmpeg"))]
    use tempfile as _;
}

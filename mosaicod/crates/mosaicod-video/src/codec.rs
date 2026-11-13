//! The encoder and the decoder, with FFmpeg.

use std::path::Path;
use std::sync::Once;

use ffmpeg_the_third::packet::Mut;
use ffmpeg_the_third::{
    Dictionary, Packet, Rational, codec, decoder, encoder, ffi, format, frame, media,
    util::format::Pixel,
};
use mosaicod_image::{ColorType, Image, ImageInfo, Samples};

use super::{DecodedFrame, Error, MAX_FRAME_DURATION_US, Result, SINGLE_FRAME_DURATION_US};

pub use ffmpeg_the_third::Error as FfmpegError;

/// The encoder and the decoder: FFmpeg's own FFV1.
const CODEC: &str = "ffv1";
/// The muxer and the demuxer: MP4. The demuxer is named `mov,mp4,m4a,3gp,3g2,mj2`.
const CONTAINER: &str = "mp4";

/// The muxer's settings: fragmented MP4, a fragment per frame (every frame is a keyframe), each
/// with its own start time (`tfdt`) and data offsets from its own start (`default_base_moof`),
/// so that it is read on its own, after the header; with the timestamps in microseconds. No index
/// of the fragments at the end (`mfra`, `skip_trailer`): the file ends with the last fragment.
const MP4_OPTIONS: [(&str, &str); 2] = [
    (
        "movflags",
        "frag_keyframe+empty_moov+default_base_moof+skip_trailer",
    ),
    ("video_track_timescale", "1000000"),
];

/// FFV1's settings: version 3, which checks every slice with a CRC, in 16
/// slices encoded and decoded in parallel.
const FFV1_OPTIONS: [(&str, &str); 3] = [("level", "3"), ("slices", "16"), ("slicecrc", "1")];

/// Time base of the timestamps given to and taken from this crate.
const MICROSECONDS: Rational = Rational(1, 1_000_000);

/// Adds a context to FFmpeg's errors, made only on an error.
trait Context<T> {
    fn context(self, context: &str) -> Result<T>;
    fn with_context(self, context: impl FnOnce() -> String) -> Result<T>;
}

impl<T> Context<T> for std::result::Result<T, FfmpegError> {
    fn context(self, context: &str) -> Result<T> {
        self.with_context(|| context.to_owned())
    }

    fn with_context(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|source| Error::Ffmpeg {
            context: context(),
            source,
        })
    }
}

/// Initializes FFmpeg once, keeping its own log to errors.
fn init() -> Result<()> {
    static INIT: Once = Once::new();
    let mut result = Ok(());
    INIT.call_once(|| {
        result = ffmpeg_the_third::init().context("initializing FFmpeg");
        ffmpeg_the_third::util::log::set_level(ffmpeg_the_third::util::log::Level::Error);
    });
    result
}

/// Checks that the FFmpeg this process is linked to has what this crate needs: the FFV1 encoder
/// and decoder, and the MP4 muxer and demuxer. Returns its version, `libavcodec` and
/// `libavformat`'s, to be logged.
///
/// That the FFmpeg libraries are there, and of the major versions the bindings were built for, is
/// checked by the system's dynamic loader, before the process starts: this checks what they were
/// built with, which a system FFmpeg may lack (e.g. one configured with `--disable-everything`).
pub fn check_ffmpeg() -> Result<String> {
    init()?;

    let mut missing = Vec::new();
    if encoder::find_by_name(CODEC).is_none() {
        missing.push(format!("the {CODEC} encoder"));
    }
    if decoder::find_by_name(CODEC).is_none() {
        missing.push(format!("the {CODEC} decoder"));
    }
    if !format::format::list_muxers().any(|m| m.name() == CONTAINER) {
        missing.push(format!("the {CONTAINER} muxer"));
    }
    if !format::format::list_demuxers().any(|d| d.name().split(',').any(|n| n == CONTAINER)) {
        missing.push(format!("the {CONTAINER} demuxer"));
    }

    let version = |v: u32| format!("{}.{}.{}", v >> 16, (v >> 8) & 0xff, v & 0xff);
    let versions = format!(
        "libavcodec {}, libavformat {}",
        version(codec::version()),
        version(format::version())
    );
    if missing.is_empty() {
        Ok(versions)
    } else {
        Err(Error::Codec(format!(
            "FFmpeg ({versions}) has no {}",
            missing.join(", no ")
        )))
    }
}

/// Where a channel of the image goes in a frame: its plane, its offset in
/// bytes inside a pixel, and the bytes of a pixel in that plane.
#[derive(Clone, Copy)]
struct Place {
    plane: usize,
    offset: usize,
    stride: usize,
}

const fn at(plane: usize, offset: usize, stride: usize) -> Place {
    Place {
        plane,
        offset,
        stride,
    }
}

/// How the samples of an image are laid out in an FFmpeg frame.
struct Layout {
    color: ColorType,
    bit_depth: u8,
    pixel: Pixel,
    /// Where each channel of the image goes, in the image's channel order.
    channels: &'static [Place],
    /// Planes that hold no channel of the image, with the sample they hold.
    fills: &'static [(usize, u16)],
}

/// Every image layout and its frame (see the crate documentation). In a
/// packed frame, the channels of a pixel follow each other in one plane; in
/// a planar frame, each channel has its own plane.
const LAYOUTS: &[Layout] = &[
    Layout {
        color: ColorType::Gray,
        bit_depth: 8,
        pixel: Pixel::GRAY8,
        channels: &[at(0, 0, 1)],
        fills: &[],
    },
    Layout {
        color: ColorType::Gray,
        bit_depth: 16,
        pixel: Pixel::GRAY16LE,
        channels: &[at(0, 0, 2)],
        fills: &[],
    },
    Layout {
        color: ColorType::GrayAlpha,
        bit_depth: 8,
        pixel: Pixel::YA8,
        channels: &[at(0, 0, 2), at(0, 1, 2)],
        fills: &[],
    },
    // FFV1 has no 16-bit gray with alpha: gray goes in Y, with neutral U and V.
    Layout {
        color: ColorType::GrayAlpha,
        bit_depth: 16,
        pixel: Pixel::YUVA444P16LE,
        channels: &[at(0, 0, 2), at(3, 0, 2)],
        fills: &[(1, 1 << 15), (2, 1 << 15)],
    },
    // B, G, R and an unused byte.
    Layout {
        color: ColorType::Rgb,
        bit_depth: 8,
        pixel: Pixel::BGRZ,
        channels: &[at(0, 2, 4), at(0, 1, 4), at(0, 0, 4)],
        fills: &[],
    },
    // Planes G, B, R.
    Layout {
        color: ColorType::Rgb,
        bit_depth: 16,
        pixel: Pixel::GBRP16LE,
        channels: &[at(2, 0, 2), at(0, 0, 2), at(1, 0, 2)],
        fills: &[],
    },
    Layout {
        color: ColorType::Rgba,
        bit_depth: 8,
        pixel: Pixel::BGRA,
        channels: &[at(0, 2, 4), at(0, 1, 4), at(0, 0, 4), at(0, 3, 4)],
        fills: &[],
    },
    // Planes G, B, R, A.
    Layout {
        color: ColorType::Rgba,
        bit_depth: 16,
        pixel: Pixel::GBRAP16LE,
        channels: &[at(2, 0, 2), at(0, 0, 2), at(1, 0, 2), at(3, 0, 2)],
        fills: &[],
    },
];

impl Layout {
    fn of_image(info: &ImageInfo) -> Result<&'static Self> {
        LAYOUTS
            .iter()
            .find(|l| l.color == info.color && l.bit_depth == info.bit_depth)
            .ok_or_else(|| {
                Error::Frame(format!(
                    "no frame for {} images of {} bits",
                    info.color, info.bit_depth
                ))
            })
    }

    fn of_frame(pixel: Pixel) -> Result<&'static Self> {
        LAYOUTS
            .iter()
            .find(|l| l.pixel == pixel)
            .ok_or_else(|| Error::Frame(format!("decoded a {pixel:?} frame, not one of an image")))
    }

    /// Copies `image` into a new frame, whose rows may be padded.
    ///
    /// Row by row: each frame row is a slice of its plane, each of its pixels a slice of
    /// `place.stride` bytes starting at the channel's offset, zipped with the image's pixels.
    fn to_frame(&self, image: &Image) -> frame::Video {
        let info = image.info();
        let width = info.width as usize;
        let channels = self.channels.len();
        let mut frame = frame::Video::new(self.pixel, info.width, info.height);
        for (channel, place) in self.channels.iter().enumerate() {
            let line = frame.stride(place.plane);
            let rows = frame.data_mut(place.plane).chunks_exact_mut(line);
            match image.samples() {
                Samples::Eight(samples) => {
                    for (row, pixels) in rows.zip(samples.chunks_exact(width * channels)) {
                        let to = row[place.offset..].chunks_mut(place.stride);
                        for (to, pixel) in to.zip(pixels.chunks_exact(channels)) {
                            to[0] = pixel[channel];
                        }
                    }
                }
                Samples::Sixteen(samples) => {
                    for (row, pixels) in rows.zip(samples.chunks_exact(width * channels)) {
                        let to = row[place.offset..].chunks_mut(place.stride);
                        for (to, pixel) in to.zip(pixels.chunks_exact(channels)) {
                            to[..2].copy_from_slice(&pixel[channel].to_le_bytes());
                        }
                    }
                }
            }
        }
        for &(plane, value) in self.fills {
            let line = frame.stride(plane);
            for row in frame.data_mut(plane).chunks_exact_mut(line) {
                row[..2 * width]
                    .as_chunks_mut::<2>()
                    .0
                    .fill(value.to_le_bytes());
            }
        }
        frame
    }

    /// Copies a decoded frame back into the image it was made from.
    fn to_image(&self, frame: &frame::Video) -> Result<Image> {
        let samples = match self.bit_depth {
            8 => Samples::Eight(self.read_samples(frame, |sample| sample[0])),
            _ => Samples::Sixteen(
                self.read_samples(frame, |sample| u16::from_le_bytes([sample[0], sample[1]])),
            ),
        };
        let info = ImageInfo {
            width: frame.width(),
            height: frame.height(),
            color: self.color,
            bit_depth: self.bit_depth,
        };
        Ok(Image::new(info, samples)?)
    }

    /// The samples of `frame`, in the image's order, each read by `read` from the bytes of the
    /// frame starting at it: the reverse of [`Self::to_frame`].
    fn read_samples<T: Copy + Default>(
        &self,
        frame: &frame::Video,
        read: impl Fn(&[u8]) -> T,
    ) -> Vec<T> {
        let (width, height) = (frame.width() as usize, frame.height() as usize);
        let channels = self.channels.len();
        let mut samples = vec![T::default(); width * height * channels];
        for (channel, place) in self.channels.iter().enumerate() {
            let rows = frame
                .data(place.plane)
                .chunks_exact(frame.stride(place.plane));
            for (pixels, row) in samples.chunks_exact_mut(width * channels).zip(rows) {
                let from = row[place.offset..].chunks(place.stride);
                for (pixel, from) in pixels.chunks_exact_mut(channels).zip(from) {
                    pixel[channel] = read(from);
                }
            }
        }
        samples
    }
}

/// An FFV1 video being written: images go in with [`VideoEncoder::push`],
/// and [`VideoEncoder::finish`] closes the file.
///
/// Encoding is CPU-bound and blocking: call it from a blocking thread.
pub struct VideoEncoder {
    info: ImageInfo,
    layout: &'static Layout,
    output: format::context::Output,
    encoder: encoder::video::Encoder,
    packets: Packets,
    last_us: Option<i64>,
    frames: u64,
    path: std::path::PathBuf,
}

impl VideoEncoder {
    /// Creates the video `path`, a fragmented MP4 file, and opens the
    /// encoder, for images of `info`.
    pub fn create(path: &Path, info: ImageInfo) -> Result<Self> {
        init()?;
        let layout = Layout::of_image(&info)?;
        let codec = encoder::find_by_name(CODEC)
            .ok_or_else(|| Error::Codec(format!("this FFmpeg has no {CODEC} encoder")))?;
        let mut output = format::output_as(path, CONTAINER)
            .with_context(|| format!("creating {}", path.display()))?;
        let global_header = output
            .format()
            .flags()
            .contains(format::Flags::GLOBAL_HEADER);

        let mut context = codec::context::Context::new_with_codec(codec);
        context.set_threading(codec::threading::Config::kind(
            codec::threading::Type::Slice,
        ));
        let mut config = context
            .encoder()
            .video()
            .context("configuring the encoder")?;
        config.set_width(info.width);
        config.set_height(info.height);
        config.set_format(layout.pixel);
        config.set_time_base(MICROSECONDS);
        // Every frame a keyframe: by default FFV1 carries its context from a keyframe to the next
        // 300 frames, which then can't be decoded without it. Each frame then decodes on its own,
        // in a fragment of its own.
        config.set_gop(1);
        if global_header {
            config.set_flags(codec::Flags::GLOBAL_HEADER);
        }
        let mut options = Dictionary::new();
        for (name, value) in FFV1_OPTIONS {
            options.set(name, value);
        }
        let encoder = config
            .open_as_with(codec, options)
            .with_context(|| format!("opening the {CODEC} encoder"))?;

        let mut stream = output
            .add_stream(codec)
            .context("adding the video stream")?;
        stream.set_time_base(MICROSECONDS);
        stream.copy_parameters_from_context(&encoder);
        let mut options = Dictionary::new();
        for (name, value) in MP4_OPTIONS {
            options.set(name, value);
        }
        let unused = output
            .write_header_with(options)
            .context("writing the video header")?;
        if let Some((name, _)) = unused.iter().next() {
            return Err(Error::Frame(format!("the muxer has no option `{name}`")));
        }
        let stream_time_base = output
            .stream(0)
            .ok_or_else(|| Error::Frame("the video has no stream".to_owned()))?
            .time_base();

        Ok(Self {
            packets: Packets::new(stream_time_base),
            info,
            layout,
            output,
            encoder,
            last_us: None,
            frames: 0,
            path: path.to_owned(),
        })
    }

    /// Encodes `image`, shown at `timestamp_us` microseconds: after the
    /// previous frame's timestamp, and of the size, channels and bit depth of
    /// the video.
    pub fn push(&mut self, image: &Image, timestamp_us: i64) -> Result<()> {
        let (info, expected) = (image.info(), &self.info);
        if info != expected {
            return Err(Error::Frame(format!(
                "image {} is {}x{} {} of {} bits, the video {}x{} {} of {} bits",
                self.frames,
                info.width,
                info.height,
                info.color,
                info.bit_depth,
                expected.width,
                expected.height,
                expected.color,
                expected.bit_depth
            )));
        }
        if self.last_us.is_some_and(|last| timestamp_us <= last) {
            return Err(Error::Frame(format!(
                "image {} at {timestamp_us} us is not after the previous one",
                self.frames
            )));
        }
        self.last_us = Some(timestamp_us);

        let mut picture = self.layout.to_frame(image);
        picture.set_pts(Some(timestamp_us));
        self.encoder
            .send_frame(&picture)
            .with_context(|| format!("encoding image {}", self.frames))?;
        self.packets.receive(&mut self.encoder, &mut self.output)?;
        self.frames += 1;
        Ok(())
    }

    /// Encodes the frames the encoder still holds and closes the file: returns its size.
    pub fn finish(mut self) -> Result<u64> {
        if self.frames == 0 {
            return Err(Error::Frame("no images to encode".to_owned()));
        }
        self.encoder.send_eof().context("flushing the encoder")?;
        self.packets.receive(&mut self.encoder, &mut self.output)?;
        let written = self.packets.finish(&mut self.output)?;
        if written != self.frames {
            return Err(Error::Frame(format!(
                "the encoder output {written} packets for {} images",
                self.frames
            )));
        }
        self.output.write_trailer().context("closing the video")?;
        drop(self.output); // closes the file
        Ok(std::fs::metadata(&self.path)?.len())
    }
}

/// The encoder's packets on their way to the muxer, each with the duration
/// of its frame.
///
/// FFV1 compresses every frame on its own, so it outputs one packet per
/// frame, in order, with the frame's timestamp. Every frame lasts until the
/// next one, so a packet is held until the next one comes. The last frame
/// lasts as long as the gap before it, or [`SINGLE_FRAME_DURATION_US`] if it
/// is the only one.
///
/// The muxer starts each fragment where the previous one ends, from the
/// durations: a frame lasts exactly until the next one, so each fragment
/// starts at its frame's timestamp. A gap longer than
/// [`MAX_FRAME_DURATION_US`] can't be a duration: the frame before it lasts
/// that long, and the muxer is told that the next fragment starts at its own
/// timestamp (see [`start_discontinuous_fragment`]); otherwise it would lose
/// the gap.
struct Packets {
    /// The last packet received, waiting for the next one to end it.
    held: Option<Packet>,
    /// The gap before the last packet written, in microseconds.
    last_gap: Option<i64>,
    /// Whether the next packet comes after a gap longer than a duration.
    discontinuous: bool,
    stream_time_base: Rational,
    written: u64,
}

impl Packets {
    fn new(stream_time_base: Rational) -> Self {
        Self {
            held: None,
            last_gap: None,
            discontinuous: false,
            stream_time_base,
            written: 0,
        }
    }

    /// Takes the packets the encoder has ready, writing every one but the
    /// last.
    fn receive(
        &mut self,
        encoder: &mut encoder::video::Encoder,
        output: &mut format::context::Output,
    ) -> Result<()> {
        let mut packet = Packet::empty();
        while encoder.receive_packet(&mut packet).is_ok() {
            let pts = packet.pts().ok_or_else(|| {
                Error::Frame("the encoder output a packet with no timestamp".to_owned())
            })?;
            packet.set_dts(Some(pts));
            if let Some(previous) = self.held.take() {
                let gap = pts - previous.pts().unwrap_or(pts);
                self.last_gap = Some(gap);
                self.write(previous, gap, output)?;
            }
            self.held = Some(std::mem::replace(&mut packet, Packet::empty()));
        }
        Ok(())
    }

    /// Writes the last packet, and returns how many were written.
    fn finish(mut self, output: &mut format::context::Output) -> Result<u64> {
        if let Some(last) = self.held.take() {
            let duration = self
                .last_gap
                .unwrap_or(SINGLE_FRAME_DURATION_US)
                .min(MAX_FRAME_DURATION_US);
            self.write(last, duration, output)?;
        }
        Ok(self.written)
    }

    fn write(
        &mut self,
        mut packet: Packet,
        gap_us: i64,
        output: &mut format::context::Output,
    ) -> Result<()> {
        packet.set_duration(gap_us.min(MAX_FRAME_DURATION_US));
        packet.set_stream(0);
        packet.rescale_ts(MICROSECONDS, self.stream_time_base);
        if self.discontinuous {
            let dts = packet
                .dts()
                .ok_or_else(|| Error::Frame("a packet with no timestamp".to_owned()))?;
            start_discontinuous_fragment(output, dts)?;
        }
        self.discontinuous = gap_us > MAX_FRAME_DURATION_US;
        // One stream: written in order, with no interleaving queue, so that the fragments are
        // flushed where `start_discontinuous_fragment` says.
        packet
            .write(output)
            .with_context(|| format!("writing frame {}", self.written))?;
        self.written += 1;
        Ok(())
    }
}

/// Makes the next packet, at `dts` in the stream's time base, start a fragment at its own
/// timestamp, not where the previous one ends: after a gap longer than a packet can last.
///
/// The MP4 muxer checks every packet's timestamp against the previous packet of its fragment, or
/// against where the previous fragment ends, and moves it 1 tick after that if the gap is too long
/// for a duration: the gap is lost, with only a warning in FFmpeg's log, and every later frame is
/// early by as much. So the fragment of the previous packet is flushed, and the muxer is told the
/// next fragment is discontinuous (`frag_discont`, as FFmpeg's DASH muxer does between segments),
/// which it applies on the next packet it is given: an empty one, which it then discards.
///
/// This relies on how the muxer works inside (`libavformat/movenc.c`, read for FFmpeg 9.0):
/// `mov_write_single_packet` checks a packet's timestamp (`check_pkt`) before it applies
/// `frag_discont` to the tracks, which is why the flag goes in with an empty packet; and only
/// then does it flush a fragment, which is why the fragment is flushed by hand first. The
/// fragment's start time (`tfdt`) is then the packet's own. If a later FFmpeg does this
/// differently, `tests::frames_come_back_at_their_timestamps`, which has gaps longer than a
/// frame can last, fails.
fn start_discontinuous_fragment(output: &mut format::context::Output, dts: i64) -> Result<()> {
    let failed = |context: &str, code: i32| Error::Ffmpeg {
        context: context.to_owned(),
        source: FfmpegError::from(code),
    };
    // SAFETY: `output` is an open MP4 muxer, whose private data holds the `movflags` option;
    // the strings are NUL-terminated, and a null packet flushes the muxer.
    unsafe {
        let context = output.as_mut_ptr();
        let flushed = ffi::av_write_frame(context, std::ptr::null_mut());
        if flushed < 0 {
            return Err(failed("flushing a fragment", flushed));
        }
        let set = ffi::av_opt_set(
            (*context).priv_data,
            c"movflags".as_ptr(),
            c"+frag_discont".as_ptr(),
            0,
        );
        if set < 0 {
            return Err(failed("marking a fragment discontinuous", set));
        }
    }
    // Before the next packet: timestamps must increase. Written with FFmpeg's own call: the
    // bindings refuse empty packets.
    let mut empty = Packet::empty();
    empty.set_stream(0);
    empty.set_dts(Some(dts - 1));
    empty.set_pts(Some(dts - 1));
    // SAFETY: `output` is an open muxer, and `empty` a valid packet of its only stream.
    let written = unsafe { ffi::av_write_frame(output.as_mut_ptr(), empty.as_mut_ptr()) };
    if written < 0 {
        return Err(failed("applying a fragment discontinuity", written));
    }
    Ok(())
}

/// Decodes every frame of the video `path`, in order, handing each to `each`
/// as the image it was made from, with its timestamp in microseconds: returns
/// how many.
///
/// `path` can hold only some of the frames of a video: its header (`ftyp`
/// and `moov`), then any of its fragments, in order. Each decodes on its own,
/// at its timestamp in the whole video.
pub fn decode(path: &Path, mut each: impl FnMut(DecodedFrame) -> Result<()>) -> Result<u64> {
    init()?;
    let mut input = format::input(path).with_context(|| format!("opening {}", path.display()))?;
    let stream = input
        .streams()
        .best(media::Type::Video)
        .ok_or_else(|| Error::Frame(format!("{} has no video stream", path.display())))?;
    let (index, time_base) = (stream.index(), stream.time_base());
    let mut context = codec::context::Context::from_parameters(stream.parameters())
        .context("configuring the decoder")?;
    context.set_threading(codec::threading::Config::kind(
        codec::threading::Type::Slice,
    ));
    let codec = decoder::find_by_name(CODEC)
        .ok_or_else(|| Error::Codec(format!("this FFmpeg has no {CODEC} decoder")))?;
    let mut decoder = context
        .decoder()
        .open_as(codec)
        .and_then(|d| d.video())
        .with_context(|| format!("opening the {CODEC} decoder"))?;

    let mut frames = 0;
    let mut picture = frame::Video::empty();
    let mut receive = |decoder: &mut decoder::Video| -> Result<()> {
        while decoder.receive_frame(&mut picture).is_ok() {
            let pts = picture
                .pts()
                .ok_or_else(|| Error::Frame(format!("frame {frames} has no timestamp")))?;
            let timestamp_us = (i128::from(pts) * 1_000_000 * i128::from(time_base.numerator())
                / i128::from(time_base.denominator())) as i64;
            let image = Layout::of_frame(picture.format())?.to_image(&picture)?;
            each(DecodedFrame {
                frame: frames,
                image,
                timestamp_us,
            })?;
            frames += 1;
        }
        Ok(())
    };
    for packet in input.packets() {
        let (stream, packet) = packet.context("reading the video")?;
        if stream.index() == index {
            decoder.send_packet(&packet).context("decoding")?;
            receive(&mut decoder)?;
        }
    }
    decoder.send_eof().context("flushing the decoder")?;
    receive(&mut decoder)?;
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use mosaicod_image::testing::gradient;

    use super::*;
    use crate::{SEGMENT_TYPE_BOX, mp4_layout};

    #[test]
    fn ffmpeg_has_what_is_needed() {
        let versions = check_ffmpeg().unwrap();
        assert!(versions.starts_with("libavcodec "), "{versions}");
    }

    /// Encodes `images` at `timestamps_us` in `dir`.
    fn encode(dir: &Path, images: &[Image], timestamps_us: &[i64]) -> std::path::PathBuf {
        let path = dir.join("video.mp4");
        let mut encoder = VideoEncoder::create(&path, *images[0].info()).unwrap();
        for (image, &timestamp) in images.iter().zip(timestamps_us) {
            encoder.push(image, timestamp).unwrap();
        }
        let size = encoder.finish().unwrap();
        assert_eq!(size, std::fs::metadata(&path).unwrap().len());
        path
    }

    fn decode_all(path: &Path) -> Vec<DecodedFrame> {
        let mut decoded = Vec::new();
        let count = decode(path, |frame| {
            decoded.push(frame);
            Ok(())
        })
        .unwrap();
        assert_eq!(count, decoded.len() as u64);
        decoded
    }

    #[test]
    fn every_layout_comes_back_exactly() {
        for color in [
            ColorType::Gray,
            ColorType::GrayAlpha,
            ColorType::Rgb,
            ColorType::Rgba,
        ] {
            for depth in [8, 16] {
                // Odd sizes, and a transparent pixel when there is alpha.
                let image = gradient(65, 49, color, depth);
                let image = if color.has_alpha() {
                    let alpha = color.channels() - 1;
                    let samples = match image.samples().clone() {
                        Samples::Eight(mut samples) => {
                            samples[alpha] = 0;
                            Samples::Eight(samples)
                        }
                        Samples::Sixteen(mut samples) => {
                            samples[alpha] = 1234;
                            Samples::Sixteen(samples)
                        }
                    };
                    Image::new(*image.info(), samples).unwrap()
                } else {
                    image
                };
                let dir = tempfile::tempdir().unwrap();
                let path = encode(dir.path(), std::slice::from_ref(&image), &[0]);
                let decoded = decode_all(&path);
                assert_eq!(decoded.len(), 1, "{color} {depth}");
                assert!(decoded[0].image == image, "{color} {depth}");
            }
        }
    }

    /// Frames at a variable rate, with gaps longer than a frame can last, one after the other:
    /// they come back at their timestamps, from the whole video, and from its header followed by
    /// some of its fragments.
    #[test]
    fn frames_come_back_at_their_timestamps() {
        let images: Vec<Image> = (0..6)
            .map(|k| {
                let image = gradient(16, 8, ColorType::Gray, 16);
                let Samples::Sixteen(mut samples) = image.samples().clone() else {
                    unreachable!()
                };
                samples[0] = k;
                Image::new(*image.info(), Samples::Sixteen(samples)).unwrap()
            })
            .collect();
        let two_hours = 2 * 3_600_000_000;
        let timestamps = [
            0,
            33_367,
            2 * two_hours,
            3 * two_hours,
            3 * two_hours + 1,
            3 * two_hours + 2,
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = encode(dir.path(), &images, &timestamps);
        let check = |decoded: Vec<DecodedFrame>, frames: &[usize]| {
            assert_eq!(decoded.len(), frames.len());
            for (frame, &k) in decoded.iter().zip(frames) {
                assert_eq!(frame.timestamp_us, timestamps[k], "frame {k}");
                assert!(frame.image == images[k], "frame {k}");
            }
        };
        check(decode_all(&path), &[0, 1, 2, 3, 4, 5]);

        let file = std::fs::read(&path).unwrap();
        let layout = mp4_layout(std::io::Cursor::new(&file), file.len() as u64).unwrap();
        assert_eq!(layout.fragments.len(), images.len());
        let bytes = |range: &std::ops::Range<u64>| &file[range.start as usize..range.end as usize];
        // Every frame is a keyframe: any of them can start a segment.
        for (k, fragment) in layout.fragments.iter().enumerate() {
            assert!(is_keyframe(bytes(fragment)), "frame {k}");
        }
        let mut part = bytes(&layout.header).to_vec();
        part.extend_from_slice(&SEGMENT_TYPE_BOX);
        for k in [1, 3, 4] {
            part.extend_from_slice(bytes(&layout.fragments[k]));
        }
        let partial = dir.path().join("partial.mp4");
        std::fs::write(&partial, part).unwrap();
        check(decode_all(&partial), &[1, 3, 4]);
    }

    /// The children of the MP4 box whose body is `body`: (type, body).
    fn children(mut body: &[u8]) -> Vec<(&[u8], &[u8])> {
        let mut boxes = Vec::new();
        while body.len() >= 8 {
            let size = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
            boxes.push((&body[4..8], &body[8..size]));
            body = &body[size..];
        }
        boxes
    }

    /// Whether the sample of `fragment` (a `moof` and its `mdat`) is a sync sample: the
    /// `sample_is_non_sync_sample` bit of its flags, from its `trun`, else its `tfhd`, unset.
    fn is_keyframe(fragment: &[u8]) -> bool {
        const NON_SYNC: u32 = 0x1_0000;
        let word =
            |body: &[u8], at: usize| u32::from_be_bytes(body[at..at + 4].try_into().unwrap());
        let (_, moof) = children(fragment)[0];
        let traf = children(moof)
            .into_iter()
            .find(|(t, _)| *t == b"traf")
            .unwrap()
            .1;
        let mut flags = None;
        for (kind, body) in children(traf) {
            let box_flags = word(body, 0) & 0xff_ffff;
            if kind == b"tfhd" && box_flags & 0x20 != 0 && flags.is_none() {
                // After the track id, the optional fields before the default flags.
                let at = 8 + [(0x1, 8), (0x2, 4), (0x8, 4), (0x10, 4)]
                    .iter()
                    .filter(|(flag, _)| box_flags & flag != 0)
                    .map(|(_, size)| size)
                    .sum::<usize>();
                flags = Some(word(body, at));
            }
            if kind == b"trun" {
                // After the sample count, the data offset, then the first sample's flags.
                let at = 8 + if box_flags & 0x1 != 0 { 4 } else { 0 };
                if box_flags & 0x4 != 0 {
                    flags = Some(word(body, at));
                } else if box_flags & 0x400 != 0 {
                    let before = [(0x100, 4), (0x200, 4)]
                        .iter()
                        .filter(|(flag, _)| box_flags & flag != 0)
                        .map(|(_, size)| size)
                        .sum::<usize>();
                    flags = Some(word(body, at + before));
                }
            }
        }
        flags.expect("the sample's flags") & NON_SYNC == 0
    }

    #[test]
    fn bad_images_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("video.mp4");
        let image = gradient(64, 48, ColorType::Rgb, 8);
        let mut encoder = VideoEncoder::create(&path, *image.info()).unwrap();
        encoder.push(&image, 10).unwrap();
        // Not after the previous one.
        assert!(matches!(encoder.push(&image, 10), Err(Error::Frame(_))));
        for other in [
            gradient(32, 32, ColorType::Rgb, 8),  // another size
            gradient(64, 48, ColorType::Rgb, 16), // another bit depth
            gradient(64, 48, ColorType::Rgba, 8), // other channels
        ] {
            assert!(matches!(encoder.push(&other, 20), Err(Error::Frame(_))));
        }
    }
}

//! `do_get` of a topic of the `ImageSequence` serialization format turned into a video by the store
//! optimizer (see `mosaicod_rw::image_sequence_video`). What is streamed depends on the ticket's
//! [`ImageSequenceView`]:
//!
//! - [`ImageSequenceView::Images`]: the images as they were uploaded, with the topic's schema
//!   ([`FrameIndex::source_schema`], its metadata included), sorted by `timestamp_ns`: each the
//!   row of the frame index, with its frame decoded as its `data`. A raw image comes back as
//!   exactly its bytes (see `mosaicod_image::encode_raw`); an encoded one as its pixels encoded
//!   again in its format, not its bytes (see `mosaicod_image::encode`). A time range keeps the
//!   images whose `timestamp_ns` is in it, as for any topic. Needs the `video` feature.
//! - [`ImageSequenceView::Video`]: the video as MPEG-DASH: a manifest, an initialization segment
//!   and a media segment per second of video, of the seconds of its interval or of all of them,
//!   in pieces, with the exact timestamps of their frames ([`segments_schema`]).
//!
//! The video is a header, then a fragment per frame (see `mosaicod_video::Mp4Layout`), every
//! frame a keyframe, whose places are in the frame index: the DASH segments are its header and
//! the fragments of their frames, read from the store without the others, with no FFmpeg; so are
//! the frames decoded into images.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, BooleanArray, ListArray, RecordBatch, StringArray};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use arrow_flight::error::FlightError;
use futures::stream::{BoxStream, StreamExt};
use mosaicod_core as core;
#[allow(unused_imports)] // In the documentation.
use mosaicod_core::types::flight::ImageSequenceView;
use mosaicod_core::types::flight::VideoInterval;
use mosaicod_facade as facade;
use mosaicod_grpc_common::PublicErrorGrpcExt;
use mosaicod_rw::image_sequence_video::{self as layout, FrameIndex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// A stream of record batches for a [`arrow_flight::encode::FlightDataEncoder`].
pub type BatchStream = BoxStream<'static, Result<RecordBatch, FlightError>>;

/// Schema metadata: the media type of the segments.
pub const CONTENT_TYPE_KEY: &str = "mosaico:content_type";
pub const CONTENT_TYPE: &str = "video/mp4";
/// Schema metadata: the codec of the video, `ffv1`.
pub const CODEC_KEY: &str = "mosaico:codec";
/// Schema metadata: the number of frames of the video.
pub const FRAMES_KEY: &str = "mosaico:frames";
/// Schema metadata: the capture time of the first frame of the video, in nanoseconds: the times
/// of the frames in the video are from it.
pub const CAPTURE_START_KEY: &str = "mosaico:capture_start_ns";
/// The duration of a media segment, in microseconds of video: one second.
pub const SEGMENT_DURATION_US: i64 = 1_000_000;

/// Columns of the segments.
pub const NAME_COLUMN: &str = "name";
pub const DATA_COLUMN: &str = "data";
pub const LAST_COLUMN: &str = "last";

/// The files of the DASH stream, as the manifest names them.
pub const MANIFEST_FILE: &str = "manifest.mpd";
pub const INIT_FILE: &str = "init.mp4";

/// The file of the media segment whose first frame is at `time_us` in the video.
pub fn segment_file(time_us: i64) -> String {
    format!("segment-{time_us}.m4s")
}

/// Batches built ahead of the client.
const BATCHES_AHEAD: usize = 2;

fn internal(message: impl ToString) -> core::Error {
    core::Error::internal(Some(message.to_string()))
}

/// An error in the middle of a stream, with its status.
fn flight_error(error: core::Error) -> FlightError {
    FlightError::Tonic(Box::new(error.to_public_error().log_to_status()))
}

/// The metadata of the schema of the segments: the video's.
fn video_metadata(frame_index: &FrameIndex) -> HashMap<String, String> {
    HashMap::from([
        (CONTENT_TYPE_KEY.to_owned(), CONTENT_TYPE.to_owned()),
        (CODEC_KEY.to_owned(), "ffv1".to_owned()),
        (FRAMES_KEY.to_owned(), frame_index.len().to_string()),
        (
            CAPTURE_START_KEY.to_owned(),
            frame_index
                .capture_timestamps_ns
                .first()
                .copied()
                .unwrap_or_default()
                .to_string(),
        ),
    ])
}

/// The rows of a file of `bytes`, cut in pieces of at most `piece` bytes: (data, last). The
/// pieces are `bytes` itself, not a copy.
fn pieces(bytes: Vec<u8>, piece: usize) -> (BinaryArray, BooleanArray) {
    let len = bytes.len();
    let count = len.div_ceil(piece).max(1);
    let lengths = (0..count).map(|i| (len - i * piece).min(piece));
    (
        BinaryArray::new(
            OffsetBuffer::from_lengths(lengths),
            Buffer::from_vec(bytes),
            None,
        ),
        BooleanArray::from_iter((0..count).map(|i| Some(i + 1 == count))),
    )
}

/// The seconds of the video of `frame_index`: up to that of its last frame.
fn video_seconds(frame_index: &FrameIndex) -> u64 {
    frame_index
        .video_timestamps_us
        .last()
        .map_or(0, |&t| (t / SEGMENT_DURATION_US) as u64 + 1)
}

/// The frames whose time in the video is in the seconds of `interval` (all of them if `None`), in
/// frame order. An interval that ends after the video is a bad request.
fn frames_of(
    frame_index: &FrameIndex,
    interval: Option<VideoInterval>,
) -> Result<Vec<u64>, core::Error> {
    let all = 0..frame_index.len() as u64;
    let Some(VideoInterval { start_s, end_s }) = interval else {
        return Ok(all.collect());
    };
    let seconds = video_seconds(frame_index);
    if u64::from(end_s) > seconds {
        return Err(core::Error::bad_request(format!(
            "video interval [{start_s}, {end_s}) s: the video has {seconds} s"
        )));
    }
    let window = i64::from(start_s) * SEGMENT_DURATION_US..i64::from(end_s) * SEGMENT_DURATION_US;
    Ok(all
        .filter(|&k| window.contains(&frame_index.video_timestamps_us[k as usize]))
        .collect())
}

/// A DASH media segment: the frames whose time in the video is in its second.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Segment {
    /// Its number: the second of video it is.
    number: u64,
    /// Its frames, in frame order.
    frames: Vec<u64>,
}

/// The frames of each second of video, of `frames` (in frame order), in order: only the seconds
/// with at least one frame.
fn group_segments(frame_index: &FrameIndex, frames: &[u64]) -> Vec<Segment> {
    let mut segments: Vec<Segment> = Vec::new();
    for &k in frames {
        let number = (frame_index.video_timestamps_us[k as usize] / SEGMENT_DURATION_US) as u64;
        match segments.last_mut() {
            Some(segment) if segment.number == number => segment.frames.push(k),
            _ => segments.push(Segment {
                number,
                frames: vec![k],
            }),
        }
    }
    segments
}

/// Where frame `k` ends in the video, in microseconds, as the encoder wrote it: at the next frame,
/// the last one as long as the gap before it, for at most `MAX_FRAME_DURATION_US`.
fn frame_end_us(frame_index: &FrameIndex, k: usize) -> i64 {
    let times = &frame_index.video_timestamps_us;
    let duration = match (times.get(k + 1), k.checked_sub(1).map(|p| times[p])) {
        (Some(&next), _) => next - times[k],
        (None, Some(previous)) => times[k] - previous,
        (None, None) => mosaicod_video::SINGLE_FRAME_DURATION_US,
    };
    times[k] + duration.min(mosaicod_video::MAX_FRAME_DURATION_US)
}

/// `us` microseconds as an `xs:duration` of seconds.
fn duration(us: i64) -> String {
    format!("PT{}.{:06}S", us / 1_000_000, us % 1_000_000)
}

/// The DASH manifest of `segments`, of `sizes` bytes: a static presentation, its timeline from
/// the frames of the segments.
fn manifest(frame_index: &FrameIndex, segments: &[Segment], sizes: &[u64]) -> String {
    let mut timeline = String::new();
    let mut end = 0;
    for segment in segments {
        let first = segment.frames[0] as usize;
        let last = *segment.frames.last().expect("a segment has frames") as usize;
        let t = frame_index.video_timestamps_us[first];
        end = frame_end_us(frame_index, last);
        let _ = writeln!(timeline, "            <S t=\"{t}\" d=\"{}\"/>", end - t);
    }
    let start = segments
        .first()
        .map(|s| frame_index.video_timestamps_us[s.frames[0] as usize])
        .unwrap_or_default();
    let presented_us = (end - start).max(1);
    let bandwidth =
        (u128::from(sizes.iter().sum::<u64>()) * 8 * 1_000_000 / presented_us as u128).max(1);
    let source = &frame_index.source;
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" profiles="urn:mpeg:dash:profile:isoff-live:2011" type="static" minBufferTime="{min_buffer}" mediaPresentationDuration="{presentation}">
  <Period id="0" start="PT0S">
    <AdaptationSet id="0" contentType="video" mimeType="video/mp4" segmentAlignment="true" startWithSAP="1">
      <Representation id="0" codecs="ffv1" bandwidth="{bandwidth}" width="{width}" height="{height}">
        <SegmentTemplate timescale="{timescale}" initialization="{init}" media="segment-$Time$.m4s">
          <SegmentTimeline>
{timeline}          </SegmentTimeline>
        </SegmentTemplate>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>
"#,
        min_buffer = duration(SEGMENT_DURATION_US),
        presentation = duration(end),
        width = source.width,
        height = source.height,
        timescale = layout::VIDEO_TIMESCALE,
        init = INIT_FILE,
    )
}

fn timestamps_list(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
        true,
    )
}

/// The schema of [`ImageSequenceView::Video`]: the files of an MPEG-DASH stream, in pieces, a
/// row per piece: the manifest ([`MANIFEST_FILE`]), the initialization segment ([`INIT_FILE`]),
/// then the media segments, in order, each the frames whose time in the video is in its second,
/// named by its first frame's time ([`segment_file`]). A file's pieces come in order: joined,
/// up to the one that is `last`, they are the file.
///
/// ```txt
/// name (Utf8)                            the file, as the manifest names it
/// data (Binary)                          the piece
/// last (Boolean)                         whether it is the last piece of its file
/// timestamp_ns (List<Int64>)             of each frame of a media segment, on its first piece
/// capture_timestamp_ns (List<Int64>)     of each frame of a media segment, on its first piece
/// video_timestamp_us (List<Int64>)       of each frame of a media segment, on its first piece
/// ```
///
/// The lists are null but on the first piece of a media segment. The
/// metadata has [`CONTENT_TYPE_KEY`], [`CODEC_KEY`], [`FRAMES_KEY`] and [`CAPTURE_START_KEY`], of the whole video. The initialization segment followed by any media
/// segments is an MP4 video of their frames, at their time in the whole video: followed by all
/// of them, the whole video.
pub fn segments_schema(frame_index: &FrameIndex) -> SchemaRef {
    Arc::new(
        Schema::new(vec![
            Field::new(NAME_COLUMN, DataType::Utf8, false),
            Field::new(DATA_COLUMN, DataType::Binary, false),
            Field::new(LAST_COLUMN, DataType::Boolean, false),
            timestamps_list(layout::TIMESTAMP_COLUMN),
            timestamps_list(layout::CAPTURE_TIMESTAMP_COLUMN),
            timestamps_list(layout::VIDEO_TIMESTAMP_COLUMN),
        ])
        .with_metadata(video_metadata(frame_index)),
    )
}

/// The rows of the file `name` of `bytes`, in pieces of `piece` bytes; for a media segment, the
/// timestamps of its frames, on its first piece.
fn file_batch(
    schema: &SchemaRef,
    name: &str,
    segment: Option<&Segment>,
    frame_index: &FrameIndex,
    bytes: Vec<u8>,
    piece: usize,
) -> Result<RecordBatch, core::Error> {
    let (data, last) = pieces(bytes, piece);
    let rows = last.len();
    let list = |values: &[i64]| -> ArrayRef {
        let lists = (0..rows).map(|row| {
            segment.filter(|_| row == 0).map(|s| {
                s.frames
                    .iter()
                    .map(|&k| Some(values[k as usize]))
                    .collect::<Vec<_>>()
            })
        });
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(lists))
    };
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![name; rows])),
            Arc::new(data),
            Arc::new(last),
            list(&frame_index.timestamps_ns),
            list(&frame_index.capture_timestamps_ns),
            list(&frame_index.video_timestamps_us),
        ],
    )
    .map_err(internal)
}

/// Streams the video of `video` as MPEG-DASH, a segment per second, of the seconds of `interval`
/// (all of them if `None`), each file in pieces of at most `piece` bytes ([`segments_schema`]):
/// read from the store, the header and the fragments of the frames of each segment, one segment
/// at a time. An interval that ends after the video is a bad request.
pub async fn segments(
    ctx: &facade::Context,
    video: facade::topic::ImageSequenceVideo,
    interval: Option<VideoInterval>,
    piece: usize,
) -> Result<(SchemaRef, BatchStream), core::Error> {
    let frame_index = Arc::new(video.frame_index);
    let schema = segments_schema(&frame_index);
    let frames = frames_of(&frame_index, interval)?;
    let segments = group_segments(&frame_index, &frames);
    let sizes: Vec<u64> = segments
        .iter()
        .map(|s| {
            mosaicod_video::SEGMENT_TYPE_BOX.len() as u64
                + s.frames
                    .iter()
                    .map(|&k| {
                        let fragment = &frame_index.fragments[k as usize];
                        fragment.end - fragment.start
                    })
                    .sum::<u64>()
        })
        .collect();
    let manifest = manifest(&frame_index, &segments, &sizes);

    let store = ctx.store.clone();
    let path = video.video_path;
    let (sender, batches) = mpsc::channel(BATCHES_AHEAD);
    let batch_schema = schema.clone();
    tokio::spawn(async move {
        let file = |name: &str, segment: Option<&Segment>, bytes: Vec<u8>| {
            file_batch(&batch_schema, name, segment, &frame_index, bytes, piece)
                .map_err(flight_error)
        };
        if sender
            .send(file(MANIFEST_FILE, None, manifest.into_bytes()))
            .await
            .is_err()
        {
            return;
        }
        let header = store
            .read_ranges(&path, &[frame_index.header()])
            .await
            .map_err(|e| flight_error(internal(format!("reading the video: {e}"))))
            .and_then(|parts| file(INIT_FILE, None, parts.concat()));
        let failed = header.is_err();
        if sender.send(header).await.is_err() || failed {
            return;
        }
        for (segment, size) in segments.iter().zip(sizes) {
            let ranges: Vec<_> = segment
                .frames
                .iter()
                .map(|&k| frame_index.fragments[k as usize].clone())
                .collect();
            let batch = store
                .read_ranges(&path, &ranges)
                .await
                .map_err(|e| flight_error(internal(format!("reading the video: {e}"))))
                .and_then(|parts| {
                    let mut bytes = Vec::with_capacity(size as usize);
                    bytes.extend_from_slice(&mosaicod_video::SEGMENT_TYPE_BOX);
                    for part in parts {
                        bytes.extend_from_slice(&part);
                    }
                    let time = frame_index.video_timestamps_us[segment.frames[0] as usize];
                    file(&segment_file(time), Some(segment), bytes)
                });
            let failed = batch.is_err();
            if sender.send(batch).await.is_err() || failed {
                return;
            }
        }
    });
    Ok((schema, ReceiverStream::new(batches).boxed()))
}

#[cfg(feature = "video")]
pub use decoded::images;

#[cfg(feature = "video")]
mod decoded {
    use std::collections::{HashMap, VecDeque};

    use arrow::array::BinaryBuilder;
    use arrow::compute::{cast, take};
    use mosaicod_core::types;
    use mosaicod_image::{Image, ImageFormat, RawLayout};
    use mosaicod_rw::ImageSequenceFormatProperties as Columns;
    use mosaicod_rw::image_sequence_video::RawFrames;
    use tokio::io::AsyncWriteExt;
    use tracing::debug;

    use super::*;

    /// Images encoded at the same time, while the next ones are decoded: encoding a large PNG
    /// is slower than decoding its frame.
    const ENCODE_AHEAD: usize = 4;

    /// Fragments read from the store at a time, while they are written in the local video.
    const FRAGMENTS_AT_ONCE: usize = 64;

    /// Whether `timestamp` is in `range`: `start <= timestamp < end`, as for any topic.
    fn in_range(range: Option<types::TimestampRange>, timestamp: i64) -> bool {
        range.is_none_or(|r| {
            (r.start.is_unbounded() || timestamp >= r.start.as_i64())
                && (r.end.is_unbounded() || timestamp < r.end.as_i64())
        })
    }

    /// The frames whose `timestamp_ns` is in `range`, in frame order.
    pub(super) fn frames_in(
        frame_index: &FrameIndex,
        range: Option<types::TimestampRange>,
    ) -> Vec<u64> {
        (0..frame_index.len() as u64)
            .filter(|&k| in_range(range, frame_index.timestamps_ns[k as usize]))
            .collect()
    }

    fn video_error(e: mosaicod_video::Error) -> core::Error {
        internal(format!("video: {e}"))
    }

    /// Writes the video of `frames` of `video` (in frame order) in a temporary folder: its header
    /// and the fragments of those frames, read from the store.
    async fn local_video(
        ctx: &facade::Context,
        video: &facade::topic::ImageSequenceVideo,
        frames: &[u64],
    ) -> Result<(tempfile::TempDir, std::path::PathBuf), core::Error> {
        let index = &video.frame_index;
        let folder = tempfile::tempdir().map_err(|e| internal(format!("temporary folder: {e}")))?;
        let local = folder.path().join(layout::VIDEO_FILE);
        let write_failed = |e: std::io::Error| internal(format!("writing the video: {e}"));
        let mut file = tokio::fs::File::create(&local)
            .await
            .map_err(write_failed)?;
        let read = |ranges: Vec<std::ops::Range<u64>>| async move {
            ctx.store
                .read_ranges(&video.video_path, &ranges)
                .await
                .map_err(|e| internal(format!("reading the video: {e}")))
        };
        for part in read(vec![index.header()]).await? {
            file.write_all(&part).await.map_err(write_failed)?;
        }
        for chunk in frames.chunks(FRAGMENTS_AT_ONCE) {
            let ranges = chunk
                .iter()
                .map(|&k| index.fragments[k as usize].clone())
                .collect();
            for part in read(ranges).await? {
                file.write_all(&part).await.map_err(write_failed)?;
            }
        }
        file.flush().await.map_err(write_failed)?;
        debug!(
            "{} frames of {} read from the store",
            frames.len(),
            video.video_path.display()
        );
        Ok((folder, local))
    }

    /// What the images of a video are encoded back into.
    enum Back {
        /// Their format.
        Encoded(ImageFormat),
        /// Their raw bytes, each laid out as its row says.
        Raw(Box<RawFrames>),
    }

    impl Back {
        fn of(frame_index: &FrameIndex) -> Result<Self, core::Error> {
            if frame_index.source.format == layout::RAW_FORMAT {
                Ok(Self::Raw(Box::new(
                    RawFrames::new(&frame_index.rows).map_err(internal)?,
                )))
            } else {
                Ok(Self::Encoded(
                    frame_index.source.format.parse().map_err(internal)?,
                ))
            }
        }

        /// The bytes of `image`, the image of frame `frame`.
        fn encode(&self, frame: u64, image: &Image) -> Result<Vec<u8>, String> {
            match self {
                Self::Encoded(format) => {
                    mosaicod_image::encode(*format, image).map_err(|e| e.to_string())
                }
                Self::Raw(frames) => {
                    let raw = frames.get(frame as usize);
                    let layout = RawLayout {
                        width: raw.width,
                        height: raw.height,
                        stride: raw.stride,
                        encoding: raw.encoding,
                        big_endian: raw.big_endian,
                    };
                    mosaicod_image::encode_raw(&layout, image).map_err(|e| e.to_string())
                }
            }
        }
    }

    /// A batch of `rows` (frame, image bytes), with the schema the images were uploaded with: the
    /// row of each frame in the frame index, its bytes as `data`.
    fn images_batch(
        schema: &SchemaRef,
        frame_index: &FrameIndex,
        rows: &[(u64, Vec<u8>)],
    ) -> Result<RecordBatch, core::Error> {
        let frames =
            arrow::array::UInt64Array::from_iter_values(rows.iter().map(|(frame, _)| *frame));
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let array: ArrayRef = if field.name() == Columns::DATA_COLUMN {
                    // Allocated once, of the size of all the images.
                    let bytes = rows.iter().map(|(_, bytes)| bytes.len()).sum();
                    let mut data = BinaryBuilder::with_capacity(rows.len(), bytes);
                    for (_, bytes) in rows {
                        data.append_value(bytes);
                    }
                    Arc::new(data.finish())
                } else {
                    let column =
                        frame_index
                            .rows
                            .column_by_name(field.name())
                            .ok_or_else(|| {
                                internal(format!("no `{}` in the frame index", field.name()))
                            })?;
                    take(column, &frames, None).map_err(internal)?
                };
                // The types of the topic's schema (e.g. `BinaryView`).
                if array.data_type() == field.data_type() {
                    Ok(array)
                } else {
                    cast(&array, field.data_type()).map_err(internal)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(schema.clone(), columns).map_err(internal)
    }

    /// Streams the images of `video`, decoded from its frames and encoded back in their format,
    /// those whose `timestamp_ns` is in `timestamp_range`, sorted by `timestamp_ns`, in batches
    /// of at most `batch_bytes` bytes of images.
    pub async fn images(
        ctx: &facade::Context,
        video: facade::topic::ImageSequenceVideo,
        timestamp_range: Option<types::TimestampRange>,
        batch_bytes: usize,
    ) -> Result<(SchemaRef, BatchStream), core::Error> {
        let schema = video.frame_index.source_schema.clone();
        let back = Arc::new(Back::of(&video.frame_index)?);
        let frames = frames_in(&video.frame_index, timestamp_range);
        // The order of the rows, as for any topic: by `timestamp_ns`, then frame order.
        let mut order = frames.clone();
        order.sort_by_key(|&k| (video.frame_index.timestamps_ns[k as usize], k));

        let (folder, local) = local_video(ctx, &video, &frames).await?;
        let frame_index = Arc::new(video.frame_index);

        // Frames decoded on a blocking thread, then encoded on blocking threads, in order. The
        // local video holds only `frames`: its frame `i` is `frames[i]`.
        let (frame_sender, mut decoded) = mpsc::channel(2);
        let decoder = tokio::task::spawn_blocking(move || {
            mosaicod_video::decode(&local, |frame| {
                frame_sender
                    .blocking_send(frame)
                    .map_err(|_| mosaicod_video::Error::Frame("the stream ended".to_owned()))
            })
        });

        let (sender, batches) = mpsc::channel(BATCHES_AHEAD);
        let batch_schema = schema.clone();
        tokio::spawn(async move {
            let _folder = folder;
            let send = |batch: Result<RecordBatch, core::Error>| {
                let sender = sender.clone();
                async move { sender.send(batch.map_err(flight_error)).await.is_ok() }
            };

            let mut encoding = VecDeque::with_capacity(ENCODE_AHEAD);
            // Encoded images not yet in a batch, by frame: only those ahead of their turn when
            // the frame order is not that of `timestamp_ns`.
            let mut pending: HashMap<u64, Vec<u8>> = HashMap::new();
            let mut next = 0;
            let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
            let mut row_bytes = 0;
            let mut frames_left = true;
            while frames_left || !encoding.is_empty() {
                if frames_left && encoding.len() < ENCODE_AHEAD {
                    match decoded.recv().await {
                        Some(mosaicod_video::DecodedFrame { frame, image, .. }) => {
                            let Some(&frame) = frames.get(frame as usize) else {
                                send(Err(internal("more frames decoded than read"))).await;
                                return;
                            };
                            let back = back.clone();
                            encoding.push_back(tokio::task::spawn_blocking(move || {
                                back.encode(frame, &image).map(|bytes| (frame, bytes))
                            }));
                        }
                        None => frames_left = false,
                    }
                    continue;
                }
                let Some(job) = encoding.pop_front() else {
                    continue;
                };
                let encoded = job.await.map_err(|e| e.to_string()).and_then(|e| e);
                let (frame, bytes) = match encoded {
                    Ok(encoded) => encoded,
                    Err(e) => {
                        send(Err(internal(format!("encoding an image: {e}")))).await;
                        return;
                    }
                };
                pending.insert(frame, bytes);
                while let Some(bytes) = order.get(next).and_then(|k| pending.remove(k)) {
                    row_bytes += bytes.len();
                    rows.push((order[next], bytes));
                    next += 1;
                    if row_bytes >= batch_bytes {
                        let batch = images_batch(&batch_schema, &frame_index, &rows);
                        if !send(batch).await {
                            return;
                        }
                        rows.clear();
                        row_bytes = 0;
                    }
                }
            }
            drop(decoded);
            match decoder.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    send(Err(video_error(e))).await;
                    return;
                }
                Err(e) => {
                    send(Err(internal(format!("decoding the video: {e}")))).await;
                    return;
                }
            }
            if next != order.len() {
                send(Err(internal(format!(
                    "{next} images decoded of {}",
                    order.len()
                ))))
                .await;
                return;
            }
            if !rows.is_empty() {
                send(images_batch(&batch_schema, &frame_index, &rows)).await;
            }
        });
        Ok((schema, ReceiverStream::new(batches).boxed()))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, Int64Array};
    #[cfg(feature = "video")]
    use mosaicod_core::types;
    use mosaicod_rw::image_sequence_video::SourceImages;

    #[cfg(feature = "video")]
    use super::decoded::frames_in;
    use super::*;

    /// Frames at 0, 900, 1100, 3000 and 4200 ms of video.
    fn index() -> FrameIndex {
        let captures = vec![
            1_000_000_000,
            1_900_000_000,
            2_100_000_000,
            4_000_000_000,
            5_200_000_000,
        ];
        let rows = RecordBatch::try_from_iter([
            (
                layout::TIMESTAMP_COLUMN,
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef,
            ),
            (
                layout::CAPTURE_TIMESTAMP_COLUMN,
                Arc::new(Int64Array::from(captures.clone())),
            ),
        ])
        .unwrap();
        FrameIndex::new(
            rows,
            layout::video_timestamps_us(&captures),
            vec![100..200, 200..300, 300..400, 400..500, 500..600],
            100,
            SourceImages {
                format: "png".to_owned(),
                width: 64,
                height: 48,
            },
            Arc::new(Schema::empty()),
        )
        .unwrap()
    }

    #[cfg(feature = "video")]
    #[test]
    fn frames_are_kept_by_their_timestamp() {
        let index = index();
        assert_eq!(frames_in(&index, None), [0, 1, 2, 3, 4]);
        let range = types::TimestampRange::between(20.into(), 40.into());
        assert_eq!(frames_in(&index, Some(range)), [1, 2]);
        let range = types::TimestampRange::starting_at(35.into());
        assert_eq!(frames_in(&index, Some(range)), [3, 4]);
    }

    /// Seconds of video `[start_s, end_s)`.
    fn interval(start_s: u32, end_s: u32) -> Option<VideoInterval> {
        Some(VideoInterval { start_s, end_s })
    }

    #[test]
    fn frames_are_kept_by_their_second_of_video() {
        let index = index();
        assert_eq!(video_seconds(&index), 5);
        assert_eq!(frames_of(&index, None).unwrap(), [0, 1, 2, 3, 4]);
        assert_eq!(frames_of(&index, interval(0, 5)).unwrap(), [0, 1, 2, 3, 4]);
        assert_eq!(frames_of(&index, interval(0, 1)).unwrap(), [0, 1]);
        assert_eq!(frames_of(&index, interval(1, 4)).unwrap(), [2, 3]);
        // A second with no frames, in a gap of the video.
        assert_eq!(frames_of(&index, interval(2, 3)).unwrap(), [] as [u64; 0]);
        // After the video.
        let error = frames_of(&index, interval(4, 6)).unwrap_err();
        assert!(error.to_string().contains("the video has 5 s"), "{error}");
    }

    #[test]
    fn segments_are_seconds_of_video_in_the_manifest() {
        let index = index();
        let segments = group_segments(&index, &[0, 1, 2, 3, 4]);
        let numbers: Vec<(u64, Vec<u64>)> = segments
            .iter()
            .map(|s| (s.number, s.frames.clone()))
            .collect();
        assert_eq!(
            numbers,
            [(0, vec![0, 1]), (1, vec![2]), (3, vec![3]), (4, vec![4])]
        );

        let mpd = manifest(&index, &segments, &vec![1_000; segments.len()]);
        // Each until the next frame; the last one as long as the gap before it.
        for timeline in [
            r#"<S t="0" d="1100000"/>"#,
            r#"<S t="1100000" d="1900000"/>"#,
            r#"<S t="3000000" d="1200000"/>"#,
            r#"<S t="4200000" d="1200000"/>"#,
            r#"minBufferTime="PT1.000000S""#,
            r#"timescale="1000000""#,
            r#"codecs="ffv1""#,
            r#"width="64" height="48""#,
            r#"mediaPresentationDuration="PT5.400000S""#,
            r#"media="segment-$Time$.m4s""#,
        ] {
            assert!(mpd.contains(timeline), "{timeline} in {mpd}");
        }
        assert_eq!(segment_file(4_200_000), "segment-4200000.m4s");
    }

    #[test]
    fn files_are_cut_in_pieces_with_their_frames_on_the_first() {
        let index = index();
        let segments = group_segments(&index, &[3, 4]);
        let schema = segments_schema(&index);
        let bytes: Vec<u8> = (0..10).collect();
        let batch = file_batch(
            &schema,
            "segment-3000000.m4s",
            Some(&segments[0]),
            &index,
            bytes,
            4,
        )
        .unwrap();
        assert_eq!(batch[DATA_COLUMN].as_binary::<i32>().value(0), [0, 1, 2, 3]);
        assert_eq!(batch[DATA_COLUMN].as_binary::<i32>().value(2), [8, 9]);
        assert_eq!(
            batch[LAST_COLUMN].as_boolean().iter().collect::<Vec<_>>(),
            [Some(false), Some(false), Some(true)]
        );
        let lists = batch[layout::TIMESTAMP_COLUMN].as_list::<i32>();
        assert!(lists.is_valid(0) && lists.is_null(1) && lists.is_null(2));

        let manifest =
            file_batch(&schema, MANIFEST_FILE, None, &index, b"<MPD/>".to_vec(), 4).unwrap();
        assert!(manifest[layout::TIMESTAMP_COLUMN].is_null(0));
    }
}

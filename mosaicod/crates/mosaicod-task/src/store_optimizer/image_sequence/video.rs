//! Step 2: turns the checked images of an image sequence into a lossless FFV1 video, in a local
//! file, with its frame index: the rows of its images but their data, in frame order.

use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::Int64Type;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::col;
use futures::StreamExt;
use mosaicod_core::{constants, types};
use mosaicod_image::{
    Error as ImageError, Image, ImageFormat, ImageInfo, RawError, RawLayout, decode, decode_raw,
};
use mosaicod_rw::ImageSequenceFormatProperties as Columns;
use mosaicod_rw::ImageSequenceRawFormatProperties as RawColumns;
use mosaicod_rw::image_sequence_video::{self as layout, FrameIndex, RawFrames, SourceImages};
use mosaicod_video::VideoEncoder;

use super::super::{AcquiredTopic, StoreOptimizer};
use super::{format_name, image_bytes, image_issue};

/// Images decoded at the same time, while the encoder encodes the previous ones: decoding an 8K
/// PNG is slower than encoding it in FFV1.
const DECODE_AHEAD: usize = 4;

/// Why an image sequence did not become a video.
pub(super) enum Failure {
    /// An image's pixel data is corrupted, with the details: the topic's issue is
    /// `OptimizationIssue::Undecodable`.
    Undecodable(String),
    /// Its images can't be a video, for the given reason: the topic's issue is
    /// `OptimizationIssue::Unsupported`.
    Unsupported(String),
    /// Something failed (FFmpeg, the local filesystem, reading the topic), for the given reason:
    /// the topic is to be tried again.
    Failed(String),
}

fn failed(message: impl ToString) -> Failure {
    Failure::Failed(message.to_string())
}

/// A video error: about the frames (their size, timestamps), the images can't be a video;
/// anything else (FFmpeg, the filesystem) is a failure.
fn video_failure(error: mosaicod_video::Error) -> Failure {
    match error {
        mosaicod_video::Error::Frame(_) | mosaicod_video::Error::Image(_) => {
            Failure::Unsupported(error.to_string())
        }
        _ => failed(format!("video encoding failed: {error}")),
    }
}

/// The video of an image sequence, in a local folder deleted when dropped.
pub(super) struct LocalVideo {
    /// Keeps the folder of [`Self::video`] until the video is written in the store.
    _folder: tempfile::TempDir,
    /// The video, in FFV1, a fragmented MP4 file.
    pub(super) video: std::path::PathBuf,
    /// The frame index, [`layout::FRAME_INDEX_FILE`].
    pub(super) frame_index_parquet: Vec<u8>,
    /// The number of frames, one per image.
    pub(super) frames: usize,
    /// The column statistics of the topic, as recorded for its chunks.
    pub(super) stats: types::OntologyModelStats,
    /// In-memory size of the columns but the images, for the chunk record.
    pub(super) arrow_size_bytes: usize,
}

/// What the images of a sequence are, to decode them.
#[derive(Debug)]
pub(super) enum Source {
    /// Encoded, all in this format (the `ImageSequence` serialization format).
    Encoded(ImageFormat),
    /// Raw, each laid out as its row says (the `ImageSequenceRaw` serialization format), in frame
    /// order.
    Raw(Box<RawFrames>),
}

/// The rows of the image sequence but their images, in frame order.
struct Index {
    source: Source,
    /// The schema of the topic, with its metadata: the images are read back with it.
    source_schema: SchemaRef,
    rows: RecordBatch,
    /// The `timestamp_ns` column of [`Self::rows`], not a copy.
    timestamps_ns: ScalarBuffer<i64>,
    /// The `capture_timestamp_ns` column of [`Self::rows`], not a copy.
    capture_timestamps_ns: ScalarBuffer<i64>,
    stats: types::OntologyModelStats,
    arrow_size_bytes: usize,
}

/// The order of the frames: capture time, then the time the image was recorded.
fn frame_order() -> Vec<datafusion::logical_expr::SortExpr> {
    vec![
        col(Columns::CAPTURE_TIMESTAMP_COLUMN).sort(true, false),
        col(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP).sort(true, false),
    ]
}

/// The format of the first image of `rows`, the format of every image of the topic: checked in
/// step 1.
fn first_format(rows: &RecordBatch) -> Result<ImageFormat, Failure> {
    let column = rows
        .column_by_name(Columns::FORMAT_COLUMN)
        .ok_or_else(|| failed("no format column"))?;
    format_name(column, 0)
        .ok_or_else(|| failed("the first image has no format"))?
        .parse()
        .map_err(failed)
}

/// The columns an image sequence must have, `raw` or not, besides `data`.
fn required_columns(raw: bool) -> &'static [&'static str] {
    if raw {
        &[
            constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
            Columns::CAPTURE_TIMESTAMP_COLUMN,
            RawColumns::WIDTH_COLUMN,
            RawColumns::HEIGHT_COLUMN,
            RawColumns::STRIDE_COLUMN,
            RawColumns::ENCODING_COLUMN,
        ]
    } else {
        &[
            constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
            Columns::CAPTURE_TIMESTAMP_COLUMN,
            Columns::FORMAT_COLUMN,
        ]
    }
}

/// Decodes frame `frame` of `source`, the image of `row` of `data`, recorded at `timestamp_ns`.
/// An encoded image's structure was checked in step 1: its pixel data can be corrupted, or of a
/// layout that can't be decoded. A raw image's bytes can not match its layout, or its encoding
/// not be supported.
fn decode_frame(
    source: &Source,
    data: &ArrayRef,
    row: usize,
    frame: usize,
    timestamp_ns: i64,
) -> Result<Image, Failure> {
    let bytes = image_bytes(data, row)
        .ok_or_else(|| Failure::Undecodable(image_issue(timestamp_ns, "no data")))?;
    match source {
        Source::Encoded(format) => decode(*format, bytes).map_err(|e| match e {
            ImageError::Invalid { .. } => Failure::Undecodable(image_issue(timestamp_ns, e)),
            _ => Failure::Unsupported(format!("image {frame}: {e}")),
        }),
        Source::Raw(frames) => {
            let raw = frames.get(frame);
            let layout = RawLayout {
                width: raw.width,
                height: raw.height,
                stride: raw.stride,
                encoding: raw.encoding,
                big_endian: raw.big_endian,
            };
            decode_raw(&layout, bytes).map_err(|e| match e {
                RawError::Invalid(_) => Failure::Undecodable(image_issue(timestamp_ns, e)),
                RawError::Unsupported(_) => Failure::Unsupported(image_issue(timestamp_ns, e)),
            })
        }
    }
}

/// A video encoded by [`encode`].
struct Encoded {
    size_bytes: u64,
    /// What every image is.
    info: ImageInfo,
    /// The time of each frame in the video, given to [`encode`].
    timestamps_us: Vec<i64>,
}

/// Encodes the decoded images received from `images` into the video `path`, image `k` at
/// `timestamps_us[k]` microseconds. The video takes the [`ImageInfo`] of the first image, and
/// every image must have the same.
///
/// Blocking: run on a blocking thread.
fn encode(
    mut images: tokio::sync::mpsc::Receiver<Image>,
    path: std::path::PathBuf,
    timestamps_us: Vec<i64>,
) -> Result<Encoded, Failure> {
    let mut encoder: Option<(VideoEncoder, ImageInfo)> = None;
    let mut count = 0;
    while let Some(image) = images.blocking_recv() {
        let info = *image.info();
        let (video, first) = match &mut encoder {
            Some(open) => open,
            None => {
                let video = VideoEncoder::create(&path, info).map_err(video_failure)?;
                encoder.insert((video, info))
            }
        };
        if info != *first {
            return Err(Failure::Unsupported(format!(
                "image {count} is {}x{} {} of {} bits, the first one {}x{} {} of {} bits",
                info.width,
                info.height,
                info.color,
                info.bit_depth,
                first.width,
                first.height,
                first.color,
                first.bit_depth
            )));
        }
        let timestamp_us = *timestamps_us
            .get(count)
            .ok_or_else(|| failed("more images than timestamps"))?;
        video.push(&image, timestamp_us).map_err(video_failure)?;
        count += 1;
    }

    if count != timestamps_us.len() {
        return Err(failed(format!(
            "{count} images read, {} timestamps",
            timestamps_us.len()
        )));
    }
    let (video, info) = encoder.ok_or_else(|| Failure::Unsupported("no images".to_owned()))?;
    Ok(Encoded {
        size_bytes: video.finish().map_err(video_failure)?,
        info,
        timestamps_us,
    })
}

/// Where the header and the fragment of each frame are in the video `path`, of `frames` frames.
fn fragments(path: &std::path::Path, frames: usize) -> Result<mosaicod_video::Mp4Layout, Failure> {
    let file = std::fs::File::open(path).map_err(|e| failed(format!("reading the video: {e}")))?;
    let len = file
        .metadata()
        .map_err(|e| failed(format!("reading the video: {e}")))?
        .len();
    let layout = mosaicod_video::mp4_layout(std::io::BufReader::new(file), len)
        .map_err(|e| failed(format!("reading the video: {e}")))?;
    if layout.fragments.len() != frames {
        return Err(failed(format!(
            "{} fragments in the video of {frames} images",
            layout.fragments.len()
        )));
    }
    Ok(layout)
}

impl StoreOptimizer {
    /// Encodes the images of `acquired_topic`, checked in step 1, into an FFV1 video in a local
    /// folder, with the timestamps of its frames.
    pub(super) async fn encode_video(
        &self,
        acquired_topic: &AcquiredTopic,
        raw: bool,
    ) -> Result<LocalVideo, Failure> {
        let index = self.read_index(acquired_topic, raw).await?;
        let frames = index.timestamps_ns.len();
        if frames == 0 {
            return Err(Failure::Unsupported("no images".to_owned()));
        }
        let video_timestamps_us = layout::video_timestamps_us(&index.capture_timestamps_ns);

        // The video is written on the local filesystem, then in the store in step 3.
        let folder = tempfile::tempdir().map_err(|e| failed(format!("temporary folder: {e}")))?;
        let video = folder.path().join(layout::VIDEO_FILE);
        let format = match &index.source {
            Source::Encoded(format) => format.to_string(),
            Source::Raw(_) => layout::RAW_FORMAT.to_owned(),
        };
        let encoded = self
            .encode_images(
                acquired_topic,
                Arc::new(index.source),
                video.clone(),
                &index.timestamps_ns,
                video_timestamps_us,
            )
            .await?;
        tracing::debug!(
            "topic {}: {frames} images encoded into {} bytes of video",
            acquired_topic.topic_record.locator(),
            encoded.size_bytes
        );

        let mp4 = fragments(&video, frames)?;
        let frame_index = FrameIndex::new(
            index.rows,
            encoded.timestamps_us,
            mp4.fragments,
            mp4.header.end,
            SourceImages {
                format,
                width: encoded.info.width,
                height: encoded.info.height,
            },
            index.source_schema,
        )
        .map_err(failed)?;
        let frame_index_parquet = frame_index.into_parquet().map_err(failed)?;

        Ok(LocalVideo {
            _folder: folder,
            video,
            frame_index_parquet,
            frames,
            stats: index.stats,
            arrow_size_bytes: index.arrow_size_bytes,
        })
    }

    /// Reads every column of the image sequence but its images, in frame order: the columns of
    /// its ontology, `raw` or not, and any other, which the frame index keeps.
    async fn read_index(
        &self,
        acquired_topic: &AcquiredTopic,
        raw: bool,
    ) -> Result<Index, Failure> {
        const BATCH_SIZE: usize = 8192;

        let df = self
            .read_topic(acquired_topic, BATCH_SIZE)
            .await
            .map_err(failed)?;
        let source_schema = Arc::new(df.schema().as_arrow().clone());
        let has = |name: &str| source_schema.column_with_name(name).is_some();
        if let Some(missing) = required_columns(raw)
            .iter()
            .chain([&Columns::DATA_COLUMN])
            .find(|name| !has(name))
        {
            return Err(Failure::Unsupported(format!("no `{missing}` column")));
        }
        if let Some(own) = layout::OWN_COLUMNS.iter().find(|name| has(name)) {
            return Err(Failure::Unsupported(format!(
                "a column named `{own}`, a name the video's frame index has for itself"
            )));
        }

        let columns: Vec<&str> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .filter(|&name| name != Columns::DATA_COLUMN)
            .collect();
        let index = df
            .select_columns(&columns)
            .and_then(|df| df.sort(frame_order()))
            .map_err(failed)?;
        let schema = Arc::new(index.schema().as_arrow().clone());
        let mut batches = index.execute_stream().await.map_err(failed)?;

        let mut stats = mosaicod_ext::arrow::ontology_model_stats_from_schema(&schema);
        let mut arrow_size_bytes = 0;
        let mut rows = Vec::new();
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(failed)?;
            mosaicod_ext::arrow::ontology_model_stats_inspect_record_batch(&mut stats, &batch)
                .map_err(failed)?;
            arrow_size_bytes += batch.get_array_memory_size();
            rows.push(batch);
        }
        let rows = datafusion::arrow::compute::concat_batches(&schema, &rows).map_err(failed)?;
        if rows.num_rows() == 0 {
            return Err(Failure::Unsupported("no images".to_owned()));
        }

        // The timestamps of the frames, which the video and the frame index need.
        let unsupported = |e: layout::FrameIndexError| Failure::Unsupported(e.to_string());
        let timestamps = |name: &str| -> Result<ScalarBuffer<i64>, Failure> {
            let column = rows
                .column_by_name(name)
                .and_then(|c| c.as_primitive_opt::<Int64Type>())
                .ok_or_else(|| Failure::Unsupported(format!("`{name}` is not a 64-bit integer")))?;
            if column.null_count() > 0 {
                return Err(Failure::Unsupported(format!("images with no `{name}`")));
            }
            Ok(column.values().clone())
        };
        let timestamps_ns = timestamps(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)?;
        let capture_timestamps_ns = timestamps(Columns::CAPTURE_TIMESTAMP_COLUMN)?;
        let source = if raw {
            Source::Raw(Box::new(RawFrames::new(&rows).map_err(unsupported)?))
        } else {
            Source::Encoded(first_format(&rows)?)
        };
        Ok(Index {
            source,
            source_schema,
            rows,
            timestamps_ns,
            capture_timestamps_ns,
            stats,
            arrow_size_bytes,
        })
    }

    /// Decodes the images of the topic in frame order, [`DECODE_AHEAD`] at a time, and encodes
    /// them into the video `path` on a blocking thread. `timestamps_ns` are those of the images,
    /// in frame order, for the details of an issue.
    async fn encode_images(
        &self,
        acquired_topic: &AcquiredTopic,
        source: Arc<Source>,
        path: std::path::PathBuf,
        timestamps_ns: &[i64],
        timestamps_us: Vec<i64>,
    ) -> Result<Encoded, Failure> {
        let batch_size = self
            .batch_size(&acquired_topic.topic_record)
            .await
            .map_err(failed)?;
        let mut batches = self
            .read_topic(acquired_topic, batch_size)
            .await
            .map_err(failed)?
            .sort(frame_order())
            .and_then(|df| df.select_columns(&[Columns::DATA_COLUMN]))
            .map_err(failed)?
            .execute_stream()
            .await
            .map_err(failed)?;

        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let encoder = tokio::task::spawn_blocking(move || encode(receiver, path, timestamps_us));

        // The images being decoded, oldest first, at most `DECODE_AHEAD`: each is sent to the
        // encoder once decoded, in order. Decoding stops at the first error, or when the encoder
        // stops (then its error is the one to report).
        let mut decoding = VecDeque::with_capacity(DECODE_AHEAD);
        let decoded: Result<(), Failure> = async {
            let mut frame = 0;
            // The images of the batch being read, and the next one to decode.
            let mut batch: Option<(ArrayRef, usize)> = None;
            let mut images_left = true;
            loop {
                while images_left && decoding.len() < DECODE_AHEAD {
                    match &mut batch {
                        Some((data, row)) if *row < data.len() => {
                            let (data, image_row) = (data.clone(), *row);
                            let timestamp_ns = timestamps_ns[frame];
                            let source = source.clone();
                            decoding.push_back(tokio::task::spawn_blocking(move || {
                                decode_frame(&source, &data, image_row, frame, timestamp_ns)
                            }));
                            *row += 1;
                            frame += 1;
                        }
                        _ => match batches.next().await {
                            Some(next) => {
                                batch = Some((next.map_err(failed)?.column(0).clone(), 0))
                            }
                            None => images_left = false,
                        },
                    }
                }
                let Some(next) = decoding.pop_front() else {
                    break;
                };
                let decoded = next
                    .await
                    .map_err(|e| failed(format!("decoding an image: {e}")))??;
                if sender.send(decoded).await.is_err() {
                    break;
                }
            }
            Ok(())
        }
        .await;
        drop(sender);
        let encoded = encoder
            .await
            .map_err(|e| failed(format!("encoding the video: {e}")))?;
        decoded?;
        encoded
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BinaryArray, StringArray, UInt32Array};
    use mosaicod_image::{ColorType, testing};

    use super::*;

    #[test]
    fn images_are_decoded_or_their_issue_named() {
        let png = mosaicod_image::encode(
            ImageFormat::Png,
            &testing::gradient(8, 6, ColorType::Rgb, 8),
        )
        .unwrap();
        let mut corrupted = png.clone();
        let idat = corrupted.windows(4).position(|w| w == b"IDAT").unwrap();
        corrupted[idat + 8] ^= 0xff;
        let data: ArrayRef = Arc::new(BinaryArray::from_iter_values([png, corrupted]));

        let png = Source::Encoded(ImageFormat::Png);
        assert!(decode_frame(&png, &data, 0, 0, 10).is_ok());
        let Err(Failure::Undecodable(details)) = decode_frame(&png, &data, 1, 1, 20) else {
            panic!("a corrupted image is undecodable");
        };
        assert!(
            details.starts_with("image recorded at 20 ns: invalid png image"),
            "{details}"
        );

        // Raw images: of the size of their layout, of an encoding a video can hold.
        let size = || Arc::new(UInt32Array::from(vec![2; 3])) as ArrayRef;
        let rows = RecordBatch::try_from_iter([
            ("width", size()),
            ("height", size()),
            ("stride", size()),
            (
                "encoding",
                Arc::new(StringArray::from(vec!["mono8", "mono8", "32FC1"])),
            ),
        ])
        .unwrap();
        let raw = Source::Raw(Box::new(RawFrames::new(&rows).unwrap()));
        let data: ArrayRef = Arc::new(BinaryArray::from_iter_values([
            vec![1, 2, 3, 4],
            vec![1, 2, 3],
            vec![0; 4],
        ]));
        assert!(decode_frame(&raw, &data, 0, 0, 10).is_ok());
        let Err(Failure::Undecodable(details)) = decode_frame(&raw, &data, 1, 1, 20) else {
            panic!("raw bytes of another size are undecodable");
        };
        assert!(
            details.starts_with("image recorded at 20 ns: invalid raw image"),
            "{details}"
        );
        let Err(Failure::Unsupported(details)) = decode_frame(&raw, &data, 2, 2, 30) else {
            panic!("an encoding a video can't hold is unsupported");
        };
        assert!(details.contains("`32FC1`"), "{details}");
    }
}

//! Turns the images of an image sequence topic into a lossless FFV1 video: of the `ImageSequence`
//! serialization format (encoded images, PNG) or of the `ImageSequenceRaw` one (raw images, as a
//! camera driver writes them, e.g. a ROS `sensor_msgs/Image`).
//!
//! An image sequence with no images (finalized without any message) has nothing to optimize: its
//! optimization ends, with no video and no notification.
//!
//! `do_put` accepts an image sequence without checking it, which would take too long for an
//! upload: the store optimizer checks it, in four steps. When step 1 or 2 finds an issue
//! (`OptimizationIssue`), the topic is left as the client sent it, the issue is written in an
//! `error` notification on it (`store optimizer/<issue>: <details>`), and its optimization ends:
//! it is never optimized again. Steps 3 and 4 can fail for a reason that may not last (the store,
//! the network, the database): the issue `failed` is notified at every attempt, and the topic is
//! optimized again on the next run.
//!
//! 1. Checks the encoded images, one after the other, without decoding them, stopping at the first
//!    one that is not right (raw images have no structure to check without decoding them):
//!    - `format_mismatch`: the image has no format, a format that is not supported, a format other
//!      than the first image's (the images of a sequence all have the same format), or its bytes
//!      are an image of another supported format;
//!    - `undecodable`: its bytes are not an image of any supported format (garbage, truncated).
//! 2. Decodes the images, once, and encodes them into an FFV1 video, in a local file
//!    (`image_sequence/video.rs`). An encoded image whose pixel data is corrupted, or a raw image
//!    whose bytes don't match its size and stride, is `undecodable`. If the images can't be a
//!    video without losing information, the issue is `unsupported`: a column of the ontology
//!    missing, images of different sizes, color types or bit depths, images of a layout that can't
//!    be decoded, raw images of an encoding a video can't hold or with row padding other than
//!    zero, or no images. The encoder failing is `failed` instead.
//! 3. Writes the video and its frame index in the topic's new folder, with its metadata:
//!
//!    ```txt
//!    tp_<id>/metadata.json             copied as it is
//!    tp_<id>/data/video.mp4            the images, in FFV1, a fragmented MP4 file
//!    tp_<id>/data/frame_index.parquet  the frame index: the row of every image but its data, and
//!                                      where its frame is in the video
//!    ```
//!
//! 4. Switches the topic to its new folder, the video as its only chunk, in a single transaction.
//!    Nothing references its previous folder anymore: the cleanup routine marks it to be deleted on
//!    its next run, and deletes it once the retention duration is over, as for a compacted topic.
//!
//! A folder written by a step 3 or 4 that failed is not referenced by the topic: the cleanup
//! routine deletes it.
//!
//! # The video
//!
//! The images are decoded with `mosaicod_image`, then encoded with `mosaicod_video` into FFV1,
//! which is lossless: every frame decodes back to exactly the pixels of its image, with its
//! channels, alpha included, and its bit depth. A raw image is read back as exactly its bytes; an
//! encoded one, as its pixels encoded again. Besides:
//!
//! - one frame per image, in capture order (`capture_timestamp_ns`, then `timestamp_ns`), none
//!   dropped or repeated, every one a keyframe in a fragment of its own: the header of the video
//!   followed by any of its fragments is a video of those frames, which is how the server reads a
//!   range of frames, or MPEG-DASH segments, without the others;
//! - each frame at its capture time, in microseconds from the first one, rounded, and moved 1 µs
//!   past the previous frame's when two would be at the same time.
//!
//! # The frame index
//!
//! `frame_index.parquet`, the frame index, is a Parquet file with one row per frame, in the order of
//! the frames: where its fragment is in the video, and the row of its image but its data, as
//! uploaded (its exact timestamps, its `frame_id` and `sample_counter`, the layout of a raw image,
//! any other column):
//!
//! ```txt
//! frame | video_timestamp_us | fragment_offset | fragment_size | timestamp_ns        | capture_timestamp_ns | ...
//! 0     | 0                  | 812             | 1730141       | 1767225600000000000 | 1767225599990000000  | ...
//! 1     | 33367              | 1730953         | 1729877       | 1767225600033366667 | 1767225600023366667  | ...
//! ```
//!
//! It also keeps the topic's schema, with its metadata, which the images are read back with, and
//! their format and size. The layout of the video and of its frame index, which the server reads
//! back, is in `mosaicod_rw::image_sequence_video`.

mod video;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use futures::StreamExt;
use mosaicod_core::{self as core, constants, types, types::OptimizationIssue};
use mosaicod_db as db;
use mosaicod_image::{ImageFormat, format_of};
use mosaicod_rw::image_sequence_video as layout;
use mosaicod_rw::{self as rw, ImageSequenceFormatProperties as Columns, SerializedChunk};
use tracing::info;

use super::{AcquiredTopic, StoreOptimizer, df_to_internal_error};

/// Images checked at the same time: decoding an image is slow, and takes the memory of its pixels
/// (an 8K RGBA image of 16 bits is ~265 MB).
const CHECK_PARALLELISM: usize = 4;

/// An issue of an image sequence, with its details.
pub(super) type Issue = (OptimizationIssue, String);

/// What became of an image sequence.
pub(super) enum Outcome {
    /// It is now a video.
    Optimized,
    /// It has no images, so nothing to optimize: it is left as it is, with no notification.
    Empty,
    /// It is not, for the given issue, with its details, to be written in a notification on the
    /// topic.
    Issue(OptimizationIssue, String),
}

/// The first image of `rows` of the batch that is not an image of `expected` (the format of the
/// first image of the sequence, set by the first image checked), with its issue.
///
/// `formats` and `data` are the format and data columns of the batch, `timestamps` its timestamps,
/// to identify the image in the details.
fn check_rows(
    expected: &mut Option<ImageFormat>,
    formats: &ArrayRef,
    data: &ArrayRef,
    timestamps: &[i64],
    rows: std::ops::Range<usize>,
) -> Option<Issue> {
    for row in rows {
        let issue = |issue, message: String| Some((issue, image_issue(timestamps[row], message)));

        let format = match format_name(formats, row) {
            None => return issue(OptimizationIssue::FormatMismatch, "no format".to_owned()),
            Some(name) => match name.parse::<ImageFormat>() {
                Ok(format) => format,
                Err(e) => return issue(OptimizationIssue::FormatMismatch, e.to_string()),
            },
        };
        match *expected {
            Some(first) if first != format => {
                return issue(
                    OptimizationIssue::FormatMismatch,
                    format!(
                        "declared `{format}`, but the images of the sequence are `{first}` images"
                    ),
                );
            }
            Some(_) => {}
            None => *expected = Some(format),
        }

        let Some(bytes) = image_bytes(data, row) else {
            return issue(OptimizationIssue::Undecodable, "no data".to_owned());
        };
        match format_of(bytes) {
            Some(found) if found == format => {}
            Some(found) => {
                return issue(
                    OptimizationIssue::FormatMismatch,
                    format!("declared `{format}`, but it is a `{found}` image"),
                );
            }
            None => {
                return issue(
                    OptimizationIssue::Undecodable,
                    format!("declared `{format}`, but it is not an image of any supported format"),
                );
            }
        }
    }
    None
}

/// The details of an issue of the image recorded at `timestamp_ns`.
pub(super) fn image_issue(timestamp_ns: i64, message: impl std::fmt::Display) -> String {
    format!("image recorded at {timestamp_ns} ns: {message}")
}

/// The first image of `batch` that is not right, with its issue: see [`check_rows`]. The images
/// are checked [`CHECK_PARALLELISM`] at a time.
///
/// Blocking: run on a blocking thread.
fn check_batch(
    expected: &mut Option<ImageFormat>,
    batch: &RecordBatch,
) -> Result<Option<Issue>, core::Error> {
    let internal = |message: String| core::Error::internal(Some(message));
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .ok_or_else(|| internal(format!("no `{name}` column")))
    };
    let formats = column(Columns::FORMAT_COLUMN)?;
    // DataFusion reads the strings of Parquet as any string type.
    if !matches!(
        formats.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Err(internal(format!(
            "the image formats are {}, not strings",
            formats.data_type()
        )));
    }
    let data = column(Columns::DATA_COLUMN)?;
    let timestamps = column(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP)?
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| internal("the timestamps are not 64-bit integers".to_owned()))?
        .values();

    // The first image sets the format of the sequence: checked alone.
    let rows = batch.num_rows();
    let first = usize::from(expected.is_none()).min(rows);
    if let Some(issue) = check_rows(expected, formats, data, timestamps, 0..first) {
        return Ok(Some(issue));
    }

    // The others in parallel, each thread on a contiguous range: the issue of the earliest range
    // with one is the first image that is not right.
    let ranges = split(first..rows, CHECK_PARALLELISM);
    let issues: Vec<Option<Issue>> = std::thread::scope(|scope| {
        let threads: Vec<_> = ranges
            .into_iter()
            .map(|range| {
                let mut expected = *expected;
                scope.spawn(move || check_rows(&mut expected, formats, data, timestamps, range))
            })
            .collect();
        threads
            .into_iter()
            .map(|thread| thread.join().expect("checking images panicked"))
            .collect()
    });
    Ok(issues.into_iter().flatten().next())
}

/// `rows` split into at most `parts` contiguous ranges of about the same length.
fn split(rows: std::ops::Range<usize>, parts: usize) -> Vec<std::ops::Range<usize>> {
    let size = rows.len().div_ceil(parts.max(1)).max(1);
    (rows.start..rows.end)
        .step_by(size)
        .map(|start| start..(start + size).min(rows.end))
        .collect()
}

/// The format name of `row` of the string column `formats`, `None` if null.
pub(super) fn format_name(formats: &ArrayRef, row: usize) -> Option<&str> {
    if formats.is_null(row) {
        return None;
    }
    match formats.data_type() {
        DataType::Utf8 => Some(formats.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Some(formats.as_string::<i64>().value(row)),
        DataType::Utf8View => Some(formats.as_string_view().value(row)),
        _ => None,
    }
}

/// The image of `row` of the binary column `data`.
pub(super) fn image_bytes(data: &ArrayRef, row: usize) -> Option<&[u8]> {
    if data.is_null(row) {
        return None;
    }
    match data.data_type() {
        DataType::Binary => Some(data.as_binary::<i32>().value(row)),
        DataType::LargeBinary => Some(data.as_binary::<i64>().value(row)),
        DataType::BinaryView => Some(data.as_binary_view().value(row)),
        _ => None,
    }
}

impl StoreOptimizer {
    /// Turns the image sequence of `acquired_topic` into an FFV1 video, in the four steps of the
    /// module documentation; or returns its issue.
    pub(super) async fn optimize_image_sequence(&self, acquired_topic: &AcquiredTopic) -> Outcome {
        let locator = acquired_topic.topic_record.locator();
        let failed = |step: &str, error: &dyn std::fmt::Display| {
            Outcome::Issue(OptimizationIssue::Failed, format!("{step}: {error}"))
        };
        let raw = acquired_topic.topic_record.serialization_format()
            == Some(types::Format::ImageSequenceRaw);

        // 0. There are images: a topic finalized without any (e.g. every image refused by the
        // client) has no data files, which can't be read, and nothing to optimize
        let topic_id = acquired_topic.topic_record.topic_id;
        match db::topic_get_stats(&mut self.db.connection(), topic_id).await {
            Ok(stats) if stats.total_row_count == 0 => return Outcome::Empty,
            Ok(_) => {}
            Err(e) => return failed("reading the statistics of the topic", &e),
        }

        // 1. The encoded images are right
        if !raw {
            match self.check_image_sequence(acquired_topic).await {
                Ok(None) => {}
                Ok(Some((issue, message))) => return Outcome::Issue(issue, message),
                Err(e) => return failed("checking the images", &e),
            }
        }

        // 2. They are a video
        let local = match self.encode_video(acquired_topic, raw).await {
            Ok(local) => local,
            Err(video::Failure::Undecodable(reason)) => {
                return Outcome::Issue(OptimizationIssue::Undecodable, reason);
            }
            Err(video::Failure::Unsupported(reason)) => {
                return Outcome::Issue(OptimizationIssue::Unsupported, reason);
            }
            Err(video::Failure::Failed(reason)) => return failed("encoding the video", &reason),
        };

        // 3. The video is in the topic's new folder
        let frames = local.frames;
        let chunk = match self.write_video(acquired_topic, local).await {
            Ok(chunk) => chunk,
            Err(e) => return failed("writing the video", &e),
        };

        // 4. The topic is the video: nothing references its previous folder anymore, which the
        // cleanup routine deletes
        if let Err(e) = self
            .complete_optimization(acquired_topic, vec![chunk])
            .await
        {
            return failed("switching the topic to the video", &e);
        }
        info!("topic {locator}: {frames} images turned into an FFV1 video");

        Outcome::Optimized
    }

    /// Writes `local`, the video of `acquired_topic`, and the timestamps of its frames in the
    /// topic's new folder, with its metadata: returns the video's chunk.
    async fn write_video(
        &self,
        acquired_topic: &AcquiredTopic,
        local: video::LocalVideo,
    ) -> core::error::PublicResult<SerializedChunk> {
        let data_folder = acquired_topic.opt_path_in_store.data_folder_path();
        let video_path = data_folder.join(layout::VIDEO_FILE);
        let video_bytes = self
            .store
            .write_from_file(&video_path, &local.video)
            .await?;
        let frame_index_bytes = local.frame_index_parquet.len();
        self.store
            .write_bytes(
                data_folder.join(layout::FRAME_INDEX_FILE),
                local.frame_index_parquet,
            )
            .await?;
        self.copy_metadata(acquired_topic).await?;

        // The video is the topic's only chunk.
        Ok(SerializedChunk {
            path: video_path,
            ontology_stats: local.stats,
            metadata: rw::ChunkMetadata {
                size_bytes: (video_bytes as usize) + frame_index_bytes,
                arrow_size_bytes: local.arrow_size_bytes,
                row_count: local.frames,
            },
        })
    }

    /// Checks the images of the image sequence of `acquired_topic`: returns the issue of the first
    /// one that is not right, if any.
    async fn check_image_sequence(
        &self,
        acquired_topic: &AcquiredTopic,
    ) -> core::error::PublicResult<Option<Issue>> {
        let batch_size = self.batch_size(&acquired_topic.topic_record).await?;
        let mut batches = self
            .read_topic(acquired_topic, batch_size)
            .await?
            .execute_stream()
            .await
            .map_err(df_to_internal_error)?;

        let mut expected: Option<ImageFormat> = None;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(df_to_internal_error)?;
            let (issue, format) = tokio::task::spawn_blocking(move || {
                let mut format = expected;
                check_batch(&mut format, &batch).map(|issue| (issue, format))
            })
            .await
            .map_err(|e| core::Error::internal(Some(format!("checking the images: {e}"))))??;
            if issue.is_some() {
                return Ok(issue);
            }
            expected = format;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{BinaryArray, Int64Array, StringViewArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use mosaicod_image::{ColorType, testing};

    use super::*;

    /// A batch of images of `formats` and `data`, recorded at 0, 10, 20... ns.
    fn batch(formats: Vec<Option<&str>>, data: Vec<Vec<u8>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
                DataType::Int64,
                false,
            ),
            Field::new(Columns::FORMAT_COLUMN, DataType::Utf8View, true),
            Field::new(Columns::DATA_COLUMN, DataType::Binary, false),
        ]));
        let timestamps = Int64Array::from_iter_values((0..formats.len() as i64).map(|i| i * 10));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamps),
                Arc::new(StringViewArray::from(formats)),
                Arc::new(BinaryArray::from_iter_values(data)),
            ],
        )
        .unwrap()
    }

    fn png() -> Vec<u8> {
        mosaicod_image::encode(
            ImageFormat::Png,
            &testing::gradient(8, 6, ColorType::Rgb, 8),
        )
        .unwrap()
    }

    /// The start of a TIFF: an image, but not of a supported format.
    fn tiff() -> Vec<u8> {
        b"II*\0\x08\0\0\0".to_vec()
    }

    /// Images, by format and data, and the issue they have, with when its image was recorded.
    type Case<'a> = (
        Vec<Option<&'a str>>,
        Vec<Vec<u8>>,
        Option<(OptimizationIssue, i64)>,
    );

    #[test]
    fn the_first_wrong_image_is_the_issue() {
        use OptimizationIssue::{FormatMismatch, Undecodable};

        let png = png();
        let mut corrupted = png.clone();
        let idat = corrupted.windows(4).position(|w| w == b"IDAT").unwrap();
        corrupted[idat + 8] ^= 0xff;
        let mut twenty = vec![png.clone(); 20];
        twenty[7] = b"garbage".to_vec();
        twenty[15] = tiff();

        let cases: Vec<Case> = vec![
            (vec![], vec![], None),
            (vec![Some("png"); 10], vec![png.clone(); 10], None),
            // The first wrong one, at 70 ns.
            (vec![Some("png"); 20], twenty, Some((Undecodable, 70))),
            // No format, a format that is not supported, mixed formats.
            (
                vec![Some("png"), None],
                vec![png.clone(); 2],
                Some((FormatMismatch, 10)),
            ),
            (
                vec![Some("jpeg")],
                vec![png.clone()],
                Some((FormatMismatch, 0)),
            ),
            (
                vec![Some("PNG")],
                vec![png.clone()],
                Some((FormatMismatch, 0)),
            ),
            (
                vec![Some("png"), Some("tiff")],
                vec![png.clone(), tiff()],
                Some((FormatMismatch, 10)),
            ),
            // Not a PNG: garbage, another format, truncated.
            (
                vec![Some("png")],
                vec![b"garbage".to_vec()],
                Some((Undecodable, 0)),
            ),
            (vec![Some("png")], vec![tiff()], Some((Undecodable, 0))),
            (
                vec![Some("png")],
                vec![png[..png.len() - 20].to_vec()],
                Some((Undecodable, 0)),
            ),
            // Its pixel data corrupted: its structure is right, found decoding it in step 2.
            (vec![Some("png")], vec![corrupted], None),
        ];
        for (formats, data, expected) in cases {
            let found = check_batch(&mut None, &batch(formats.clone(), data)).unwrap();
            match (found, expected) {
                (None, None) => {}
                (Some((issue, message)), Some((expected, at))) => {
                    assert_eq!(issue, expected, "{formats:?}: {message}");
                    let at = format!("image recorded at {at} ns");
                    assert!(message.starts_with(&at), "{formats:?}: {message}");
                }
                (found, expected) => panic!("{formats:?}: {found:?}, not {expected:?}"),
            }
        }
    }

    #[test]
    fn the_format_is_kept_across_batches() {
        let mut expected = None;
        assert!(
            check_batch(&mut expected, &batch(vec![Some("png")], vec![png()]))
                .unwrap()
                .is_none()
        );
        assert_eq!(expected, Some(ImageFormat::Png));
        let (issue, _) = check_batch(&mut expected, &batch(vec![Some("tiff")], vec![tiff()]))
            .unwrap()
            .unwrap();
        assert_eq!(issue, OptimizationIssue::FormatMismatch);
    }

    #[test]
    fn rows_are_split() {
        assert_eq!(split(0..10, 4), vec![0..3, 3..6, 6..9, 9..10]);
        assert_eq!(split(1..3, 4), vec![1..2, 2..3]);
        assert!(split(5..5, 4).is_empty());
    }
}

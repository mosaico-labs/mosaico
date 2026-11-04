//! How an image sequence turned into a video by the store optimizer is laid out in the store:
//! what the optimizer writes and the server reads back. Nothing here needs FFmpeg.
//!
//! ```txt
//! tp_<id>/data/video.mp4            the images, in FFV1, a fragmented MP4 file
//! tp_<id>/data/frame_index.parquet  the frame index
//! ```
//!
//! The video is a header (`ftyp` and `moov`), then a fragment (`moof` and `mdat`) per frame, in
//! order, each at its time in microseconds from the first capture time: the header followed by
//! any of the fragments is a video of those frames, as MPEG-DASH segments are.
//!
//! The frame index ([`FrameIndex`]) has a row per frame, in the order of the frames:
//!
//! ```txt
//! frame (UInt64)                 its number, from 0
//! video_timestamp_us (Int64)     the frame's time in the video (see `video_timestamps_us`)
//! fragment_offset (UInt64)       where its fragment starts in the video
//! fragment_size (UInt64)         the size of its fragment
//! ...                            every column of the image's row but `data`, as uploaded:
//!                                `timestamp_ns`, `capture_timestamp_ns`, `frame_id`...
//! ```
//!
//! and, in its key-value metadata, the Arrow schema the images were uploaded with (with its
//! metadata, so that they are read back with it), their format and size, and the size of the
//! video's header. An image is read back as its row, with its frame decoded as its `data`.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, UInt64Type};
use arrow::error::ArrowError;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterVersion};

/// The video, in the topic's data folder.
pub const VIDEO_FILE: &str = "video.mp4";
/// The frame index, in the topic's data folder.
pub const FRAME_INDEX_FILE: &str = "frame_index.parquet";

/// Ticks per second of the video's timestamps: microseconds.
pub const VIDEO_TIMESCALE: i64 = 1_000_000;

/// Columns of the frame index of its own; the others are those of the images.
pub const FRAME_COLUMN: &str = "frame";
pub const VIDEO_TIMESTAMP_COLUMN: &str = "video_timestamp_us";
pub const FRAGMENT_OFFSET_COLUMN: &str = "fragment_offset";
pub const FRAGMENT_SIZE_COLUMN: &str = "fragment_size";
/// The columns of the frame index of its own: an image sequence can't have a column of these names.
pub const OWN_COLUMNS: [&str; 4] = [
    FRAME_COLUMN,
    VIDEO_TIMESTAMP_COLUMN,
    FRAGMENT_OFFSET_COLUMN,
    FRAGMENT_SIZE_COLUMN,
];
/// Columns of the images that every frame index has.
pub const TIMESTAMP_COLUMN: &str = "timestamp_ns";
pub const CAPTURE_TIMESTAMP_COLUMN: &str = "capture_timestamp_ns";

/// The format of [`SourceImages`] of raw images (the `ImageSequenceRaw` serialization format):
/// each frame is read back as the bytes its row describes.
pub const RAW_FORMAT: &str = "raw";

/// Keys of the frame index's metadata.
const SOURCE_SCHEMA_KEY: &str = "mosaico:source_schema";
const SOURCE_FORMAT_KEY: &str = "mosaico:source_format";
const WIDTH_KEY: &str = "mosaico:width";
const HEIGHT_KEY: &str = "mosaico:height";
const HEADER_SIZE_KEY: &str = "mosaico:header_size";

#[derive(Debug, thiserror::Error)]
pub enum FrameIndexError {
    #[error("frame index: {0}")]
    Parquet(#[from] ParquetError),
    #[error("frame index: {0}")]
    Arrow(#[from] ArrowError),
    #[error("frame index: {0}")]
    Invalid(String),
}

fn invalid(message: impl Into<String>) -> FrameIndexError {
    FrameIndexError::Invalid(message.into())
}

/// What the images of a video were.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceImages {
    /// Their format, as in their `format` column (e.g. `png`), or [`RAW_FORMAT`]: what the frames
    /// are encoded back into.
    pub format: String,
    pub width: u32,
    pub height: u32,
}

/// The frames of a video, frame `k` at index `k` of each list and row `k` of [`Self::rows`]: built
/// with [`Self::new`].
#[derive(Debug, Clone, PartialEq)]
pub struct FrameIndex {
    /// The index timestamp of the image of each frame (`timestamp_ns`): the column of
    /// [`Self::rows`], not a copy.
    pub timestamps_ns: ScalarBuffer<i64>,
    /// The capture time of the image of each frame (`capture_timestamp_ns`): the column of
    /// [`Self::rows`], not a copy.
    pub capture_timestamps_ns: ScalarBuffer<i64>,
    /// The time of each frame in the video, from [`video_timestamps_us`].
    pub video_timestamps_us: ScalarBuffer<i64>,
    /// Where the fragment of each frame is in the video.
    pub fragments: Vec<Range<u64>>,
    /// The size of the video's header, before the first fragment.
    pub header_size: u64,
    /// What the images were.
    pub source: SourceImages,
    /// The Arrow schema the images were uploaded with, with its metadata.
    pub source_schema: SchemaRef,
    /// The row of the image of each frame, but its `data`, as uploaded.
    pub rows: RecordBatch,
}

/// The values of the column `name` of `batch`, of type `T`, none null: its buffer, not a copy.
fn primitive_column<T: arrow::datatypes::ArrowPrimitiveType>(
    batch: &RecordBatch,
    name: &str,
) -> Result<ScalarBuffer<T::Native>, FrameIndexError> {
    let column = batch
        .column_by_name(name)
        .and_then(|c| c.as_primitive_opt::<T>())
        .ok_or_else(|| invalid(format!("no `{name}` column")))?;
    if column.null_count() > 0 {
        return Err(invalid(format!("nulls in `{name}`")));
    }
    Ok(column.values().clone())
}

/// How a raw image is laid out (the `ImageSequenceRaw` serialization format), from its row: see
/// [`RawFrames::get`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawFrame<'a> {
    pub width: u32,
    pub height: u32,
    /// Bytes per row, its padding included.
    pub stride: u32,
    /// The pixel encoding, as in ROS (`mono8`, `bgr8`...).
    pub encoding: &'a str,
    /// Whether the samples of 16 bits are big-endian: `false` if null.
    pub big_endian: bool,
}

/// The layout of the raw image of each row of a batch, from its `width`, `height`, `stride`,
/// `encoding` and `is_bigendian` columns (see `ImageSequenceRawFormatProperties`): checked once
/// for every row by [`Self::new`], then read row by row with [`Self::get`].
#[derive(Debug, Clone)]
pub struct RawFrames {
    widths: Vec<u32>,
    heights: Vec<u32>,
    strides: Vec<u32>,
    encodings: StringArray,
    big_endian: Option<BooleanArray>,
}

impl RawFrames {
    /// The layouts of the raw images of `rows`, whose columns can be of any integer and string
    /// types. Fails if one is missing, or null but `is_bigendian`, or a size is not a `u32`.
    pub fn new(rows: &RecordBatch) -> Result<Self, FrameIndexError> {
        use crate::ImageSequenceRawFormatProperties as Columns;

        let cast = |name: &str, to: &DataType| -> Result<ArrayRef, FrameIndexError> {
            let column = rows
                .column_by_name(name)
                .ok_or_else(|| invalid(format!("no `{name}` column")))?;
            if column.null_count() > 0 && name != Columns::IS_BIGENDIAN_COLUMN {
                return Err(invalid(format!("images with no `{name}`")));
            }
            Ok(arrow::compute::cast(column, to)?)
        };
        let size = |name: &str| -> Result<Vec<u32>, FrameIndexError> {
            cast(name, &DataType::Int64)?
                .as_primitive::<Int64Type>()
                .values()
                .iter()
                .map(|&v| u32::try_from(v).map_err(|_| invalid(format!("a `{name}` of {v}"))))
                .collect()
        };
        Ok(Self {
            widths: size(Columns::WIDTH_COLUMN)?,
            heights: size(Columns::HEIGHT_COLUMN)?,
            strides: size(Columns::STRIDE_COLUMN)?,
            encodings: cast(Columns::ENCODING_COLUMN, &DataType::Utf8)?
                .as_string::<i32>()
                .clone(),
            big_endian: match rows.column_by_name(Columns::IS_BIGENDIAN_COLUMN) {
                Some(_) => Some(
                    cast(Columns::IS_BIGENDIAN_COLUMN, &DataType::Boolean)?
                        .as_boolean()
                        .clone(),
                ),
                None => None,
            },
        })
    }

    /// The layout of the raw image of `row`.
    ///
    /// # Panics
    ///
    /// If `row` is not a row of the batch.
    pub fn get(&self, row: usize) -> RawFrame<'_> {
        RawFrame {
            width: self.widths[row],
            height: self.heights[row],
            stride: self.strides[row],
            encoding: self.encodings.value(row),
            big_endian: self
                .big_endian
                .as_ref()
                .is_some_and(|b| b.is_valid(row) && b.value(row)),
        }
    }
}

/// The frames' times in the video, in microseconds from the first capture time. Each is its
/// capture time minus the first one, rounded to the microsecond (halves up), and moved 1 µs past
/// the previous frame's when the rounding (or two images captured at the same time) would put it
/// on or before it: the exact times are in the frame index.
pub fn video_timestamps_us(capture_timestamps_ns: &[i64]) -> Vec<i64> {
    let Some(&first) = capture_timestamps_ns.first() else {
        return Vec::new();
    };
    let mut previous: Option<i64> = None;
    capture_timestamps_ns
        .iter()
        .map(|&capture| {
            let us = ((i128::from(capture) - i128::from(first) + 500) / 1_000) as i64;
            let us = previous.map_or(us, |p| us.max(p + 1));
            previous = Some(us);
            us
        })
        .collect()
}

impl FrameIndex {
    /// The frame index of the images of `rows`, a row per frame, in frame order: their columns but
    /// `data`, among which `timestamp_ns` and `capture_timestamp_ns`, and none of [`OWN_COLUMNS`].
    pub fn new(
        rows: RecordBatch,
        video_timestamps_us: impl Into<ScalarBuffer<i64>>,
        fragments: Vec<Range<u64>>,
        header_size: u64,
        source: SourceImages,
        source_schema: SchemaRef,
    ) -> Result<Self, FrameIndexError> {
        if let Some(own) = OWN_COLUMNS
            .iter()
            .find(|name| rows.column_by_name(name).is_some())
        {
            return Err(invalid(format!("a column of the images named `{own}`")));
        }
        let index = Self {
            timestamps_ns: primitive_column::<Int64Type>(&rows, TIMESTAMP_COLUMN)?,
            capture_timestamps_ns: primitive_column::<Int64Type>(&rows, CAPTURE_TIMESTAMP_COLUMN)?,
            video_timestamps_us: video_timestamps_us.into(),
            fragments,
            header_size,
            source,
            source_schema,
            rows,
        };
        index.check()?;
        Ok(index)
    }

    /// The number of frames.
    pub fn len(&self) -> usize {
        self.timestamps_ns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps_ns.is_empty()
    }

    /// Where the video's header is in the video.
    pub fn header(&self) -> Range<u64> {
        0..self.header_size
    }

    fn check(&self) -> Result<(), FrameIndexError> {
        let len = self.len();
        if self.capture_timestamps_ns.len() != len
            || self.video_timestamps_us.len() != len
            || self.fragments.len() != len
            || self.rows.num_rows() != len
        {
            return Err(invalid("columns of different lengths"));
        }
        Ok(())
    }

    /// The frame index as a Parquet file ([`FRAME_INDEX_FILE`]).
    pub fn into_parquet(self) -> Result<Vec<u8>, FrameIndexError> {
        self.check()?;
        let frames = self.len() as u64;
        let metadata = [
            (SOURCE_SCHEMA_KEY, encode_schema(&self.source_schema)?),
            (SOURCE_FORMAT_KEY, self.source.format),
            (WIDTH_KEY, self.source.width.to_string()),
            (HEIGHT_KEY, self.source.height.to_string()),
            (HEADER_SIZE_KEY, self.header_size.to_string()),
        ];
        let mut fields = vec![
            Arc::new(Field::new(FRAME_COLUMN, DataType::UInt64, false)),
            Arc::new(Field::new(VIDEO_TIMESTAMP_COLUMN, DataType::Int64, false)),
            Arc::new(Field::new(FRAGMENT_OFFSET_COLUMN, DataType::UInt64, false)),
            Arc::new(Field::new(FRAGMENT_SIZE_COLUMN, DataType::UInt64, false)),
        ];
        fields.extend(self.rows.schema().fields().iter().cloned());
        let schema = Arc::new(Schema::new(fields));
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from_iter_values(0..frames)),
            Arc::new(Int64Array::new(self.video_timestamps_us, None)),
            Arc::new(UInt64Array::from_iter_values(
                self.fragments.iter().map(|f| f.start),
            )),
            Arc::new(UInt64Array::from_iter_values(
                self.fragments.iter().map(|f| f.end - f.start),
            )),
        ];
        columns.extend(self.rows.columns().iter().cloned());
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_key_value_metadata(Some(
                metadata
                    .into_iter()
                    .map(|(k, v)| KeyValue::new(k.to_owned(), v))
                    .collect(),
            ))
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props))?;
        writer.write(&batch)?;
        Ok(writer.into_inner()?)
    }

    /// Reads a frame index written by [`Self::into_parquet`].
    pub fn from_parquet(bytes: impl Into<bytes::Bytes>) -> Result<Self, FrameIndexError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.into())?;
        // Every row in a single batch: the batches are not concatenated, so not copied.
        let rows = usize::try_from(builder.metadata().file_metadata().num_rows()).unwrap_or(0);
        let builder = builder.with_batch_size(rows.max(1));
        let metadata: HashMap<&str, &str> = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .into_iter()
            .flatten()
            .filter_map(|kv| Some((kv.key.as_str(), kv.value.as_deref()?)))
            .collect();
        let value = |key: &str| -> Result<&str, FrameIndexError> {
            metadata
                .get(key)
                .copied()
                .ok_or_else(|| invalid(format!("no `{key}`")))
        };
        fn number<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, FrameIndexError> {
            value
                .parse()
                .map_err(|_| invalid(format!("`{key}` is `{value}`")))
        }
        let source = SourceImages {
            format: value(SOURCE_FORMAT_KEY)?.to_owned(),
            width: number(WIDTH_KEY, value(WIDTH_KEY)?)?,
            height: number(HEIGHT_KEY, value(HEIGHT_KEY)?)?,
        };
        let header_size = number(HEADER_SIZE_KEY, value(HEADER_SIZE_KEY)?)?;
        let source_schema = decode_schema(value(SOURCE_SCHEMA_KEY)?)?;

        // The file's metadata is the frame index's, not that of the images' rows.
        let schema = Arc::new(
            builder
                .schema()
                .as_ref()
                .clone()
                .with_metadata(HashMap::new()),
        );
        let batches = builder
            .build()?
            .map(|batch| batch?.with_schema(schema.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        // A single batch is not copied.
        let all = arrow::compute::concat_batches(&schema, &batches)?;
        let video_timestamps_us = primitive_column::<Int64Type>(&all, VIDEO_TIMESTAMP_COLUMN)?;
        let offsets = primitive_column::<UInt64Type>(&all, FRAGMENT_OFFSET_COLUMN)?;
        let sizes = primitive_column::<UInt64Type>(&all, FRAGMENT_SIZE_COLUMN)?;
        let fragments = offsets
            .iter()
            .zip(sizes.iter())
            .map(|(&offset, &size)| offset..offset + size)
            .collect();
        let rows = all.project(
            &(0..schema.fields().len())
                .filter(|&i| !OWN_COLUMNS.contains(&schema.field(i).name().as_str()))
                .collect::<Vec<_>>(),
        )?;
        Self::new(
            rows,
            video_timestamps_us,
            fragments,
            header_size,
            source,
            source_schema,
        )
    }
}

/// `schema` as an Arrow IPC schema message, hex-encoded: Parquet metadata values are strings.
fn encode_schema(schema: &Schema) -> Result<String, FrameIndexError> {
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), schema)?;
    writer.finish()?;
    let bytes = writer.into_inner()?;
    let mut hex = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        hex.push(char::from_digit(u32::from(byte >> 4), 16).expect("a hex digit"));
        hex.push(char::from_digit(u32::from(byte & 0xf), 16).expect("a hex digit"));
    }
    Ok(hex)
}

fn decode_schema(hex: &str) -> Result<SchemaRef, FrameIndexError> {
    if !hex.len().is_multiple_of(2) {
        return Err(invalid("source schema of an odd number of hex digits"));
    }
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .map_err(|e| invalid(format!("source schema: {e}")))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_timestamps_are_rounded_and_increasing() {
        let start = 1_767_225_600_000_000_000;
        let captures = [
            start,
            start + 33_366_667,
            start + 66_733_333,
            start + 66_733_333, // captured with the previous one
            start + 66_733_700, // rounds onto the previous one
            start + 100_000_499,
            start + 100_000_500,
            // Two hours later.
            start + 7_200_000_000_000,
        ];
        assert_eq!(
            video_timestamps_us(&captures),
            [
                0,
                33_367,
                66_733,
                66_734,
                66_735,
                100_000,
                100_001,
                7_200_000_000
            ]
        );
        assert!(video_timestamps_us(&[]).is_empty());
    }

    #[test]
    fn the_frame_index_is_read_back() {
        use arrow::array::{BooleanArray, StringViewArray, StructArray};

        let source_schema = Arc::new(
            Schema::new(vec![
                Field::new("timestamp_ns", DataType::Int64, false),
                Field::new("data", DataType::LargeBinary, true)
                    .with_metadata(HashMap::from([("unit".to_owned(), "pixels".to_owned())])),
            ])
            .with_metadata(HashMap::from([("client".to_owned(), "camera".to_owned())])),
        );
        // The rows of the images, as uploaded: any column is kept, a struct of nullable fields too.
        let frame_id: ArrayRef = Arc::new(StringViewArray::from(vec![Some("camera"), None]));
        let header = StructArray::new(
            vec![Field::new("frame_id", DataType::Utf8View, true)].into(),
            vec![frame_id],
            None,
        );
        let rows = RecordBatch::try_from_iter([
            (
                "timestamp_ns",
                Arc::new(Int64Array::from(vec![110, 120])) as ArrayRef,
            ),
            (
                "capture_timestamp_ns",
                Arc::new(Int64Array::from(vec![100, 2_000_100])),
            ),
            (
                "is_bigendian",
                Arc::new(BooleanArray::from(vec![None, Some(true)])),
            ),
            ("header", Arc::new(header)),
        ])
        .unwrap();
        let source = SourceImages {
            format: RAW_FORMAT.to_owned(),
            width: 640,
            height: 480,
        };
        let new = |rows: RecordBatch, fragments: Vec<Range<u64>>| {
            FrameIndex::new(
                rows,
                vec![0, 2_000],
                fragments,
                700,
                source.clone(),
                source_schema.clone(),
            )
        };
        let index = new(rows.clone(), vec![700..1_000, 1_000..1_450]).unwrap();
        assert_eq!(index.timestamps_ns[..], [110, 120]);
        assert_eq!(index.capture_timestamps_ns[..], [100, 2_000_100]);
        let parquet = index.clone().into_parquet().unwrap();
        assert_eq!(FrameIndex::from_parquet(parquet).unwrap(), index);

        // Columns of different lengths, an image column of the frame index's own.
        let one_fragment = std::iter::once(700..1_000).collect();
        assert!(new(rows.clone(), one_fragment).is_err());
        let mut own = rows.columns().to_vec();
        own.push(Arc::new(Int64Array::from(vec![1, 2])));
        let mut fields = rows.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(FRAME_COLUMN, DataType::Int64, false)));
        let own = RecordBatch::try_new(Arc::new(Schema::new(fields)), own).unwrap();
        assert!(new(own, vec![700..1_000, 1_000..1_450]).is_err());
    }

    #[test]
    fn raw_frames_are_read_from_their_columns() {
        use arrow::array::{BooleanArray, Int32Array, StringViewArray, UInt32Array};

        let rows = |is_bigendian: Option<ArrayRef>, stride: ArrayRef| {
            let mut columns = vec![
                ("width", Arc::new(Int32Array::from(vec![4, 4])) as ArrayRef),
                ("height", Arc::new(UInt32Array::from(vec![2, 2]))),
                ("stride", stride),
                (
                    "encoding",
                    Arc::new(StringViewArray::from(vec!["mono16", "bgr8"])),
                ),
            ];
            columns.extend(is_bigendian.map(|b| ("is_bigendian", b)));
            RecordBatch::try_from_iter(columns).unwrap()
        };
        let strides: ArrayRef = Arc::new(Int64Array::from(vec![8, 12]));
        let frames = RawFrames::new(&rows(
            Some(Arc::new(BooleanArray::from(vec![Some(true), None]))),
            strides.clone(),
        ))
        .unwrap();
        assert_eq!(
            [frames.get(0), frames.get(1)],
            [
                RawFrame {
                    width: 4,
                    height: 2,
                    stride: 8,
                    encoding: "mono16",
                    big_endian: true,
                },
                RawFrame {
                    width: 4,
                    height: 2,
                    stride: 12,
                    encoding: "bgr8",
                    big_endian: false,
                },
            ]
        );
        // With no `is_bigendian`: little-endian.
        let frames = RawFrames::new(&rows(None, strides)).unwrap();
        assert!(!frames.get(0).big_endian && !frames.get(1).big_endian);
        // A null or negative size.
        let null = Arc::new(Int64Array::from(vec![Some(8), None]));
        assert!(RawFrames::new(&rows(None, null)).is_err());
        let negative = Arc::new(Int64Array::from(vec![8, -1]));
        assert!(RawFrames::new(&rows(None, negative)).is_err());
    }
}

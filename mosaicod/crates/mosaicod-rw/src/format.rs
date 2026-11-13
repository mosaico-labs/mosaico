//!
//! Format properties definitions
//!
//! This module implements the Strategy pattern to encapsulate format-specific
//! configurations. Each format variant has its own strategy that defines compression settings,
//! file extensions, and reading options.

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use mosaicod_config::params as config_params;
use mosaicod_core::{constants, traits::AsExtension, types};
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
    schema::types::ColumnPath,
};
use std::sync::Arc;

// ////////////////////////////////////////////////////////////////////////////
// Format Properties Traits
// ////////////////////////////////////////////////////////////////////////////

/// Base strategy trait for all storage formats.
///
/// This trait defines the minimal interface that all formats must satisfy,
/// regardless of their underlying storage mechanism (Parquet, point clouds, etc.).
///
/// This follows the Strategy pattern to adhere to the Open/Closed Principle:
/// - Open for extension: New formats can be added by implementing this trait
/// - Closed for modification: Existing code doesn't need to change when adding formats.
pub trait FormatProperties: AsExtension + Send + Sync {
    /// Returns a human-readable name for this format strategy.
    fn name(&self) -> &'static str;
}

/// Strategy trait for Parquet-based storage formats.
///
/// Extends `FormatStrategy` with Parquet-specific configuration for compression,
/// statistics, and DataFusion integration. Formats that store data as Parquet
/// files should implement this trait.
pub trait ParquetFormatProperties: FormatProperties {
    /// Returns the Parquet writer properties configured for this format.
    fn writer_properties(&self) -> WriterProperties;

    /// Returns DataFusion ListingOptions configured for reading files in this format.
    fn listing_options(&self) -> ListingOptions {
        // Keep schema/field-level metadata (e.g. client-supplied metadata written at
        // `do_put` time) instead of having DataFusion strip it during schema inference.
        let parquet_format = ParquetFormat::default().with_skip_metadata(false);

        ListingOptions::new(Arc::new(parquet_format))
            .with_collect_stat(true)
            .with_file_extension(format!(".{}", self.as_extension()))
            .with_file_sort_order(vec![vec![
                // null_first set to FALSE to match datafusion ASC ordering while retrieving data.
                col(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP).sort(true, false),
            ]])
    }

    /// Preallocates memory for the internal writer buffer to avoid dynamic resizing.
    ///
    /// Set this close to your target chunk size, accounting for potential
    /// serialization overshoot. Different compression formats and schemas
    /// will affect the final peak memory footprint.
    fn buffer_capacity(&self) -> usize {
        // (cabba) NOTE: not tuned yet :)
        ((config_params::params()
            .parquet_in_memory_encoding_buffer_size
            .value as f64)
            * 1.1) as usize
    }
}

// ////////////////////////////////////////////////////////////////////////////
// Formats Implementation
// ////////////////////////////////////////////////////////////////////////////

/// Strategy for standard columnar data with fixed-width columns.
/// Uses Parquet 2.0 with default compression settings.
pub struct DefaultFormatProperties;

impl AsExtension for DefaultFormatProperties {
    fn as_extension(&self) -> String {
        constants::ext::PARQUET.to_owned()
    }
}

impl FormatProperties for DefaultFormatProperties {
    fn name(&self) -> &'static str {
        "default"
    }
}

impl ParquetFormatProperties for DefaultFormatProperties {
    fn writer_properties(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build()
    }
}

/// Properties for ragged/variable-length data (nested or list-like structures).
///
/// Uses ZSTD level 5 compression with optimized timestamp column handling:
/// - Timestamp column is uncompressed for fast range queries
/// - Bloom filters enabled on timestamp for efficient filtering
/// - Page-level statistics on timestamp for predicate pushdown.
pub struct RaggedFormatProperties;

impl RaggedFormatProperties {
    /// ZSTD compression level 5 provides good balance between compression ratio
    /// and speed for variable-length data structures.
    const COMPRESSION_LEVEL: i32 = 5;
}

impl AsExtension for RaggedFormatProperties {
    fn as_extension(&self) -> String {
        constants::ext::PARQUET.to_owned()
    }
}

impl FormatProperties for RaggedFormatProperties {
    fn name(&self) -> &'static str {
        "ragged"
    }
}

impl ParquetFormatProperties for RaggedFormatProperties {
    fn writer_properties(&self) -> WriterProperties {
        let ts_path = ColumnPath::from(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP);

        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(Self::COMPRESSION_LEVEL).expect("valid ZSTD compression level"),
            ))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            // Timestamp column: uncompressed for fast seeking
            .set_column_compression(ts_path.clone(), Compression::UNCOMPRESSED)
            .set_column_statistics_enabled(ts_path.clone(), EnabledStatistics::Page)
            .set_column_bloom_filter_enabled(ts_path, true)
            .build()
    }
}

/// Format properties for images and dense multi-dimensional arrays.
///
/// Uses ZSTD level 5 compression with optimized timestamp column handling:
/// - Timestamp column is uncompressed for fast range queries
/// - Bloom filters enabled on timestamp for efficient filtering
/// - Page-level statistics on timestamp for predicate pushdown.
pub struct ImageFormatProperties;

impl ImageFormatProperties {
    /// ZSTD compression level 5 provides good balance between compression ratio
    /// and speed for image data.
    const COMPRESSION_LEVEL: i32 = 5;
}

impl AsExtension for ImageFormatProperties {
    fn as_extension(&self) -> String {
        constants::ext::PARQUET.to_owned()
    }
}

impl FormatProperties for ImageFormatProperties {
    fn name(&self) -> &'static str {
        "image"
    }
}

impl ParquetFormatProperties for ImageFormatProperties {
    fn writer_properties(&self) -> WriterProperties {
        let ts_path = ColumnPath::from(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP);

        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(Self::COMPRESSION_LEVEL).expect("valid ZSTD compression level"),
            ))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            // Timestamp column: uncompressed for fast seeking
            .set_column_compression(ts_path.clone(), Compression::UNCOMPRESSED)
            .set_column_statistics_enabled(ts_path.clone(), EnabledStatistics::Page)
            .set_column_bloom_filter_enabled(ts_path, true)
            .build()
    }

    fn buffer_capacity(&self) -> usize {
        // (cabba) NOTE: not tuned yet :)
        ((config_params::params()
            .parquet_in_memory_encoding_buffer_size
            .value as f64)
            * 1.3) as usize
    }
}

/// Format properties for sequences of losslessly compressed images (PNG), each
/// with its capture time. All the images of a sequence have the same format.
///
/// Configures each column of the image sequence ontology:
/// - Timestamp column is uncompressed, with page-level statistics and bloom
///   filters
/// - `capture_timestamp_ns` is ZSTD level 5 compressed, with page-level
///   statistics for predicate pushdown
/// - `format` and `frame_id` are dictionary encoded and ZSTD level 5
///   compressed, with page-level statistics for predicate pushdown
/// - Image data is uncompressed, the images being compressed already, without
///   statistics
/// - Any other column is uncompressed, without statistics
pub struct ImageSequenceFormatProperties;

impl ImageSequenceFormatProperties {
    const COMPRESSION_LEVEL: i32 = 5;
    /// Column holding the capture time of each image, in nanoseconds.
    pub const CAPTURE_TIMESTAMP_COLUMN: &str = "capture_timestamp_ns";
    /// Column holding the encoding of each image (`png`).
    pub const FORMAT_COLUMN: &str = "format";
    /// Column holding the encoded images.
    pub const DATA_COLUMN: &str = "data";
    /// Column holding the coordinate frame of each image, from its ROS header.
    pub const FRAME_ID_COLUMN: &str = "frame_id";

    // FIXME: `ZstdLevel` can only be built at runtime with the fallible `try_new`,
    // even for a level known to be valid. Propose upstream (apache/arrow-rs) an
    // infallible API for valid levels (e.g. a `const fn`), then remove this `expect`.
    fn zstd(level: i32) -> Compression {
        Compression::ZSTD(ZstdLevel::try_new(level).expect("valid ZSTD compression level"))
    }

    /// The writer properties of the columns of every image sequence: the
    /// timestamps, then `describe` for the columns of the images (one with few
    /// distinct values, dictionary encoded and compressed, with statistics for
    /// pruning) and `data` with `data_compression`.
    fn image_sequence_properties(
        describe: &[&str],
        data_compression: Compression,
    ) -> WriterProperties {
        let ts_path = ColumnPath::from(constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP);
        let capture_path = ColumnPath::from(Self::CAPTURE_TIMESTAMP_COLUMN);
        let mut builder = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            // Any column not configured below: uncompressed, without
            // dictionary and statistics
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            // Timestamp column: uncompressed for fast seeking
            .set_column_compression(ts_path.clone(), Compression::UNCOMPRESSED)
            .set_column_statistics_enabled(ts_path.clone(), EnabledStatistics::Page)
            .set_column_bloom_filter_enabled(ts_path, true)
            // Capture time: compressed, with statistics for pruning
            .set_column_compression(capture_path.clone(), Self::zstd(Self::COMPRESSION_LEVEL))
            .set_column_statistics_enabled(capture_path, EnabledStatistics::Page)
            .set_column_compression(ColumnPath::from(Self::DATA_COLUMN), data_compression);
        for column in describe {
            let path = ColumnPath::from(*column);
            builder = builder
                .set_column_dictionary_enabled(path.clone(), true)
                .set_column_compression(path.clone(), Self::zstd(Self::COMPRESSION_LEVEL))
                .set_column_statistics_enabled(path, EnabledStatistics::Page);
        }
        builder.build()
    }

    fn image_sequence_buffer_capacity() -> usize {
        // Same as the image format: not tuned yet
        ((config_params::params()
            .parquet_in_memory_encoding_buffer_size
            .value as f64)
            * 1.3) as usize
    }
}

impl AsExtension for ImageSequenceFormatProperties {
    fn as_extension(&self) -> String {
        constants::ext::PARQUET.to_owned()
    }
}

impl FormatProperties for ImageSequenceFormatProperties {
    fn name(&self) -> &'static str {
        "image_sequence"
    }
}

impl ParquetFormatProperties for ImageSequenceFormatProperties {
    fn writer_properties(&self) -> WriterProperties {
        // Image data: uncompressed. PNGs are compressed already: compressing
        // them again saves nearly nothing, and costs time on every write and
        // read.
        Self::image_sequence_properties(
            &[Self::FORMAT_COLUMN, Self::FRAME_ID_COLUMN],
            Compression::UNCOMPRESSED,
        )
    }

    fn buffer_capacity(&self) -> usize {
        Self::image_sequence_buffer_capacity()
    }
}

/// Format properties for sequences of raw images, as a camera driver writes
/// them (e.g. a ROS `sensor_msgs/Image`), each with its capture time.
///
/// Configures each column of the raw image sequence ontology as those of
/// [`ImageSequenceFormatProperties`], with:
/// - `width`, `height`, `stride`, `encoding`, `is_bigendian` and `frame_id`
///   dictionary encoded and ZSTD level 5 compressed, with page-level
///   statistics
/// - Image data ZSTD level 3 compressed
pub struct ImageSequenceRawFormatProperties;

impl ImageSequenceRawFormatProperties {
    /// ZSTD level of the image data. On real raw images (`mono8`, `rgb8`, `16UC1` of ROS bags),
    /// level 3 stores them in ~39% of their size against ~46% with level 1, still at ~440 MB/s
    /// per core, faster than a typical upload; level 5 saves ~3% more at half the speed.
    const DATA_COMPRESSION_LEVEL: i32 = 3;

    /// Columns describing each image.
    pub const WIDTH_COLUMN: &str = "width";
    pub const HEIGHT_COLUMN: &str = "height";
    /// Bytes per row, its padding included.
    pub const STRIDE_COLUMN: &str = "stride";
    /// The pixel encoding, as in ROS (`mono8`, `bgr8`, `16UC1`...).
    pub const ENCODING_COLUMN: &str = "encoding";
    /// Whether the samples of 16 bits are big-endian: little-endian if null.
    pub const IS_BIGENDIAN_COLUMN: &str = "is_bigendian";
}

impl AsExtension for ImageSequenceRawFormatProperties {
    fn as_extension(&self) -> String {
        constants::ext::PARQUET.to_owned()
    }
}

impl FormatProperties for ImageSequenceRawFormatProperties {
    fn name(&self) -> &'static str {
        "image_sequence_raw"
    }
}

impl ParquetFormatProperties for ImageSequenceRawFormatProperties {
    fn writer_properties(&self) -> WriterProperties {
        ImageSequenceFormatProperties::image_sequence_properties(
            &[
                Self::WIDTH_COLUMN,
                Self::HEIGHT_COLUMN,
                Self::STRIDE_COLUMN,
                Self::ENCODING_COLUMN,
                Self::IS_BIGENDIAN_COLUMN,
                ImageSequenceFormatProperties::FRAME_ID_COLUMN,
            ],
            ImageSequenceFormatProperties::zstd(Self::DATA_COMPRESSION_LEVEL),
        )
    }

    fn buffer_capacity(&self) -> usize {
        ImageSequenceFormatProperties::image_sequence_buffer_capacity()
    }
}

/// Returns the base properties for this format variant.
///
/// Use this method when you only need format-agnostic behavior like
/// file extension or format name.
///
/// # Example
///
/// ```
/// use mosaicod_core::types::Format;
/// use mosaicod_rw::ToProperties;
///
/// let props = Format::Default.to_properties();
/// let file_ext = props.as_extension();
/// let name = props.name();
/// ```
pub trait ToProperties {
    fn to_properties(&self) -> Box<dyn FormatProperties>;
}

fn as_format_property(format: &types::Format) -> Box<dyn FormatProperties> {
    match format {
        types::Format::Default => Box::new(DefaultFormatProperties),
        types::Format::Ragged => Box::new(RaggedFormatProperties),
        types::Format::Image => Box::new(ImageFormatProperties),
        types::Format::ImageSequence => Box::new(ImageSequenceFormatProperties),
        types::Format::ImageSequenceRaw => Box::new(ImageSequenceRawFormatProperties),
    }
}

impl ToProperties for types::Format {
    fn to_properties(&self) -> Box<dyn FormatProperties> {
        as_format_property(self)
    }
}

/// Returns the Parquet-specific properties if this format uses Parquet storage.
///
/// Use this method when you need Parquet-specific configuration like
/// writer properties or DataFusion listing options. Returns `None` for
/// formats that don't use Parquet as their underlying storage.
///
/// # Example
///
/// ```
/// use mosaicod_core::types::Format;
/// use mosaicod_rw::ToParquetProperties;
///
/// // Returns option since not every format is based on parquet
/// if let Some(props) = Format::Default.to_parquet_properties(){
///     let wprops = props.writer_properties();
///     let loptions = props.listing_options();
/// }
/// ```
pub trait ToParquetProperties {
    fn to_parquet_properties(&self) -> Option<Box<dyn ParquetFormatProperties>>;
}

fn as_parquet_properties(format: &types::Format) -> Option<Box<dyn ParquetFormatProperties>> {
    match format {
        types::Format::Default => Some(Box::new(DefaultFormatProperties)),
        types::Format::Ragged => Some(Box::new(RaggedFormatProperties)),
        types::Format::Image => Some(Box::new(ImageFormatProperties)),
        types::Format::ImageSequence => Some(Box::new(ImageSequenceFormatProperties)),
        types::Format::ImageSequenceRaw => Some(Box::new(ImageSequenceRawFormatProperties)),
        // Future non-Parquet formats would return None here
    }
}

impl ToParquetProperties for types::Format {
    fn to_parquet_properties(&self) -> Option<Box<dyn ParquetFormatProperties>> {
        as_parquet_properties(self)
    }
}

// ////////////////////////////////////////////////////////////////////////////
// TEST
// ////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use mosaicod_core::types::Format;

    use super::*;

    #[test]
    fn strategy_names() {
        assert_eq!(Format::Default.to_properties().name(), "default");
        assert_eq!(Format::Ragged.to_properties().name(), "ragged");
        assert_eq!(Format::Image.to_properties().name(), "image");
        assert_eq!(
            Format::ImageSequence.to_properties().name(),
            "image_sequence"
        );
        assert_eq!(
            Format::ImageSequenceRaw.to_properties().name(),
            "image_sequence_raw"
        );
    }

    #[test]
    fn strategy_extensions() {
        assert_eq!(Format::Default.to_properties().as_extension(), "parquet");
        assert_eq!(Format::Ragged.to_properties().as_extension(), "parquet");
        assert_eq!(Format::Image.to_properties().as_extension(), "parquet");
        assert_eq!(
            Format::ImageSequence.to_properties().as_extension(),
            "parquet"
        );
        assert_eq!(
            Format::ImageSequenceRaw.to_properties().as_extension(),
            "parquet"
        );
    }

    #[test]
    fn parquet_strategy_writer_properties() {
        let _ = Format::Default
            .to_parquet_properties()
            .unwrap()
            .writer_properties();
        let _ = Format::Ragged
            .to_parquet_properties()
            .unwrap()
            .writer_properties();
        let _ = Format::Image
            .to_parquet_properties()
            .unwrap()
            .writer_properties();
        let _ = Format::ImageSequence
            .to_parquet_properties()
            .unwrap()
            .writer_properties();
        let _ = Format::ImageSequenceRaw
            .to_parquet_properties()
            .unwrap()
            .writer_properties();
    }

    #[test]
    fn parquet_strategy_listing_options() {
        let _ = Format::Default
            .to_parquet_properties()
            .unwrap()
            .listing_options();
        let _ = Format::Ragged
            .to_parquet_properties()
            .unwrap()
            .listing_options();
        let _ = Format::Image
            .to_parquet_properties()
            .unwrap()
            .listing_options();
        let _ = Format::ImageSequence
            .to_parquet_properties()
            .unwrap()
            .listing_options();
        let _ = Format::ImageSequenceRaw
            .to_parquet_properties()
            .unwrap()
            .listing_options();
    }

    #[test]
    fn as_parquet_returns_some_for_parquet_formats() {
        assert!(Format::Default.to_parquet_properties().is_some());
        assert!(Format::Ragged.to_parquet_properties().is_some());
        assert!(Format::Image.to_parquet_properties().is_some());
        assert!(Format::ImageSequence.to_parquet_properties().is_some());
        assert!(Format::ImageSequenceRaw.to_parquet_properties().is_some());
    }
}

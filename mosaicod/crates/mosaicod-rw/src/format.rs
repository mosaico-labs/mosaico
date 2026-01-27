//!
//! Format properties definitions
//!
//! This module implements the Strategy pattern to encapsulate format-specific
//! configurations. Each format variant has its own strategy that defines compression settings,
//! file extensions, and reading options.

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use mosaicod_core::{params, traits::AsExtension, types};
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
/// - Closed for modification: Existing code doesn't need to change when adding formats
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
    fn listing_options(&self) -> ListingOptions;
}

// ////////////////////////////////////////////////////////////////////////////
// Formats Implementation
// ////////////////////////////////////////////////////////////////////////////

/// Strategy for standard columnar data with fixed-width columns.
/// Uses Parquet 2.0 with default compression settings.
pub struct DefaultFormatProperties;

impl AsExtension for DefaultFormatProperties {
    fn as_extension(&self) -> String {
        params::ext::PARQUET.to_owned()
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

    fn listing_options(&self) -> ListingOptions {
        ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(format!(".{}", self.as_extension()))
    }
}

/// Properties for ragged/variable-length data (nested or list-like structures).
///
/// Uses ZSTD level 5 compression with optimized timestamp column handling:
/// - Timestamp column is uncompressed for fast range queries
/// - Bloom filters enabled on timestamp for efficient filtering
/// - Page-level statistics on timestamp for predicate pushdown
pub struct RaggedFormatProperties;

impl RaggedFormatProperties {
    /// ZSTD compression level 5 provides good balance between compression ratio
    /// and speed for variable-length data structures.
    const COMPRESSION_LEVEL: i32 = 5;
}

impl AsExtension for RaggedFormatProperties {
    fn as_extension(&self) -> String {
        params::ext::PARQUET.to_owned()
    }
}

impl FormatProperties for RaggedFormatProperties {
    fn name(&self) -> &'static str {
        "ragged"
    }
}

impl ParquetFormatProperties for RaggedFormatProperties {
    fn writer_properties(&self) -> WriterProperties {
        let ts_path = ColumnPath::from(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP);

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

    fn listing_options(&self) -> ListingOptions {
        ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(format!(".{}", self.as_extension()))
    }
}

/// Format properties for images and dense multi-dimensional arrays.
///
/// Uses maximum ZSTD compression (level 22) since:
/// - Image data is written once and read many times
/// - Higher compression ratio reduces storage costs
/// - Decompression speed is less critical than compression ratio
pub struct ImageFormatProperties;

impl ImageFormatProperties {
    /// Maximum ZSTD compression level for best compression ratio.
    /// Suitable for write-once, read-many image data.
    const COMPRESSION_LEVEL: i32 = 22;
}

impl AsExtension for ImageFormatProperties {
    fn as_extension(&self) -> String {
        params::ext::PARQUET.to_owned()
    }
}

impl FormatProperties for ImageFormatProperties {
    fn name(&self) -> &'static str {
        "image"
    }
}

impl ParquetFormatProperties for ImageFormatProperties {
    fn writer_properties(&self) -> WriterProperties {
        let ts_path = ColumnPath::from(params::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP);

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

    fn listing_options(&self) -> ListingOptions {
        ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(format!(".{}", self.as_extension()))
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
    }

    #[test]
    fn strategy_extensions() {
        assert_eq!(Format::Default.to_properties().as_extension(), "parquet");
        assert_eq!(Format::Ragged.to_properties().as_extension(), "parquet");
        assert_eq!(Format::Image.to_properties().as_extension(), "parquet");
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
    }

    #[test]
    fn as_parquet_returns_some_for_parquet_formats() {
        assert!(Format::Default.to_parquet_properties().is_some());
        assert!(Format::Ragged.to_parquet_properties().is_some());
        assert!(Format::Image.to_parquet_properties().is_some());
    }
}

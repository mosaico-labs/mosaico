//!
//! Format definitions and strategy pattern for format-specific behavior.
//!
//! This module implements the Strategy pattern to encapsulate format-specific
//! configuration for Parquet serialization. Each format variant has its own
//! strategy that defines compression settings, file extensions, and reading options.

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
// FormatStrategy Traits
// ////////////////////////////////////////////////////////////////////////////

/// Base strategy trait for all storage formats.
///
/// This trait defines the minimal interface that all formats must satisfy,
/// regardless of their underlying storage mechanism (Parquet, point clouds, etc.).
///
/// This follows the Strategy pattern to adhere to the Open/Closed Principle:
/// - Open for extension: New formats can be added by implementing this trait
/// - Closed for modification: Existing code doesn't need to change when adding formats
pub trait FormatStrategy: AsExtension + Send + Sync {
    /// Returns a human-readable name for this format strategy.
    fn name(&self) -> &'static str;
}

/// Strategy trait for Parquet-based storage formats.
///
/// Extends `FormatStrategy` with Parquet-specific configuration for compression,
/// statistics, and DataFusion integration. Formats that store data as Parquet
/// files should implement this trait.
pub trait ParquetFormatStrategy: FormatStrategy {
    /// Returns the Parquet writer properties configured for this format.
    fn writer_properties(&self) -> WriterProperties;

    /// Returns DataFusion ListingOptions configured for reading files in this format.
    fn listing_options(&self) -> ListingOptions;
}

// ////////////////////////////////////////////////////////////////////////////
// Strategy Implementations
// ////////////////////////////////////////////////////////////////////////////

/// Strategy for standard columnar data with fixed-width columns.
/// Uses Parquet 2.0 with default compression settings.
pub struct DefaultFormatStrategy;

impl AsExtension for DefaultFormatStrategy {
    fn as_extension(&self) -> String {
        params::ext::PARQUET.to_owned()
    }
}

impl FormatStrategy for DefaultFormatStrategy {
    fn name(&self) -> &'static str {
        "default"
    }
}

impl ParquetFormatStrategy for DefaultFormatStrategy {
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

/// Strategy for ragged/variable-length data (nested or list-like structures).
///
/// Uses ZSTD level 5 compression with optimized timestamp column handling:
/// - Timestamp column is uncompressed for fast range queries
/// - Bloom filters enabled on timestamp for efficient filtering
/// - Page-level statistics on timestamp for predicate pushdown
pub struct RaggedFormatStrategy;

impl RaggedFormatStrategy {
    /// ZSTD compression level 5 provides good balance between compression ratio
    /// and speed for variable-length data structures.
    const COMPRESSION_LEVEL: i32 = 5;
}

impl AsExtension for RaggedFormatStrategy {
    fn as_extension(&self) -> String {
        params::ext::PARQUET.to_owned()
    }
}

impl FormatStrategy for RaggedFormatStrategy {
    fn name(&self) -> &'static str {
        "ragged"
    }
}

impl ParquetFormatStrategy for RaggedFormatStrategy {
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

/// Strategy for images and dense multi-dimensional arrays.
///
/// Uses maximum ZSTD compression (level 22) since:
/// - Image data is written once and read many times
/// - Higher compression ratio reduces storage costs
/// - Decompression speed is less critical than compression ratio
pub struct ImageFormatStrategy;

impl ImageFormatStrategy {
    /// Maximum ZSTD compression level for best compression ratio.
    /// Suitable for write-once, read-many image data.
    const COMPRESSION_LEVEL: i32 = 22;
}

impl AsExtension for ImageFormatStrategy {
    fn as_extension(&self) -> String {
        params::ext::PARQUET.to_owned()
    }
}

impl FormatStrategy for ImageFormatStrategy {
    fn name(&self) -> &'static str {
        "image"
    }
}

impl ParquetFormatStrategy for ImageFormatStrategy {
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

/// Returns the base strategy implementation for this format variant.
///
/// Use this method when you only need format-agnostic behavior like
/// file extension or format name.
///
/// # Example
///
/// ```
/// use mosaicod_core::types::Format;
///
/// let format = Format::Default;
/// if let Some(format_strategy) = format.as_strategy() {
///     let file_ext = format_strategy.file_extension();
///     let name = format_strategy.name();
/// }
/// ```
pub trait AsStrategy {
    fn as_strategy(&self) -> Box<dyn FormatStrategy>;
}

fn as_format_strategy(format: &types::Format) -> Box<dyn FormatStrategy> {
    match format {
        types::Format::Default => Box::new(DefaultFormatStrategy),
        types::Format::Ragged => Box::new(RaggedFormatStrategy),
        types::Format::Image => Box::new(ImageFormatStrategy),
    }
}

impl AsStrategy for types::Format {
    fn as_strategy(&self) -> Box<dyn FormatStrategy> {
        as_format_strategy(self)
    }
}

/// Returns the Parquet strategy if this format uses Parquet storage.
///
/// Use this method when you need Parquet-specific configuration like
/// writer properties or DataFusion listing options. Returns `None` for
/// formats that don't use Parquet as their underlying storage.
///
/// # Example
///
/// ```
/// use mosaicod_core::types::Format;
///
/// let format = Format::Default;
/// if let Some(parquet_strategy) = format.as_parquet(){
///     let props = parquet_strategy.writer_properties();
///     let options = parquet_strategy.listing_options();
/// }
/// ```
pub trait AsParquet {
    fn as_parquet(&self) -> Option<Box<dyn ParquetFormatStrategy>>;
}

fn as_parquet_strategy(format: &types::Format) -> Option<Box<dyn ParquetFormatStrategy>> {
    match format {
        types::Format::Default => Some(Box::new(DefaultFormatStrategy)),
        types::Format::Ragged => Some(Box::new(RaggedFormatStrategy)),
        types::Format::Image => Some(Box::new(ImageFormatStrategy)),
        // Future non-Parquet formats would return None here
    }
}

impl AsParquet for types::Format {
    fn as_parquet(&self) -> Option<Box<dyn ParquetFormatStrategy>> {
        as_parquet_strategy(self)
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
        assert_eq!(Format::Default.as_strategy().name(), "default");
        assert_eq!(Format::Ragged.as_strategy().name(), "ragged");
        assert_eq!(Format::Image.as_strategy().name(), "image");
    }

    #[test]
    fn strategy_extensions() {
        assert_eq!(Format::Default.as_strategy().as_extension(), "parquet");
        assert_eq!(Format::Ragged.as_strategy().as_extension(), "parquet");
        assert_eq!(Format::Image.as_strategy().as_extension(), "parquet");
    }

    #[test]
    fn parquet_strategy_writer_properties() {
        // Verify that as_parquet() returns Some for all current formats
        // and writer_properties() doesn't panic
        let _ = Format::Default.as_parquet().unwrap().writer_properties();
        let _ = Format::Ragged.as_parquet().unwrap().writer_properties();
        let _ = Format::Image.as_parquet().unwrap().writer_properties();
    }

    #[test]
    fn parquet_strategy_listing_options() {
        // Verify that as_parquet() returns Some for all current formats
        // and listing_options() doesn't panic
        let _ = Format::Default.as_parquet().unwrap().listing_options();
        let _ = Format::Ragged.as_parquet().unwrap().listing_options();
        let _ = Format::Image.as_parquet().unwrap().listing_options();
    }

    #[test]
    fn as_parquet_returns_some_for_parquet_formats() {
        assert!(Format::Default.as_parquet().is_some());
        assert!(Format::Ragged.as_parquet().is_some());
        assert!(Format::Image.as_parquet().is_some());
    }
}

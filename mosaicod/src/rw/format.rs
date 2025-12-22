//! Format definitions and strategy pattern for format-specific behavior.
//!
//! This module implements the Strategy pattern to encapsulate format-specific
//! configuration for Parquet serialization. Each format variant has its own
//! strategy that defines compression settings, file extensions, and reading options.

use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
    schema::types::ColumnPath,
};
use serde::{Deserialize, Serialize};

use crate::{params, rw::Error, traits};

// ============================================================================
// FormatStrategy Trait
// ============================================================================

/// Strategy trait that encapsulates format-specific behavior for Parquet serialization.
///
/// Each format variant (Default, Ragged, Image) implements this trait to provide
/// its own configuration for compression, statistics, and reading options.
///
/// This follows the Strategy pattern to adhere to the Open/Closed Principle:
/// - Open for extension: New formats can be added by implementing this trait
/// - Closed for modification: Existing code doesn't need to change when adding formats
pub trait FormatStrategy: Send + Sync {
    /// Returns the file extension for this format (without leading dot).
    fn file_extension(&self) -> &'static str;

    /// Returns the Parquet writer properties configured for this format.
    fn writer_properties(&self) -> WriterProperties;

    /// Returns DataFusion ListingOptions configured for reading files in this format.
    fn listing_options(&self) -> ListingOptions;

    /// Returns a human-readable name for this format.
    fn name(&self) -> &'static str;
}

// ============================================================================
// Strategy Implementations
// ============================================================================

/// Strategy for standard columnar data with fixed-width columns.
/// Uses Parquet 2.0 with default compression settings.
pub struct DefaultFormatStrategy;

impl FormatStrategy for DefaultFormatStrategy {
    fn file_extension(&self) -> &'static str {
        params::ext::PARQUET
    }

    fn writer_properties(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build()
    }

    fn listing_options(&self) -> ListingOptions {
        ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(format!(".{}", self.file_extension()))
    }

    fn name(&self) -> &'static str {
        "default"
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

impl FormatStrategy for RaggedFormatStrategy {
    fn file_extension(&self) -> &'static str {
        params::ext::PARQUET
    }

    fn writer_properties(&self) -> WriterProperties {
        let ts_path = ColumnPath::from(params::ARROW_SCHEMA_COLUMN_NAME_TIMESTAMP);

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
            .with_file_extension(format!(".{}", self.file_extension()))
    }

    fn name(&self) -> &'static str {
        "ragged"
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

impl FormatStrategy for ImageFormatStrategy {
    fn file_extension(&self) -> &'static str {
        params::ext::PARQUET
    }

    fn writer_properties(&self) -> WriterProperties {
        let ts_path = ColumnPath::from(params::ARROW_SCHEMA_COLUMN_NAME_TIMESTAMP);

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
            .with_file_extension(format!(".{}", self.file_extension()))
    }

    fn name(&self) -> &'static str {
        "image"
    }
}

// ============================================================================
// Format Enum
// ============================================================================

/// This enum allows choosing the appropriate storage strategy based on the
/// structure of the data being written.
#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    /// Serialization format used to store data in a columnar format.
    /// This is suitable for structured data where each row has a fixed number of columns.
    Default,
    /// Serialization format for ragged data, where each record can contain a
    /// variable number of items. This is ideal for representing nested or list-like
    /// structures.
    Ragged,

    /// Serialization format for images and dense multi-dimensional arrays.
    /// This format is optimized for storing high-dimensional data efficiently.
    Image,
}

impl Format {
    /// Returns the strategy implementation for this format variant.
    ///
    /// This is the primary method for accessing format-specific behavior.
    /// All format-dependent logic should go through the returned strategy.
    pub fn strategy(&self) -> Box<dyn FormatStrategy> {
        match self {
            Self::Default => Box::new(DefaultFormatStrategy),
            Self::Ragged => Box::new(RaggedFormatStrategy),
            Self::Image => Box::new(ImageFormatStrategy),
        }
    }
}

impl traits::AsExtension for Format {
    fn as_extension(&self) -> String {
        self.strategy().file_extension().to_owned()
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.strategy().name())
    }
}

impl std::str::FromStr for Format {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "default" => Ok(Self::Default),
            "ragged" => Ok(Self::Ragged),
            "image" => Ok(Self::Image),
            _ => Err(Error::UnkownFormat(value.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::traits::AsExtension;
    use std::str::FromStr;

    use super::*;

    #[test]
    fn from_str() {
        let default = Format::from_str("default");
        assert!(default.is_ok());
        assert_eq!(default.as_ref().unwrap(), &Format::Default);
        assert_eq!(default.unwrap().as_extension(), params::ext::PARQUET);

        let ragged = Format::from_str("ragged");
        assert!(ragged.is_ok());
        assert_eq!(ragged.as_ref().unwrap(), &Format::Ragged);
        assert_eq!(ragged.unwrap().as_extension(), params::ext::PARQUET);

        let image = Format::from_str("image");
        assert!(image.is_ok());
        assert_eq!(image.as_ref().unwrap(), &Format::Image);
        assert_eq!(image.unwrap().as_extension(), params::ext::PARQUET);
    }

    #[test]
    fn to_str() {
        assert_eq!("ragged", Format::Ragged.to_string());
        assert_eq!("default", Format::Default.to_string());
        assert_eq!("image", Format::Image.to_string());
    }

    #[test]
    fn strategy_names() {
        assert_eq!(Format::Default.strategy().name(), "default");
        assert_eq!(Format::Ragged.strategy().name(), "ragged");
        assert_eq!(Format::Image.strategy().name(), "image");
    }

    #[test]
    fn strategy_extensions() {
        assert_eq!(Format::Default.strategy().file_extension(), "parquet");
        assert_eq!(Format::Ragged.strategy().file_extension(), "parquet");
        assert_eq!(Format::Image.strategy().file_extension(), "parquet");
    }

    #[test]
    fn strategy_writer_properties() {
        // Just verify that writer_properties() doesn't panic
        let _ = Format::Default.strategy().writer_properties();
        let _ = Format::Ragged.strategy().writer_properties();
        let _ = Format::Image.strategy().writer_properties();
    }

    #[test]
    fn strategy_listing_options() {
        // Just verify that listing_options() doesn't panic
        let _ = Format::Default.strategy().listing_options();
        let _ = Format::Ragged.strategy().listing_options();
        let _ = Format::Image.strategy().listing_options();
    }
}

use std::sync::Arc;

use arrow::datatypes::Schema;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::{WriterProperties, WriterVersion},
    schema::types::ColumnPath,
};

use super::{Error, Format};

pub enum Writer {
    /// Parquet file format <https://parquet.apache.org/docs/file-format/>
    /// (cabba) TODO: evaluate `AsyncArrowWriter`
    Parquet(ArrowWriter<Vec<u8>>),
}

impl Writer {
    pub fn new(schema: &Arc<Schema>, format: Format) -> Result<Self, Error> {
        match format {
            Format::Default => {
                let props = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    .build();

                Ok(Self::Parquet(ArrowWriter::try_new(
                    Vec::new(),
                    schema.clone(),
                    Some(props),
                )?))
            }
            Format::Ragged => {
                let ts_path = ColumnPath::from("timestamp");

                let props = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    // Data will be compressed with ZSTD at a lower compression rate
                    .set_compression(Compression::ZSTD(ZstdLevel::try_new(5).unwrap()))
                    .set_dictionary_enabled(false)
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                    // set timestamp specific parameters
                    .set_column_compression(ts_path.clone(), Compression::UNCOMPRESSED)
                    .set_column_statistics_enabled(
                        ts_path.clone(),
                        parquet::file::properties::EnabledStatistics::Page,
                    )
                    .set_column_bloom_filter_enabled(ts_path, true)
                    .build();

                Ok(Self::Parquet(ArrowWriter::try_new(
                    Vec::new(),
                    schema.clone(),
                    Some(props),
                )?))
            }
            Format::Image => {
                let ts_path = ColumnPath::from("timestamp");

                let props = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    // Data will be compressed with ZSTD at maximum compression rate
                    .set_compression(Compression::ZSTD(ZstdLevel::try_new(22).unwrap()))
                    .set_dictionary_enabled(false)
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                    // set timestamp specific parameters
                    .set_column_compression(ts_path.clone(), Compression::UNCOMPRESSED)
                    .set_column_statistics_enabled(
                        ts_path.clone(),
                        parquet::file::properties::EnabledStatistics::Page,
                    )
                    .set_column_bloom_filter_enabled(ts_path, true)
                    .build();

                Ok(Self::Parquet(ArrowWriter::try_new(
                    Vec::new(),
                    schema.clone(),
                    Some(props),
                )?))
            }
        }
    }
}

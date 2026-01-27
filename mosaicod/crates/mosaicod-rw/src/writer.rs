use crate::ToParquetProperties;

use super::Error;
use arrow::datatypes::Schema;
use mosaicod_core::types;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

pub enum Writer {
    /// Parquet file format <https://parquet.apache.org/docs/file-format/>
    /// (cabba) TODO: evaluate `AsyncArrowWriter`
    Parquet(ArrowWriter<Vec<u8>>),
}

impl Writer {
    pub fn new(schema: &Arc<Schema>, format: types::Format) -> Result<Self, Error> {
        let parquet_strategy = format
            .to_parquet_properties()
            .expect("Writer::new requires a Parquet-based format");

        let props = parquet_strategy.writer_properties();

        Ok(Self::Parquet(ArrowWriter::try_new(
            Vec::new(),
            schema.clone(),
            Some(props),
        )?))
    }
}

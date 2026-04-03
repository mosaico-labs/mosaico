use crate::ToParquetProperties;

use super::Error;
use arrow::datatypes::Schema;
use mosaicod_core::types;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

pub struct ParquetWriter(ArrowWriter<Vec<u8>>);

impl std::ops::Deref for ParquetWriter {
    type Target = ArrowWriter<Vec<u8>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ParquetWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ParquetWriter {
    pub fn buffer(self) -> Result<Vec<u8>, Error> {
        Ok(self.0.into_inner()?)
    }
}

pub enum Writer {
    /// Parquet file format <https://parquet.apache.org/docs/file-format/>
    /// (cabba) TODO: evaluate `AsyncArrowWriter`
    Parquet(ParquetWriter),
}

impl Writer {
    pub fn new(schema: &Arc<Schema>, format: types::Format) -> Result<Self, Error> {
        let parquet_strategy = format
            .to_parquet_properties()
            .expect("Writer::new requires a Parquet-based format");

        let props = parquet_strategy.writer_properties();

        Ok(Self::Parquet(ParquetWriter(ArrowWriter::try_new(
            Vec::with_capacity(parquet_strategy.buffer_capacity()),
            schema.clone(),
            Some(props),
        )?)))
    }
}

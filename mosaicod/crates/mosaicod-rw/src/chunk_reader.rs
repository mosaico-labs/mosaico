use super::Error;
use arrow::datatypes::SchemaRef;
use mosaicod_core::types;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

pub enum Reader {
    /// Parquet file format https://parquet.apache.org/docs/file-format/
    Parquet {
        reader: ParquetRecordBatchReader,
        schema: SchemaRef,
    },
}

impl Reader {
    pub fn try_new(_format: types::Format, buffer: bytes::Bytes) -> Result<Self, Error> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(buffer)?;
        Ok(Self::Parquet {
            schema: builder.schema().clone(),
            reader: builder.build()?,
        })
    }
}
pub struct ChunkReader {
    reader: Reader,
}

impl ChunkReader {
    pub fn new(format: types::Format, buffer: bytes::Bytes) -> Result<Self, Error> {
        Ok(Self {
            reader: Reader::try_new(format, buffer)?,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        match &self.reader {
            Reader::Parquet { schema, .. } => schema.clone(),
        }
    }
}

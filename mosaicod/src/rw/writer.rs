use std::sync::Arc;

use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;

use super::{Error, Format};

pub enum Writer {
    /// Parquet file format <https://parquet.apache.org/docs/file-format/>
    /// (cabba) TODO: evaluate `AsyncArrowWriter`
    Parquet(ArrowWriter<Vec<u8>>),
}

impl Writer {
    pub fn new(schema: &Arc<Schema>, format: Format) -> Result<Self, Error> {
        // Delegate to Parquet strategy for format-specific writer properties
        let parquet_strategy = format
            .as_parquet()
            .expect("Writer::new requires a Parquet-based format");
        let props = parquet_strategy.writer_properties();

        Ok(Self::Parquet(ArrowWriter::try_new(
            Vec::new(),
            schema.clone(),
            Some(props),
        )?))
    }
}

use std::sync::Arc;

use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;

use super::{Error, Format};

pub enum Writer {
    /// Parquet file format <https://parquet.apache.org/docs/file-format/>
    // TODO: evaluate `AsyncArrowWriter`
    Parquet(ArrowWriter<Vec<u8>>),
}

impl Writer {
    pub fn new(schema: &Arc<Schema>, format: Format) -> Result<Self, Error> {
        // Delegate to strategy for format-specific writer properties
        let props = format.strategy().writer_properties();

        Ok(Self::Parquet(ArrowWriter::try_new(
            Vec::new(),
            schema.clone(),
            Some(props),
        )?))
    }
}

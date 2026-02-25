use super::Error;
use mosaicod_core::types;
use mosaicod_db as db;

pub struct Chunk<'a> {
    tx: db::Tx<'a>,
    chunk: db::ChunkRecord,
}

impl<'a> Chunk<'a> {
    pub async fn create(
        topic_id: i32,
        datafile: impl AsRef<std::path::Path>,
        size_bytes: i64,
        row_count: i64,
        db: &'a db::Database,
    ) -> Result<Self, Error> {
        let mut tx = db.transaction().await?;

        let chunk = db::chunk_create(
            &mut tx,
            &db::ChunkRecord::new(topic_id, datafile, size_bytes, row_count),
        )
        .await?;

        Ok(Self { tx, chunk })
    }

    /// Push all column statistics using batch inserts for better performance.
    /// This method collects all stats, resolves column IDs, then performs
    /// two batch INSERT operations (one for numeric, one for textual stats).
    pub async fn push_ontology_model_stats(
        &mut self,
        ontology_tag: &str,
        cstats: types::OntologyModelStats,
    ) -> Result<(), Error> {
        let mut numeric_batch: Vec<db::ColumnChunkNumericRecord> = Vec::new();
        let mut textual_batch: Vec<db::ColumnChunkTextualRecord> = Vec::new();

        // First pass: resolve column IDs and collect stats for batch insert
        for (field, stats) in cstats.cols {
            if stats.is_unsupported() {
                continue;
            }

            let column = db::column_get_or_create(&mut self.tx, &field, ontology_tag).await?;

            match stats {
                types::Stats::Textual(stats) => {
                    let (min, max, has_null) = stats.into_owned();
                    textual_batch.push(db::ColumnChunkTextualRecord::try_new(
                        column.column_id,
                        self.chunk.chunk_id,
                        min,
                        max,
                        has_null,
                    )?);
                }
                types::Stats::Numeric(stats) => {
                    numeric_batch.push(db::ColumnChunkNumericRecord::new(
                        column.column_id,
                        self.chunk.chunk_id,
                        stats.min,
                        stats.max,
                        stats.has_null,
                        stats.has_nan,
                    ));
                }
                types::Stats::Unsupported => {}
            }
        }

        db::column_chunk_numeric_create_batch(&mut self.tx, &numeric_batch).await?;
        db::column_chunk_textual_create_batch(&mut self.tx, &textual_batch).await?;

        Ok(())
    }

    pub async fn finalize(self) -> Result<(), Error> {
        self.tx.commit().await?;
        Ok(())
    }
}

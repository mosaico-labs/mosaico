use super::FacadeError;
use crate::{repo, types};

pub struct FacadeChunk<'a> {
    tx: repo::Tx<'a>,
    chunk: repo::Chunk,
}

impl<'a> FacadeChunk<'a> {
    pub async fn create(
        topic_id: i32,
        datafile: impl AsRef<std::path::Path>,
        size_bytes: i64,
        row_count: i64,
        repo: &'a repo::Repository,
    ) -> Result<Self, FacadeError> {
        let mut tx = repo.transaction().await?;

        let chunk = repo::chunk_create(
            &mut tx,
            &repo::Chunk::new(topic_id, datafile, size_bytes, row_count),
        )
        .await?;

        Ok(Self { tx, chunk })
    }

    pub async fn push_stats(
        &mut self,
        ontology_tag: &str,
        field: &str,
        stats: types::Stats,
    ) -> Result<(), FacadeError> {
        if stats.is_unsupported() {
            return Ok(());
        }

        let column = repo::column_get_or_create(&mut self.tx, field, ontology_tag).await?;

        match stats {
            types::Stats::Text(stats) => {
                let (min, max, has_null) = stats.into_owned();
                repo::column_chunk_literal_create(
                    &mut self.tx,
                    &repo::ColumnChunkLiteral::try_new(
                        column.column_id,
                        self.chunk.chunk_id,
                        min,
                        max,
                        has_null,
                    )?,
                )
                .await?;
            }
            types::Stats::Numeric(stats) => {
                repo::column_chunk_numeric_create(
                    &mut self.tx,
                    &repo::ColumnChunkNumeric::new(
                        column.column_id,
                        self.chunk.chunk_id,
                        stats.min,
                        stats.max,
                        stats.has_null,
                        stats.has_nan,
                    ),
                )
                .await?;
            }
            _ => {}
        }

        Ok(())
    }

    pub async fn finalize(self) -> Result<(), FacadeError> {
        self.tx.commit().await?;
        Ok(())
    }
}

use mosaicod_core::{error::PublicResult as Result, types};
use mosaicod_db as db;
use mosaicod_rw as rw;

pub async fn update_chunk_stats(
    tx: &mut db::Tx<'_>,
    topic_uuid: &types::Uuid,
    datafile: impl AsRef<std::path::Path>,
    metadata: rw::ChunkMetadata,
    ontology_tag: &str,
    cstats: types::OntologyModelStats,
) -> Result<()> {
    let topic_id = db::topic_find_by_uuid(tx, topic_uuid).await?.topic_id;

    let chunk = db::chunk_create(
        tx,
        &db::ChunkRecord::new(
            topic_id,
            datafile,
            metadata.size_bytes as i64,
            metadata.arrow_size_bytes as i64,
            metadata.row_count as i64,
            metadata.first_sample_timestamp_ns,
            metadata.last_sample_timestamp_ns,
        ),
    )
    .await?;

    let mut numeric_batch: Vec<db::ColumnChunkNumericRecord> = Vec::new();
    let mut textual_batch: Vec<db::ColumnChunkTextualRecord> = Vec::new();

    // First pass: resolve column IDs and collect stats for batch insert
    for (field, stats) in cstats.cols {
        if stats.is_unsupported() {
            continue;
        }

        let column = db::column_get_or_create(tx, &field, ontology_tag).await?;
        let is_list = stats.is_list();

        match stats {
            types::Stats::Textual(stats) | types::Stats::ListTextual(stats) => {
                textual_batch.push(db::ColumnChunkTextualRecord::new(
                    column.column_id,
                    chunk.chunk_id,
                    stats,
                    is_list,
                ));
            }
            types::Stats::Numeric(stats) | types::Stats::ListNumeric(stats) => {
                numeric_batch.push(db::ColumnChunkNumericRecord::new(
                    column.column_id,
                    chunk.chunk_id,
                    &stats,
                    is_list,
                ));
            }
            types::Stats::Unsupported => {}
        }
    }

    db::column_chunk_numeric_create_batch(tx, &numeric_batch).await?;
    db::column_chunk_textual_create_batch(tx, &textual_batch).await?;

    Ok(())
}

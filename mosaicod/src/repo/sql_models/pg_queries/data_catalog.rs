use sqlx::{Row, postgres::PgRow};

use crate::{
    query,
    repo::{self, sql_models},
};

pub async fn column_get_or_create(
    exec: &mut impl repo::AsExec,
    column_name: &str,
    ontology_tag: &str,
) -> Result<sql_models::Column, repo::Error> {
    // The UPDATE part of the query is a no-op update: it forces the query to return the existing row
    // from the COLUMN table without changing any data.
    let res = sqlx::query_as!(
        sql_models::Column,
        r#"INSERT INTO column_t (column_name, ontology_tag)
        VALUES ($1, $2)
        ON CONFLICT (column_name, ontology_tag)
        DO UPDATE SET
            column_name = EXCLUDED.column_name  -- no-op
        RETURNING *"#,
        column_name,
        ontology_tag,
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

pub async fn chunk_create(
    exec: &mut impl repo::AsExec,
    chunk: &sql_models::Chunk,
) -> Result<sql_models::Chunk, repo::Error> {
    let res = sqlx::query_as!(
        sql_models::Chunk,
        r#"INSERT INTO chunk_t(chunk_uuid, topic_id, data_file, size_bytes, row_count)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *"#,
        chunk.chunk_uuid,
        chunk.topic_id,
        chunk.data_file,
        chunk.size_bytes,
        chunk.row_count,
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

pub async fn column_chunk_literal_create(
    exec: &mut impl repo::AsExec,
    val: &sql_models::ColumnChunkLiteral,
) -> Result<sql_models::ColumnChunkLiteral, repo::Error> {
    let res = sqlx::query_as!(
        sql_models::ColumnChunkLiteral,
        r#"INSERT INTO column_chunk_literal_t(
            column_id, chunk_id,
            min_value, max_value,
            has_null 
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *"#,
        val.column_id,
        val.chunk_id,
        val.min_value,
        val.max_value,
        val.has_null,
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

pub async fn column_chunk_numeric_create(
    exec: &mut impl repo::AsExec,
    val: &sql_models::ColumnChunkNumeric,
) -> Result<sql_models::ColumnChunkNumeric, repo::Error> {
    let res = sqlx::query_as!(
        sql_models::ColumnChunkNumeric,
        r#"INSERT INTO column_chunk_numeric_t(
            column_id, chunk_id,
            min_value, max_value,
            has_null, has_nan
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *"#,
        val.column_id,
        val.chunk_id,
        val.min_value,
        val.max_value,
        val.has_null,
        val.has_nan,
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

/// Returns the list of chunks matching the provided `filter` criteria.
/// Optionally the query can be fitlered across a list of topics (`on_topics`).
pub async fn chunks_from_filters(
    exec: &mut impl repo::AsExec,
    filter: query::OntologyFilter,
    on_topics: Option<&Vec<sql_models::TopicRecord>>, // (cabba) TODO: pas only topic names or ids?
) -> Result<Vec<sql_models::Chunk>, repo::Error> {
    // Collect topic ids, if any
    let ids: Vec<i64> = if let Some(topics) = on_topics {
        topics.iter().map(|t| t.topic_id as i64).collect()
    } else {
        Vec::new()
    };

    let (query, values) = super::ChunkQueryBuilder::build(filter, ids)?;

    dbg!(&query);

    let mut r = sqlx::query(&query);

    for v in values.into_iter() {
        match v {
            query::Value::Integer(v) => r = r.bind(v),
            query::Value::Float(v) => r = r.bind(v),
            query::Value::Text(v) => r = r.bind(v),
            // Cast boolean value to numeric, since for now there is no custom column for boolean values
            query::Value::Boolean(v) => r = r.bind(if v { 1.0 } else { 0.0 }),
        }
    }

    let r = r.map(cast_chunk_data).fetch_all(exec.as_exec()).await?;
    r.into_iter().collect()
}

fn cast_chunk_data(row: PgRow) -> Result<sql_models::Chunk, repo::Error> {
    Ok(sql_models::Chunk {
        chunk_id: row.try_get("chunk_id")?,
        chunk_uuid: row.try_get("chunk_uuid")?,
        topic_id: row.try_get("topic_id")?,
        data_file: row.try_get("data_file")?,
        size_bytes: row.try_get("size_bytes")?,
        row_count: row.try_get("row_count")?,
    })
}

/// Aggregated statistics for a topic's chunks.
#[derive(Debug, Clone, Default)]
pub struct TopicStats {
    pub total_size_bytes: i64,
    pub total_row_count: i64,
}

/// Returns aggregated size and row count statistics for all chunks belonging to a topic.
pub async fn topic_get_stats(
    exec: &mut impl repo::AsExec,
    topic_name: &str,
) -> Result<TopicStats, repo::Error> {
    let res = sqlx::query!(
        r#"SELECT
            COALESCE(SUM(size_bytes), 0)::BIGINT as "total_size_bytes!",
            COALESCE(SUM(row_count), 0)::BIGINT as "total_row_count!"
        FROM chunk_t
        WHERE topic_id = (SELECT topic_id FROM topic_t WHERE topic_name = $1)"#,
        topic_name,
    )
    .fetch_one(exec.as_exec())
    .await?;

    Ok(TopicStats {
        total_size_bytes: res.total_size_bytes,
        total_row_count: res.total_row_count,
    })
}

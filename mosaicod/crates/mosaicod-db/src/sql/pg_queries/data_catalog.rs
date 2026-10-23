use std::collections::HashMap;

use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types::{self};
use mosaicod_query as query;
use sqlx::{Row, postgres::PgRow};
use tracing::{trace, warn};

pub async fn column_get_or_create(
    exec: &mut impl AsExec,
    column_name: &str,
    ontology_tag: &str,
) -> Result<schema::ColumnRecord, Error> {
    // The UPDATE part of the query is a no-op update: it forces the query to return the existing row
    // from the COLUMN table without changing any data.
    let res = sqlx::query_as!(
        schema::ColumnRecord,
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
    exec: &mut impl AsExec,
    chunk: &schema::ChunkRecord,
) -> Result<schema::ChunkRecord, Error> {
    let res = sqlx::query_as!(
        schema::ChunkRecord,
        r#"INSERT INTO chunk_t(
            chunk_uuid, topic_id, data_file, size_bytes, arrow_size_bytes, row_count,
            first_sample_timestamp_ns, last_sample_timestamp_ns
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING *"#,
        chunk.chunk_uuid,
        chunk.topic_id,
        chunk.data_file,
        chunk.size_bytes,
        chunk.arrow_size_bytes,
        chunk.row_count,
        chunk.first_sample_timestamp_ns,
        chunk.last_sample_timestamp_ns,
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

pub async fn chunk_delete_by_topic_id(
    exec: &mut impl AsExec,
    topic_id: i32,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!(
        "(data loss) deleting chunk stats for topic with id {}",
        topic_id
    );

    sqlx::query!(r#"DELETE FROM chunk_t WHERE topic_id = $1"#, topic_id)
        .execute(exec.as_exec())
        .await?;

    Ok(())
}

pub async fn column_chunk_textual_create(
    exec: &mut impl AsExec,
    val: &schema::ColumnChunkTextualRecord,
) -> Result<schema::ColumnChunkTextualRecord, Error> {
    let res = sqlx::query_as!(
        schema::ColumnChunkTextualRecord,
        r#"INSERT INTO column_chunk_textual_t(
            column_id, chunk_id,
            min_value, max_value,
            null_count,
            first_sample_value, last_sample_value,
            is_list
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING *"#,
        val.column_id,
        val.chunk_id,
        val.min_value,
        val.max_value,
        val.null_count,
        val.first_sample_value,
        val.last_sample_value,
        val.is_list,
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

pub async fn column_chunk_numeric_create(
    exec: &mut impl AsExec,
    val: &schema::ColumnChunkNumericRecord,
) -> Result<schema::ColumnChunkNumericRecord, Error> {
    let res = sqlx::query_as!(
        schema::ColumnChunkNumericRecord,
        r#"INSERT INTO column_chunk_numeric_t(
            column_id, chunk_id,
            min_value, max_value,
            null_count, nan_count,
            first_sample_value, last_sample_value,
            mean_value, m2_value,
            is_list
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING *"#,
        val.column_id,
        val.chunk_id,
        val.min_value,
        val.max_value,
        val.null_count,
        val.nan_count,
        val.first_sample_value,
        val.last_sample_value,
        val.mean_value,
        val.m2_value,
        val.is_list,
    )
    .fetch_one(exec.as_exec())
    .await?;
    Ok(res)
}

/// Batch insert multiple numeric column chunk stats in a single query.
/// More efficient than individual inserts when inserting many stats.
pub async fn column_chunk_numeric_create_batch(
    exec: &mut impl AsExec,
    values: &[schema::ColumnChunkNumericRecord],
) -> Result<(), Error> {
    if values.is_empty() {
        return Ok(());
    }

    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO column_chunk_numeric_t(column_id, chunk_id, min_value, max_value, null_count, nan_count, first_sample_value, last_sample_value, mean_value, m2_value, is_list) ",
    );

    query_builder.push_values(values, |mut b, val| {
        b.push_bind(val.column_id)
            .push_bind(val.chunk_id)
            .push_bind(val.min_value)
            .push_bind(val.max_value)
            .push_bind(val.null_count)
            .push_bind(val.nan_count)
            .push_bind(val.first_sample_value)
            .push_bind(val.last_sample_value)
            .push_bind(val.mean_value)
            .push_bind(val.m2_value)
            .push_bind(val.is_list);
    });

    query_builder.build().execute(exec.as_exec()).await?;
    Ok(())
}

/// Batch insert multiple textual column chunk stats in a single query.
/// More efficient than individual inserts when inserting many stats.
pub async fn column_chunk_textual_create_batch(
    exec: &mut impl AsExec,
    values: &[schema::ColumnChunkTextualRecord],
) -> Result<(), Error> {
    if values.is_empty() {
        return Ok(());
    }

    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO column_chunk_textual_t(column_id, chunk_id, min_value, max_value, null_count, first_sample_value, last_sample_value, is_list) ",
    );

    query_builder.push_values(values, |mut b, val| {
        b.push_bind(val.column_id)
            .push_bind(val.chunk_id)
            .push_bind(&val.min_value)
            .push_bind(&val.max_value)
            .push_bind(val.null_count)
            .push_bind(&val.first_sample_value)
            .push_bind(&val.last_sample_value)
            .push_bind(val.is_list);
    });

    query_builder.build().execute(exec.as_exec()).await?;
    Ok(())
}

/// Returns the list of chunks matching the provided `filter` criteria.
/// Optionally the query can be filtered across a list of topics (`on_topics`).
pub async fn chunks_from_filters(
    exec: &mut impl AsExec,
    filter: query::OntologyPredicateGroup<query::Value>,
    on_topics: Option<&Vec<schema::TopicRecord>>, // (cabba) TODO: pass only topic names or ids?
) -> Result<Vec<schema::ChunkRecord>, Error> {
    // Collect topic ids, if any
    let ids: Vec<i64> = if let Some(topics) = on_topics {
        topics.iter().map(|t| t.topic_id as i64).collect()
    } else {
        Vec::new()
    };

    let (query, values) = super::ChunkQueryBuilder::build(filter, ids)?;

    trace!("chunk SQL query values: {:?}", values);
    trace!("chunk SQL query: {}", query);

    // Do not cache the prepared statement: sqlx caches statements by query text, and
    // the same text is built for operands of different types (e.g. `3` and `3.5`).
    // A cached statement keeps the parameter types of its first execution, so an
    // operand of another type would be silently misread (an integer decoded as a float).
    let mut r = sqlx::query(&query).persistent(false);

    for v in values {
        r = bind_value(r, v);
    }

    let r = r.map(cast_chunk_data).fetch_all(exec.as_exec()).await?;
    r.into_iter().collect()
}

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

/// Binds a query value to the next placeholder, casting it to the matching Postgres type.
fn bind_value(r: PgQuery<'_>, v: query::Value) -> PgQuery<'_> {
    match v {
        query::Value::Integer(v) => r.bind(v),
        query::Value::Float(v) => r.bind(v),
        query::Value::Text(v) => r.bind(v),
        // Cast boolean value to numeric, since for now there is no custom column for boolean values
        query::Value::Boolean(v) => r.bind(if v { 1.0 } else { 0.0 }),
        query::Value::IntegerArray(v) => r.bind(v),
        query::Value::FloatArray(v) => r.bind(v),
        query::Value::TextArray(v) => r.bind(v),
        // Cast boolean array to numeric, since for now there is no custom column for boolean values
        query::Value::BooleanArray(v) => r.bind(
            v.into_iter()
                .map(|b| if b { 1.0 } else { 0.0 })
                .collect::<Vec<f64>>(),
        ),
    }
}

/// Returns the ids of the topics whose aggregated values satisfy every predicate
/// in `filter`. Optionally the search can be restricted to a list of topics (`on_topics`).
pub async fn topic_ids_from_aggregated_filters(
    exec: &mut impl AsExec,
    filter: query::OntologyPredicateGroup<query::Value>,
    on_topics: Option<&Vec<schema::TopicRecord>>,
) -> Result<Vec<i32>, Error> {
    let ids: Vec<i64> = if let Some(topics) = on_topics {
        topics.iter().map(|t| t.topic_id as i64).collect()
    } else {
        Vec::new()
    };

    // The stats table of a field is usually inferred from the operand type (e.g. a text
    // operand for MAX means a textual field), but some aggregators always take a numeric
    // operand whatever the field type: e.g. COUNT on a text column is compared with a number,
    // so its stats must be read from the textual table. For these aggregators the actual
    // kind of the field is looked up in the db. The same lookup tells whether the field is
    // a list, to reject the aggregators not defined on lists.
    let mut field_kinds = HashMap::new();
    // Several predicates can aggregate the same field, its stats are looked up once
    let mut stats_by_field = HashMap::new();
    for predicate in &filter.group {
        let Some(aggr) = predicate.aggregator() else {
            continue;
        };
        if !aggr.requires_numeric_operand() && aggr.supports_list_field() {
            continue;
        }

        let field = predicate.ontology_field();
        let field_str = field.field_str();
        let stats = match stats_by_field.get(&field_str) {
            Some(stats) => *stats,
            None => {
                let stats = field_stats(exec, field).await?;
                stats_by_field.insert(field_str.clone(), stats);
                stats
            }
        };
        let Some((kind, is_list)) = stats else {
            continue;
        };

        if is_list && !aggr.supports_list_field() {
            return Err(query::Error::AggregatorError {
                field: field_str,
                err: query::AggregatorError::UnsupportedListField,
            }
            .into());
        }

        if aggr.requires_numeric_operand() {
            field_kinds.insert(field_str, kind);
        }
    }

    let (query, values) = super::TopicAggregateQueryBuilder::build(filter, ids, &field_kinds)?;

    trace!("aggregated topic SQL query values: {:?}", values);
    trace!("aggregated topic SQL query: {}", query);

    // Do not cache the prepared statement: sqlx caches statements by query text, and
    // the same text is built for operands of different types (e.g. `3` and `3.5`).
    // A cached statement keeps the parameter types of its first execution, so an
    // operand of another type would be silently misread (an integer decoded as a float).
    let mut r = sqlx::query(&query).persistent(false);
    for v in values {
        r = bind_value(r, v);
    }

    let rows = r.fetch_all(exec.as_exec()).await?;
    rows.iter()
        .map(|row| row.try_get("topic_id").map_err(Error::from))
        .collect()
}

/// Returns the kind of the stats stored for `field` and whether they are computed on
/// the elements of a list column, `None` if the field has no stats.
async fn field_stats(
    exec: &mut impl AsExec,
    field: &query::OntologyField,
) -> Result<Option<(super::StatsKind, bool)>, Error> {
    let res = sqlx::query!(
        r#"SELECT
            EXISTS(SELECT 1 FROM column_chunk_textual_t WHERE column_id = column_t.column_id) AS "textual!",
            EXISTS(SELECT 1 FROM column_chunk_numeric_t WHERE column_id = column_t.column_id) AS "numeric!",
            -- every chunk of a column has the same is_list, a single row is enough
            COALESCE(
                (SELECT is_list FROM column_chunk_textual_t WHERE column_id = column_t.column_id LIMIT 1),
                (SELECT is_list FROM column_chunk_numeric_t WHERE column_id = column_t.column_id LIMIT 1),
                FALSE
            ) AS "is_list!"
        FROM column_t
        WHERE ontology_tag = $1 AND column_name = $2"#,
        field.ontology_tag(),
        field.field(),
    )
    .fetch_optional(exec.as_exec())
    .await?;

    Ok(match res {
        Some(r) if r.textual => Some((super::StatsKind::Textual, r.is_list)),
        Some(r) if r.numeric => Some((super::StatsKind::Numeric, r.is_list)),
        _ => None,
    })
}

fn cast_chunk_data(row: PgRow) -> Result<schema::ChunkRecord, Error> {
    Ok(schema::ChunkRecord {
        chunk_id: row.try_get("chunk_id")?,
        chunk_uuid: row.try_get("chunk_uuid")?,
        topic_id: row.try_get("topic_id")?,
        data_file: row.try_get("data_file")?,
        size_bytes: row.try_get("size_bytes")?,
        arrow_size_bytes: row.try_get("arrow_size_bytes")?,
        row_count: row.try_get("row_count")?,
        first_sample_timestamp_ns: row.try_get("first_sample_timestamp_ns")?,
        last_sample_timestamp_ns: row.try_get("last_sample_timestamp_ns")?,
    })
}

/// Returns the largest `size_bytes / row_count` ratio across all chunks belonging to a topic,
/// i.e. the size of the biggest row observed for that topic.
/// Returns `None` if the topic has no chunks (or none with a non-zero row count).
pub async fn topic_chunk_max_avg_row_size(
    exec: &mut impl AsExec,
    topic_id: i32,
) -> Result<Option<i64>, Error> {
    let res = sqlx::query!(
        r#"SELECT MAX(size_bytes / NULLIF(row_count, 0)) as max_row_size
        FROM chunk_t
        WHERE topic_id = $1"#,
        topic_id
    )
    .fetch_one(exec.as_exec())
    .await?;

    Ok(res.max_row_size)
}

/// Returns aggregated size and row count statistics for all chunks belonging to a topic.
pub async fn topic_get_stats(
    exec: &mut impl AsExec,
    topic_id: i32,
) -> Result<types::TopicChunksStats, Error> {
    let res = sqlx::query!(
        r#"SELECT
            COUNT(*) as "chunks_count!",
            COALESCE(SUM(size_bytes), 0)::BIGINT as "total_size_bytes!",
            COALESCE(SUM(row_count), 0)::BIGINT as "total_row_count!",
            COALESCE(
                SUM(COALESCE(arrow_size_bytes, size_bytes))
                    / NULLIF(SUM(row_count), 0),
                0
            )::BIGINT as "avg_bytes_per_row!"
        FROM chunk_t
        WHERE topic_id = $1"#,
        topic_id
    )
    .fetch_one(exec.as_exec())
    .await?;

    Ok(types::TopicChunksStats {
        chunks_count: res.chunks_count as u64,
        total_size_bytes: res.total_size_bytes as u64,
        total_row_count: res.total_row_count as u64,
        avg_bytes_per_row: res.avg_bytes_per_row as u64,
    })
}

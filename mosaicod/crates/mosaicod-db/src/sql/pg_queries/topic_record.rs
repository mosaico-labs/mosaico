use crate::{Error, core::AsExec, sql::schema};
use log::{trace, warn};
use mosaicod_core::types::{self, Resource};
use mosaicod_marshal as marshal;
use mosaicod_query as query;
use sqlx::{Row, postgres::PgRow};

fn cast_topic_data(row: PgRow) -> Result<schema::TopicRecord, Error> {
    Ok(schema::TopicRecord {
        topic_id: row.try_get("topic_id")?,
        topic_uuid: row.try_get("topic_uuid")?,
        locator_name: row.try_get("locator_name")?,
        sequence_id: row.try_get("sequence_id")?,
        session_id: row.try_get("session_id")?,
        ontology_tag: row.try_get("ontology_tag")?,
        serialization_format: row.try_get("serialization_format")?,
        user_metadata: row.try_get("user_metadata")?,
        creation_unix_tstamp: row.try_get("creation_unix_tstamp")?,
        locked: row.try_get("locked")?,
        chunks_number: row.try_get("chunks_number")?,
        total_bytes: row.try_get("total_bytes")?,
        start_index_timestamp: row.try_get("start_index_timestamp")?,
        end_index_timestamp: row.try_get("end_index_timestamp")?,
    })
}

/// Find a topic given its uuid.
pub async fn topic_find_by_ids(
    exe: &mut impl AsExec,
    ids: &[i32],
) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("searching topics with the following ids `{:?}`", ids);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        "SELECT * FROM topic_t WHERE topic_id = ANY($1)",
        ids
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its name.
pub async fn topic_find_by_locator(
    exe: &mut impl AsExec,
    topic: &types::TopicResourceLocator,
) -> Result<schema::TopicRecord, Error> {
    trace!("searching by resource name `{}`", topic);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        "SELECT * FROM topic_t WHERE locator_name=$1",
        topic.locator()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Return all sequences
pub async fn topic_find_all(exe: &mut impl AsExec) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("retrieving all topics");
    Ok(
        sqlx::query_as!(schema::TopicRecord, "SELECT * FROM topic_t")
            .fetch_all(exe.as_exec())
            .await?,
    )
}

/// Deletes a topic record from the database **only if it is unlocked**.
///
/// This function safely removes a topic whose `locked` field is set to `FALSE`.  
/// If the topic is locked or does not exist, the operation has no effect.
pub async fn topic_delete_unlocked(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
) -> Result<(), Error> {
    trace!("deleting (unlocked) topic `{}`", loc);
    sqlx::query!(
        "DELETE FROM topic_t WHERE locator_name=$1 AND locked=FALSE",
        loc.locator()
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

/// Deletes a topic record from the database by its name, **bypassing any lock state**.
///
/// This function requires a [`DataLossToken`] since permanently removes the record
/// from the database without checking whether it is locked or referenced
/// elsewhere. Improper use can lead to data inconsistency or loss.
pub async fn topic_delete(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!("(data loss) deleting topic `{}`", loc);
    sqlx::query!("DELETE FROM topic_t WHERE locator_name=$1", loc.locator())
        .execute(exe.as_exec())
        .await?;
    Ok(())
}

pub async fn topic_create(
    exe: &mut impl AsExec,
    record: &schema::TopicRecord,
) -> Result<schema::TopicRecord, Error> {
    trace!("creating a new topic record {:?}", record);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            INSERT INTO topic_t
                (
                    topic_uuid, sequence_id, session_id, locator_name, creation_unix_tstamp, 
                    serialization_format, ontology_tag, locked, user_metadata, chunks_number,
                    total_bytes, start_index_timestamp, end_index_timestamp
                ) 
            VALUES 
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING 
                *
    "#,
        record.topic_uuid,
        record.sequence_id,
        record.session_id,
        record.locator_name,
        record.creation_unix_tstamp,
        record.serialization_format,
        record.ontology_tag,
        record.locked,
        record.user_metadata,
        record.chunks_number,
        record.total_bytes,
        record.start_index_timestamp,
        record.end_index_timestamp,
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

pub async fn topic_lock(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
) -> Result<(), Error> {
    trace!("locking `{}`", loc);
    sqlx::query!(
        r#"
            UPDATE topic_t 
            SET locked = TRUE
            WHERE locator_name = $1
    "#,
        loc.locator(),
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

pub async fn topic_update_serialization_format(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
    serialization_format: &str,
) -> Result<schema::TopicRecord, Error> {
    trace!(
        "updating serialization_format to `{}` for `{}`",
        serialization_format, loc
    );
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET serialization_format = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        serialization_format,
        loc.locator()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

pub async fn topic_update_ontology_tag(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
    ontology_tag: &str,
) -> Result<schema::TopicRecord, Error> {
    trace!("updating ontology_tag to `{}` for `{}`", ontology_tag, loc);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET ontology_tag = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        ontology_tag,
        loc.locator(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_update_user_metadata(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
    user_metadata: marshal::JsonMetadataBlob,
) -> Result<schema::TopicRecord, Error> {
    trace!("updating user_metadata for `{}`", loc);
    let metadata = serde_json::to_value(user_metadata)?;
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET user_metadata = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        metadata,
        loc.locator(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_update_system_info(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
    system_info: &types::TopicDataInfo,
) -> Result<schema::TopicRecord, Error> {
    trace!("updating system info to `{:?}` for `{}`", system_info, loc);
    let res = sqlx::query_as!(
        schema::TopicRecord,
        r#"
            UPDATE topic_t
            SET chunks_number = $1, total_bytes = $2, start_index_timestamp = $3, end_index_timestamp = $4
            WHERE locator_name = $5
            RETURNING *
    "#,
        system_info.chunks_number as i64,
        system_info.total_bytes as i64,
        system_info.timestamp_range.start.as_i64(),
        system_info.timestamp_range.end.as_i64(),
        loc.locator(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_from_query_filter(
    exe: &mut impl AsExec,
    filter_seq: Option<query::SequenceFilter>,
    filter_top: Option<query::TopicFilter>,
) -> Result<Vec<schema::TopicRecord>, Error> {
    // Return empty vector if there is nothing to filter
    if filter_seq.is_none() && filter_top.is_none() {
        return Ok(Vec::new());
    }

    let select = r#"
        SELECT topic.*
        FROM topic_t topic
        INNER JOIN sequence_t sequence
            ON topic.sequence_id = sequence.sequence_id
    "#;

    let mut qb = query::ClausesCompiler::new();

    let placeholder = query::Placeholder::new();

    let mut sql_fmt = super::SqlQueryCompiler::new(placeholder.clone());
    let mut json_fmt = super::JsonQueryCompiler::new(placeholder);

    if let Some(seq) = filter_seq {
        if let Some(op) = seq.name {
            qb = qb.expr("sequence.locator_name", op, &mut sql_fmt);
        }

        if let Some(op) = seq.creation {
            qb = qb.expr("sequence.creation_unix_tstamp", op, &mut sql_fmt);
        }

        let fmt = json_fmt.with_field("sequence.user_metadata".into());

        for (field, op) in seq.user_metadata {
            qb = qb.expr(&field, op, fmt);
        }
    }

    if let Some(top) = filter_top {
        if let Some(op) = top.name {
            // Substring has a +1 for zero based indexing
            //
            // So using sequence locator '/a/b' and topic locator 'a/b/c/d'
            // the query produces '/c/d', slash included
            qb = qb.expr(
                "SUBSTRING(topic.locator_name, LENGTH(sequence.locator_name) + 1)",
                op,
                &mut sql_fmt,
            );
        }

        if let Some(op) = top.creation {
            qb = qb.expr("topic.creation_unix_tstamp", op, &mut sql_fmt);
        }

        if let Some(op) = top.ontology_tag {
            qb = qb.expr("topic.ontology_tag", op, &mut sql_fmt);
        }

        if let Some(op) = top.serialization_format {
            qb = qb.expr("topic.serialization_format", op, &mut sql_fmt);
        }

        let fmt = json_fmt.with_field("topic.user_metadata".into());

        for (field, op) in top.user_metadata {
            qb = qb.expr(&field, op, fmt);
        }
    }

    let qr = qb.compile()?;

    // If the query has no filters skip, to avoid retuning too mutch elements
    if qr.is_unfiltered() {
        return Ok(Vec::new());
    }

    // Since we have do an early-return is the query is unfiltered there is always a WHERE clause
    let query = format!("{select} WHERE {}", qr.clauses.join(" AND "));

    trace!("query values: {:?}", qr.values);
    trace!("generated SQL query: {}", query);

    let mut r = sqlx::query(&query);

    for v in qr.values.into_iter() {
        match v {
            query::Value::Integer(v) => r = r.bind(v),
            query::Value::Float(v) => r = r.bind(v),
            query::Value::Text(v) => r = r.bind(v),
            query::Value::Boolean(v) => r = r.bind(v),
        }
    }

    let r = r.map(cast_topic_data).fetch_all(exe.as_exec()).await?;
    trace!("query returned {} results", r.len());
    r.into_iter().collect()
}

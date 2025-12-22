use log::trace;
use sqlx::{Row, postgres::PgRow};

use crate::{
    marshal, query,
    repo::{self, sql_models},
    types::{self, Resource},
};

fn cast_topic_data(row: PgRow) -> Result<sql_models::TopicRecord, repo::Error> {
    Ok(sql_models::TopicRecord {
        topic_id: row.try_get("topic_id")?,
        topic_uuid: row.try_get("topic_uuid")?,
        locator_name: row.try_get("locator_name")?,
        sequence_id: row.try_get("sequence_id")?,
        ontology_tag: row.try_get("ontology_tag")?,
        serialization_format: row.try_get("serialization_format")?,
        user_metadata: row.try_get("user_metadata")?,
        creation_unix_tstamp: row.try_get("creation_unix_tstamp")?,
        locked: row.try_get("locked")?,
    })
}

/// Find a sequence given its uuid.
pub async fn topic_find_by_ids(
    exe: &mut impl repo::AsExec,
    ids: &[i32],
) -> Result<Vec<sql_models::TopicRecord>, repo::Error> {
    trace!("searching topics with the following ids `{:?}`", ids);
    let res = sqlx::query_as!(
        sql_models::TopicRecord,
        "SELECT * FROM topic_t WHERE topic_id = ANY($1)",
        ids
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its name.
pub async fn topic_find_by_locator(
    exe: &mut impl repo::AsExec,
    topic: &types::TopicResourceLocator,
) -> Result<sql_models::TopicRecord, repo::Error> {
    trace!("searching by resource name `{}`", topic);
    let res = sqlx::query_as!(
        sql_models::TopicRecord,
        "SELECT * FROM topic_t WHERE locator_name=$1",
        topic.name()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Return all sequences
pub async fn topic_find_all(
    exe: &mut impl repo::AsExec,
) -> Result<Vec<sql_models::TopicRecord>, repo::Error> {
    trace!("retrieving all topics");
    Ok(
        sqlx::query_as!(sql_models::TopicRecord, "SELECT * FROM topic_t")
            .fetch_all(exe.as_exec())
            .await?,
    )
}

/// Deletes a topic record from the repository **only if it is unlocked**.
///
/// This function safely removes a topic whose `locked` field is set to `FALSE`.  
/// If the topic is locked or does not exist, the operation has no effect.
pub async fn topic_delete_unlocked(
    exe: &mut impl repo::AsExec,
    loc: &types::TopicResourceLocator,
) -> Result<(), repo::Error> {
    trace!("deleting (unlocked) topic `{}`", loc);
    sqlx::query!(
        "DELETE FROM topic_t WHERE locator_name=$1 AND locked=FALSE",
        loc.name()
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

/// Deletes a topic record from the repository by its name, **bypassing any lock state**.
///
/// This function is marked `unsafe` because it permanently removes the record
/// from the database without checking whether it is locked or referenced
/// elsewhere. Improper use can lead to data inconsistency or loss.
pub async unsafe fn topic_delete(
    exe: &mut impl repo::AsExec,
    loc: &types::TopicResourceLocator,
) -> Result<(), repo::Error> {
    trace!("(unsafe) deleting `{}`", loc);
    sqlx::query!("DELETE FROM topic_t WHERE locator_name=$1", loc.name())
        .execute(exe.as_exec())
        .await?;
    Ok(())
}

pub async fn topic_create(
    exe: &mut impl repo::AsExec,
    record: &sql_models::TopicRecord,
) -> Result<sql_models::TopicRecord, repo::Error> {
    trace!("creating a new topic record {:?}", record);
    let res = sqlx::query_as!(
        sql_models::TopicRecord,
        r#"
            INSERT INTO topic_t
                (
                    topic_uuid, sequence_id, locator_name, creation_unix_tstamp, 
                    serialization_format, ontology_tag, locked, user_metadata
                ) 
            VALUES 
                ($1, $2, $3, $4, $5, $6, $7, $8) 
            RETURNING 
                *
    "#,
        record.topic_uuid,
        record.sequence_id,
        record.locator_name,
        record.creation_unix_tstamp,
        record.serialization_format,
        record.ontology_tag,
        record.locked,
        record.user_metadata
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

pub async fn topic_lock(
    exe: &mut impl repo::AsExec,
    loc: &types::TopicResourceLocator,
) -> Result<(), repo::Error> {
    trace!("locking `{}`", loc);
    sqlx::query!(
        r#"
            UPDATE topic_t 
            SET locked = TRUE
            WHERE locator_name = $1
    "#,
        loc.name(),
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

pub async fn topic_update_serialization_format(
    exe: &mut impl repo::AsExec,
    loc: &types::TopicResourceLocator,
    serialization_format: &str,
) -> Result<sql_models::TopicRecord, repo::Error> {
    trace!(
        "updating serialization_format to `{}` for `{}`",
        serialization_format, loc
    );
    let res = sqlx::query_as!(
        sql_models::TopicRecord,
        r#"
            UPDATE topic_t
            SET serialization_format = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        serialization_format,
        loc.name()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

pub async fn topic_update_ontology_tag(
    exe: &mut impl repo::AsExec,
    loc: &types::TopicResourceLocator,
    ontology_tag: &str,
) -> Result<sql_models::TopicRecord, repo::Error> {
    trace!("updating ontology_tag to `{}` for `{}`", ontology_tag, loc);
    let res = sqlx::query_as!(
        sql_models::TopicRecord,
        r#"
            UPDATE topic_t
            SET ontology_tag = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        ontology_tag,
        loc.name(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_update_user_metadata(
    exe: &mut impl repo::AsExec,
    loc: &types::TopicResourceLocator,
    user_metadata: marshal::JsonMetadataBlob,
) -> Result<sql_models::TopicRecord, repo::Error> {
    trace!("updating user_metadata for `{}`", loc);
    let metadata = serde_json::to_value(user_metadata)?;
    let res = sqlx::query_as!(
        sql_models::TopicRecord,
        r#"
            UPDATE topic_t
            SET user_metadata = $1
            WHERE locator_name = $2
            RETURNING * 
    "#,
        metadata,
        loc.name(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

pub async fn topic_from_query_filter(
    exe: &mut impl repo::AsExec,
    filter_seq: Option<query::SequenceFilter>,
    filter_top: Option<query::TopicFilter>,
) -> Result<Vec<sql_models::TopicRecord>, repo::Error> {
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
    let mut sql_fmt = super::SqlQueryCompiler::new();
    let mut json_fmt = super::JsonQueryCompiler::new();

    if let Some(seq) = filter_seq {
        if let Some(op) = seq.name {
            qb = qb.expr("sequence.locator_name", op, &mut sql_fmt);
        }

        if let Some(op) = seq.creation {
            qb = qb.expr("sequence.creation_unix_tstamp", op, &mut sql_fmt);
        }

        if let Some(mdata) = seq.user_metadata {
            qb = qb.filter(
                mdata.into_expr_group(),
                json_fmt.with_field_and_placeholder(
                    "sequence.user_metadata".into(),
                    sql_fmt.current_placeholder(),
                ),
            );
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

        if let Some(mdata) = top.user_metadata {
            qb = qb.filter(
                mdata.into_expr_group(),
                json_fmt.with_field_and_placeholder(
                    "topic.user_metadata".into(),
                    sql_fmt.current_placeholder(),
                ),
            );
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

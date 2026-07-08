use crate::{Error, core::AsExec, sql::schema};
use log::trace;
use mosaicod_core::types;

/// Add topics not yet optimized to topic_optimization_t table.
pub async fn topic_update_optimization_list(exe: &mut impl AsExec) -> Result<u64, Error> {
    trace!("updating topic optimization list");

    let res = sqlx::query!(
        r#"INSERT INTO topic_optimization_t (topic_id)
           SELECT topic_id
           FROM topic_t
           WHERE optimization_end_unix_tstamp IS NULL
           ON CONFLICT (topic_id) DO NOTHING
           "#
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected())
}

/// Retrieves the first topic inside topic_optimization_t table for which the optimization process has not yet started.
///
/// Note: the row containing the returned record is locked by default.
pub async fn topic_next_to_be_optimized(
    exe: &mut impl AsExec,
) -> Result<Option<schema::TopicOptimizationRecord>, Error> {
    trace!("updating topic optimization list");

    Ok(sqlx::query_as!(
        schema::TopicOptimizationRecord,
        r#"SELECT topic_optimization_t.*
           FROM topic_optimization_t
           WHERE start_unix_tstamp IS NULL
           ORDER BY topic_id ASC
           LIMIT 1
           FOR UPDATE SKIP LOCKED
           "#,
    )
    .fetch_optional(exe.as_exec())
    .await?)
}

pub async fn topic_start_optimization(
    exe: &mut impl AsExec,
    topic_id: i32,
    start_timestamp: types::Timestamp,
    opt_path_in_store: types::TopicPathInStore,
) -> Result<Option<schema::TopicOptimizationRecord>, Error> {
    trace!(
        "updating topic optimization start timestamp and path in store for topic with id {}",
        topic_id
    );

    Ok(sqlx::query_as!(
        schema::TopicOptimizationRecord,
        r#"UPDATE topic_optimization_t
           SET start_unix_tstamp = $1, opt_path_in_store = $2
           WHERE topic_id = $3
           RETURNING *
           "#,
        Some(start_timestamp.as_i64()),
        Some(String::from(opt_path_in_store)),
        topic_id
    )
    .fetch_optional(exe.as_exec())
    .await?)
}

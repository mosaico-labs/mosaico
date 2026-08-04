use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;
use tracing::{trace, warn};

/// Deletes from the optimization table topics for which the optimization routine appears to take too long.
pub async fn topic_optimization_delete_stale(
    exe: &mut impl AsExec,
    threshold: types::Timestamp,
) -> Result<u64, Error> {
    trace!("topic_optimization delete stale");

    let res = sqlx::query!(
        r#"DELETE FROM topic_optimization_t
           WHERE start_unix_tstamp < $1
           "#,
        threshold.as_i64()
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected())
}

/// Add completed topics, but not yet optimized, to topic_optimization_t table.
pub async fn topic_update_optimization_list(exe: &mut impl AsExec) -> Result<u64, Error> {
    trace!("updating topic optimization list");

    let res = sqlx::query!(
        r#"INSERT INTO topic_optimization_t (topic_id)
           SELECT topic_id
           FROM topic_t
           WHERE completion_unix_tstamp IS NOT NULL AND optimization_end_unix_tstamp IS NULL
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
) -> Result<schema::TopicOptimizationRecord, Error> {
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
    .fetch_one(exe.as_exec())
    .await?)
}

/// Returns the number of topics in the optimization list.
///
/// This is used for testing purposes.
pub async fn topic_optimization_count(exe: &mut impl AsExec) -> Result<i64, Error> {
    trace!("topic optimization count");

    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!"
           FROM topic_optimization_t
           "#,
    )
    .fetch_one(exe.as_exec())
    .await?)
}

pub async fn topic_optimization_delete(
    exe: &mut impl AsExec,
    topic_id: i32,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!(
        "(data loss) deleting topic with id {} from optimization list",
        topic_id
    );
    let result = sqlx::query!(
        r#"DELETE FROM topic_optimization_t WHERE topic_id=$1"#,
        topic_id
    )
    .execute(exe.as_exec())
    .await?;

    if result.rows_affected() == 0 {
        return Err(Error::NotFound);
    }

    Ok(())
}

/// Search for a topic with the given path_in_store.
pub async fn topic_optimization_find_path_in_store(
    exe: &mut impl AsExec,
    path_in_store: &str,
) -> Result<bool, Error> {
    trace!(
        "searching if path_in_store `{}` is assigned to a topic optimization",
        path_in_store
    );
    let found: bool = sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM topic_optimization_t WHERE opt_path_in_store=$1) as "found!""#,
        path_in_store
    )
        .fetch_one(exe.as_exec())
        .await?;

    Ok(found)
}

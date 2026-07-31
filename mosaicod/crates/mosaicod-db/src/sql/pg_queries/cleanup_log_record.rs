use crate::{
    Error,
    core::{AsExec, Tx},
    sql::schema,
};
use tracing::trace;

/// Acquires a transaction-level advisory lock to prevent race conditions. The caller must run
/// this on a connection held open for the duration of a single transaction (see `AsExec` impl
/// for `Tx`): if run through a bare pooled connection, each statement below is auto-committed
/// separately, and the lock is useless.
pub async fn acquire_transaction_lock(exe: &mut Tx<'_>, lock_id: i64) -> Result<(), Error> {
    sqlx::query!(r#"SELECT pg_advisory_xact_lock($1)"#, lock_id)
        .execute(exe.as_exec())
        .await?;

    Ok(())
}

/// Creates a new cleanup log entry, leaving end timestamp column empty.
pub async fn cleanup_log_create(
    exe: &mut impl AsExec,
    start_unix_tstamp_secs: i64,
) -> Result<schema::CleanupLogRecord, Error> {
    trace!(
        "creating a new cleanup log record starting at {}",
        start_unix_tstamp_secs
    );

    let res = sqlx::query_as!(
        schema::CleanupLogRecord,
        r#"
            INSERT INTO cleanup_log_t (start_unix_tstamp_secs, marked_folders, deleted_folders, failed_folders)
            VALUES ($1, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb)
            RETURNING *
            "#,
        start_unix_tstamp_secs,
    )
        .fetch_one(exe.as_exec())
        .await?;

    Ok(res)
}

/// Retrieves cleanup log history.
///
/// [`limit`] sets the number of rows to retrieve.
///
/// Returns an empty vector if no cleanup operation has been started yet.
/// First item in the vector is the most recent log.
pub async fn cleanup_log_history(
    exe: &mut impl AsExec,
    limit: u16,
) -> Result<Vec<schema::CleanupLogRecord>, Error> {
    trace!(
        "retrieving cleanup log history, limited to {} records",
        limit
    );

    let res = sqlx::query_as!(
        schema::CleanupLogRecord,
        r#"SELECT * FROM cleanup_log_t ORDER BY start_unix_tstamp_secs DESC LIMIT $1"#,
        limit as i64
    )
    .fetch_all(exe.as_exec())
    .await?;

    Ok(res)
}

/// Retrieves the most recent cleanup log (even if it's still running).
///
/// Returns None if no cleanup operation has been started yet.
pub async fn cleanup_log_latest(
    exe: &mut impl AsExec,
) -> Result<Option<schema::CleanupLogRecord>, Error> {
    Ok(cleanup_log_history(exe, 1).await?.into_iter().next())
}

/// Closes the given cleanup log entry, setting its end timestamp.
///
/// Returns False if the log was already closed, True otherwise.
pub async fn cleanup_log_close(
    exe: &mut impl AsExec,
    cleanup_id: i32,
    end_unix_tstamp_secs: i64,
    marked_folders: &Vec<String>,
    deleted_folders: &Vec<String>,
    failed_folders: &Vec<(String, String)>,
) -> Result<bool, Error> {
    trace!(
        "closing cleanup log {}. End timestamp: `{}`",
        cleanup_id, end_unix_tstamp_secs
    );
    let res = sqlx::query!(
        "UPDATE cleanup_log_t
         SET end_unix_tstamp_secs = $1, marked_folders = $2, deleted_folders = $3, failed_folders = $4
         WHERE cleanup_id = $5 AND end_unix_tstamp_secs IS NULL",
        end_unix_tstamp_secs,
        serde_json::to_value(marked_folders)?,
        serde_json::to_value(deleted_folders)?,
        serde_json::to_value(failed_folders)?,
        cleanup_id,
    )
        .execute(exe.as_exec())
        .await?;

    Ok(res.rows_affected() != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DatabaseType, testing};
    use sqlx::Pool;

    #[sqlx::test]
    async fn test_cleanup_log_create(pool: Pool<DatabaseType>) -> sqlx::Result<()> {
        let start_ts = chrono::Utc::now().timestamp();
        let database = testing::Database::new(pool);
        let rrecord = cleanup_log_create(&mut database.connection(), start_ts)
            .await
            .unwrap();

        assert_eq!(start_ts, rrecord.start_unix_tstamp_secs);
        assert!(rrecord.end_unix_tstamp_secs.is_none());
        assert!(rrecord.marked_folders().is_empty());
        assert!(rrecord.deleted_folders().is_empty());
        assert!(rrecord.failed_folders().is_empty());

        Ok(())
    }

    #[sqlx::test]
    async fn test_cleanup_log_latest(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        let mut cx = database.connection();

        // Check with no logs.
        assert!(cleanup_log_latest(&mut cx).await.unwrap().is_none());

        // Check with one log.
        let start_ts = chrono::Utc::now().timestamp();
        cleanup_log_create(&mut cx, start_ts).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 1);
        assert!(latest_log.end_unix_tstamp_secs.is_none());

        // Check with more than one log.
        for _ in 0..8 {
            cleanup_log_create(&mut cx, chrono::Utc::now().timestamp())
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        let start_ts = chrono::Utc::now().timestamp();
        cleanup_log_create(&mut cx, start_ts).await.unwrap();

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 10);
        assert_eq!(latest_log.start_unix_tstamp_secs, start_ts);
        assert!(latest_log.end_unix_tstamp_secs.is_none());

        // Check with end timestamp set.

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let record = cleanup_log_create(&mut cx, chrono::Utc::now().timestamp())
            .await
            .unwrap();
        assert_eq!(record.cleanup_id, 11);

        let end_unix_ts = chrono::Utc::now().timestamp();
        assert!(
            cleanup_log_close(
                &mut cx,
                record.cleanup_id,
                end_unix_ts,
                &vec![],
                &vec![],
                &vec![]
            )
            .await
            .unwrap()
        );

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 11);
        assert_eq!(
            latest_log.start_unix_tstamp_secs,
            record.start_unix_tstamp_secs
        );
        assert_eq!(latest_log.end_unix_tstamp_secs.unwrap(), end_unix_ts);
    }

    #[sqlx::test]
    async fn test_cleanup_log_close(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        let mut cx = database.connection();

        let start_unix_ts = chrono::Utc::now().timestamp();

        let record = cleanup_log_create(&mut cx, start_unix_ts).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let end_unix_ts = chrono::Utc::now().timestamp();
        assert!(
            cleanup_log_close(
                &mut cx,
                record.cleanup_id,
                end_unix_ts,
                &vec![],
                &vec![],
                &vec![]
            )
            .await
            .unwrap()
        );

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 1);
        assert_eq!(latest_log.start_unix_tstamp_secs, start_unix_ts);
        assert_eq!(latest_log.end_unix_tstamp_secs.unwrap(), end_unix_ts);
    }
}

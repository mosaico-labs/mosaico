use crate::{Error, core::AsExec, sql::schema};
use log::trace;

/// Tries to create a new cleanup log entry, leaving end timestamp column empty.
/// A new cleanup log is added if the last one is older than [`start_unix_tstamp_secs`] - [`time_interval_secs`].
pub async fn cleanup_log_try_create(
    exe: &mut impl AsExec,
    start_unix_tstamp_secs: i64,
    time_interval_secs: i64,
) -> Result<Option<schema::CleanupLogRecord>, Error> {
    trace!(
        "trying to create a new cleanup log record starting at {}",
        start_unix_tstamp_secs
    );

    // Acquire a transaction-level advisory lock to prevent race conditions.
    sqlx::query!("SELECT pg_advisory_xact_lock(777777)")
        .execute(exe.as_exec())
        .await?;

    let res = sqlx::query_as!(
        schema::CleanupLogRecord,
        r#"
            INSERT INTO cleanup_log_t (start_unix_tstamp_secs, marked_folders, deleted_folders, failed_folders)
            SELECT $1, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb
            WHERE NOT EXISTS (
                SELECT 1 FROM cleanup_log_t
                WHERE start_unix_tstamp_secs > ($1::bigint - $2::bigint)
            )
            RETURNING *
    "#,
        start_unix_tstamp_secs,
        time_interval_secs
    )
    .fetch_optional(exe.as_exec())
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

/// Closes the currently running cleanup log setting its end timestamp.
///
/// Returns False if the last log was already closed, True otherwise.
pub async fn cleanup_log_close(
    exe: &mut impl AsExec,
    end_unix_tstamp_secs: i64,
    marked_folders: &Vec<String>,
    deleted_folders: &Vec<String>,
    failed_folders: &Vec<(String, String)>,
) -> Result<bool, Error> {
    trace!(
        "closing last cleanup log. End timestamp: `{}`",
        end_unix_tstamp_secs
    );
    let res = sqlx::query!(
        "UPDATE cleanup_log_t
         SET end_unix_tstamp_secs = $1, marked_folders = $2, deleted_folders = $3, failed_folders = $4
         WHERE cleanup_id = (SELECT cleanup_id FROM cleanup_log_t ORDER BY cleanup_id DESC LIMIT 1) AND end_unix_tstamp_secs IS NULL",
        end_unix_tstamp_secs,
        serde_json::to_value(marked_folders)?,
        serde_json::to_value(deleted_folders)?,
        serde_json::to_value(failed_folders)?,
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
    async fn test_cleanup_log_try_create(pool: Pool<DatabaseType>) -> sqlx::Result<()> {
        let start_ts = chrono::Utc::now().timestamp();
        let database = testing::Database::new(pool);
        let rrecord = cleanup_log_try_create(&mut database.connection(), start_ts, 1)
            .await
            .unwrap()
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
        cleanup_log_try_create(&mut cx, start_ts, 1).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 1);
        assert!(latest_log.end_unix_tstamp_secs.is_none());

        // Check with more than one log.
        for _ in 0..8 {
            cleanup_log_try_create(&mut cx, chrono::Utc::now().timestamp(), 0)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        let start_ts = chrono::Utc::now().timestamp();
        cleanup_log_try_create(&mut cx, start_ts, 0).await.unwrap();

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 10);
        assert_eq!(latest_log.start_unix_tstamp_secs, start_ts);
        assert!(latest_log.end_unix_tstamp_secs.is_none());

        // Check with end timestamp set.

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let record = cleanup_log_try_create(&mut cx, chrono::Utc::now().timestamp(), 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.cleanup_id, 11);

        let end_unix_ts = chrono::Utc::now().timestamp();
        assert!(
            cleanup_log_close(&mut cx, end_unix_ts, &vec![], &vec![], &vec![])
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

        cleanup_log_try_create(&mut cx, start_unix_ts, 0)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let end_unix_ts = chrono::Utc::now().timestamp();
        assert!(
            cleanup_log_close(&mut cx, end_unix_ts, &vec![], &vec![], &vec![])
                .await
                .unwrap()
        );

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 1);
        assert_eq!(latest_log.start_unix_tstamp_secs, start_unix_ts);
        assert_eq!(latest_log.end_unix_tstamp_secs.unwrap(), end_unix_ts);
    }
}

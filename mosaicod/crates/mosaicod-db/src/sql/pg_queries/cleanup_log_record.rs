use crate::{Error, core::AsExec, sql::schema};
use log::trace;

pub enum Table {
    Sequence,
    Topic,
}

/// Creates a new cleanup log entry, leaving end timestamp column empty.
pub async fn cleanup_log_create(
    exe: &mut impl AsExec,
    record: &schema::CleanupLogRecord,
) -> Result<schema::CleanupLogRecord, Error> {
    trace!("creating a new cleanup log record {:?}", record);
    let res = sqlx::query_as!(
        schema::CleanupLogRecord,
        r#"
            INSERT INTO cleanup_log_t (start_unix_tstamp_secs, end_unix_tstamp_secs)
            VALUES ($1, $2)
            RETURNING *
    "#,
        record.start_unix_tstamp_secs,
        record.end_unix_tstamp_secs,
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

/// Retrieves the most recent cleanup log (even if it's still running).
///
/// Returns None if no cleanup operation has been started yet.
pub async fn cleanup_log_latest(
    exe: &mut impl AsExec,
) -> Result<Option<schema::CleanupLogRecord>, Error> {
    trace!("retrieving latest cleanup log");

    let res = sqlx::query_as!(
        schema::CleanupLogRecord,
        "SELECT * FROM cleanup_log_t ORDER BY start_unix_tstamp_secs DESC LIMIT 1"
    )
    .fetch_optional(exe.as_exec())
    .await?;

    Ok(res)
}

/// Closes the currently running cleanup log setting its end timestamp.
///
/// Returns False if the last log was already closed, True otherwise.
pub async fn cleanup_log_close(
    exe: &mut impl AsExec,
    end_unix_tstamp_secs: i64,
    marked_folders: Option<&Vec<String>>,
    deleted_folders: Option<&Vec<String>>,
    failed_folders: Option<&Vec<(String, String)>>,
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
    async fn test_cleanup_log_create(pool: Pool<DatabaseType>) -> sqlx::Result<()> {
        let record = schema::CleanupLogRecord::default();
        let database = testing::Database::new(pool);
        let rrecord = cleanup_log_create(&mut database.connection(), &record)
            .await
            .unwrap();

        assert_eq!(
            record.start_unix_tstamp_secs,
            rrecord.start_unix_tstamp_secs
        );
        assert_eq!(record.end_unix_tstamp_secs, rrecord.end_unix_tstamp_secs);

        Ok(())
    }

    #[sqlx::test]
    async fn test_cleanup_log_latest(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        let mut cx = database.connection();

        // Check with no logs.
        assert!(cleanup_log_latest(&mut cx).await.unwrap().is_none());

        // Check with one log.
        let record = schema::CleanupLogRecord::default();
        cleanup_log_create(&mut cx, &record).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 1);
        assert!(latest_log.end_unix_tstamp_secs.is_none());

        // Check with more than one log.
        for _ in 0..8 {
            let record = schema::CleanupLogRecord::default();
            cleanup_log_create(&mut cx, &record).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        let record = schema::CleanupLogRecord::default();
        cleanup_log_create(&mut cx, &record).await.unwrap();

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 10);
        assert_eq!(
            latest_log.start_unix_tstamp_secs,
            record.start_unix_tstamp_secs
        );
        assert!(latest_log.end_unix_tstamp_secs.is_none());

        // Check with end timestamp set.

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let record = schema::CleanupLogRecord::default();
        let record = cleanup_log_create(&mut cx, &record).await.unwrap();
        assert_eq!(record.cleanup_id, 11);

        let end_unix_ts = chrono::Utc::now().timestamp();
        assert!(
            cleanup_log_close(&mut cx, end_unix_ts, None, None, None)
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

        let record = schema::CleanupLogRecord::default();
        cleanup_log_create(&mut cx, &record).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let end_unix_ts = chrono::Utc::now().timestamp();
        assert!(
            cleanup_log_close(&mut cx, end_unix_ts, None, None, None)
                .await
                .unwrap()
        );

        let latest_log = cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert_eq!(latest_log.cleanup_id, 1);
        assert_eq!(
            latest_log.start_unix_tstamp_secs,
            record.start_unix_tstamp_secs
        );
        assert_eq!(latest_log.end_unix_tstamp_secs.unwrap(), end_unix_ts);
    }
}

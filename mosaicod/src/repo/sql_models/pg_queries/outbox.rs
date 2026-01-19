use crate::repo::{self, sql_models};
use sqlx::Row;

/// Inserts a new entry into the chunks outbox table.
pub async fn outbox_create(
    exec: &mut impl repo::AsExec,
    entry: &sql_models::ChunkOutboxEntry,
) -> Result<sql_models::ChunkOutboxEntry, repo::Error> {
    let row = sqlx::query(
        r#"INSERT INTO chunks_outbox (chunk_id, s3_path, parquet_data)
        VALUES ($1, $2, $3)
        RETURNING
            outbox_id,
            chunk_id,
            s3_path,
            parquet_data,
            created_at,
            processed_at,
            error_message,
            retry_count"#,
    )
    .bind(entry.chunk_id)
    .bind(&entry.s3_path)
    .bind(&entry.parquet_data)
    .fetch_one(exec.as_exec())
    .await?;

    Ok(sql_models::ChunkOutboxEntry {
        outbox_id: row.try_get("outbox_id")?,
        chunk_id: row.try_get("chunk_id")?,
        s3_path: row.try_get("s3_path")?,
        parquet_data: row.try_get("parquet_data")?,
        created_at: row.try_get("created_at")?,
        processed_at: row.try_get("processed_at")?,
        error_message: row.try_get("error_message")?,
        retry_count: row.try_get("retry_count")?,
    })
}

/// Fetches pending outbox entries for processing.
/// Uses `FOR UPDATE SKIP LOCKED` to allow concurrent workers safely.
pub async fn outbox_fetch_pending(
    exec: &mut impl repo::AsExec,
    batch_size: i32,
    max_retries: i32,
) -> Result<Vec<sql_models::ChunkOutboxEntry>, repo::Error> {
    let rows = sqlx::query(
        r#"SELECT
            outbox_id,
            chunk_id,
            s3_path,
            parquet_data,
            created_at,
            processed_at,
            error_message,
            retry_count
        FROM chunks_outbox
        WHERE processed_at IS NULL
          AND retry_count < $2
        ORDER BY created_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED"#,
    )
    .bind(batch_size)
    .bind(max_retries)
    .fetch_all(exec.as_exec())
    .await?;

    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        entries.push(sql_models::ChunkOutboxEntry {
            outbox_id: row.try_get("outbox_id")?,
            chunk_id: row.try_get("chunk_id")?,
            s3_path: row.try_get("s3_path")?,
            parquet_data: row.try_get("parquet_data")?,
            created_at: row.try_get("created_at")?,
            processed_at: row.try_get("processed_at")?,
            error_message: row.try_get("error_message")?,
            retry_count: row.try_get("retry_count")?,
        });
    }
    Ok(entries)
}

/// Marks an outbox entry as successfully processed.
/// Clears the parquet_data to free up storage.
pub async fn outbox_mark_processed(
    exec: &mut impl repo::AsExec,
    outbox_id: i32,
) -> Result<(), repo::Error> {
    sqlx::query(
        r#"UPDATE chunks_outbox
        SET processed_at = NOW(), parquet_data = NULL
        WHERE outbox_id = $1"#,
    )
    .bind(outbox_id)
    .execute(exec.as_exec())
    .await?;
    Ok(())
}

/// Records a failed processing attempt with error message.
pub async fn outbox_mark_failed(
    exec: &mut impl repo::AsExec,
    outbox_id: i32,
    error_message: &str,
) -> Result<(), repo::Error> {
    sqlx::query(
        r#"UPDATE chunks_outbox
        SET retry_count = retry_count + 1, error_message = $2
        WHERE outbox_id = $1"#,
    )
    .bind(outbox_id)
    .bind(error_message)
    .execute(exec.as_exec())
    .await?;
    Ok(())
}

/// Deletes old processed entries for cleanup.
/// Returns the number of deleted entries.
pub async fn outbox_cleanup_processed(
    exec: &mut impl repo::AsExec,
    older_than_hours: i32,
) -> Result<u64, repo::Error> {
    let result = sqlx::query(
        r#"DELETE FROM chunks_outbox
        WHERE processed_at IS NOT NULL
          AND processed_at < NOW() - ($1 || ' hours')::INTERVAL"#,
    )
    .bind(older_than_hours.to_string())
    .execute(exec.as_exec())
    .await?;
    Ok(result.rows_affected())
}

/// Returns the count of pending (unprocessed) outbox entries.
pub async fn outbox_count_pending(exec: &mut impl repo::AsExec) -> Result<i64, repo::Error> {
    let row =
        sqlx::query(r#"SELECT COUNT(*) as count FROM chunks_outbox WHERE processed_at IS NULL"#)
            .fetch_one(exec.as_exec())
            .await?;
    Ok(row.try_get("count")?)
}

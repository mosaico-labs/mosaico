use crate::{
    Error,
    core::{AsExec, RowLocking},
    sql::schema,
};
use log::{trace, warn};
use mosaicod_core::types;

fn rowlock2str(rl: RowLocking) -> &'static str {
    match rl {
        RowLocking::None => "",
        RowLocking::Shared => "FOR SHARE",
        RowLocking::Exclusive => "FOR UPDATE",
    }
}

pub async fn session_create(
    exe: &mut impl AsExec,
    locator: &types::SessionLocator,
) -> Result<schema::SessionRecord, Error> {
    trace!("creating a new session with locator: {}", locator);

    let res = sqlx::query_as!(
        schema::SessionRecord,
        r#"
            INSERT INTO session_t (locator_name, session_uuid, creation_unix_tstamp, sequence_id)
            SELECT $1, $2, $3, seq.sequence_id
            FROM sequence_t AS seq
            WHERE seq.locator_name = $4
            RETURNING *
            "#,
        locator.to_string(),
        uuid::Uuid::from(types::Uuid::new()),
        types::Timestamp::now().as_i64(),
        locator.sequence.to_string()
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

/// Find a sequence given its id.
pub async fn session_find_by_id(
    exe: &mut impl AsExec,
    id: i32,
    row_locking: RowLocking,
) -> Result<schema::SessionRecord, Error> {
    trace!("searching session by id `{}`", id);

    let query = format!(
        "SELECT * FROM session_t WHERE session_id=$1 {}",
        rowlock2str(row_locking)
    );

    let res = sqlx::query_as::<_, schema::SessionRecord>(&query)
        .bind(id)
        .fetch_one(exe.as_exec())
        .await?;

    Ok(res)
}

/// Find a sequence given its uuid.
pub async fn session_find_by_uuid(
    exe: &mut impl AsExec,
    uuid: &types::Uuid,
    row_locking: RowLocking,
) -> Result<schema::SessionRecord, Error> {
    trace!("searching session by uuid `{}`", uuid);

    let query = format!(
        "SELECT * FROM session_t WHERE session_uuid=$1 {}",
        rowlock2str(row_locking)
    );

    let res = sqlx::query_as::<_, schema::SessionRecord>(&query)
        .bind(uuid.as_ref())
        .fetch_one(exe.as_exec())
        .await?;

    Ok(res)
}

/// Find a sequence given its locator.
pub async fn session_find_by_locator(
    exe: &mut impl AsExec,
    session_locator: &types::SessionLocator,
) -> Result<schema::SessionRecord, Error> {
    trace!("searching session by locator name `{}`", session_locator);
    let res = sqlx::query_as!(
        schema::SessionRecord,
        "SELECT * FROM session_t WHERE locator_name=$1",
        session_locator.to_string()
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

/// Returns true if the session has already been finalized.
pub async fn session_finalized(exe: &mut impl AsExec, session_id: i32) -> Result<bool, Error> {
    trace!("session (id=`{}`) locked? ", session_id);
    let finalized = sqlx::query_scalar!(
        r#"SELECT (completion_unix_tstamp IS NOT NULL) AS "finalized!" FROM session_t WHERE session_id=$1"#,
        session_id
    )
        .fetch_one(exe.as_exec())
        .await?;
    Ok(finalized)
}

/// Deletes a session record from the database by its UUID, **bypassing any lock state**.
///
/// This function requires a [`DataLossToken`] because it permanently removes the record from the database
/// elsewhere. Improper use can lead to data inconsistency or loss.
pub async fn session_delete(
    exe: &mut impl AsExec,
    locator: &types::SessionLocator,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!("(data loss) deleting session `{}`", locator);

    let result = sqlx::query!(
        "DELETE FROM session_t WHERE locator_name=$1",
        locator.to_string()
    )
    .execute(exe.as_exec())
    .await?;

    if result.rows_affected() == 0 {
        return Err(Error::NotFound);
    }

    Ok(())
}

/// Find all topic associated with a session
pub async fn session_find_all_topics(
    exe: &mut impl AsExec,
    id: i32,
) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("searching topics for session with id `{}`", id);
    Ok(sqlx::query_as!(
        schema::TopicRecord,
        r#"SELECT * FROM topic_t WHERE session_id = $1"#,
        id,
    )
    .fetch_all(exe.as_exec())
    .await?)
}

/// Tries to update completion_unix_tstamp column for the given session.
///
/// Returns False if the value was already set, otherwise True.
pub async fn session_finalize(
    exe: &mut impl AsExec,
    session_id: i32,
    completion_ts: i64,
) -> Result<bool, Error> {
    trace!("finalizing session '{}' at '{}'", completion_ts, session_id);
    let res = sqlx::query!(
        r#"
            UPDATE session_t
            SET completion_unix_tstamp = $1
            WHERE session_id = $2
            "#,
        completion_ts,
        session_id,
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected() != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        UNREGISTERED,
        core::{DatabaseType, testing},
        sequence_create,
    };
    use sqlx::Pool;

    #[sqlx::test]
    async fn test_session_create(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        let mut cx = database.connection();

        let seq_record = sequence_create(
            &mut cx,
            &"my_sequence".parse().unwrap(),
            &"/my/path/in/store".to_owned().into(),
            None,
        )
        .await
        .unwrap();

        let session_locator = types::SessionLocator::new(seq_record.locator());

        let session_record = session_create(&mut cx, &session_locator).await.unwrap();

        assert_ne!(session_record.session_id, UNREGISTERED);
        assert_eq!(session_record.sequence_id, seq_record.sequence_id);
        assert_eq!(session_record.locator(), session_locator);
        assert_eq!(session_record.completion_unix_tstamp, None);
        assert!(session_record.creation_unix_tstamp <= types::Timestamp::now().as_i64());
    }

    #[sqlx::test]
    async fn test_session_create_with_non_existent_sequence(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);

        let mut cx = database.connection();

        let seq_locator = "ghost_sequence".parse::<types::SequenceLocator>().unwrap();

        let session_locator = types::SessionLocator::new(seq_locator);

        let err = session_create(&mut cx, &session_locator).await.unwrap_err();

        assert!(matches!(err, Error::NotFound));
    }
}

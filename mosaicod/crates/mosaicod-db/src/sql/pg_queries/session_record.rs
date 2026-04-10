use crate::{Error, core::AsExec, sql::schema};
use log::{trace, warn};
use mosaicod_core::types;

pub async fn session_create(
    exe: &mut impl AsExec,
    record: &schema::SessionRecord,
) -> Result<schema::SessionRecord, Error> {
    trace!("creating a new topic record {:?}", record);
    let res = sqlx::query_as!(
        schema::SessionRecord,
        r#"
            INSERT INTO session_t 
                (
                    session_uuid, sequence_id,
                    creation_unix_tstamp, completion_unix_tstamp
                ) 
            VALUES 
                ($1, $2, $3, $4)
            RETURNING 
                *
    "#,
        record.session_uuid,
        record.sequence_id,
        record.creation_unix_tstamp,
        record.completion_unix_tstamp,
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its id.
pub async fn session_find_by_id(
    exe: &mut impl AsExec,
    id: i32,
) -> Result<schema::SessionRecord, Error> {
    trace!("searching session by id `{}`", id);
    let res = sqlx::query_as!(
        schema::SessionRecord,
        "SELECT * FROM session_t WHERE session_id=$1",
        id
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its uuid.
pub async fn session_find_by_uuid(
    exe: &mut impl AsExec,
    uuid: &types::Uuid,
) -> Result<schema::SessionRecord, Error> {
    trace!("searching session by uuid `{}`", uuid);
    let res = sqlx::query_as!(
        schema::SessionRecord,
        "SELECT * FROM session_t WHERE session_uuid=$1",
        uuid.as_ref()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a session by resource lookup
pub async fn session_lookup(
    exec: &mut impl AsExec,
    id_lookup: &types::IdLookup,
) -> Result<schema::SessionRecord, Error> {
    match id_lookup {
        types::IdLookup::Id(id) => session_find_by_id(exec, *id).await,
        types::IdLookup::Uuid(uuid) => session_find_by_uuid(exec, uuid).await,
    }
}

/// Returns true if the session is locked.
pub async fn session_locked(exe: &mut impl AsExec, session_id: i32) -> Result<bool, Error> {
    trace!("session (id=`{}`) locked? ", session_id);
    let locked = sqlx::query_scalar!(
        r#"SELECT (completion_unix_tstamp IS NOT NULL) AS "locked!" FROM session_t WHERE session_id=$1"#,
        session_id
    )
        .fetch_one(exe.as_exec())
        .await?;
    Ok(locked)
}

/// Deletes a session record from the database by its name, **bypassing any lock state**.
///
/// This function requires a [`DataLossToken`] because it permanently removes the record from the database
/// elsewhere. Improper use can lead to data inconsistency or loss.
pub async fn session_delete(
    exe: &mut impl AsExec,
    uuid: &types::Uuid,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!("(data loss) deleting session `{}`", uuid);
    sqlx::query!("DELETE FROM session_t WHERE session_uuid=$1", uuid.as_ref())
        .execute(exe.as_exec())
        .await?;
    Ok(())
}

/// Find all topic associated with a session
pub async fn session_find_all_topics(
    exe: &mut impl AsExec,
    uuid: &types::Uuid,
) -> Result<Vec<schema::TopicRecord>, Error> {
    trace!("searching topics for session `{}`", uuid);
    Ok(sqlx::query_as!(
        schema::TopicRecord,
        r#"
        SELECT topic.*
        FROM topic_t AS topic
        JOIN session_t AS session 
            ON topic.session_id = session.session_id
        WHERE session.session_uuid = $1
        "#,
        uuid.as_ref(),
    )
    .fetch_all(exe.as_exec())
    .await?)
}

pub async fn session_update_completion_tstamp(
    exe: &mut impl AsExec,
    session_id: i32,
    completion_ts: i64,
) -> Result<(), Error> {
    trace!(
        "updating completion timestamp to `{}` for session `{}`",
        completion_ts, session_id
    );
    sqlx::query!(
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

    Ok(())
}

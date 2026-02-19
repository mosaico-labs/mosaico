use crate::{self as repo, Error, sql_models};
use log::trace;
use mosaicod_core::types;

pub async fn session_create(
    exe: &mut impl repo::AsExec,
    record: &sql_models::SessionRecord,
) -> Result<sql_models::SessionRecord, repo::Error> {
    trace!("creating a new topic record {:?}", record);
    let res = sqlx::query_as!(
        sql_models::SessionRecord,
        r#"
            INSERT INTO session_t 
                (
                    session_id, session_uuid, sequence_id, locked, 
                    creation_unix_tstamp, completion_unix_tstamp
                ) 
            VALUES 
                ($1, $2, $3, $4, $5, $6) 
            RETURNING 
                *
    "#,
        record.session_id,
        record.session_uuid,
        record.sequence_id,
        record.locked,
        record.creation_unix_tstamp,
        record.completion_unix_tstamp,
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its id.
pub async fn session_find_by_id(
    exe: &mut impl repo::AsExec,
    id: i32,
) -> Result<sql_models::SessionRecord, Error> {
    trace!("searching session by id `{}`", id);
    let res = sqlx::query_as!(
        sql_models::SessionRecord,
        "SELECT * FROM session_t WHERE session_id=$1",
        id
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence given its uuid.
pub async fn session_find_by_uuid(
    exe: &mut impl repo::AsExec,
    uuid: &types::Uuid,
) -> Result<sql_models::SessionRecord, Error> {
    trace!("searching session by uuid `{}`", uuid);
    let res = sqlx::query_as!(
        sql_models::SessionRecord,
        "SELECT * FROM session_t WHERE session_uuid=$1",
        uuid.as_ref()
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find a sequence by resource lookup
pub async fn session_lookup(
    exec: &mut impl repo::AsExec,
    id_lookup: &types::IdLookup,
) -> Result<repo::SessionRecord, Error> {
    match id_lookup {
        types::IdLookup::Id(id) => repo::session_find_by_id(exec, *id).await,
        types::IdLookup::Uuid(uuid) => repo::session_find_by_uuid(exec, uuid).await,
    }
}

/// Locks the sequence in the db and sets the completion timestamp
pub async fn session_lock(
    exe: &mut impl repo::AsExec,
    uuid: &types::Uuid,
    completion_timestamp: &types::Timestamp,
) -> Result<(), repo::Error> {
    trace!("locking `{}`", uuid);
    sqlx::query!(
        r#"
        UPDATE session_t 
        SET 
            locked = TRUE,
            completion_unix_tstamp = $1
        WHERE session_uuid = $2"#,
        completion_timestamp.as_i64(),
        uuid.as_ref(),
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

pub async fn session_find_all_topic_names(
    exe: &mut impl repo::AsExec,
    uuid: &types::Uuid,
) -> Result<Vec<types::TopicResourceLocator>, Error> {
    trace!("searching topic locators by session `{}`", uuid);
    let res = sqlx::query_scalar!(
        r#"
        SELECT topic.locator_name
        FROM topic_t AS topic
        JOIN session_t AS session 
            ON topic.session_id = session.session_id
        WHERE session.session_uuid = $1
        "#,
        uuid.as_ref(),
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res
        .into_iter()
        .map(types::TopicResourceLocator::from)
        .collect())
}

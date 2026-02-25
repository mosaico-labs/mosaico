use crate::{core::AsExec, Error, sql::schema};
use log::trace;
use mosaicod_core::types::{self, Resource};

/// Creates a new notify associated with a topic
pub async fn topic_notify_create(
    exe: &mut impl AsExec,
    notify: &schema::TopicNotifyRecord,
) -> Result<schema::TopicNotifyRecord, Error> {
    trace!("creating a new topic notify {:?}", notify);
    let res = sqlx::query_as!(
        schema::TopicNotifyRecord,
        r#"
            INSERT INTO topic_notify_t
                (topic_id, notify_type, msg, creation_unix_tstamp) 
            VALUES 
                ($1, $2, $3, $4) 
            RETURNING 
                *
    "#,
        notify.topic_id,
        notify.notify_type,
        notify.msg,
        notify.creation_unix_tstamp,
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find al notifies associated with a topic name
pub async fn topic_notifies_find_by_locator(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
) -> Result<Vec<schema::TopicNotifyRecord>, Error> {
    trace!("searching notifies for {}", loc);
    let res = sqlx::query_as!(
        schema::TopicNotifyRecord,
        r#"
          SELECT notify.* FROM topic_notify_t AS notify
          JOIN topic_t AS topic ON notify.topic_id = topic.topic_id
          WHERE topic.locator_name=$1
    "#,
        loc.name(),
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res)
}

/// Deletes a sequence notify from the database
///
/// If the notify does not exist, the operation has no effect.
pub async fn topic_notify_delete(exe: &mut impl AsExec, id: i32) -> Result<(), Error> {
    trace!("deleting topic report `{}`", id);
    sqlx::query!("DELETE FROM topic_notify_t WHERE topic_notify_id=$1", id)
        .execute(exe.as_exec())
        .await?;
    Ok(())
}

pub async fn sequence_notify_create(
    exe: &mut impl AsExec,
    notify: &schema::SequenceNotifyRecord,
) -> Result<schema::SequenceNotifyRecord, Error> {
    trace!("creating a new sequence notify {:?}", notify);
    let res = sqlx::query_as!(
        schema::SequenceNotifyRecord,
        r#"
            INSERT INTO sequence_notify_t
                (sequence_id, notify_type, msg, creation_unix_tstamp) 
            VALUES 
                ($1, $2, $3, $4) 
            RETURNING 
                *
    "#,
        notify.sequence_id,
        notify.notify_type,
        notify.msg,
        notify.creation_unix_tstamp,
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find al reports associated with a sequence name
pub async fn sequence_notifies_find_by_name(
    exe: &mut impl AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<Vec<schema::SequenceNotifyRecord>, Error> {
    trace!("searching notifies for `{}`", loc);
    let res = sqlx::query_as!(
        schema::SequenceNotifyRecord,
        r#"
          SELECT notify.* FROM sequence_notify_t AS notify
          JOIN sequence_t AS seq ON notify.sequence_id = seq.sequence_id
          WHERE seq.locator_name=$1
    "#,
        loc.name(),
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res)
}

/// Deletes a sequence report from the database
///
/// If the report does not exist, the operation has no effect.
pub async fn sequence_notify_delete(
    exe: &mut impl AsExec,
    id: i32,
) -> Result<(), Error> {
    trace!("deleting sequence notify `{}`", id);
    sqlx::query!(
        "DELETE FROM sequence_notify_t WHERE sequence_notify_id=$1",
        id
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

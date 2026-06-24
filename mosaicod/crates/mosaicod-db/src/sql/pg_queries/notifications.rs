use crate::{Error, core::AsExec, sql::schema};
use log::trace;
use mosaicod_core::types;

/// Creates a new notification associated with a topic
pub async fn topic_notification_create(
    exe: &mut impl AsExec,
    topic_locator: &types::TopicLocator,
    ntype: types::NotificationType,
    msg: &str,
) -> Result<schema::TopicNotificationRecord, Error> {
    trace!("creating a new notification for topic {}", topic_locator);

    let res = sqlx::query_as!(
        schema::TopicNotificationRecord,
        r#"
            INSERT INTO topic_notification_t
                (topic_notification_uuid, topic_id, notification_type, msg, creation_unix_tstamp)
            SELECT $1, topic.topic_id, $2, $3, $4
            FROM topic_t AS topic
            WHERE topic.locator_name = $5
            RETURNING *
            "#,
        uuid::Uuid::from(types::Uuid::new()),
        ntype.to_string(),
        msg,
        types::Timestamp::now().as_i64(),
        topic_locator.to_string(),
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

/// Find all notifications associated with a topic name
pub async fn topic_notifications_find_by_locator(
    exe: &mut impl AsExec,
    loc: &types::TopicLocator,
) -> Result<Vec<schema::TopicNotificationRecord>, Error> {
    trace!("searching notifications for {}", loc);
    let res = sqlx::query_as!(
        schema::TopicNotificationRecord,
        r#"
          SELECT notification.* FROM topic_notification_t AS notification
          JOIN topic_t AS topic ON notification.topic_id = topic.topic_id
          WHERE topic.locator_name=$1
    "#,
        loc.to_string(),
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res)
}

/// Deletes a sequence notification from the database
///
/// If the notification does not exist, the operation has no effect.
pub async fn topic_notification_delete(exe: &mut impl AsExec, id: i32) -> Result<(), Error> {
    trace!("deleting topic report `{}`", id);
    sqlx::query!(
        "DELETE FROM topic_notification_t WHERE topic_notification_id=$1",
        id
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

pub async fn sequence_notification_create(
    exe: &mut impl AsExec,
    sequence_locator: &types::SequenceLocator,
    ntype: types::NotificationType,
    msg: &str,
) -> Result<schema::SequenceNotificationRecord, Error> {
    trace!(
        "creating a new notification for sequence {}",
        sequence_locator
    );

    let res = sqlx::query_as!(
        schema::SequenceNotificationRecord,
        r#"
            INSERT INTO sequence_notification_t
                (sequence_notification_uuid, sequence_id, notification_type, msg, creation_unix_tstamp)
            SELECT $1, seq.sequence_id, $2, $3, $4
            FROM sequence_t AS seq
            WHERE seq.locator_name = $5
            RETURNING *
            "#,
        uuid::Uuid::from(types::Uuid::new()),
        ntype.to_string(),
        msg,
        types::Timestamp::now().as_i64(),
        sequence_locator as &str,
    )
    .fetch_one(exe.as_exec())
    .await?;

    Ok(res)
}

/// Find all reports associated to the sequence with the given [`locator`]
pub async fn sequence_notifications_find_by_locator(
    exe: &mut impl AsExec,
    sequence_locator: &types::SequenceLocator,
) -> Result<Vec<schema::SequenceNotificationRecord>, Error> {
    trace!(
        "searching notifications for sequence `{}`",
        sequence_locator
    );
    let res = sqlx::query_as!(
        schema::SequenceNotificationRecord,
        r#"
          SELECT notification.* FROM sequence_notification_t AS notification
          JOIN sequence_t AS seq ON notification.sequence_id = seq.sequence_id
          WHERE seq.locator_name=$1
          "#,
        sequence_locator as &str,
    )
    .fetch_all(exe.as_exec())
    .await?;
    Ok(res)
}

/// Deletes a sequence report from the database
///
/// If the report does not exist, the operation has no effect.
pub async fn sequence_notification_delete(exe: &mut impl AsExec, id: i32) -> Result<(), Error> {
    trace!("deleting sequence notification `{}`", id);
    sqlx::query!(
        "DELETE FROM sequence_notification_t WHERE sequence_notification_id=$1",
        id
    )
    .execute(exe.as_exec())
    .await?;
    Ok(())
}

/// Deletes all reports from the database for the given sequence.
pub async fn sequence_notifications_purge(
    exe: &mut impl AsExec,
    locator: &types::SequenceLocator,
) -> Result<(), Error> {
    trace!("deleting notifications for sequence `{}`", locator);

    sqlx::query!(
        r#"DELETE FROM sequence_notification_t AS notification
           USING sequence_t AS seq
           WHERE seq.locator_name = $1 AND seq.sequence_id = notification.sequence_id"#,
        locator as &str
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

/// Deletes all reports from the database for the given tppic.
pub async fn topic_notifications_purge(
    exe: &mut impl AsExec,
    locator: &types::TopicLocator,
) -> Result<(), Error> {
    trace!("deleting notifications for topic `{}`", locator);

    sqlx::query!(
        r#"DELETE FROM topic_notification_t AS notification
           USING topic_t AS topic
           WHERE topic.locator_name = $1 AND topic.topic_id = notification.topic_id"#,
        locator.to_string()
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

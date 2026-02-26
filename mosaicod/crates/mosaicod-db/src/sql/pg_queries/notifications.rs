use crate::{Error, core::AsExec, sql::schema};
use log::trace;
use mosaicod_core::types::{self, Resource};

/// Creates a new notification associated with a topic
pub async fn topic_notification_create(
    exe: &mut impl AsExec,
    notification: &schema::TopicNotificationRecord,
) -> Result<schema::TopicNotificationRecord, Error> {
    trace!("creating a new topic notification {:?}", notification);
    let res = sqlx::query_as!(
        schema::TopicNotificationRecord,
        r#"
            INSERT INTO topic_notification_t
                (topic_notification_uuid, topic_id, notification_type, msg, creation_unix_tstamp)
            VALUES 
                ($1, $2, $3, $4, $5)
            RETURNING 
                *
    "#,
        notification.topic_notification_uuid,
        notification.topic_id,
        notification.notification_type,
        notification.msg,
        notification.creation_unix_tstamp,
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find all notifications associated with a topic name
pub async fn topic_notifications_find_by_locator(
    exe: &mut impl AsExec,
    loc: &types::TopicResourceLocator,
) -> Result<Vec<schema::TopicNotificationRecord>, Error> {
    trace!("searching notifications for {}", loc);
    let res = sqlx::query_as!(
        schema::TopicNotificationRecord,
        r#"
          SELECT notification.* FROM topic_notification_t AS notification
          JOIN topic_t AS topic ON notification.topic_id = topic.topic_id
          WHERE topic.locator_name=$1
    "#,
        loc.name(),
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
    notification: &schema::SequenceNotificationRecord,
) -> Result<schema::SequenceNotificationRecord, Error> {
    trace!("creating a new sequence notification {:?}", notification);
    let res = sqlx::query_as!(
        schema::SequenceNotificationRecord,
        r#"
            INSERT INTO sequence_notification_t
                (sequence_notification_uuid, sequence_id, notification_type, msg, creation_unix_tstamp)
            VALUES 
                ($1, $2, $3, $4, $5)
            RETURNING 
                *
    "#,
        notification.sequence_notification_uuid,
        notification.sequence_id,
        notification.notification_type,
        notification.msg,
        notification.creation_unix_tstamp,
    )
    .fetch_one(exe.as_exec())
    .await?;
    Ok(res)
}

/// Find al reports associated with a sequence name
pub async fn sequence_notifications_find_by_name(
    exe: &mut impl AsExec,
    loc: &types::SequenceResourceLocator,
) -> Result<Vec<schema::SequenceNotificationRecord>, Error> {
    trace!("searching notifications for `{}`", loc);
    let res = sqlx::query_as!(
        schema::SequenceNotificationRecord,
        r#"
          SELECT notification.* FROM sequence_notification_t AS notification
          JOIN sequence_t AS seq ON notification.sequence_id = seq.sequence_id
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

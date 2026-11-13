use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;
use tracing::{trace, warn};

/// Deletes from the optimization table topics for which the optimization routine appears to take too long.
pub async fn topic_optimization_delete_stale(
    exe: &mut impl AsExec,
    threshold: types::Timestamp,
) -> Result<u64, Error> {
    trace!("topic_optimization delete stale");

    let res = sqlx::query!(
        r#"DELETE FROM topic_optimization_t
           WHERE start_unix_tstamp < $1
           "#,
        threshold.as_i64()
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected())
}

/// Add completed topics, but not yet optimized, to topic_optimization_t table.
///
/// A topic the store optimizer left as it is, for an issue of its data, is not added: its
/// optimization ended (see [`topic_optimization_record_issue`]).
pub async fn topic_update_optimization_list(exe: &mut impl AsExec) -> Result<u64, Error> {
    trace!("updating topic optimization list");

    let res = sqlx::query!(
        r#"INSERT INTO topic_optimization_t (topic_id)
           SELECT topic_id
           FROM topic_t
           WHERE completion_unix_tstamp IS NOT NULL AND optimization_end_unix_tstamp IS NULL
           ON CONFLICT (topic_id) DO NOTHING
           "#
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected())
}

/// Retrieves the first topic inside topic_optimization_t table for which the optimization process has not yet started.
///
/// Topics of the serialization formats in `skipped_formats` are left in the list, for an
/// optimizer that can optimize them (e.g. one built with video support, for image sequences).
///
/// Note: the row containing the returned record is locked by default.
pub async fn topic_next_to_be_optimized(
    exe: &mut impl AsExec,
    skipped_formats: &[types::Format],
) -> Result<Option<schema::TopicOptimizationRecord>, Error> {
    trace!("updating topic optimization list");

    let skipped_formats: Vec<String> = skipped_formats.iter().map(|f| f.to_string()).collect();

    Ok(sqlx::query_as!(
        schema::TopicOptimizationRecord,
        r#"SELECT topic_optimization_t.*
           FROM topic_optimization_t
           JOIN topic_t ON topic_t.topic_id = topic_optimization_t.topic_id
           WHERE topic_optimization_t.start_unix_tstamp IS NULL
             AND NOT (topic_t.serialization_format = ANY($1))
           ORDER BY topic_optimization_t.topic_id ASC
           LIMIT 1
           FOR UPDATE OF topic_optimization_t SKIP LOCKED
           "#,
        &skipped_formats,
    )
    .fetch_optional(exe.as_exec())
    .await?)
}

pub async fn topic_start_optimization(
    exe: &mut impl AsExec,
    topic_id: i32,
    start_timestamp: types::Timestamp,
    opt_path_in_store: types::TopicPathInStore,
) -> Result<schema::TopicOptimizationRecord, Error> {
    trace!(
        "updating topic optimization start timestamp and path in store for topic with id {}",
        topic_id
    );

    Ok(sqlx::query_as!(
        schema::TopicOptimizationRecord,
        r#"UPDATE topic_optimization_t
           SET start_unix_tstamp = $1, opt_path_in_store = $2
           WHERE topic_id = $3
           RETURNING *
           "#,
        Some(start_timestamp.as_i64()),
        Some(String::from(opt_path_in_store)),
        topic_id
    )
    .fetch_one(exe.as_exec())
    .await?)
}

/// Records what the store optimizer found wrong with the topic `topic_id`, with `details`, and
/// removes it from the optimization list. The issue is a notification on the topic, of type
/// `error` (see [`types::OptimizationIssue::notification`]), which clients list as any other.
///
/// A retried issue (see [`types::OptimizationIssue::is_retried`]) is all: the topic is added to
/// the list again on the next run. Any other issue also ends the topic's optimization
/// (`optimization_end_unix_tstamp`), its data left as the client sent it: it is no longer added.
///
/// Run it in a transaction, for its writes to be all or none.
pub async fn topic_optimization_record_issue(
    exe: &mut impl AsExec,
    topic_id: i32,
    issue: types::OptimizationIssue,
    details: &str,
    timestamp: types::Timestamp,
) -> Result<(), Error> {
    let message = issue.notification(details);
    warn!("topic with id {}: {}", topic_id, message);

    sqlx::query!(
        r#"INSERT INTO topic_notification_t
               (topic_notification_uuid, topic_id, notification_type, msg, creation_unix_tstamp)
           VALUES ($1, $2, $3, $4, $5)
           "#,
        uuid::Uuid::from(types::Uuid::new()),
        topic_id,
        types::NotificationType::Error.to_string(),
        message,
        timestamp.as_i64(),
    )
    .execute(exe.as_exec())
    .await?;

    if issue.is_retried() {
        topic_optimization_remove(exe, topic_id).await
    } else {
        topic_optimization_end(exe, topic_id, timestamp).await
    }
}

/// Ends the optimization of the topic `topic_id` at `timestamp`, with no notification, its data
/// left as it is: it is removed from the optimization list, and no longer added to it (e.g. an
/// image sequence with no images, which has nothing to optimize).
///
/// Run it in a transaction, for its writes to be all or none.
pub async fn topic_optimization_end(
    exe: &mut impl AsExec,
    topic_id: i32,
    timestamp: types::Timestamp,
) -> Result<(), Error> {
    sqlx::query!(
        r#"UPDATE topic_t
               SET optimization_end_unix_tstamp = $1
               WHERE topic_id = $2
               "#,
        timestamp.as_i64(),
        topic_id
    )
    .execute(exe.as_exec())
    .await?;

    topic_optimization_remove(exe, topic_id).await
}

/// Removes the topic `topic_id` from the optimization list.
async fn topic_optimization_remove(exe: &mut impl AsExec, topic_id: i32) -> Result<(), Error> {
    sqlx::query!(
        r#"DELETE FROM topic_optimization_t WHERE topic_id = $1"#,
        topic_id
    )
    .execute(exe.as_exec())
    .await?;

    Ok(())
}

/// Returns the number of topics in the optimization list.
///
/// This is used for testing purposes.
pub async fn topic_optimization_count(exe: &mut impl AsExec) -> Result<i64, Error> {
    trace!("topic optimization count");

    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*) as "count!"
           FROM topic_optimization_t
           "#,
    )
    .fetch_one(exe.as_exec())
    .await?)
}

pub async fn topic_optimization_delete(
    exe: &mut impl AsExec,
    topic_id: i32,
    _: types::DataLossToken,
) -> Result<(), Error> {
    warn!(
        "(data loss) deleting topic with id {} from optimization list",
        topic_id
    );
    let result = sqlx::query!(
        r#"DELETE FROM topic_optimization_t WHERE topic_id=$1"#,
        topic_id
    )
    .execute(exe.as_exec())
    .await?;

    if result.rows_affected() == 0 {
        return Err(Error::NotFound);
    }

    Ok(())
}

/// Search for a topic with the given path_in_store.
pub async fn topic_optimization_find_path_in_store(
    exe: &mut impl AsExec,
    path_in_store: &str,
) -> Result<bool, Error> {
    trace!(
        "searching if path_in_store `{}` is assigned to a topic optimization",
        path_in_store
    );
    let found: bool = sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM topic_optimization_t WHERE opt_path_in_store=$1) as "found!""#,
        path_in_store
    )
        .fetch_one(exe.as_exec())
        .await?;

    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DatabaseType, testing};
    use crate::{
        sequence_create, session_create, topic_create, topic_find_by_id,
        topic_notifications_find_by_locator, topic_update_completion_tstamp,
    };
    use sqlx::Pool;

    /// Completed topics of `my_sequence`, by name and format: their ids, in the optimization list.
    async fn completed_topics(cx: &mut impl AsExec, topics: &[(&str, types::Format)]) -> Vec<i32> {
        let sequence = sequence_create(
            cx,
            &"my_sequence".parse().unwrap(),
            &types::SequencePathInStore::new(),
            None,
        )
        .await
        .unwrap();
        let session = session_create(cx, &types::SessionLocator::new(sequence.locator()))
            .await
            .unwrap();
        let mut ids = Vec::new();
        for (name, format) in topics {
            let topic = topic_create(
                cx,
                &format!("my_sequence/{name}").parse().unwrap(),
                session.uuid(),
                "",
                &format.to_string(),
                Some(types::TopicPathInStore::new()),
                None,
            )
            .await
            .unwrap();
            topic_update_completion_tstamp(cx, topic.topic_id, 1)
                .await
                .unwrap();
            ids.push(topic.topic_id);
        }
        topic_update_optimization_list(cx).await.unwrap();
        ids
    }

    #[sqlx::test]
    async fn test_topic_optimization_record_issue(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();
        let topic_id =
            completed_topics(&mut cx, &[("images", types::Format::ImageSequence)]).await[0];

        // A failure: a notification, and the topic is optimized again on the next run.
        topic_optimization_record_issue(
            &mut cx,
            topic_id,
            types::OptimizationIssue::Failed,
            "the store is down",
            types::Timestamp::now(),
        )
        .await
        .unwrap();
        let topic = topic_find_by_id(&mut cx, topic_id).await.unwrap();
        assert!(topic.optimization_end_timestamp().is_none());
        assert_eq!(topic_optimization_count(&mut cx).await.unwrap(), 0);
        assert_eq!(topic_update_optimization_list(&mut cx).await.unwrap(), 1);

        // An issue of its data: a notification, and its optimization ends.
        topic_optimization_record_issue(
            &mut cx,
            topic_id,
            types::OptimizationIssue::Undecodable,
            "image recorded at 10 ns: garbage",
            types::Timestamp::now(),
        )
        .await
        .unwrap();
        let topic = topic_find_by_id(&mut cx, topic_id).await.unwrap();
        assert!(topic.optimization_end_timestamp().is_some());
        assert_eq!(topic_optimization_count(&mut cx).await.unwrap(), 0);
        assert_eq!(topic_update_optimization_list(&mut cx).await.unwrap(), 0);

        let notifications =
            topic_notifications_find_by_locator(&mut cx, &"my_sequence/images".parse().unwrap())
                .await
                .unwrap();
        let mut messages: Vec<String> = notifications
            .into_iter()
            .map(|notification| {
                assert!(matches!(
                    notification.notification_type(),
                    types::NotificationType::Error
                ));
                notification.msg.unwrap_or_default()
            })
            .collect();
        messages.sort();
        assert_eq!(
            messages,
            [
                "store optimizer/failed: the store is down",
                "store optimizer/undecodable: image recorded at 10 ns: garbage",
            ]
        );
    }

    #[sqlx::test]
    async fn test_topic_next_to_be_optimized_leaves_skipped_formats(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();
        let topics = completed_topics(
            &mut cx,
            &[
                ("images", types::Format::ImageSequence),
                ("raw_images", types::Format::ImageSequenceRaw),
                ("imu", types::Format::Default),
            ],
        )
        .await;

        // The image sequences are skipped, and left in the list.
        let skipped = [
            types::Format::ImageSequence,
            types::Format::ImageSequenceRaw,
        ];
        let next = topic_next_to_be_optimized(&mut cx, &skipped)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.topic_id, topics[2]);
        topic_start_optimization(
            &mut cx,
            next.topic_id,
            types::Timestamp::now(),
            types::TopicPathInStore::new(),
        )
        .await
        .unwrap();
        assert!(
            topic_next_to_be_optimized(&mut cx, &skipped)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(topic_optimization_count(&mut cx).await.unwrap(), 3);

        // Not skipped, the first one is the next one.
        let next = topic_next_to_be_optimized(&mut cx, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.topic_id, topics[0]);
    }
}

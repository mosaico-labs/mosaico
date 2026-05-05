//! This module provides the cleanup routine in charge of deleting obsolete files
//! (not associated with any entry in the database) from the object storage.

use log::warn;
use mosaicod_core::{error::PublicResult as Result, types};
use mosaicod_db as db;
use mosaicod_facade as facade;

const TO_DELETE_MARKER_FILE_NAME: &str = "TO_DELETE";

/// Checks if the conditions to start a new cleanup are met.
///
/// It can start:
/// - if it's the first cleanup running in absolute
/// - if the latest cleanup has finished and its end time + cleanup time interval (secs) <= current time.
///
/// When [`time_interval`] = 0, the cleanup must NOT start.
pub async fn cleanup_can_start(context: &facade::Context, time_interval_secs: u32) -> Result<bool> {
    if time_interval_secs == 0 {
        return Ok(false);
    }

    let mut cx = context.db.connection();
    let latest_cleanup = db::cleanup_log_latest(&mut cx).await?;

    Ok(latest_cleanup.is_none_or(|cleanup_log| {
        cleanup_log.end_datetime().is_some_and(|end_dt| {
            end_dt + chrono::Duration::seconds(time_interval_secs as i64) <= chrono::Utc::now()
        })
    }))
}

/// Launches the cleanup of the Store.
///
/// Returns how many folders have been marked TO_DELETE and how many folder have actually been deleted.
pub async fn cleanup_start(
    context: &facade::Context,
    retention_duration: chrono::Duration,
) -> Result<(Vec<String>, Vec<String>)> {
    let store = context.store.clone();

    let root_subfolders = store.list_subfolders("").await?;

    let mut marked_folders = Vec::new();
    let mut deleted_folders = Vec::new();

    for dir in root_subfolders {
        let to_delete_marker_file_path =
            std::path::PathBuf::from(dir.clone()).join(TO_DELETE_MARKER_FILE_NAME);

        let file_meta = store.meta(&to_delete_marker_file_path).await?;

        if let Some(meta) = file_meta {
            // Permanently delete the folder.
            if meta.last_modified + retention_duration <= chrono::Utc::now() {
                store.delete_recursive(&dir).await?;
                deleted_folders.push(dir);
            }
        } else if !find_db_reference(context, &dir).await? {
            // Mark it as TO_DELETE.
            store
                .write_bytes(to_delete_marker_file_path, vec![])
                .await?;
            marked_folders.push(dir);
        }
    }

    Ok((marked_folders, deleted_folders))
}

async fn find_db_reference(context: &facade::Context, path_in_store: &str) -> Result<bool> {
    let mut cx = context.db.connection();
    if path_in_store.starts_with(types::SEQUENCE_FOLDER_PREFIX) {
        return Ok(db::sequence_find_path_in_store(&mut cx, path_in_store).await?);
    } else if path_in_store.starts_with(types::TOPIC_FOLDER_PREFIX) {
        return Ok(db::topic_find_path_in_store(&mut cx, path_in_store).await?);
    } else {
        warn!(
            "Found unexpected file in Store: {}. Was it added manually?",
            path_in_store
        );
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types;
    use mosaicod_query as query;
    use mosaicod_store as store;
    use rand::seq::IteratorRandom;
    use std::sync::Arc;

    struct TestContext {
        db: db::testing::Database,
        store: store::testing::Store,
        ts_gw: query::TimeseriesEngineRef,
    }

    impl TestContext {
        fn facade_context(&self) -> facade::Context {
            facade::Context::new(
                (*self.store).clone(),
                (*self.db).clone(),
                self.ts_gw.clone(),
            )
        }
    }

    async fn create_fake_cleanup_logs(
        ctx: &facade::Context,
        nums: u16,
        min_duration: Option<u16>,
        max_duration: Option<u16>,
    ) {
        let mut tx = ctx.db.transaction().await.unwrap();

        for i in 1..=nums {
            let record = db::schema::CleanupLogRecord::default();
            let record = db::cleanup_log_create(&mut tx, &record).await.unwrap();
            assert_eq!(record.cleanup_id, i as i32);

            let rnd_sleep_time =
                rand::random_range(min_duration.unwrap_or(100)..=max_duration.unwrap_or(3000));
            tokio::time::sleep(std::time::Duration::from_millis(rnd_sleep_time as u64)).await;

            assert!(
                db::cleanup_log_close(&mut tx, chrono::Utc::now().timestamp())
                    .await
                    .unwrap()
            );
        }

        tx.commit().await.unwrap();
    }

    /// Populates the Store with the given number of sequences [`num_seqs`] and topics [`num_topics`].
    ///
    /// [`retention_duration`] value must be the same passed to [`cleanup_start`]
    ///
    /// Returns two vectors as output, both containing a tuple with:
    /// - the path in store generated within the function (sequence or topic)
    /// - the record on DB (randomly chosen when to create it)
    /// - the TO_DELETE file creation unix timestamp (whether to create or not this file for a
    ///   sequence is randomly chosen within this test function. For the Topics this file is created
    ///   if it has been created also for the parent sequence).
    async fn populate_random_store(
        context: &TestContext,
        num_seqs: u16,
        num_topics: u16,
        retention_duration: chrono::Duration,
    ) -> (
        Vec<(
            types::SequencePathInStore,
            Option<db::SequenceRecord>,
            Option<i64>,
        )>,
        Vec<(
            types::TopicPathInStore,
            Option<db::TopicRecord>,
            Option<i64>,
        )>,
    ) {
        let sample = r#"Some example text"#;
        let buffer = sample.as_bytes();

        let mut seqs_info = Vec::with_capacity(num_seqs as usize);
        let mut topics_info = Vec::with_capacity(num_topics as usize);

        let mut tx = context.db.transaction().await.unwrap();

        for i in 0..num_seqs {
            let pis = types::SequencePathInStore::new();

            context
                .store
                .write_bytes(pis.path_metadata(), buffer)
                .await
                .unwrap();

            if rand::random() {
                let seq_record = db::schema::SequenceRecord::new(
                    format!("seq_{i}").parse().unwrap(),
                    pis.clone(),
                );
                let seq_record = db::sequence_create(&mut tx, &seq_record).await.unwrap();
                seqs_info.push((pis, Some(seq_record), None));
            } else if rand::random() {
                let to_delete_marker_file_path = context
                    .store
                    .root
                    .join(pis.root())
                    .join(TO_DELETE_MARKER_FILE_NAME);

                std::fs::File::create(&to_delete_marker_file_path).unwrap();

                let now = filetime::FileTime::now().unix_seconds();
                let rnd_time = filetime::FileTime::from_unix_time(
                    rand::random_range(now - retention_duration.num_seconds() * 2..=now),
                    0,
                );
                filetime::set_file_mtime(&to_delete_marker_file_path, rnd_time).unwrap();

                seqs_info.push((pis, None, Some(rnd_time.seconds())));
            } else {
                seqs_info.push((pis, None, None));
            }
        }

        for i in 0..num_topics {
            let pis = types::TopicPathInStore::new();

            context
                .store
                .write_bytes(pis.path_metadata(), buffer)
                .await
                .unwrap();

            if rand::random() {
                let parent_seq_id = seqs_info
                    .iter()
                    .filter_map(|x| x.1.as_ref())
                    .choose(&mut rand::rng())
                    .unwrap()
                    .sequence_id;
                let parent_seq_locator: types::SequenceLocator =
                    format!("seq_{parent_seq_id}").parse().unwrap();

                let session_record = db::schema::SessionRecord::new(
                    types::SessionLocator::new(parent_seq_locator.clone()),
                    parent_seq_id,
                );
                db::session_create(&mut tx, &session_record).await.unwrap();

                let topic_record = db::schema::TopicRecord::new(
                    format!("{parent_seq_locator}/{i}").parse().unwrap(),
                    parent_seq_id,
                    1,
                    "",
                    "",
                    Some(pis.clone()),
                );
                db::topic_create(&mut tx, &topic_record).await.unwrap();

                topics_info.push((pis, Some(topic_record), None));
            } else {
                let parent_seq = seqs_info
                    .iter()
                    .filter(|x| x.1.is_none())
                    .choose(&mut rand::rng())
                    .unwrap();

                if let Some(seq_to_delete_created_at) = parent_seq.2 {
                    let to_delete_marker_file_path = context
                        .store
                        .root
                        .join(pis.root())
                        .join(TO_DELETE_MARKER_FILE_NAME);

                    std::fs::File::create(&to_delete_marker_file_path).unwrap();

                    filetime::set_file_mtime(
                        &to_delete_marker_file_path,
                        filetime::FileTime::from_unix_time(seq_to_delete_created_at, 0),
                    )
                    .unwrap();

                    topics_info.push((pis, None, parent_seq.2));
                } else {
                    topics_info.push((pis, None, None));
                }
            }
        }

        tx.commit().await.unwrap();

        (seqs_info, topics_info)
    }

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> TestContext {
        let db = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new((*store).clone(), 0).unwrap());

        TestContext { store, db, ts_gw }
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_cleanup_can_start(pool: sqlx::Pool<db::DatabaseType>) {
        let ctx = test_context(pool).facade_context();

        // Cleanup must not start if time_interval = 0.
        assert!(!cleanup_can_start(&ctx, 0).await.unwrap());

        // Cleanup can always start if it's the first time ever.
        assert!(cleanup_can_start(&ctx, 1).await.unwrap());
        assert!(cleanup_can_start(&ctx, 100).await.unwrap());
        assert!(cleanup_can_start(&ctx, 10000).await.unwrap());
        assert!(cleanup_can_start(&ctx, u32::MAX).await.unwrap());

        // Cleanup should start based on latest cleanup end time.
        create_fake_cleanup_logs(&ctx, 1, Some(100), Some(100)).await;

        assert!(!cleanup_can_start(&ctx, 0).await.unwrap());
        assert!(!cleanup_can_start(&ctx, 1).await.unwrap());
        assert!(!cleanup_can_start(&ctx, 10).await.unwrap());

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(cleanup_can_start(&ctx, 1).await.unwrap());
        assert!(!cleanup_can_start(&ctx, 5).await.unwrap());
        assert!(!cleanup_can_start(&ctx, 10).await.unwrap());
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_cleanup_start(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);
        let facade_context = context.facade_context();

        let num_seqs = rand::random_range(1..=20);
        let num_topics = rand::random_range(1..=50);

        let retention_duration = chrono::Duration::days(rand::random_range(1..180));

        let stats = populate_random_store(&context, num_seqs, num_topics, retention_duration).await;

        let now_unix_ts = filetime::FileTime::now().unix_seconds();

        let res = cleanup_start(&facade_context, retention_duration)
            .await
            .unwrap();

        let test_marked_seqs = stats
            .0
            .iter()
            .filter(|x| x.1.is_none() && x.2.is_none())
            .count();
        let test_marked_topics = stats
            .1
            .iter()
            .filter(|x| x.1.is_none() && x.2.is_none())
            .count();

        assert_eq!(res.0.len(), test_marked_seqs + test_marked_topics);

        let test_deleted_seqs = stats
            .0
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && x.2
                        .is_some_and(|t| now_unix_ts > t + retention_duration.num_seconds())
            })
            .count();
        let test_deleted_topics = stats
            .1
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && x.2
                        .is_some_and(|t| now_unix_ts > t + retention_duration.num_seconds())
            })
            .count();

        assert_eq!(res.1.len(), test_deleted_seqs + test_deleted_topics);

        for seq in stats.0 {
            if seq.1.is_some() {
                assert!(context.store.exists(seq.0.path_metadata()).await.unwrap());
                assert!(
                    !context
                        .store
                        .exists(seq.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
            } else if seq
                .2
                .is_none_or(|t| now_unix_ts <= t + retention_duration.num_seconds())
            {
                assert!(
                    context
                        .store
                        .exists(seq.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
            } else {
                assert!(!context.store.exists(seq.0.root()).await.unwrap());
            }
        }

        for topic in stats.1 {
            if topic.1.is_some() {
                assert!(context.store.exists(topic.0.path_metadata()).await.unwrap());
                assert!(
                    !context
                        .store
                        .exists(topic.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
            } else if topic
                .2
                .is_none_or(|t| now_unix_ts <= t + retention_duration.num_seconds())
            {
                assert!(
                    context
                        .store
                        .exists(topic.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
            } else {
                assert!(!context.store.exists(topic.0.root()).await.unwrap());
            }
        }
    }
}

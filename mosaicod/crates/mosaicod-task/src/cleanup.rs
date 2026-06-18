//! This module provides the cleanup routine in charge of deleting obsolete files
//! (not associated with any entry in the database) from the object storage.

use mosaicod_core::{error::PublicResult as Result, types};
use mosaicod_db as db;
use mosaicod_store as store;
use std::ops::Deref;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const TO_DELETE_MARKER_FILE_NAME: &str = "TO_DELETE";
const DEFAULT_TIME_INTERVAL: u32 = 86400;
const DEFAULT_RETENTION_DURATION: u32 = 86400;

/// Utility type to accept only non-negative durations (u32).
#[derive(Debug, Clone, Copy)]
pub struct Duration(chrono::Duration);

impl Duration {
    pub fn seconds(secs: u32) -> Self {
        Self(chrono::Duration::seconds(secs as i64))
    }

    pub fn minutes(mins: u32) -> Self {
        Self(chrono::Duration::minutes(mins as i64))
    }

    pub fn hours(hours: u32) -> Self {
        Self(chrono::Duration::hours(hours as i64))
    }

    pub fn days(days: u32) -> Self {
        Self(chrono::Duration::days(days as i64))
    }
}

impl Deref for Duration {
    type Target = chrono::Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Statistics resulting from a performed cleaning operation.
#[derive(Debug, Default)]
pub struct CleanupStats {
    executed: bool,
    marked_folders: Vec<String>,
    deleted_folders: Vec<String>,
    // First string is the folder, second is the error.
    failed_folders: Vec<(String, String)>,
}

/// Describes the possible actions performed on a folder in the store.
enum ActionPerformed {
    None,
    Marked,
    Deleted,
}

/// This is the entry point to create, configure and run a cleanup routine.
pub struct Cleanup {
    db: db::Database,
    store: store::StoreRef,
    time_interval: Duration,
    retention_duration: Duration,
}

impl Cleanup {
    /// Creates a new cleanup routine with default [`time_interval`] and [`retention_duration`].
    pub fn new(db: db::Database, store: store::StoreRef) -> Self {
        Self {
            db,
            store,
            time_interval: Duration::seconds(DEFAULT_TIME_INTERVAL),
            retention_duration: Duration::seconds(DEFAULT_RETENTION_DURATION),
        }
    }

    pub fn with_time_interval(mut self, time_interval: Duration) -> Self {
        self.time_interval = time_interval;
        self
    }

    pub fn with_retention_duration(mut self, retention_duration: Duration) -> Self {
        self.retention_duration = retention_duration;
        self
    }

    /// Starts the cleanup routine that every [`time_interval`] tries to actually perform a cleanup of the store.
    pub async fn run(mut self, shutdown_notifier: CancellationToken) {
        info!("Launching cleanup background routine");

        loop {
            let cleanup_res = self.try_cleanup().await;

            match cleanup_res {
                Ok(stats) => {
                    if stats.executed {
                        info!(
                            "Cleanup completed. {} items marked as ready to be deleted. {} items deleted. {} items failed",
                            stats.marked_folders.len(),
                            stats.deleted_folders.len(),
                            stats.failed_folders.len()
                        );
                    } else {
                        info!("Cleanup not executed.");
                    }
                }
                Err(e) => {
                    // Don't exit the cleanup routine if something went wrong. Just log the error.
                    error!("Cleanup failed. {}", e);
                }
            }

            // match self.can_start().await {
            //     Ok(can_start) => {
            //         if can_start {
            //             info!("Cleanup started");
            //
            //             let cleanup_res = self.do_cleanup().await;
            //
            //             match cleanup_res {
            //                 Ok(stats) => {
            //                     info!(
            //                         "Cleanup completed. {} items marked as ready to be deleted. {} items deleted.",
            //                         stats.marked_folders.len(),
            //                         stats.deleted_folders.len()
            //                     );
            //                 }
            //                 Err(e) => {
            //                     // Don't exit the cleanup routine if something went wrong. Just log the error.
            //                     error!("Cleanup failed. {}", e);
            //                 }
            //             }
            //         }
            //     }
            //     Err(e) => {
            //         // Don't exit the cleanup routine if something went wrong. Just log the error.
            //         error!("{}", e);
            //     }
            // }

            tokio::select! {
                // Here we can call .unwrap() safely because duration is non-negative by construction.
                _ = tokio::time::sleep(self.time_interval.to_std().unwrap()) => {
                }
                _ = shutdown_notifier.cancelled() => {
                    info!("Exiting cleanup background routine. Shutdown received.");
                    break; // Exit the loop immediately
                }
            }
        }
    }

    /// Launches the cleanup of the store.
    ///
    /// Returns how many folders have been marked TO_DELETE and how many folder have actually been deleted.
    pub async fn try_cleanup(&mut self) -> Result<CleanupStats> {
        let mut stats = CleanupStats::default();

        if self.time_interval.is_zero() {
            return Ok(stats);
        }

        let start_time = chrono::Utc::now();

        let can_start = db::cleanup_log_try_create(
            &mut self.db.connection(),
            start_time.timestamp(),
            self.time_interval.num_seconds(),
        )
        .await?
        .is_some();

        if !can_start {
            return Ok(stats);
        }

        let root_subfolders = self.store.list_subfolders("").await?;

        for folder in root_subfolders {
            match self.analyze_folder(&folder, start_time).await {
                Ok(action_performed) => match action_performed {
                    ActionPerformed::Deleted => {
                        stats.deleted_folders.push(folder);
                    }
                    ActionPerformed::Marked => {
                        stats.marked_folders.push(folder);
                    }
                    ActionPerformed::None => {
                        // Do nothing.
                    }
                },
                Err(e) => {
                    stats.failed_folders.push((folder, e.to_string()));
                }
            }
        }

        db::cleanup_log_close(
            &mut self.db.connection(),
            chrono::Utc::now().timestamp(),
            &stats.marked_folders,
            &stats.deleted_folders,
            &stats.failed_folders,
        )
        .await?;

        stats.executed = true;

        Ok(stats)
    }

    async fn analyze_folder(
        &mut self,
        folder: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<ActionPerformed> {
        let to_delete_marker_file_path =
            std::path::PathBuf::from(folder.to_owned()).join(TO_DELETE_MARKER_FILE_NAME);

        let file_meta = self.store.meta(&to_delete_marker_file_path).await?;

        if let Some(meta) = file_meta {
            // Marker exists. Check if it's expired.
            if meta.last_modified + *self.retention_duration <= now {
                self.store.delete_recursive(folder).await?;
                return Ok(ActionPerformed::Deleted);
            }
        } else if !self.find_db_reference(folder).await? {
            // If retention duration is equal to 0, delete the directory right away.
            // Otherwise, mark the directory as TO_DELETE.
            return if self.retention_duration.is_zero() {
                self.store.delete_recursive(folder).await?;
                Ok(ActionPerformed::Deleted)
            } else {
                self.store
                    .write_bytes(to_delete_marker_file_path, vec![])
                    .await?;
                Ok(ActionPerformed::Marked)
            };
        }

        Ok(ActionPerformed::None)
    }

    async fn find_db_reference(&self, path_in_store: &str) -> Result<bool> {
        let mut cx = self.db.connection();
        if path_in_store.starts_with(types::SEQUENCE_FOLDER_PREFIX) {
            return Ok(db::sequence_find_path_in_store(&mut cx, path_in_store).await?);
        } else if path_in_store.starts_with(types::TOPIC_FOLDER_PREFIX) {
            return Ok(db::topic_find_path_in_store(&mut cx, path_in_store).await?);
        } else {
            warn!(
                "Found unexpected file in store: {}. Was it added manually?",
                path_in_store
            );
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::types;
    use mosaicod_store as store;
    use rand::seq::IteratorRandom;

    struct TestContext {
        db: db::testing::Database,
        store: store::testing::Store,
    }

    /// Populates the store with the given number of sequences [`num_seqs`] and topics [`num_topics`].
    ///
    /// [`retention_duration`] value must be the same passed to [`cleanup`]
    ///
    /// Returns two vectors as output, both containing a tuple with:
    /// 1. the path in store generated within the function (sequence or topic)
    /// 2. the record on DB (randomly chosen when to create it)
    /// 3. the TO_DELETE file creation unix timestamp (whether to create or not this file for a
    ///    sequence is randomly chosen within this test function. For the Topics this file is created
    ///    if it has been created also for the parent sequence).
    async fn populate_random_store(
        context: &TestContext,
        num_seqs: u16,
        num_topics: u16,
        retention_duration: Duration,
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

        // For each sequence:
        // 1. Create a fake metadata.json in the store
        // 1. Randomly decide whether to create the corresponding sequence record on DB or not.
        // 2. If no DB record is created, randomly decide whether to create the TO_DELETE file
        //    with a random last modified timestamp that can goes back until retention_duration*2
        let mut seq_id = 1;
        for _ in 0..num_seqs {
            let pis = types::SequencePathInStore::new();

            context
                .store
                .write_bytes(pis.path_metadata(), buffer)
                .await
                .unwrap();

            if rand::random() {
                let seq_record = db::sequence_create(
                    &mut tx,
                    &format!("seq_{seq_id}").parse().unwrap(),
                    &pis,
                    None,
                )
                .await
                .unwrap();
                seq_id += 1;
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

        let seqs_with_db_ref = seqs_info.iter().filter(|s| s.1.is_some()).count() as u16;

        // For each topic:
        // 1. Create a fake metadata.json in the store
        // 2. Randomly decide whether to create the corresponding topic record on DB or not,
        //    associating it to a random sequence for which the DB record was created at the previous step.
        // 3. If no DB record is created, randomly decide whether to create the TO_DELETE file
        //    with the same last modified timestamp as the parent sequence.
        for i in 0..num_topics {
            let pis = types::TopicPathInStore::new();

            context
                .store
                .write_bytes(pis.path_metadata(), buffer)
                .await
                .unwrap();

            if seqs_with_db_ref == num_seqs || (seqs_with_db_ref > 0 && rand::random()) {
                let parent_seq_id = seqs_info
                    .iter()
                    .filter_map(|x| x.1.as_ref())
                    .choose(&mut rand::rng())
                    .unwrap()
                    .sequence_id;
                let parent_seq_locator: types::SequenceLocator =
                    format!("seq_{parent_seq_id}").parse().unwrap();

                let session_record = db::session_create(
                    &mut tx,
                    &types::SessionLocator::new(parent_seq_locator.clone()),
                )
                .await
                .unwrap();

                let topic_record = db::topic_create(
                    &mut tx,
                    &format!("{parent_seq_locator}/{i}").parse().unwrap(),
                    session_record.uuid(),
                    "",
                    "",
                    Some(pis.clone()),
                    None,
                )
                .await
                .unwrap();

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

        TestContext { store, db }
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_try_cleanup(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let num_seqs = rand::random_range(1..=20);
        let num_topics = rand::random_range(1..=50);

        let retention_duration = Duration::days(rand::random_range(0..=1));

        let stats = populate_random_store(&context, num_seqs, num_topics, retention_duration).await;

        let mut cleanup = Cleanup::new((*context.db).clone(), (*context.store).clone());
        cleanup = cleanup.with_retention_duration(retention_duration);

        // This should simulate the internal time used by do_cleanup() to check against retention_duration.
        let now_unix_ts = filetime::FileTime::now().unix_seconds();

        let cleanup_stats = cleanup.try_cleanup().await.unwrap();

        assert!(cleanup_stats.executed);

        let mut cx = context.db.connection();
        let latest_cleanup = db::cleanup_log_latest(&mut cx).await.unwrap().unwrap();
        assert!(
            latest_cleanup.start_datetime().timestamp() > 0
                && latest_cleanup.start_datetime().timestamp() <= chrono::Utc::now().timestamp()
        );
        assert!(
            latest_cleanup.end_datetime().unwrap().timestamp() > 0
                && latest_cleanup.end_datetime().unwrap().timestamp()
                    <= chrono::Utc::now().timestamp()
        );

        let test_marked_seqs = stats
            .0
            .iter()
            .filter(|x| x.1.is_none() && x.2.is_none() && retention_duration.num_seconds() > 0)
            .count();
        let test_marked_topics = stats
            .1
            .iter()
            .filter(|x| x.1.is_none() && x.2.is_none() && retention_duration.num_seconds() > 0)
            .count();

        assert_eq!(
            cleanup_stats.marked_folders.len(),
            test_marked_seqs + test_marked_topics
        );

        let test_deleted_seqs = stats
            .0
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && (retention_duration.num_seconds() == 0
                        || x.2
                            .is_some_and(|t| now_unix_ts >= t + retention_duration.num_seconds()))
            })
            .count();
        let test_deleted_topics = stats
            .1
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && (retention_duration.num_seconds() == 0
                        || x.2
                            .is_some_and(|t| now_unix_ts >= t + retention_duration.num_seconds()))
            })
            .count();

        assert_eq!(
            cleanup_stats.deleted_folders.len(),
            test_deleted_seqs + test_deleted_topics
        );

        for seq in stats.0 {
            if seq.1.is_some() {
                // Folder does not need to be deleted.
                assert!(context.store.exists(seq.0.path_metadata()).await.unwrap());
                assert!(
                    !context
                        .store
                        .exists(seq.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
            } else if retention_duration.num_seconds() > 0
                && seq
                    .2
                    .is_none_or(|t| now_unix_ts <= t + retention_duration.num_seconds())
            {
                // Folder near to deletion.
                assert!(
                    context
                        .store
                        .exists(seq.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
                assert!(context.store.exists(seq.0.path_metadata()).await.unwrap());
            } else {
                // Folder must have been deleted.
                assert!(!context.store.exists(seq.0.path_metadata()).await.unwrap());
            }
        }

        for topic in stats.1 {
            if topic.1.is_some() {
                // Folder does not need to be deleted.
                assert!(context.store.exists(topic.0.path_metadata()).await.unwrap());
                assert!(
                    !context
                        .store
                        .exists(topic.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
            } else if retention_duration.num_seconds() > 0
                && topic
                    .2
                    .is_none_or(|t| now_unix_ts <= t + retention_duration.num_seconds())
            {
                // Folder near to deletion.
                assert!(
                    context
                        .store
                        .exists(topic.0.root().join(TO_DELETE_MARKER_FILE_NAME))
                        .await
                        .unwrap()
                );
                assert!(context.store.exists(topic.0.path_metadata()).await.unwrap());
            } else {
                // Folder must have been deleted.
                assert!(!context.store.exists(topic.0.path_metadata()).await.unwrap());
            }
        }
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_run(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let num_seqs = rand::random_range(1..=20);
        let num_topics = rand::random_range(0..=50);

        let time_interval = Duration::seconds(3);
        let retention_duration = Duration::seconds(1);

        let test_stats =
            populate_random_store(&context, num_seqs, num_topics, retention_duration).await;

        let notifier = CancellationToken::new();

        let notifier_clone = notifier.clone();

        let db_clone = (*context.db).clone();
        let store_clone = (*context.store).clone();

        // This should simulate the internal time used by the first do_cleanup() to check against retention_duration.
        let now_unix_ts = filetime::FileTime::now().unix_seconds();

        let handle_cleanup_task = tokio::spawn(async move {
            let cleanup = Cleanup::new(db_clone, store_clone)
                .with_time_interval(time_interval)
                .with_retention_duration(retention_duration);

            cleanup.run(notifier_clone).await
        });

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        notifier.cancel();

        let _ = tokio::join!(handle_cleanup_task);

        let mut cx = context.db.connection();
        let cleanup_history = db::cleanup_log_history(&mut cx, 10).await.unwrap();
        assert_eq!(cleanup_history.len(), 4);

        assert!(
            cleanup_history
                .iter()
                .zip(cleanup_history.iter().skip(1))
                .all(|x| x.0.start_datetime() > x.1.start_datetime())
        );

        let first_cleanup_log = cleanup_history.last().unwrap();

        let test_marked_seqs_1st_cleanup = test_stats
            .0
            .iter()
            .filter(|x| x.1.is_none() && x.2.is_none() && retention_duration.num_seconds() > 0)
            .count();
        let test_marked_topics_1st_cleanup = test_stats
            .1
            .iter()
            .filter(|x| x.1.is_none() && x.2.is_none() && retention_duration.num_seconds() > 0)
            .count();

        assert_eq!(
            first_cleanup_log.marked_folders().len(),
            test_marked_seqs_1st_cleanup + test_marked_topics_1st_cleanup
        );

        let test_deleted_seqs_1st_cleanup = test_stats
            .0
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && (retention_duration.num_seconds() == 0
                        || x.2
                            .is_some_and(|t| now_unix_ts >= t + retention_duration.num_seconds()))
            })
            .count();
        let test_deleted_topics_1st_cleanup = test_stats
            .1
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && (retention_duration.num_seconds() == 0
                        || x.2
                            .is_some_and(|t| now_unix_ts >= t + retention_duration.num_seconds()))
            })
            .count();

        assert_eq!(
            first_cleanup_log.deleted_folders().len(),
            test_deleted_seqs_1st_cleanup + test_deleted_topics_1st_cleanup
        );

        assert!(first_cleanup_log.failed_folders().is_empty());

        // During the second cleanup, all the marked folder at the previous cleanup must have been deleted,
        // plus other folders with a TO_DELETE exceeding the retention duration.
        let test_deleted_seqs_2nd_cleanup = test_stats
            .0
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && (retention_duration.num_seconds() == 0
                        || x.2.is_some_and(|t| {
                            // Exclude the elements that should have been deleted during the first cleanup
                            // and keep the ones left with a TO_DELETE exceeding the retention period.
                            now_unix_ts < t + retention_duration.num_seconds()
                                && now_unix_ts + time_interval.num_seconds()
                                    >= t + retention_duration.num_seconds()
                        }))
            })
            .count();
        let test_deleted_topics_2nd_cleanup = test_stats
            .1
            .iter()
            .filter(|x| {
                x.1.is_none()
                    && (retention_duration.num_seconds() == 0
                        || x.2.is_some_and(|t| {
                            // Exclude the elements that should have been deleted during the first cleanup
                            // and keep the ones left with a TO_DELETE exceeding the retention period.
                            now_unix_ts < t + retention_duration.num_seconds()
                                && now_unix_ts + time_interval.num_seconds()
                                    >= t + retention_duration.num_seconds()
                        }))
            })
            .count();

        let second_cleanup_log = &cleanup_history[2];

        assert_eq!(
            second_cleanup_log.deleted_folders().len(),
            first_cleanup_log.marked_folders().len()
                + test_deleted_seqs_2nd_cleanup
                + test_deleted_topics_2nd_cleanup
        );

        assert!(second_cleanup_log.marked_folders().is_empty());
        assert!(second_cleanup_log.failed_folders().is_empty());

        // All subsequent cleanup must not have marked or deleted anything.
        assert!(cleanup_history.iter().rev().skip(2).all(|x| {
            x.failed_folders().is_empty()
                && x.deleted_folders().is_empty()
                && x.marked_folders().is_empty()
        }));
    }
}

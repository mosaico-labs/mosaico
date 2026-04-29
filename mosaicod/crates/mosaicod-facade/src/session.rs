//! A Session represents a new update session for adding topics to a target
//! sequence. It serves as a container for these new topic uploads,
//! ensuring that topics from previous sessions within the same sequence are not modified.
//! This provides a mechanism for versioning or snapshotting data.
//!
//! Multiple sessions can occur in parallel for the same sequence. Once a session is
//! finalized, all data associated with it becomes immutable.

use crate::{Context, topic};
use mosaicod_core::{self as core, error::PublicResult as Result, types};
use mosaicod_db as db;

/// Handle containing session identifiers.
/// It's used by all functions (except creation) in this module to indicate the session to operate on.
pub struct Handle {
    id: i32,
    uuid: types::Uuid,
    locator: types::SessionLocator,
}

impl Handle {
    pub(super) fn new(locator: types::SessionLocator, id: i32, uuid: types::Uuid) -> Self {
        Self { locator, id, uuid }
    }

    /// Try to obtain a handle from a session locator.
    /// Returns an error if the session does not exist.
    pub async fn try_from_locator(
        context: &Context,
        locator: types::SessionLocator,
    ) -> Result<Handle> {
        let mut cx = context.db.connection();

        let db_session = db::session_find_by_locator(&mut cx, &locator).await?;

        Ok(Self {
            locator,
            id: db_session.sequence_id,
            uuid: db_session.uuid(),
        })
    }

    /// Try to obtain a handle from a session UUID.
    /// Returns an error if the session does not exist.
    pub async fn try_from_uuid(context: &Context, uuid: &types::Uuid) -> Result<Self> {
        let mut cx = context.db.connection();

        let db_session = db::session_find_by_uuid(&mut cx, uuid).await?;

        Ok(Self {
            id: db_session.session_id,
            uuid: db_session.uuid(),
            locator: db_session.locator(),
        })
    }

    pub fn uuid(&self) -> &types::Uuid {
        &self.uuid
    }

    pub fn locator(&self) -> &types::SessionLocator {
        &self.locator
    }

    pub(super) fn id(&self) -> i32 {
        self.id
    }
}

/// Creates a new session in the database for the given sequence.
pub async fn try_create(
    context: &Context,
    sequence_locator: types::SequenceLocator,
) -> Result<Handle> {
    let mut tx = context.db.transaction().await?;

    let sequence = db::sequence_find_by_locator(&mut tx, &sequence_locator).await?;

    let locator = types::SessionLocator::new(sequence_locator);

    let session = db::SessionRecord::new(locator.clone(), sequence.sequence_id);
    let session = db::session_create(&mut tx, &session).await?;

    tx.commit().await?;

    Ok(Handle {
        id: session.session_id,
        uuid: session.uuid(),
        locator,
    })
}

/// Finalizes the session, making it and all its associated data immutable.
///
/// Once a session is finalized, no more topics can be added to it.
pub async fn finalize(context: &Context, handle: &Handle) -> Result<()> {
    let mut tx = context.db.transaction().await?;

    // Return an error if session has already been finalized.
    // Note: here two concurrent finalized could pass this check,
    // that's why we need later to update the completion timestamp if not already present atomically.
    if db::session_finalized(&mut tx, handle.id()).await? {
        Err(core::Error::session_already_finalized(
            handle.locator().to_string(),
        ))?;
    }

    let topics = topic_list(handle, &mut tx).await?;

    // If the session does not contain any topic, return an error and leave the session unlocked.
    if topics.is_empty() {
        Err(core::Error::empty_session(handle.locator().to_string()))?
    }

    // If not all topics are finalized, return the locator of the first one still open.
    let mut topic_not_finalized = None;

    for handle in &topics {
        let status = topic::status(context, handle).await?;
        if status != topic::Status::Finalized {
            topic_not_finalized = Some((handle.locator(), status));
            break;
        }
    }

    if let Some(topic_not_finalized) = topic_not_finalized {
        match topic_not_finalized {
            (locator, topic::Status::Empty) => {
                Err(core::Error::missing_doput(locator.to_string()))?
            }
            (locator, topic::Status::Uploading) => {
                Err(core::Error::topic_upload_in_progress(locator.to_string()))?
            }
            (_, topic::Status::Finalized) => (),
        }
    }

    // If updating the completion timestamp fails it means somebody else did it in the meantime.
    let finalize_ok = db::session_try_update_completion_tstamp(
        &mut tx,
        handle.id(),
        types::Timestamp::now().as_i64(),
    )
    .await?;

    if !finalize_ok {
        Err(core::Error::session_already_finalized(
            handle.locator().to_string(),
        ))?;
    }

    tx.commit().await?;

    Ok(())
}

/// Deletes the session from the database.
pub async fn delete(
    context: &Context,
    handle: Handle,
    allow_data_loss: types::DataLossToken,
) -> Result<()> {
    let mut cx = context.db.connection();
    db::session_delete(&mut cx, handle.uuid(), allow_data_loss).await?;
    Ok(())
}

/// Returns the topic list associated with this session.
async fn topic_list(handle: &Handle, exe: &mut impl db::AsExec) -> Result<Vec<topic::Handle>> {
    let topics = db::session_find_all_topics(exe, handle.uuid()).await?;

    Ok(topics
        .into_iter()
        .map(|record| {
            topic::Handle::new(
                record.locator(),
                record.topic_id,
                record.uuid(),
                record.path_in_store(),
            )
        })
        .collect())
}

pub async fn metadata(context: &Context, handle: &Handle) -> Result<types::SessionMetadata> {
    let mut tx = context.db.transaction().await?;

    let db_session = db::session_find_by_id(&mut tx, handle.id()).await?;

    let topics = topic_list(handle, &mut tx)
        .await?
        .into_iter()
        .map(|handle| handle.locator().clone())
        .collect();

    Ok(types::SessionMetadata {
        locator: db_session.locator(),
        created_at: db_session.creation_timestamp(),
        completed_at: db_session.completion_timestamp(),
        topics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_query as query;
    use mosaicod_store as store;
    use std::sync::Arc;

    use crate::{sequence, session};

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new((*store).clone(), 0).unwrap());

        Context::new((*store).clone(), (*database).clone(), ts_gw)
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_session_create_and_delete(
        pool: sqlx::Pool<db::DatabaseType>,
    ) -> sqlx::Result<()> {
        let context = test_context(pool);

        let seq_locator = "test_sequence".parse::<types::SequenceLocator>().unwrap();

        let seq_handle = sequence::try_create(&context, seq_locator, None)
            .await
            .expect("Error creating sequence");

        let session_handle = session::try_create(&context, seq_handle.locator().clone())
            .await
            .expect("Error creating session");

        assert_eq!(session_handle.locator.sequence, *seq_handle.locator());

        let session_uuid = session_handle.uuid().clone();

        // Check if session was correctly created on DB.
        let mut cx = context.db.connection();
        let db_session = db::session_find_by_uuid(&mut cx, &session_uuid)
            .await
            .expect("Unable to find the created session");

        assert_eq!(db_session.session_id, session_handle.id());
        assert_eq!(db_session.uuid(), *session_handle.uuid());
        assert!(db_session.creation_timestamp().as_i64() > 0);
        assert!(db_session.completion_timestamp().is_none());

        delete(&context, session_handle, types::allow_data_loss())
            .await
            .expect("Unable to delete session");

        db::session_find_by_uuid(&mut cx, &session_uuid)
            .await
            .unwrap_err();

        Ok(())
    }
}

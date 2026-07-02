//! A Session represents a new update session for adding topics to a target
//! sequence. It serves as a container for these new topic uploads,
//! ensuring that topics from previous sessions within the same sequence are not modified.
//! This provides a mechanism for versioning or snapshotting data.
//!
//! Multiple sessions can occur in parallel for the same sequence. Once a session is
//! finalized, all data associated with it becomes immutable.

use crate::{Context, topic};
use mosaicod_core::error::PublicError;
use mosaicod_core::{self as core, error::PublicResult as Result, types};
use mosaicod_db as db;

pub(super) mod internal {
    use super::*;

    pub async fn metadata(
        exe: &mut impl db::AsExec,
        session_record: &db::SessionRecord,
    ) -> Result<types::SessionMetadata> {
        let topics = db::session_find_all_topics(exe, session_record.session_id)
            .await?
            .iter()
            .map(|tr| tr.locator())
            .collect();

        Ok(types::SessionMetadata {
            locator: session_record.locator(),
            created_at: session_record.creation_timestamp(),
            completed_at: session_record.completion_timestamp(),
            topics,
        })
    }
}

/// Creates a new session in the database for the given sequence.
///
/// Returns the locator and the UUID of the newly created session.
pub async fn try_create(
    context: &Context,
    sequence_locator: types::SequenceLocator,
) -> Result<(types::SessionLocator, types::Uuid)> {
    let session_locator = types::SessionLocator::new(sequence_locator.clone());
    let mut cx = context.db.connection();
    let session = db::session_create(&mut cx, &session_locator)
        .await
        .map_err(|e| match &e {
            db::Error::NotFound | db::Error::ForeignKeyViolation => {
                core::Error::not_found(sequence_locator.to_string())
            }
            _ => e.error(),
        })?;
    Ok((session_locator, session.uuid()))
}

/// Finalizes the session, making it and all its associated data immutable.
///
/// Once a session is finalized, no more topics can be added to it.
pub async fn finalize(context: &Context, uuid: &types::Uuid) -> Result<()> {
    let mut tx = context.db.transaction().await?;

    // Getting this record with an exclusive lock prevents other concurrent session finalize on the
    // same session (if any) from making a mess.
    let session_record = db::session_find_by_uuid(&mut tx, uuid, db::RowLocking::Exclusive)
        .await
        .map_err(|e| match e {
            db::Error::NotFound => core::Error::not_found(format!("session with UUID {}", uuid)),
            _ => e.error(),
        })?;

    let session_locator = session_record.locator();

    // Return an error if session has already been finalized.
    if session_record.completion_timestamp().is_some() {
        Err(core::Error::session_already_finalized(
            session_locator.to_string(),
        ))?;
    }

    let topics = db::session_find_all_topics(&mut tx, session_record.session_id).await?;

    // If the session does not contain any topic, return an error and leave the session unlocked.
    if topics.is_empty() {
        Err(core::Error::empty_session(session_locator.to_string()))?
    }

    // If not all topics are finalized, return the locator of the first one still open.
    let mut topic_not_finalized = None;

    for topic_record in &topics {
        let status = topic::internal::status(topic_record).await?;
        if status != topic::Status::Finalized {
            topic_not_finalized = Some((topic_record.locator(), status));
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

    db::session_finalize(
        &mut tx,
        session_record.session_id,
        types::Timestamp::now().as_i64(),
    )
    .await?;

    tx.commit().await?;

    Ok(())
}

/// Deletes the session from the database.
pub async fn delete(
    context: &Context,
    locator: &types::SessionLocator,
    allow_data_loss: types::DataLossToken,
) -> Result<()> {
    let mut cx = context.db.connection();
    db::session_delete(&mut cx, locator, allow_data_loss).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_query as query;
    use mosaicod_store as store;
    use std::sync::Arc;

    use crate::sequence;

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

        sequence::try_create(&context, &seq_locator, None)
            .await
            .expect("Error creating sequence");

        let seq_record = db::sequence_find_by_locator(&mut context.db.connection(), &seq_locator)
            .await
            .unwrap();

        let (session_locator, session_uuid) = try_create(&context, seq_record.locator().clone())
            .await
            .expect("Error creating session");

        assert_eq!(*session_locator.sequence, *seq_record.locator());

        let session_record =
            db::session_find_by_locator(&mut context.db.connection(), &session_locator)
                .await
                .unwrap();
        assert_eq!(session_record.session_id, 1);
        assert!(session_record.creation_timestamp().as_i64() > 0);
        assert!(session_record.completion_timestamp().is_none());

        delete(
            &context,
            &session_record.locator(),
            types::allow_data_loss(),
        )
        .await
        .expect("Unable to delete session");

        db::session_find_by_uuid(
            &mut context.db.connection(),
            &session_uuid,
            db::RowLocking::None,
        )
        .await
        .unwrap_err();

        Ok(())
    }
}

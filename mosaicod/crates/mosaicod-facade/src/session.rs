//! A Session represents a new update session for adding topics to a target
//! sequence. It serves as a container for these new topic uploads,
//! ensuring that topics from previous sessions within the same sequence are not modified.
//! This provides a mechanism for versioning or snapshotting data.
//!
//! Multiple sessions can occur in parallel for the same sequence. Once a session is
//! finalized, all data associated with it becomes immutable.

use crate::{Context, Error, sequence, topic};
use log::trace;
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_marshal as marshal;

/// Handle containing session identifiers.
/// It's used by all functions (except creation) in this module to indicate the session to operate on.
pub struct Handle {
    identifiers: types::Identifiers,
    sequence_locator: types::SequenceResourceLocator,
}

impl Handle {
    pub(super) fn new(
        sequence_locator: types::SequenceResourceLocator,
        identifiers: types::Identifiers,
    ) -> Self {
        Self {
            sequence_locator,
            identifiers,
        }
    }

    /// Try to obtain a handle from a session UUID.
    /// Returns an error if the session does not exist.
    pub async fn try_from_uuid(context: &Context, uuid: &types::Uuid) -> Result<Self, Error> {
        let mut cx = context.db.connection();

        let db_session = db::session_find_by_uuid(&mut cx, uuid).await?;
        let db_sequence = db::sequence_find_by_id(&mut cx, db_session.sequence_id).await?;

        Ok(Self {
            identifiers: db_session.identifiers(),
            sequence_locator: db_sequence.resource_locator(),
        })
    }

    pub fn uuid(&self) -> &types::Uuid {
        &self.identifiers.uuid
    }

    pub fn sequence_locator(&self) -> &types::SequenceResourceLocator {
        &self.sequence_locator
    }

    pub(super) fn id(&self) -> i32 {
        self.identifiers.id
    }
}

pub async fn try_create(
    context: &Context,
    sequence_locator: types::SequenceResourceLocator,
) -> Result<Handle, Error> {
    let mut tx = context.db.transaction().await?;

    let sequence = db::sequence_lookup(
        &mut tx,
        &types::ResourceLookup::Locator(sequence_locator.to_string()),
    )
    .await?;

    let session = db::SessionRecord::new(sequence.sequence_id);
    let session = db::session_create(&mut tx, &session).await?;

    tx.commit().await?;

    // Create session manifest (store)
    let handle = Handle::try_from_uuid(context, &session.uuid()).await?;
    create_manifest(context, &handle).await?;

    Ok(Handle {
        identifiers: session.into(),
        sequence_locator,
    })
}

/// Creates the session manifest and saves it on store
/// TODO: find a better solution to create the manifest (without calling a method externally)
pub async fn create_manifest(context: &Context, handle: &Handle) -> Result<(), Error> {
    let mut cx = context.db.connection();

    let db_session = db::session_find_by_id(&mut cx, handle.id()).await?;

    let manifest =
        types::SessionManifest::new(handle.uuid().clone(), db_session.creation_timestamp());
    manifest_write_to_store(context, handle, manifest).await?;

    Ok(())
}

async fn manifest_write_to_store(
    context: &Context,
    handle: &Handle,
    manifest: types::SessionManifest,
) -> Result<(), Error> {
    let path = handle.sequence_locator.session_manifest(&manifest.uuid);

    trace!("converting session manifest to bytes");
    let json_manifest = marshal::SessionManifest::from(manifest);
    let bytes: Vec<u8> = json_manifest.try_into()?;

    trace!(
        "writing session manifest `{}` to store",
        &path.to_string_lossy()
    );
    context.store.write_bytes(&path, bytes).await?;

    Ok(())
}

/// Finalizes the session, making it and all its associated data immutable.
///
/// Once a session is finalized, no more topics can be added to it.
pub async fn finalize(context: &Context, handle: &Handle) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    // Collect all topics associated with this session
    let topics: Vec<topic::Handle> = db::session_find_all_topics(&mut tx, handle.uuid())
        .await?
        .into_iter()
        .map(|record| topic::Handle::new(record.locator(), record.identifiers()))
        .collect();

    // If the session does not contain any topic, return an error and leave the session unlocked.
    if topics.is_empty() {
        return Err(Error::SessionEmpty);
    }

    // If not all topics are locked, return an error and leave the session unlocked.
    let all_topics_locked = futures::future::join_all(
        topics
            .iter()
            .map(async |topic_handle| topic::manifest(context, topic_handle).await),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?
    .into_iter()
    .all(|v| v.properties.locked);

    if !all_topics_locked {
        return Err(Error::TopicUnlocked);
    }

    // Update manifest (store).
    let mut manifest = manifest(context, handle).await?;
    manifest.locked = true;
    manifest.completed_at = Some(types::Timestamp::now());
    manifest.topics = topics
        .into_iter()
        .map(|handle| handle.locator().clone())
        .collect();
    manifest_write_to_store(context, handle, manifest).await?;

    tx.commit().await?;

    Ok(())
}

/// Deletes all the topics associated with this session, deletes also the session manifest and
/// the session record from the db.
///
/// Since the session delete involves multiple deletes across the system, topics data and
/// session manifest, if operation fails a notification will be created. The notification will
/// enable the user to manually delete dangling resources if required.
///
/// # Errors
///
/// * [`Error::FailedAndNotified`]: if the error is correctly reported and notified.
/// * [`Error::FailedAndUnableToNotify`]: if the notification creation failed.
pub async fn delete(
    context: &Context,
    handle: Handle,
    only_if_unlocked: bool,
    allow_data_loss: types::DataLossToken,
) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    let error_report_msg = format!(
        "Some error occurred while deleting session `{}`",
        handle.uuid()
    );
    let mut error_report = types::ErrorReport::new(error_report_msg);

    let session_locked = manifest(context, &handle).await?.locked;

    if only_if_unlocked && session_locked {
        return Err(Error::SessionLocked);
    }

    // Deletes topic data
    let topics = topic_list(context, &handle).await?;
    for topic_handle in topics {
        let topic_locator_str = topic_handle.locator().to_string();

        // We collect all the errors to build a sequence notification reporting all error if
        // something fails.
        if let Err(e) = topic::delete(context, topic_handle, types::allow_data_loss()).await {
            error_report
                .errors
                .push(types::ErrorReportItem::new(topic_locator_str, e));
        }
    }

    // Deletes the session manifest if session was previously locked (unlocked
    // sessions have no manifest)
    if session_locked
        && let Err(e) = context
            .store
            .delete(handle.sequence_locator.session_manifest(handle.uuid()))
            .await
    {
        error_report.errors.push(types::ErrorReportItem::new(
            handle.sequence_locator.clone(),
            e,
        ));
    }

    let error_occurs = error_report.has_errors();
    let mut notification = None;
    let mut msg = "".to_owned();

    // If some error occurs create a notification with all errors stacked otherwise
    // if no error occurs delete the session record
    if error_occurs {
        msg = error_report.into();

        let sequence_handle =
            sequence::Handle::try_from_locator(context, handle.sequence_locator).await?;

        notification = Some(
            sequence::notify(
                context,
                &sequence_handle,
                types::NotificationType::Error,
                msg.clone(),
            )
            .await?,
        );
    } else {
        // This is done as last operation, otherwise multiple calls to this function will fail
        // since a session lookup is made above
        db::session_delete(&mut tx, handle.uuid(), allow_data_loss).await?;
    }

    tx.commit().await?;

    if error_occurs {
        return if let Some(notification) = notification {
            Err(Error::failed_and_notified(notification.uuid))
        } else {
            Err(Error::failed_and_unable_to_notify(msg))
        };
    }

    Ok(())
}

/// Returns the topic list associated with this session.
pub async fn topic_list(context: &Context, handle: &Handle) -> Result<Vec<topic::Handle>, Error> {
    let mut cx = context.db.connection();

    let topics = db::session_find_all_topics(&mut cx, handle.uuid()).await?;

    Ok(topics
        .into_iter()
        .map(|record| topic::Handle::new(record.locator(), record.identifiers()))
        .collect())
}

pub async fn manifest(context: &Context, handle: &Handle) -> Result<types::SessionManifest, Error> {
    let path = handle.sequence_locator.session_manifest(handle.uuid());

    if !context.store.exists(&path).await? {
        return Err(Error::NotFound(format!(
            "missing manifest file for session `{}`",
            handle.uuid()
        )));
    }

    let bytes = context.store.read_bytes(path).await?;

    let data: marshal::SessionManifest = bytes.try_into()?;

    Ok(data.try_into()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_query as query;
    use mosaicod_store as store;
    use std::sync::Arc;

    use crate::session;
    use types::Resource;

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new(store.clone(), 0).unwrap());

        Context::new(store.clone(), database.clone(), ts_gw)
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn test_session_create_and_delete(
        pool: sqlx::Pool<db::DatabaseType>,
    ) -> sqlx::Result<()> {
        let context = test_context(pool);

        let seq_locator = types::SequenceResourceLocator::from("test_sequence".to_string());

        let seq_handle = sequence::try_create(&context, seq_locator, None)
            .await
            .expect("Error creating sequence");

        let session_handle = session::try_create(&context, seq_handle.locator().clone())
            .await
            .expect("Error creating session");

        assert_eq!(
            session_handle.sequence_locator.locator(),
            seq_handle.locator().locator()
        );

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

        delete(&context, session_handle, false, types::allow_data_loss())
            .await
            .expect("Unable to delete session");

        db::session_find_by_uuid(&mut cx, &session_uuid)
            .await
            .unwrap_err();

        Ok(())
    }
}

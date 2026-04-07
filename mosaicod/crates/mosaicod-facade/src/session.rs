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
    pub(super) sequence_locator: types::SequenceResourceLocator,
}

impl Handle {
    /// Try to obtain a handle from a session UUID.
    /// Returns an error if the session does not exist.
    pub async fn try_from_uuid(uuid: &types::Uuid, context: &Context) -> Result<Self, Error> {
        let mut cx = context.db.connection();

        let db_session = db::session_find_by_uuid(&mut cx, uuid).await?;
        let db_sequence = db::sequence_find_by_id(&mut cx, db_session.sequence_id).await?;

        Ok(Self {
            identifiers: db_session.identifiers(),
            sequence_locator: db_sequence.resource_locator(),
        })
    }

    pub(super) fn uuid(&self) -> &types::Uuid {
        &self.identifiers.uuid
    }

    pub(super) fn id(&self) -> i32 {
        self.identifiers.id
    }
}

pub async fn try_create(
    sequence_locator: &types::SequenceResourceLocator,
    context: &Context,
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
    let handle = Handle::try_from_uuid(&session.uuid(), context).await?;
    create_manifest(&handle, context).await?;

    Ok(Handle {
        identifiers: session.into(),
        sequence_locator: sequence_locator.clone(),
    })
}

/// Creates the session manifest and saves it on store
/// TODO: find a better solution to create the manifest (without calling a method externally)
pub async fn create_manifest(handle: &Handle, context: &Context) -> Result<(), Error> {
    let mut cx = context.db.connection();

    let db_session = db::session_find_by_id(&mut cx, handle.id()).await?;

    let manifest =
        types::SessionManifest::new(handle.uuid().clone(), db_session.creation_timestamp());
    manifest_write_to_store(handle, manifest, context).await?;

    Ok(())
}

async fn manifest_write_to_store(
    handle: &Handle,
    manifest: types::SessionManifest,
    context: &Context,
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
pub async fn finalize(handle: &Handle, context: &Context) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    // Collect all topics associated with this session
    let topics = db::session_find_all_topic_locators(&mut tx, handle.uuid()).await?;

    // If the session does not contain any topic, return an error and leave the session unlocked.
    if topics.is_empty() {
        return Err(Error::SessionEmpty);
    }

    // If not all topics are locked, return an error and leave the session unlocked.
    let all_topics_locked = futures::future::join_all(topics.iter().map(async |topic_loc| {
        let topic_handle = topic::Handle::try_from_locator(topic_loc, context).await?;
        topic::manifest(&topic_handle, context).await
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?
    .into_iter()
    .all(|v| v.properties.locked);

    if !all_topics_locked {
        return Err(Error::TopicUnlocked);
    }

    // Update manifest (store).
    let mut manifest = manifest(handle, context).await?;
    manifest.locked = true;
    manifest.completed_at = Some(types::Timestamp::now());
    manifest.topics = topics;
    manifest_write_to_store(handle, manifest, context).await?;

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
    handle: Handle,
    only_if_unlocked: bool,
    allow_data_loss: types::DataLossToken,
    context: &Context,
) -> Result<(), Error> {
    let mut tx = context.db.transaction().await?;

    let error_report_msg = format!(
        "Some error occurred while deleting session `{}`",
        handle.uuid()
    );
    let mut error_report = types::ErrorReport::new(error_report_msg);

    let session_locked = manifest(&handle, context).await?.locked;

    if only_if_unlocked && session_locked {
        return Err(Error::SessionLocked);
    }

    // Deletes topic data
    let topics = topic_list(&handle, context).await?;
    for topic_loc in topics.clone() {
        let topic_handle = topic::Handle::try_from_locator(&topic_loc, context).await?;

        // We collect all the errors to build a sequence notification reporting all error if
        // something fails.
        if let Err(e) = topic::delete(topic_handle, allow_data_loss.clone(), context).await {
            error_report
                .errors
                .push(types::ErrorReportItem::new(topic_loc, e));
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
            sequence::Handle::try_from_locator(&handle.sequence_locator, context).await?;

        notification = Some(
            sequence::notify(
                &sequence_handle,
                types::NotificationType::Error,
                msg.clone(),
                context,
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
pub async fn topic_list(
    handle: &Handle,
    context: &Context,
) -> Result<Vec<types::TopicResourceLocator>, Error> {
    let mut cx = context.db.connection();

    let topics = db::session_find_all_topic_locators(&mut cx, handle.uuid()).await?;

    Ok(topics)
}

pub async fn manifest(handle: &Handle, context: &Context) -> Result<types::SessionManifest, Error> {
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

pub fn uuid(handle: &Handle, _context: &Context) -> types::Uuid {
    handle.uuid().clone()
}

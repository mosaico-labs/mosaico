//! A Session represents a new update session for adding topics to a target
//! sequence. It serves as a container for these new topic uploads,
//! ensuring that topics from previous sessions within the same sequence are not modified.
//! This provides a mechanism for versioning or snapshotting data.
//!
//! Multiple sessions can occur in parallel for the same sequence. Once a session is
//! finalized, all data associated with it becomes immutable.

use crate::{self as repo, FacadeError};
use log::trace;
use mosaicod_core::types;
use mosaicod_marshal as marshal;
use mosaicod_store as store;

/// A high-level facade for managing a session.
///
/// This struct provides a transactional API for creating and finalizing sessions,
/// coordinating operations between the metadata repository and the object store.
pub struct FacadeSession {
    pub uuid: types::Uuid,

    /// A reference to the underlying object store.
    store: store::StoreRef,

    /// A reference to the metadata repository.
    repo: repo::Repository,
}

impl FacadeSession {
    /// Creates a new upload session for a given sequence.
    pub fn new(session_uuid: types::Uuid, store: store::StoreRef, repo: repo::Repository) -> Self {
        Self {
            uuid: session_uuid,
            store: store,
            repo: repo,
        }
    }

    /// Finalizes the session, making it and all its associated data immutable.
    ///
    /// Once a session is finalized, no more topics can be added to it.
    pub async fn finalize(&self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let session = repo::session_find_by_uuid(&mut tx, &self.uuid).await?;

        // Collect all topics associated with this session
        let topics = repo::session_find_all_topic_names(&mut tx, &self.uuid).await?;

        let completion_timestamp = types::Timestamp::now();
        repo::session_lock(&mut tx, &self.uuid, &completion_timestamp).await?;

        let manifest = types::SessionManifest {
            uuid: session.uuid(),
            topics,
            creation_timestamp: session.creation_timestamp(),
            completion_timestamp: completion_timestamp,
        };

        // Get sequence data in order to store the manifest file inside the sequence namespace/directory
        let sequence = repo::sequence_find_by_id(&mut tx, session.sequence_id).await?;

        self.manifest_write_to_store(&sequence.resource_locator(), manifest)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn manifest_write_to_store(
        &self,
        locator: &types::SequenceResourceLocator,
        manifest: types::SessionManifest,
    ) -> Result<(), FacadeError> {
        let path = locator.session_manifest(&manifest.uuid);

        trace!("converting session manifest to bytes");
        let json_manifest = marshal::SessionManifest::from(manifest);
        let bytes: Vec<u8> = json_manifest.try_into()?;

        trace!(
            "writing session manifest `{}` to store",
            &path.to_string_lossy()
        );
        self.store.write_bytes(&path, bytes).await?;

        Ok(())
    }
}

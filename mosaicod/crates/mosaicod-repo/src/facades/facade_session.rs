//! A Session represents a new upload/update session for adding topics to a target
//! sequence. It serves as a container for these new topic uploads,
//! ensuring that topics from previous sessions within the same sequence are not modified.
//! This provides a mechanism for versioning or snapshotting data.
//!
//! Multiple sessions can occur in parallel for the same sequence. Once a session is
//! finalized, all data associated with it becomes immutable.

use crate::{self as repo, FacadeError};
use mosaicod_core::types;
use mosaicod_store as store;

/// A high-level facade for managing a session.
///
/// This struct provides a transactional API for creating and finalizing sessions,
/// coordinating operations between the metadata repository and the object store.
pub struct FacadeSession {
    /// The lookup identifier for the resource this facade operates on.
    pub lookup: types::ResourceLookup,

    /// A reference to the underlying object store.
    store: store::StoreRef,

    /// A reference to the metadata repository.
    repo: repo::Repository,
}

impl FacadeSession {
    /// Creates a new [`FacadeSession`] for a given sequence.
    pub fn new(
        sequence_lookup: types::ResourceLookup,
        store: store::StoreRef,
        repo: repo::Repository,
    ) -> Self {
        Self {
            lookup: sequence_lookup,
            store,
            repo,
        }
    }

    /// Creates a new session record for the target sequence.
    ///
    /// # Returns
    ///
    /// A `ResourceId` containing the ID and UUID of the newly created session.
    pub async fn create(&self) -> Result<types::ResourceId, FacadeError> {
        let mut tx = self.repo.transaction().await?;

        // Check if the requested sequence exists
        let srecord = repo::sequence_lookup(&mut tx, &self.lookup).await?;

        // create a session record

        Ok(srecord.into())
    }

    /// Finalizes the session, making it and all its associated data immutable.
    ///
    /// Once a session is finalized, no more topics can be added to it.
    pub async fn finalize(&self) -> Result<(), FacadeError> {
        todo!();
    }
}

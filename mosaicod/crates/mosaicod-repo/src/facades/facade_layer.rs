use super::FacadeError;
use crate as repo;
use mosaicod_core::types;
use mosaicod_store as store;

pub struct FacadeLayer {
    pub locator: types::LayerLocator,
    store: store::StoreRef,
    repo: repo::Repository,
}

impl FacadeLayer {
    pub fn new(
        locator: types::LayerLocator,
        store: store::StoreRef,
        repo: repo::Repository,
    ) -> Self {
        Self {
            locator,
            store,
            repo,
        }
    }

    pub async fn all(repo: repo::Repository) -> Result<Vec<types::Layer>, FacadeError> {
        let mut cx = repo.connection();

        let layers = repo::layer_find_all(&mut cx).await?;

        Ok(layers.into_iter().map(Into::into).collect())
    }

    pub async fn create(&self, description: String) -> Result<i32, FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let layer = types::Layer::new(self.locator.clone(), description);
        let layer = repo::layer_create(&mut tx, layer).await?;

        tx.commit().await?;
        Ok(layer.layer_id)
    }

    pub async fn delete(self) -> Result<(), FacadeError> {
        let mut tx = self.repo.transaction().await?;

        let layer = repo::layer_find_by_locator(&mut tx, &self.locator).await?;
        repo::layer_delete(&mut tx, layer.layer_id).await?;

        tx.commit().await?;

        Ok(())
    }

    pub async fn update(
        self,
        new_locator: types::LayerLocator,
        new_description: &str,
    ) -> Result<Self, FacadeError> {
        let mut tx = self.repo.transaction().await?;

        repo::layer_update(&mut tx, &self.locator, &new_locator, new_description).await?;

        tx.commit().await?;

        Ok(Self {
            locator: new_locator,
            store: self.store,
            repo: self.repo,
        })
    }
}

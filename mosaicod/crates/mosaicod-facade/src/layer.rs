use super::Error;
use mosaicod_db as db;
use mosaicod_core::types;
use mosaicod_store as store;

pub struct Layer {
    pub locator: types::LayerLocator,
    store: store::StoreRef,
    db: db::Database,
}

impl Layer {
    pub fn new(
        locator: types::LayerLocator,
        store: store::StoreRef,
        db: db::Database,
    ) -> Self {
        Self {
            locator,
            store,
            db,
        }
    }

    pub async fn all(db: db::Database) -> Result<Vec<types::Layer>, Error> {
        let mut cx = db.connection();

        let layers = db::layer_find_all(&mut cx).await?;

        Ok(layers.into_iter().map(Into::into).collect())
    }

    pub async fn create(&self, description: String) -> Result<i32, Error> {
        let mut tx = self.db.transaction().await?;

        let layer = types::Layer::new(self.locator.clone(), description);
        let layer = db::layer_create(&mut tx, layer).await?;

        tx.commit().await?;
        Ok(layer.layer_id)
    }

    pub async fn delete(self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        let layer = db::layer_find_by_locator(&mut tx, &self.locator).await?;
        db::layer_delete(&mut tx, layer.layer_id).await?;

        tx.commit().await?;

        Ok(())
    }

    pub async fn update(
        self,
        new_locator: types::LayerLocator,
        new_description: &str,
    ) -> Result<Self, Error> {
        let mut tx = self.db.transaction().await?;

        db::layer_update(&mut tx, &self.locator, &new_locator, new_description).await?;

        tx.commit().await?;

        Ok(Self {
            locator: new_locator,
            store: self.store,
            db: self.db,
        })
    }
}

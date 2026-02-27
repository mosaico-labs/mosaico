use super::Error;
use mosaicod_core::types;
use mosaicod_db as db;

pub struct Auth {
    scope: types::AuthScope,
    db: db::Database,
}

impl Auth {
    pub async fn try_from_key(api_key: types::ApiKey, db: db::Database) -> Result<Self, Error> {
        todo!()
    }

    /// Loockup an API key using its
    pub async fn try_from_fingerprint(
        _fingerprint: String,
        _db: db::Database,
    ) -> Result<Self, Error> {
        todo!();
    }

    pub async fn create(
        _permissions: types::Permissions,
        _description: Option<String>,
        _expire_duration: Option<std::time::Duration>,
        _db: db::Database,
    ) -> Result<Self, Error> {
        // let tx = db.transaction().await?;
        //
        // types::ApiKey::new(permission, description);
        // db::auth_scope_create(&mut tx, );
        //
        // Self{db}
        todo!();
    }

    pub async fn all(_db: db::Database) -> Result<Vec<Self>, Error> {
        todo!();
    }

    pub async fn delete(self) -> Result<(), Error> {
        todo!()
    }

    pub fn scope(&self) -> &types::AuthScope {
        &self.scope
    }

    pub fn into_scope(self) -> types::AuthScope {
        self.scope
    }
}

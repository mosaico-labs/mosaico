use super::Error;
use mosaicod_core::types;
use mosaicod_db as db;

pub struct Auth {
    scope: types::AuthScope,
    db: db::Database,
}

impl Auth {
    /// Create a new auth facade using an existing scope.
    ///
    /// This function does not perform any checks, if the auth scope is not existing subsequent
    /// calls will return errors
    pub fn from_scope(scope: types::AuthScope, db: db::Database) -> Self {
        Self { scope, db }
    }

    /// Lookup an auth scope using the API key fingerprint
    pub async fn try_from_fingerprint(
        fingerprint: String,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut cx = db.connection();

        let scope = db::auth_scope_find_by_fingerprint(&mut cx, &fingerprint).await?;

        Ok(Self { scope, db })
    }

    /// Creates a new auth scope in the system
    pub async fn create(
        permissions: types::Permissions,
        description: String,
        expire_duration: Option<std::time::Duration>,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut tx = db.transaction().await?;

        let scope = types::AuthScope::new(permissions, description, expire_duration);
        let scope = db::auth_scope_create(&mut tx, scope).await?;

        tx.commit().await?;

        Ok(Self { scope, db })
    }

    /// Returns a list of all auth scope in the system
    pub async fn all_scopes(db: db::Database) -> Result<Vec<types::AuthScope>, Error> {
        let mut cx = db.connection();

        Ok(db::auth_scope_find_all(&mut cx).await?)
    }

    /// Deletes the current auth scope
    pub async fn delete(self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        db::auth_scope_delete(&mut tx, self.scope.key.fingerprint()).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Returns the current auth scope
    pub fn scope(&self) -> &types::AuthScope {
        &self.scope
    }

    /// Consumes the facade and returns the inner auth scope
    pub fn into_scope(self) -> types::AuthScope {
        self.scope
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn auth_scope_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let database = db::testing::Database::new(pool);

        let fscope = Auth::create(
            types::Permissions::READ,
            "some text".to_owned(),
            None,
            database.clone(),
        )
        .await
        .unwrap();

        let scope = fscope.scope().clone();

        {
            let mut cx = database.connection();

            let res_scope = db::auth_scope_find_by_fingerprint(&mut cx, scope.key().fingerprint())
                .await
                .unwrap();

            assert_eq!(res_scope.permissions, scope.permissions);
            assert_eq!(res_scope.key(), scope.key());
        }

        fscope.delete().await.unwrap();

        {
            let mut cx = database.connection();

            let res_scope =
                db::auth_scope_find_by_fingerprint(&mut cx, scope.key().fingerprint()).await;

            assert!(res_scope.is_err());
        }

        Ok(())
    }
}

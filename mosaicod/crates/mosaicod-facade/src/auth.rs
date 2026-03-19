use super::Error;
use mosaicod_core::types;
use mosaicod_db as db;

pub struct Auth {
    api_key: types::ApiKey,
    db: db::Database,
}

impl Auth {
    /// Create a new auth facade using an existing API key.
    ///
    /// This function does not perform any checks, if the API key is not existing subsequent
    /// calls will return errors
    pub fn from_api_key(api_key: types::ApiKey, db: db::Database) -> Self {
        Self { api_key, db }
    }

    /// Lookup an API key using its fingerprint
    pub async fn try_from_fingerprint(fingerprint: &str, db: db::Database) -> Result<Self, Error> {
        let mut cx = db.connection();

        let api_key = db::api_key_find_by_fingerprint(&mut cx, fingerprint).await?;

        Ok(Self { api_key, db })
    }

    /// Creates a new API key in the system
    pub async fn create(
        permissions: types::auth::Permissions,
        description: String,
        expires_at: Option<types::Timestamp>,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut tx = db.transaction().await?;

        let api_key = types::ApiKey::new(permissions, description, expires_at);
        let api_key = db::api_key_create(&mut tx, api_key).await?;

        tx.commit().await?;

        Ok(Self { api_key, db })
    }

    /// Returns a list of all API keys in the system
    pub async fn all_keys(db: db::Database) -> Result<Vec<types::ApiKey>, Error> {
        let mut cx = db.connection();

        Ok(db::api_key_find_all(&mut cx).await?)
    }

    /// Deletes the current API key
    pub async fn delete(self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        db::api_key_delete(&mut tx, self.api_key.key.fingerprint()).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Returns the inner API key
    pub fn api_key(&self) -> &types::ApiKey {
        &self.api_key
    }

    /// Consumes the facade and returns the inner API key
    pub fn into_api_key(self) -> types::ApiKey {
        self.api_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn auth_policy_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let database = db::testing::Database::new(pool);

        let fauth = Auth::create(
            types::auth::Permissions::READ,
            "some text".to_owned(),
            None,
            database.clone(),
        )
        .await
        .unwrap();

        let key = fauth.api_key().clone();

        {
            let mut cx = database.connection();

            let res_key = db::api_key_find_by_fingerprint(&mut cx, key.token().fingerprint())
                .await
                .unwrap();

            assert_eq!(res_key.permissions, key.permissions);
            assert_eq!(res_key.token(), key.token());
        }

        fauth.delete().await.unwrap();

        {
            let mut cx = database.connection();

            let res_policy =
                db::api_key_find_by_fingerprint(&mut cx, key.token().fingerprint()).await;

            assert!(res_policy.is_err());
        }

        Ok(())
    }
}

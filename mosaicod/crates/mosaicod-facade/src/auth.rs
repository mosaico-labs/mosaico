use super::Error;
use mosaicod_core::types;
use mosaicod_db as db;

pub struct Auth {
    policy: types::AuthorizationPolicy,
    db: db::Database,
}

impl Auth {
    /// Create a new auth facade using an existing policy.
    ///
    /// This function does not perform any checks, if the authorization policy is not existing subsequent
    /// calls will return errors
    pub fn from_policy(policy: types::AuthorizationPolicy, db: db::Database) -> Self {
        Self { policy, db }
    }

    /// Lookup an authorization policy using the API key fingerprint
    pub async fn try_from_fingerprint(
        fingerprint: String,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut cx = db.connection();

        let policy = db::authz_policy_find_by_fingerprint(&mut cx, &fingerprint).await?;

        Ok(Self { policy, db })
    }

    /// Creates a new authorization policy in the system
    pub async fn create(
        permissions: types::Permissions,
        description: String,
        expire_duration: Option<std::time::Duration>,
        db: db::Database,
    ) -> Result<Self, Error> {
        let mut tx = db.transaction().await?;

        let policy = types::AuthorizationPolicy::new(permissions, description, expire_duration);
        let policy = db::authz_policy_create(&mut tx, policy).await?;

        tx.commit().await?;

        Ok(Self { policy, db })
    }

    /// Returns a list of all authorization policy in the system
    pub async fn all_policies(db: db::Database) -> Result<Vec<types::AuthorizationPolicy>, Error> {
        let mut cx = db.connection();

        Ok(db::authz_policy_find_all(&mut cx).await?)
    }

    /// Deletes the current authorization policy
    pub async fn delete(self) -> Result<(), Error> {
        let mut tx = self.db.transaction().await?;

        db::authz_policy_delete(&mut tx, self.policy.key.fingerprint()).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Returns the current authorization policy
    pub fn policy(&self) -> &types::AuthorizationPolicy {
        &self.policy
    }

    /// Consumes the facade and returns the inner authorization policy
    pub fn into_policy(self) -> types::AuthorizationPolicy {
        self.policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn auth_policy_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let database = db::testing::Database::new(pool);

        let fauth = Auth::create(
            types::Permissions::READ,
            "some text".to_owned(),
            None,
            database.clone(),
        )
        .await
        .unwrap();

        let policy = fauth.policy().clone();

        {
            let mut cx = database.connection();

            let res_policy =
                db::authz_policy_find_by_fingerprint(&mut cx, policy.key().fingerprint())
                    .await
                    .unwrap();

            assert_eq!(res_policy.permissions, policy.permissions);
            assert_eq!(res_policy.key(), policy.key());
        }

        fauth.delete().await.unwrap();

        {
            let mut cx = database.connection();

            let res_policy =
                db::authz_policy_find_by_fingerprint(&mut cx, policy.key().fingerprint()).await;

            assert!(res_policy.is_err());
        }

        Ok(())
    }
}

use super::Context;
use mosaicod_core::{error::PublicResult as Result, types};
use mosaicod_db as db;

pub struct Handle {
    api_key: types::ApiKey,
}

impl Handle {
    /// Creates an API key [`Handle`] using its fingerprint
    pub async fn try_from_fingerprint(context: &Context, fingerprint: &str) -> Result<Self> {
        let mut cx = context.db.connection();
        let api_key = db::api_key_find_by_fingerprint(&mut cx, fingerprint).await?;
        Ok(Self { api_key })
    }

    /// Returns the inner API key
    pub fn api_key(&self) -> &types::ApiKey {
        &self.api_key
    }
}

impl From<Handle> for types::ApiKey {
    fn from(h: Handle) -> types::ApiKey {
        h.api_key
    }
}

/// Creates a new API key in the system
pub async fn create(
    context: &Context,
    permissions: types::auth::Permission,
    description: String,
    expires_at: Option<types::Timestamp>,
) -> Result<Handle> {
    let api_key = types::ApiKey::new(permissions, description, expires_at);
    let mut cx = context.db.connection();
    let api_key = db::api_key_create(&mut cx, api_key).await?;
    Ok(Handle { api_key })
}

/// Returns a list of all API keys in the system
pub async fn all_keys(context: &Context) -> Result<Vec<types::ApiKey>> {
    let mut cx = context.db.connection();
    Ok(db::api_key_find_all(&mut cx).await?)
}

/// Deletes the current API key
pub async fn delete(context: &Context, handle: Handle) -> Result<()> {
    let mut cx = context.db.connection();
    db::api_key_delete(&mut cx, handle.api_key.token().fingerprint()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_query as query;
    use mosaicod_store as store;
    use std::sync::Arc;

    fn test_context(pool: sqlx::Pool<db::DatabaseType>) -> Context {
        let database = db::testing::Database::new(pool);
        let store = store::testing::Store::new_random_on_tmp().unwrap();
        let ts_gw = Arc::new(query::TimeseriesEngine::try_new((*store).clone(), 0).unwrap());

        Context::new((*store).clone(), (*database).clone(), ts_gw)
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn auth_policy_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let context = test_context(pool);

        let handle = create(
            &context,
            types::auth::Permission::Read,
            "some text".to_owned(),
            None,
        )
        .await
        .unwrap();

        let key = handle.api_key().clone();

        {
            let mut cx = context.db.connection();

            let res_key = db::api_key_find_by_fingerprint(&mut cx, key.token().fingerprint())
                .await
                .unwrap();

            assert_eq!(res_key.permission, key.permission);
            assert_eq!(res_key.token(), key.token());
        }

        delete(&context, handle).await.unwrap();

        {
            let mut cx = context.db.connection();

            let res_policy =
                db::api_key_find_by_fingerprint(&mut cx, key.token().fingerprint()).await;

            assert!(res_policy.is_err());
        }

        Ok(())
    }
}

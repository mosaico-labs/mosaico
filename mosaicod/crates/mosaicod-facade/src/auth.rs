use mosaicod_core::{
    error::{BoxPublicError, PublicResult as Result},
    types,
};
use mosaicod_db as db;

use super::Context;

/// Why [`Handle::try_from_token`] rejected a token.
///
/// This is deliberately not a [`PublicError`](mosaicod_core::error::PublicError), so it
/// cannot reach a client through `?`. Callers that answer clients must map
/// [`TokenError::UnknownFingerprint`] and [`TokenError::PayloadMismatch`] to the same
/// error, or a client could probe which fingerprints are registered.
#[derive(Debug)]
pub enum TokenError {
    /// No key is registered under the token's fingerprint.
    UnknownFingerprint,
    /// A key is registered under the token's fingerprint, but its payload differs.
    PayloadMismatch,
    /// The lookup failed for another reason, e.g. the database is unreachable.
    Other(BoxPublicError),
}

pub struct Handle {
    api_key: types::ApiKey,
}

impl Handle {
    /// Authenticates a token presented by a client and returns the [`Handle`] of its key.
    ///
    /// The key is looked up by the token's fingerprint, then the stored token is compared
    /// with the presented one in constant time. Expired keys are returned as well, and
    /// the caller decides how to handle them.
    pub async fn try_from_token(
        context: &Context,
        token: &types::auth::Token,
    ) -> std::result::Result<Self, TokenError> {
        let mut cx = context.db.connection();
        let api_key = match db::api_key_find_by_fingerprint(&mut cx, token.fingerprint()).await {
            Ok(api_key) => api_key,
            Err(db::Error::NotFound) => return Err(TokenError::UnknownFingerprint),
            Err(e) => return Err(TokenError::Other(e.into())),
        };

        if api_key.token() != token {
            return Err(TokenError::PayloadMismatch);
        }

        Ok(Self { api_key })
    }

    /// Creates an API key [`Handle`] using its fingerprint.
    ///
    /// This performs no authentication, since the fingerprint is public. Use it only for
    /// administrative lookups (status, revoke, purge). Authenticate client requests with
    /// [`Handle::try_from_token`].
    pub async fn try_from_fingerprint(context: &Context, fingerprint: &str) -> Result<Self> {
        let mut cx = context.db.connection();
        let api_key = db::api_key_find_by_fingerprint(&mut cx, fingerprint).await?;
        Ok(Self { api_key })
    }

    /// Returns the inner API key.
    pub fn api_key(&self) -> &types::ApiKey {
        &self.api_key
    }
}

impl From<Handle> for types::ApiKey {
    fn from(h: Handle) -> types::ApiKey {
        h.api_key
    }
}

/// Creates a new API key in the system.
pub async fn create(
    context: &Context,
    permissions: types::auth::Permissions,
    description: String,
    expires_at: Option<types::Timestamp>,
) -> Result<Handle> {
    let api_key = types::ApiKey::new(permissions, description, expires_at);
    let mut cx = context.db.connection();
    let api_key = db::api_key_create(&mut cx, api_key).await?;
    Ok(Handle { api_key })
}

/// Returns a list of all API keys in the system.
pub async fn all_keys(context: &Context) -> Result<Vec<types::ApiKey>> {
    let mut cx = context.db.connection();
    Ok(db::api_key_find_all(&mut cx).await?)
}

/// Deletes the current API key.
pub async fn delete(context: &Context, handle: Handle) -> Result<()> {
    let mut cx = context.db.connection();
    db::api_key_delete(&mut cx, handle.api_key.token().fingerprint()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mosaicod_query as query;
    use mosaicod_store as store;

    use super::*;

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
            types::auth::Permission::Read.into(),
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

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn try_from_token_checks_payload(pool: sqlx::Pool<db::DatabaseType>) {
        let context = test_context(pool);

        let key = create(
            &context,
            types::auth::Permission::Read.into(),
            "some text".to_owned(),
            None,
        )
        .await
        .unwrap()
        .api_key()
        .clone();
        let token = key.token();

        // The registered token authenticates.
        let handle = Handle::try_from_token(&context, token).await.unwrap();
        assert_eq!(handle.api_key().token(), token);

        // The registered fingerprint with a different payload does not.
        let other = types::auth::Token::new();
        let mismatched = types::auth::Token::from_bytes(
            other.payload().as_bytes().try_into().unwrap(),
            token.fingerprint().as_bytes().try_into().unwrap(),
        );
        assert!(matches!(
            Handle::try_from_token(&context, &mismatched).await,
            Err(TokenError::PayloadMismatch)
        ));

        assert!(matches!(
            Handle::try_from_token(&context, &other).await,
            Err(TokenError::UnknownFingerprint)
        ));
    }
}

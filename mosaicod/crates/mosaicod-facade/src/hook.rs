//! Hooks: HTTP endpoints that mosaicod sends an event to when a topic is uploaded or a session
//! is finalized.

use crate::Context;
use mosaicod_core::error::PublicError;
use mosaicod_core::{self as core, error::PublicResult as Result, types};
use mosaicod_db as db;

/// Registers a hook.
///
/// Only `http` and `https` URLs are accepted. The URL is stored normalized, so
/// `http://host:80` and `http://host/` are the same hook.
pub async fn create(context: &Context, url: &str) -> Result<types::Hook> {
    let parsed = url::Url::parse(url)
        .map_err(|e| core::Error::bad_request(format!("invalid hook URL `{url}`: {e}")))?;

    if !matches!(parsed.scheme(), "http" | "https") {
        Err(core::Error::bad_request(format!(
            "hook URL `{url}` must use `http` or `https`"
        )))?;
    }

    let mut cx = context.db.connection();
    let record = db::hook_create(&mut cx, parsed.as_str(), types::Timestamp::now().as_i64())
        .await
        .map_err(|e| match e {
            db::Error::AlreadyExists => core::Error::already_exists(format!("hook `{parsed}`")),
            _ => e.error(),
        })?;

    Ok(record.into())
}

/// Lists every hook, ordered by id.
pub async fn list(context: &Context) -> Result<Vec<types::Hook>> {
    let mut cx = context.db.connection();
    let records = db::hook_find_all(&mut cx).await?;
    Ok(records.into_iter().map(Into::into).collect())
}

/// Deletes hooks, all or none, in one transaction.
///
/// If any id is unknown, nothing is deleted and a `not_found` error names every unknown id. A
/// repeated id counts once.
pub async fn delete(context: &Context, ids: &[i32]) -> Result<()> {
    let mut ids = ids.to_vec();
    ids.sort_unstable();
    ids.dedup();

    let mut tx = context.db.transaction().await?;
    let mut unknown = Vec::new();

    for id in ids {
        if !db::hook_delete(&mut tx, id).await? {
            unknown.push(id.to_string());
        }
    }

    if unknown.is_empty() {
        tx.commit().await?;
        return Ok(());
    }

    tx.rollback().await?;

    let noun = if unknown.len() == 1 { "hook" } else { "hooks" };
    Err(core::Error::not_found(format!("{noun} {}", unknown.join(", "))).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mosaicod_core::error::ErrorKind;
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
    async fn create_list_delete(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let context = test_context(pool);

        let first = create(&context, "http://hooks.internal:8080")
            .await
            .unwrap();
        let second = create(&context, "https://example.com/mosaico")
            .await
            .unwrap();

        // Stored normalized.
        assert_eq!(first.url, "http://hooks.internal:8080/");
        assert_eq!(second.url, "https://example.com/mosaico");

        assert_eq!(
            list(&context).await.unwrap(),
            vec![first.clone(), second.clone()]
        );

        delete(&context, &[first.id]).await.unwrap();
        assert_eq!(list(&context).await.unwrap(), vec![second]);

        Ok(())
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn create_rejects_bad_urls(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let context = test_context(pool);

        for url in ["not a url", "ftp://hooks.internal/", "file:///tmp/hook"] {
            let err = create(&context, url).await.unwrap_err().error();
            assert!(
                matches!(err.kind(), ErrorKind::BadRequest(_)),
                "{url}: {err:?}"
            );
        }

        assert!(list(&context).await.unwrap().is_empty());

        Ok(())
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn create_rejects_duplicates(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let context = test_context(pool);

        create(&context, "http://hooks.internal:80/").await.unwrap();

        // The same URL once normalized: no default port, and a trailing slash.
        let err = create(&context, "http://hooks.internal")
            .await
            .unwrap_err()
            .error();
        assert!(matches!(err.kind(), ErrorKind::AlreadyExists(_)), "{err:?}");

        assert_eq!(list(&context).await.unwrap().len(), 1);

        Ok(())
    }

    #[sqlx::test(migrator = "db::testing::MIGRATOR")]
    async fn delete_is_all_or_nothing(pool: sqlx::Pool<db::DatabaseType>) -> sqlx::Result<()> {
        let context = test_context(pool);

        let hook = create(&context, "http://hooks.internal/").await.unwrap();

        // The known hook is deleted first, then rolled back.
        let err = delete(&context, &[hook.id, 43, 42])
            .await
            .unwrap_err()
            .error();
        assert!(
            matches!(err.kind(), ErrorKind::NotFound(what) if what == "hooks 42, 43"),
            "{err:?}"
        );
        assert_eq!(list(&context).await.unwrap(), vec![hook.clone()]);

        // A repeated id isn't reported as unknown after its first delete.
        delete(&context, &[hook.id, hook.id]).await.unwrap();
        assert!(list(&context).await.unwrap().is_empty());

        Ok(())
    }
}

//! This module provides access to the instance registry: a lightweight record of which
//! `mosaicod` processes (server, cleanup, ...) are currently, or were recently, running.
//! See `mosaicod ps`.

use crate::{Error, core::AsExec, sql::schema};
use mosaicod_core::types;
use tracing::{trace, warn};

/// Registers a new instance, seeding its heartbeat to `now_unix_tstamp_secs`.
///
/// [`one_shot`] should be `true` for a routine that performs a single run and exits rather than
/// looping until shut down (e.g. `mosaicod cleanup` with the default `--time-interval 0`).
pub async fn instance_registry_create(
    exe: &mut impl AsExec,
    kind: types::InstanceKind,
    hostname: &str,
    pid: i32,
    now_unix_tstamp_secs: i64,
    one_shot: bool,
) -> Result<schema::InstanceRegistryRecord, Error> {
    trace!(
        "registering a new `{}` instance (host `{}`, pid {}, one_shot {})",
        kind, hostname, pid, one_shot
    );

    let kind = kind.to_string();

    Ok(sqlx::query_as!(
        schema::InstanceRegistryRecord,
        r#"
            INSERT INTO instance_registry_t
                (kind, hostname, pid, started_unix_tstamp_secs, last_heartbeat_unix_tstamp_secs, one_shot)
            VALUES ($1, $2, $3, $4, $4, $5)
            RETURNING *
            "#,
        kind,
        hostname,
        pid,
        now_unix_tstamp_secs,
        one_shot,
    )
    .fetch_one(exe.as_exec())
    .await?)
}

/// Refreshes the heartbeat of the given instance.
///
/// Returns `false` if the instance is no longer registered (e.g. its row was already purged as
/// expired), `true` otherwise.
pub async fn instance_registry_heartbeat(
    exe: &mut impl AsExec,
    instance_id: i32,
    now_unix_tstamp_secs: i64,
) -> Result<bool, Error> {
    trace!("heartbeat for instance {}", instance_id);

    let res = sqlx::query!(
        r#"UPDATE instance_registry_t SET last_heartbeat_unix_tstamp_secs = $1 WHERE instance_id = $2"#,
        now_unix_tstamp_secs,
        instance_id,
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected() != 0)
}

/// Deregisters an instance, e.g. because it is exiting gracefully. Complements
/// [`instance_registry_delete_expired`] as the fast path: an instance that deregisters itself
/// doesn't have to wait around as a stale row until GC catches up with it.
///
/// Returns `false` if the instance was already not registered (e.g. already purged as expired),
/// `true` otherwise.
pub async fn instance_registry_delete(
    exe: &mut impl AsExec,
    instance_id: i32,
) -> Result<bool, Error> {
    trace!("deregistering instance {}", instance_id);

    let res = sqlx::query!(
        r#"DELETE FROM instance_registry_t WHERE instance_id = $1"#,
        instance_id,
    )
    .execute(exe.as_exec())
    .await?;

    Ok(res.rows_affected() != 0)
}

/// Lists every registered instance, ordered by kind and instance id.
pub async fn instance_registry_list(
    exe: &mut impl AsExec,
) -> Result<Vec<schema::InstanceRegistryRecord>, Error> {
    trace!("listing registered instances");

    Ok(sqlx::query_as!(
        schema::InstanceRegistryRecord,
        r#"SELECT * FROM instance_registry_t ORDER BY kind ASC, instance_id ASC"#,
    )
    .fetch_all(exe.as_exec())
    .await?)
}

/// Permanently deletes instances whose last heartbeat exceeds `threshold_unix_tstamp_secs`.
///
/// Returns the number of deleted rows.
pub async fn instance_registry_delete_expired(
    exe: &mut impl AsExec,
    threshold_unix_tstamp_secs: i64,
    _: types::DataLossToken,
) -> Result<u64, Error> {
    trace!("deleting expired instance registry entries");

    let res = sqlx::query!(
        r#"DELETE FROM instance_registry_t WHERE last_heartbeat_unix_tstamp_secs < $1"#,
        threshold_unix_tstamp_secs,
    )
    .execute(exe.as_exec())
    .await?;

    if res.rows_affected() > 0 {
        warn!(
            "(data loss) deleted {} expired instance registry entries",
            res.rows_affected()
        );
    }

    Ok(res.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DatabaseType, testing};
    use sqlx::Pool;

    #[sqlx::test]
    async fn test_instance_registry_register(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();

        let now = chrono::Utc::now().timestamp();

        let record = instance_registry_create(
            &mut cx,
            types::InstanceKind::Cleanup,
            "host-a",
            1234,
            now,
            true,
        )
        .await
        .unwrap();

        assert_eq!(record.kind(), Some(types::InstanceKind::Cleanup));
        assert_eq!(record.hostname, "host-a");
        assert_eq!(record.pid, 1234);
        assert_eq!(record.started_unix_tstamp_secs, now);
        assert_eq!(record.last_heartbeat_unix_tstamp_secs, now);
        assert!(record.one_shot);
    }

    #[sqlx::test]
    async fn test_instance_registry_heartbeat(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();

        let now = chrono::Utc::now().timestamp();

        let record = instance_registry_create(
            &mut cx,
            types::InstanceKind::Server,
            "host-a",
            1,
            now,
            false,
        )
        .await
        .unwrap();
        assert!(!record.one_shot);

        let later = now + 60;
        assert!(
            instance_registry_heartbeat(&mut cx, record.instance_id, later)
                .await
                .unwrap()
        );

        let instances = instance_registry_list(&mut cx).await.unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].last_heartbeat_unix_tstamp_secs, later);

        // Heartbeat for an unknown instance should report that no row was updated.
        assert!(
            !instance_registry_heartbeat(&mut cx, record.instance_id + 999, later)
                .await
                .unwrap()
        );
    }

    #[sqlx::test]
    async fn test_instance_registry_deregister(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();

        let now = chrono::Utc::now().timestamp();

        let record = instance_registry_create(
            &mut cx,
            types::InstanceKind::Server,
            "host-a",
            1,
            now,
            false,
        )
        .await
        .unwrap();

        assert!(
            instance_registry_delete(&mut cx, record.instance_id)
                .await
                .unwrap()
        );

        let instances = instance_registry_list(&mut cx).await.unwrap();
        assert!(instances.is_empty());

        // Deregistering an already-gone instance should report that no row was deleted.
        assert!(
            !instance_registry_delete(&mut cx, record.instance_id)
                .await
                .unwrap()
        );
    }

    #[sqlx::test]
    async fn test_instance_registry_list_ordering(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();

        let now = chrono::Utc::now().timestamp();

        instance_registry_create(
            &mut cx,
            types::InstanceKind::Server,
            "host-b",
            2,
            now,
            false,
        )
        .await
        .unwrap();
        instance_registry_create(
            &mut cx,
            types::InstanceKind::Cleanup,
            "host-a",
            1,
            now,
            true,
        )
        .await
        .unwrap();
        instance_registry_create(
            &mut cx,
            types::InstanceKind::Server,
            "host-a",
            3,
            now,
            false,
        )
        .await
        .unwrap();

        let instances = instance_registry_list(&mut cx).await.unwrap();

        assert_eq!(
            instances
                .iter()
                .map(|i| (i.kind(), i.instance_id))
                .collect::<Vec<_>>(),
            vec![
                (Some(types::InstanceKind::Cleanup), 2),
                (Some(types::InstanceKind::Server), 1),
                (Some(types::InstanceKind::Server), 3),
            ]
        );
    }

    #[sqlx::test]
    async fn test_instance_registry_delete_expired(pool: Pool<DatabaseType>) {
        let database = testing::Database::new(pool);
        let mut cx = database.connection();

        let now = chrono::Utc::now().timestamp();

        let stale = instance_registry_create(
            &mut cx,
            types::InstanceKind::Cleanup,
            "host-a",
            1,
            now,
            true,
        )
        .await
        .unwrap();
        let fresh = instance_registry_create(
            &mut cx,
            types::InstanceKind::Server,
            "host-a",
            2,
            now,
            false,
        )
        .await
        .unwrap();

        // Backdate the stale instance's heartbeat directly, simulating it having gone quiet a
        // long time ago.
        instance_registry_heartbeat(&mut cx, stale.instance_id, now - 1000)
            .await
            .unwrap();

        let deleted =
            instance_registry_delete_expired(&mut cx, now - 500, types::allow_data_loss())
                .await
                .unwrap();
        assert_eq!(deleted, 1);

        let instances = instance_registry_list(&mut cx).await.unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].instance_id, fresh.instance_id);
    }
}

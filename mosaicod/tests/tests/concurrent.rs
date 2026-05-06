#![allow(unused_crate_dependencies)]
use mosaicod_core::types;
use mosaicod_db as db;
use mosaicod_ext as ext;
use mosaicod_marshal as marshal;
use tests::{self, actions, common};

// ===========================================================================
// Concurrent tests
// ===========================================================================

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_sequence_create(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let port = server.port();
    let sequence_name = "concurrent_seq";

    let mut client1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut client2 = common::ClientBuilder::new(common::HOST, port).build().await;

    let h1 = tokio::spawn(async move {
        actions::sequence_create(&mut client1, "concurrent_seq", None).await
    });
    let h2 = tokio::spawn(async move {
        actions::sequence_create(&mut client2, "concurrent_seq", None).await
    });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let (success_count, already_exists_count) =
        [&r1, &r2]
            .iter()
            .fold((0usize, 0usize), |(succ, ae), r| match r {
                Ok(_) => (succ + 1, ae),
                Err(e) if e.code() == tonic::Code::AlreadyExists => (succ, ae + 1),
                Err(e) => panic!("unexpected error: {:?}", e),
            });

    assert_eq!(success_count, 1);
    assert_eq!(already_exists_count, 1);

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;
    let info = actions::get_flight_info(&mut client, sequence_name).await;
    assert!(info.is_ok());

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_session_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let port = server.port();
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/my_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();
    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, topic_name, batches, false)
        .await
        .unwrap();

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;
    let s1 = session_uuid.clone();
    let s2 = session_uuid.clone();

    let h1 = tokio::spawn(async move { actions::session_finalize(&mut c1, &s1).await });
    let h2 = tokio::spawn(async move { actions::session_finalize(&mut c2, &s2).await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let success_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert_eq!(success_count, 1);

    for r in [&r1, &r2] {
        if let Err(e) = r {
            assert!(
                matches!(e.code(), tonic::Code::FailedPrecondition),
                "unexpected error code: {:?}",
                e.code()
            );
        }
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_do_put_same_topic(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let port = server.port();
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/concurrent_topic", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;

    let t1 = topic_uuid.clone();
    let t2 = topic_uuid.clone();
    let n1 = topic_name.clone();
    let n2 = topic_name.clone();

    let h1 = tokio::spawn(async move {
        let batches = vec![ext::arrow::testing::dummy_batch()];
        actions::do_put(&mut c1, &t1, &n1, batches, false).await
    });
    let h2 = tokio::spawn(async move {
        let batches = vec![ext::arrow::testing::dummy_batch()];
        actions::do_put(&mut c2, &t2, &n2, batches, false).await
    });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let success_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert!(success_count >= 1, "at least one writer must succeed");

    for r in [&r1, &r2] {
        if let Err(e) = r {
            assert!(
                matches!(
                    e.code(),
                    tonic::Code::FailedPrecondition | tonic::Code::Aborted
                ),
                "unexpected error code: {:?}",
                e.code()
            );
        }
    }

    actions::session_finalize(&mut client, &session_uuid)
        .await
        .unwrap();

    let info = actions::get_flight_info(&mut client, topic_name)
        .await
        .unwrap();
    let app_metadata: marshal::flight::TopicAppMetadata =
        info.endpoint[0].clone().app_metadata.try_into().unwrap();
    let chunks = app_metadata.info.unwrap().chunks_number;

    assert_eq!(chunks as usize, success_count);

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_topic_create_during_finalize(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let port = server.port();
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let existing_topic = &format!("{}/existing", sequence_name);
    let new_topic = &format!("{}/new", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();

    let topic_uuid = actions::topic_create(&mut client, &session_uuid, existing_topic, None)
        .await
        .unwrap();
    let batches = vec![ext::arrow::testing::dummy_batch()];
    actions::do_put(&mut client, &topic_uuid, existing_topic, batches, false)
        .await
        .unwrap();

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;
    let s1 = session_uuid.clone();
    let s2 = session_uuid.clone();
    let new_topic_owned = new_topic.clone();

    let h_finalize = tokio::spawn(async move { actions::session_finalize(&mut c1, &s1).await });
    let h_create =
        tokio::spawn(
            async move { actions::topic_create(&mut c2, &s2, &new_topic_owned, None).await },
        );

    let r_fin = h_finalize.await.unwrap();
    let r_create = h_create.await.unwrap();

    // Expected: one of two coherent outcomes
    //  A) finalize wins -> create gets FailedPrecondition
    //  B) create wins -> finalize succeeds afterwards
    match (r_fin.is_ok(), r_create.is_ok()) {
        (true, false) => {
            assert_eq!(
                r_create.unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
        (true, true) => {}
        (false, true) => {
            assert_eq!(r_fin.unwrap_err().code(), tonic::Code::FailedPrecondition);
        }
        (false, false) => {
            panic!("not possible");
        }
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_read_during_write(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let port = server.port();
    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    let topic_name = &format!("{}/read_during_write", sequence_name);

    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();
    let (_, session_uuid) = actions::session_create(&mut client, sequence_name)
        .await
        .unwrap();
    let topic_uuid = actions::topic_create(&mut client, &session_uuid, topic_name, None)
        .await
        .unwrap();

    let mut writer = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut reader = common::ClientBuilder::new(common::HOST, port).build().await;

    let t = topic_uuid.clone();
    let n = topic_name.clone();
    let writer_task = tokio::spawn(async move {
        let batches = vec![ext::arrow::testing::dummy_batch()];
        actions::do_put(&mut writer, &t, &n, batches, false).await
    });

    let n_read = topic_name.clone();
    let reader_task = tokio::spawn(async move {
        let mut results = Vec::new();
        for _ in 0..10 {
            let info = actions::get_flight_info(&mut reader, &n_read).await;
            results.push(info);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        results
    });

    let _ = writer_task.await.unwrap().unwrap();
    let read_results = reader_task.await.unwrap();

    for info in read_results {
        let info = info.unwrap();
        let app_metadata: marshal::flight::TopicAppMetadata =
            info.endpoint[0].clone().app_metadata.try_into().unwrap();

        if app_metadata.locked {
            assert!(
                app_metadata.completed_at_ns.is_some(),
                "locked topic must have completed_at_ns"
            );
        }
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_notification_create(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let port = server.port();

    let mut client = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "test_sequence";
    actions::sequence_create(&mut client, sequence_name, None)
        .await
        .unwrap();

    let n_notifications = 20;
    let mut handles = Vec::with_capacity(n_notifications);

    for i in 0..n_notifications {
        let mut c = common::ClientBuilder::new(common::HOST, port).build().await;
        let name = sequence_name.to_string();
        handles.push(tokio::spawn(async move {
            actions::sequence_notification_create(
                &mut c,
                &name,
                types::NotificationType::Error.to_string(),
                format!("concurrent msg {}", i),
            )
            .await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    let r = actions::sequence_notification_list(&mut client, sequence_name)
        .await
        .unwrap();
    let notifications = r["notifications"].as_array().unwrap();
    assert_eq!(
        notifications.len(),
        n_notifications,
        "no notifications must be lost"
    );

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_concurrent_sequence_create_and_delete(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let port = server.port();

    let mut setup = common::ClientBuilder::new(common::HOST, port).build().await;

    let sequence_name = "race_seq";
    actions::sequence_create(&mut setup, sequence_name, None)
        .await
        .unwrap();

    let mut c1 = common::ClientBuilder::new(common::HOST, port).build().await;
    let mut c2 = common::ClientBuilder::new(common::HOST, port).build().await;

    // c1 deletes, c2 tries to create a session in it.
    let h_del = tokio::spawn(async move { actions::sequence_delete(&mut c1, "race_seq").await });
    let h_session = tokio::spawn(async move { actions::session_create(&mut c2, "race_seq").await });

    let r_del = h_del.await.unwrap();
    let r_session = h_session.await.unwrap();

    r_del.unwrap();

    if let Err(e) = r_session {
        assert_eq!(e.code(), tonic::Code::NotFound);
    }

    server.shutdown().await;
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_stress_many_sequences_in_parallel(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;

    let n_sequences = 50;
    let mut handles = Vec::with_capacity(n_sequences);
    let port = server.port();

    for i in 0..n_sequences {
        let mut c = common::ClientBuilder::new(common::HOST, port).build().await;
        handles.push(tokio::spawn(async move {
            let name = format!("stress_seq_{}", i);
            actions::sequence_create(&mut c, &name, None).await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    server.shutdown().await;
}

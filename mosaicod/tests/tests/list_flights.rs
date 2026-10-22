#![allow(unused_crate_dependencies)]
use arrow_flight::FlightInfo;
use mosaicod_db as db;
use mosaicod_proto::v1::flight as proto_flight;
use prost::Message;
use tests::{actions, common};

// ===========================================================================
// list_flights tests
// ===========================================================================

/// Builds a `ListFlightsCmd` request, as a client would.
fn cmd(
    order_by: proto_flight::SequenceOrder,
    limit: u32,
    cursor: Option<proto_flight::SequenceCursor>,
) -> Vec<u8> {
    proto_flight::ListFlightsCmd {
        order_by: Some(order_by as i32),
        limit,
        cursor,
        ..Default::default()
    }
    .encode_to_vec()
}

/// The sequence name, carried in the descriptor path.
fn name(info: &FlightInfo) -> String {
    info.flight_descriptor.as_ref().unwrap().path[0].clone()
}

fn names(infos: &[FlightInfo]) -> Vec<String> {
    infos.iter().map(name).collect()
}

/// The cursor carried in `app_metadata`, decoded as a client would.
fn cursor(info: &FlightInfo) -> proto_flight::SequenceCursor {
    proto_flight::SequenceListItemAppMetadata::decode(info.app_metadata.as_ref())
        .unwrap()
        .cursor
        .expect("every listed sequence carries a cursor")
}

/// Creates `count` sequences, in reverse name order, so that creation order and
/// name order differ. Returns their names in creation order.
async fn create_sequences(client: &mut common::Client, count: usize) -> Vec<String> {
    let names: Vec<String> = (0..count).rev().map(|i| format!("seq_{i:03}")).collect();

    for name in &names {
        actions::sequence_create(client, name, "").await.unwrap();
    }

    names
}

/// An empty request returns every sequence by creation time, each with a cursor;
/// so does the root locator `/`.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_list_flights_defaults(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let created = create_sequences(&mut client, 5).await;

    let empty = actions::list_flights(&mut client, vec![]).await.unwrap();
    assert_eq!(names(&empty), created);
    for info in &empty {
        cursor(info);
    }

    let root = proto_flight::ListFlightsCmd {
        locator: "/".to_owned(),
        ..Default::default()
    }
    .encode_to_vec();
    let root = actions::list_flights(&mut client, root).await.unwrap();
    assert_eq!(names(&root), created);

    server.shutdown().await;
}

/// A client that stops in the middle of a page resumes right after the last item
/// it handled.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_list_flights_resume_mid_page(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let created = create_sequences(&mut client, 5).await;

    let page = actions::list_flights(
        &mut client,
        cmd(proto_flight::SequenceOrder::CreatedAt, 3, None),
    )
    .await
    .unwrap();

    // Stop after the first item of the page.
    let rest = actions::list_flights(
        &mut client,
        cmd(
            proto_flight::SequenceOrder::CreatedAt,
            0,
            Some(cursor(&page[0])),
        ),
    )
    .await
    .unwrap();
    assert_eq!(names(&rest), created[1..]);

    server.shutdown().await;
}

/// Malformed or unsupported requests return `InvalidArgument`.
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_list_flights_invalid_requests(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    actions::sequence_create(&mut client, "my_sequence", "")
        .await
        .unwrap();

    let name_cursor = proto_flight::SequenceCursor {
        key: Some(proto_flight::sequence_cursor::Key::Name(
            "my_sequence".to_owned(),
        )),
    };

    let requests = [
        // A raw `/` is not a protobuf message.
        ("raw slash", b"/".to_vec()),
        ("undecodable bytes", b"my_sequence".to_vec()),
        (
            "non-root locator",
            proto_flight::ListFlightsCmd {
                locator: "my_sequence".to_owned(),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "unknown order_by",
            proto_flight::ListFlightsCmd {
                order_by: Some(7),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "cursor without key",
            cmd(
                proto_flight::SequenceOrder::CreatedAt,
                0,
                Some(proto_flight::SequenceCursor { key: None }),
            ),
        ),
        (
            "cursor order mismatch",
            cmd(proto_flight::SequenceOrder::CreatedAt, 0, Some(name_cursor)),
        ),
    ];

    for (case, expression) in requests {
        let status = actions::list_flights(&mut client, expression)
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{case}");
    }

    server.shutdown().await;
}

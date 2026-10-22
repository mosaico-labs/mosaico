#![allow(unused_crate_dependencies)]
use arrow_flight::FlightInfo;
use mosaicod_db as db;
use mosaicod_proto::v1::flight as proto_flight;
use prost::Message;
use proto_flight::list_flights_cmd::Start;
use tests::{actions, common};

// ===========================================================================
// list_flights tests
// ===========================================================================

/// Builds a first-page `ListFlightsCmd` request, as a client would.
fn first_page(order_by: proto_flight::SequenceOrder, limit: u32) -> Vec<u8> {
    proto_flight::ListFlightsCmd {
        limit,
        start: Some(Start::OrderBy(order_by as i32)),
    }
    .encode_to_vec()
}

/// Builds a next-page `ListFlightsCmd` request, as a client would.
fn next_page(cursor: proto_flight::SequenceCursor, limit: u32) -> Vec<u8> {
    proto_flight::ListFlightsCmd {
        limit,
        start: Some(Start::Cursor(cursor)),
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

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn test_list_flights_all(pool: sqlx::Pool<db::DatabaseType>) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let mut client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;

    let created = create_sequences(&mut client, 5).await;

    let infos = actions::list_flights(
        &mut client,
        first_page(proto_flight::SequenceOrder::CreatedAt, 0),
    )
    .await
    .unwrap();

    assert_eq!(names(&infos), created);
    for info in &infos {
        cursor(info);
    }

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
        first_page(proto_flight::SequenceOrder::CreatedAt, 3),
    )
    .await
    .unwrap();

    // Stop after the first item of the page.
    let rest = actions::list_flights(&mut client, next_page(cursor(&page[0]), 0))
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

    let requests = [
        ("empty expression", vec![]),
        (
            "unspecified order_by",
            first_page(proto_flight::SequenceOrder::Unspecified, 10),
        ),
        ("raw slash", b"/".to_vec()),
        ("undecodable bytes", b"my_sequence".to_vec()),
        (
            "unknown order_by",
            proto_flight::ListFlightsCmd {
                limit: 0,
                start: Some(Start::OrderBy(7)),
            }
            .encode_to_vec(),
        ),
        (
            "cursor without key",
            next_page(proto_flight::SequenceCursor { key: None }, 0),
        ),
        (
            "invalid name in cursor",
            next_page(
                proto_flight::SequenceCursor {
                    key: Some(proto_flight::sequence_cursor::Key::Name(
                        "not/a/sequence".to_owned(),
                    )),
                },
                0,
            ),
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

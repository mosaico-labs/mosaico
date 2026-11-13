//! Reading back image sequences: the images, before and after the store optimizer turns them into
//! a video, the whole video, and its segments.

#![allow(unused_crate_dependencies)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, LargeBinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use mosaicod_core::{constants, types};
use mosaicod_db as db;
use mosaicod_image::{ColorType, Image, ImageFormat, Samples, testing::gradient};
use mosaicod_marshal as marshal;
use mosaicod_proto::v1::flight::{
    ImageSequenceRequest, VideoInterval, VideoView, image_sequence_request::View,
};
use mosaicod_proto::v1::requests;
use tests::{actions, common};

const SEQUENCE: &str = "camera_sequence";
const TOPIC: &str = "camera_sequence/front";
const IMAGES: usize = 5;

/// The index timestamps of the images.
fn timestamps_ns() -> Vec<i64> {
    (0..IMAGES as i64).map(|k| 1_000 + 10 * k).collect()
}

/// The capture times of the images: at 0, 400, 900, 1600 and 2100 ms.
fn capture_timestamps_ns() -> Vec<i64> {
    [0, 400, 900, 1_600, 2_100]
        .into_iter()
        .map(|ms| 1_767_225_600_000_000_000 + ms * 1_000_000)
        .collect()
}

/// Image `k`: a gradient, each different.
fn image(k: usize) -> Image {
    let image = gradient(16, 8, ColorType::Rgb, 8);
    let Samples::Eight(mut samples) = image.samples().clone() else {
        unreachable!()
    };
    samples[0] = k as u8;
    Image::new(*image.info(), Samples::Eight(samples)).unwrap()
}

/// What the ROS header of the images has besides their capture time, nullable: image 1 has no
/// `frame_id`, image 3 no `sample_counter`.
fn ros_fields() -> [Field; 2] {
    [
        Field::new("frame_id", DataType::Utf8, true),
        Field::new("sample_counter", DataType::UInt64, true),
    ]
}

fn ros_columns() -> [ArrayRef; 2] {
    [
        Arc::new(StringArray::from_iter(
            (0..IMAGES).map(|k| (k != 1).then_some("front_optical")),
        )),
        Arc::new(UInt64Array::from_iter(
            (0..IMAGES as u64).map(|k| (k != 3).then_some(k)),
        )),
    ]
}

/// The schema the images are uploaded with: types and metadata the server must keep.
fn uploaded_schema() -> SchemaRef {
    Arc::new(
        Schema::new(
            vec![
                Field::new(
                    constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
                    DataType::Int64,
                    false,
                ),
                Field::new("format", DataType::Utf8, false),
                Field::new("capture_timestamp_ns", DataType::Int64, false),
                Field::new("data", DataType::LargeBinary, false)
                    .with_metadata(HashMap::from([("camera".to_owned(), "front".to_owned())])),
            ]
            .into_iter()
            .chain(ros_fields())
            .collect::<Vec<_>>(),
        )
        .with_metadata(HashMap::from([("rig".to_owned(), "car-1".to_owned())])),
    )
}

fn uploaded_batch() -> RecordBatch {
    let pngs: Vec<Vec<u8>> = (0..IMAGES)
        .map(|k| mosaicod_image::encode(ImageFormat::Png, &image(k)).unwrap())
        .collect();
    RecordBatch::try_new(
        uploaded_schema(),
        vec![
            Arc::new(Int64Array::from(timestamps_ns())) as ArrayRef,
            Arc::new(StringArray::from(vec!["png"; IMAGES])),
            Arc::new(Int64Array::from(capture_timestamps_ns())),
            Arc::new(LargeBinaryArray::from_iter_values(pngs.iter())),
        ]
        .into_iter()
        .chain(ros_columns())
        .collect(),
    )
    .unwrap()
}

/// Uploads the image sequence [`TOPIC`], and a topic of another format.
async fn upload(client: &mut common::Client) {
    upload_images(client, marshal::Format::ImageSequence, uploaded_batch()).await;
}

/// Uploads `images` as the image sequence [`TOPIC`] of `format`, and a topic of another format.
async fn upload_images(client: &mut common::Client, format: marshal::Format, images: RecordBatch) {
    actions::sequence_create(client, SEQUENCE, "")
        .await
        .unwrap();
    let (_, session) = actions::session_create(client, SEQUENCE).await.unwrap();

    let topic = actions::topic_create_raw(
        client,
        requests::TopicCreate {
            locator: TOPIC.to_owned(),
            session_uuid: session.to_string(),
            serialization_format: format as i32,
            ontology_tag: match format {
                marshal::Format::ImageSequenceRaw => "image_sequence_raw",
                _ => "image_sequence",
            }
            .to_owned(),
            user_metadata: b"{}".to_vec(),
        },
    )
    .await
    .unwrap();
    actions::do_put(client, &topic, TOPIC, vec![images], false)
        .await
        .unwrap();

    let other = "camera_sequence/imu";
    let topic = actions::topic_create(client, &session, other, "")
        .await
        .unwrap();
    let batches = vec![mosaicod_ext::arrow::testing::dummy_batch(3, 100, 5, 1, 1)];
    actions::do_put(client, &topic, other, batches, false)
        .await
        .unwrap();

    actions::session_finalize(client, &session).await.unwrap();
}

/// A server, and a client of it.
async fn start(pool: sqlx::Pool<db::DatabaseType>) -> (common::Server, common::Client) {
    let server = common::ServerBuilder::new(common::HOST, pool).build().await;
    let client = common::ClientBuilder::new(common::HOST, server.port())
        .build()
        .await;
    (server, client)
}

/// Runs the store optimizer once.
async fn optimize(pool: sqlx::Pool<db::DatabaseType>, server: &common::Server) {
    tests::store_optimizer::Builder::new(pool)
        .build_with_store(&server.store)
        .await
        .wait_until_finished()
        .await;
}

async fn topic(server: &common::Server, locator: &str) -> db::TopicRecord {
    db::topic_find_by_locator(&mut server.db.connection(), &locator.parse().unwrap())
        .await
        .unwrap()
}

/// A request of the video, of the seconds `[start_s, end_s)` of `interval` (all of them if
/// `None`).
fn video(interval: Option<(u32, u32)>) -> Option<ImageSequenceRequest> {
    let interval = interval.map(|(start_s, end_s)| VideoInterval { start_s, end_s });
    Some(ImageSequenceRequest {
        view: Some(View::Video(VideoView { interval })),
    })
}

/// `do_get` of the ticket of `locator`, asking for `request` of it, in `range`.
async fn get(
    client: &mut common::Client,
    locator: &str,
    range: Option<types::TimestampRange>,
    request: Option<ImageSequenceRequest>,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let info =
        actions::get_flight_info_with_image_sequence(client, locator, range, request).await?;
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    actions::do_get_with_ticket(client, ticket).await
}

fn assert_unsupported<T: std::fmt::Debug>(result: Result<T, tonic::Status>) {
    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
}

fn concat(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

/// `batch` with the types of [`uploaded_schema`]: the server reads strings and binaries back as
/// view types (`Utf8View`, `BinaryView`), as for any topic.
fn as_uploaded(batch: &RecordBatch) -> RecordBatch {
    with_types_of(batch, uploaded_schema())
}

/// `batch` with the types of `schema`.
fn with_types_of(batch: &RecordBatch, schema: SchemaRef) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|f| arrow::compute::cast(&batch[f.name().as_str()], f.data_type()).unwrap())
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

/// The images of [`TOPIC`], as `do_get` reads them back, and its schema, as `get_schema` gives it.
async fn read_back(client: &mut common::Client) -> (RecordBatch, Schema) {
    let images = concat(&get(client, TOPIC, None, None).await.unwrap());
    (images, actions::get_schema(client, TOPIC).await.unwrap())
}

#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn images_are_read_back_before_optimization(pool: sqlx::Pool<db::DatabaseType>) {
    let (_server, mut client) = start(pool).await;
    upload(&mut client).await;

    let (images, schema) = read_back(&mut client).await;
    assert_eq!(as_uploaded(&images), uploaded_batch());
    assert_eq!(*images.schema(), schema);
    assert_eq!(schema.metadata(), uploaded_schema().metadata());

    // No video yet.
    for interval in [None, Some((0, 1))] {
        assert_unsupported(get(&mut client, TOPIC, None, video(interval)).await);
    }
    // Only image sequences have a video.
    let other = actions::get_flight_info_with_image_sequence(
        &mut client,
        "camera_sequence/imu",
        None,
        video(None),
    )
    .await;
    assert_unsupported(other);
}

/// An optimizer that can't make videos (here, built without the `video` feature, as it is when its
/// FFmpeg lacks something) optimizes the other topics, and leaves the image sequence untouched.
#[cfg(not(feature = "video"))]
#[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
async fn image_sequences_are_left_without_video(pool: sqlx::Pool<db::DatabaseType>) {
    let (server, mut client) = start(pool.clone()).await;
    upload(&mut client).await;
    let (before, _) = read_back(&mut client).await;

    optimize(pool, &server).await;

    let other = topic(&server, "camera_sequence/imu").await;
    assert!(other.optimization_end_timestamp().is_some());
    assert!(
        topic(&server, TOPIC)
            .await
            .optimization_end_timestamp()
            .is_none()
    );
    assert_eq!(
        db::topic_optimization_count(&mut server.db.connection())
            .await
            .unwrap(),
        1
    );
    let notifications = actions::topic_notification_list(&mut client, TOPIC)
        .await
        .unwrap();
    assert!(notifications.notifications.is_empty());
    let (after, _) = read_back(&mut client).await;
    assert_eq!(after, before);
}

#[cfg(feature = "video")]
mod optimized {
    use super::*;
    use arrow::array::{Array, AsArray, BooleanArray, UInt32Array};
    use arrow::datatypes::Int64Type;
    use mosaicod_facade as facade;
    use mosaicod_proto::v1::query as proto_query;
    use tests::filter;

    /// Uploads the topics, and turns the image sequence into a video: returns the images as read
    /// back before, with the topic's schema.
    async fn optimized_server(
        pool: sqlx::Pool<db::DatabaseType>,
    ) -> (common::Server, common::Client, (RecordBatch, Schema)) {
        let (server, mut client) = start(pool.clone()).await;
        upload(&mut client).await;
        let before = read_back(&mut client).await;

        optimize(pool, &server).await;
        assert!(
            topic(&server, TOPIC)
                .await
                .optimization_end_timestamp()
                .is_some()
        );
        let notifications = actions::topic_notification_list(&mut client, TOPIC)
            .await
            .unwrap();
        assert!(notifications.notifications.is_empty(), "{notifications:?}");
        (server, client, before)
    }

    /// The times of the images in the video, in microseconds: their capture times.
    fn video_us() -> Vec<i64> {
        let first = capture_timestamps_ns()[0];
        capture_timestamps_ns()
            .into_iter()
            .map(|ns| (ns - first) / 1_000)
            .collect()
    }

    /// The frames of the video `bytes`: (time in microseconds, image).
    fn frames(bytes: &[u8]) -> Vec<(i64, Image)> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("video.mp4");
        std::fs::write(&path, bytes).unwrap();
        let mut frames = Vec::new();
        mosaicod_video::decode(&path, |frame| {
            frames.push((frame.timestamp_us, frame.image));
            Ok(())
        })
        .unwrap();
        frames
    }

    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn images_are_read_back_from_the_video(pool: sqlx::Pool<db::DatabaseType>) {
        let (_server, mut client, (before, before_schema)) = optimized_server(pool).await;

        // Read back as before the video: the same schema, metadata included, and rows.
        let (images, schema) = read_back(&mut client).await;
        assert_eq!(schema, before_schema);
        assert_eq!(images.schema(), before.schema());
        for column in [
            constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
            "format",
            "capture_timestamp_ns",
            "frame_id",
            "sample_counter",
        ] {
            assert_eq!(&images[column], &before[column], "{column}");
        }
        // The same pixels, in PNG.
        let images = as_uploaded(&images);
        let data = images["data"].as_binary::<i64>();
        for k in 0..IMAGES {
            let decoded = mosaicod_image::decode(ImageFormat::Png, data.value(k)).unwrap();
            assert!(decoded == image(k), "image {k}");
        }

        // A time range, as for any topic: [1010, 1030).
        let range = types::TimestampRange::between(1_010.into(), 1_030.into());
        let images = as_uploaded(&concat(
            &get(&mut client, TOPIC, Some(range), None).await.unwrap(),
        ));
        assert_eq!(
            images[constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP]
                .as_primitive::<Int64Type>()
                .values()
                .to_vec(),
            [1_010, 1_020]
        );
        let data = images["data"].as_binary::<i64>();
        assert!(mosaicod_image::decode(ImageFormat::Png, data.value(1)).unwrap() == image(2));
    }

    /// A file of a DASH stream, as `do_get` streams it.
    #[derive(Debug, Default)]
    struct DashFile {
        name: String,
        bytes: Vec<u8>,
        /// For a media segment: the timestamps of its frames.
        timestamps_ns: Vec<i64>,
        capture_timestamps_ns: Vec<i64>,
        video_timestamps_us: Vec<i64>,
    }

    /// The files of the DASH stream of `batches`, in order.
    fn read_dash(batches: &[RecordBatch]) -> Vec<DashFile> {
        let all = concat(batches);
        let list = |name: &str, row: usize| -> Vec<i64> {
            all[name]
                .as_list::<i32>()
                .value(row)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        };
        let last = all["last"].as_boolean();
        let mut files: Vec<DashFile> = Vec::new();
        // Whether the next row is the first piece of a file.
        let mut first = true;
        for row in 0..all.num_rows() {
            let name = all["name"].as_string::<i32>().value(row);
            if first {
                let mut file = DashFile {
                    name: name.to_owned(),
                    ..Default::default()
                };
                if name.starts_with("segment-") {
                    file.timestamps_ns = list("timestamp_ns", row);
                    file.capture_timestamps_ns = list("capture_timestamp_ns", row);
                    file.video_timestamps_us = list("video_timestamp_us", row);
                }
                files.push(file);
            } else {
                assert!(all["timestamp_ns"].is_null(row));
            }
            first = last.value(row);
            let file = files.last_mut().unwrap();
            assert_eq!(file.name, name);
            file.bytes
                .extend_from_slice(all["data"].as_binary::<i32>().value(row));
        }
        files
    }

    /// A media segment: its second's start in microseconds of video, and its images.
    type Segment<'a> = (i64, &'a [usize]);

    /// Checks `files`, a DASH stream of the media segments `expected`.
    fn check_dash(files: &[DashFile], expected: &[Segment]) {
        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        let mut expected_names = vec!["manifest.mpd".to_owned(), "init.mp4".to_owned()];
        expected_names.extend(
            expected
                .iter()
                .map(|(_, images)| format!("segment-{}.m4s", video_us()[images[0]])),
        );
        assert_eq!(names, expected_names);

        let manifest = String::from_utf8(files[0].bytes.clone()).unwrap();
        for attribute in [
            r#"codecs="ffv1""#,
            r#"timescale="1000000""#,
            r#"initialization="init.mp4""#,
        ] {
            assert!(manifest.contains(attribute), "{attribute} in {manifest}");
        }
        let init = &files[1].bytes;

        for (file, (start_us, images)) in files[2..].iter().zip(expected) {
            // In the second of the segment.
            assert!(
                file.video_timestamps_us
                    .iter()
                    .all(|us| (*start_us..start_us + 1_000_000).contains(us)),
                "{}",
                file.name
            );
            let at = |values: Vec<i64>| -> Vec<i64> { images.iter().map(|&k| values[k]).collect() };
            assert_eq!(file.timestamps_ns, at(timestamps_ns()));
            assert_eq!(file.capture_timestamps_ns, at(capture_timestamps_ns()));
            assert_eq!(file.video_timestamps_us, at(video_us()));
            // In the manifest's timeline, from its first frame.
            let t = format!(r#"<S t="{}""#, video_us()[images[0]]);
            assert!(manifest.contains(&t), "{t} in {manifest}");

            // The initialization segment and the media segment: a video of its frames, at
            // their time in the whole video.
            let mut video = init.clone();
            video.extend_from_slice(&file.bytes);
            let decoded = frames(&video);
            assert_eq!(decoded.len(), images.len(), "{}", file.name);
            for ((us, frame), &k) in decoded.iter().zip(images.iter()) {
                assert_eq!(*us, video_us()[k]);
                assert!(*frame == image(k), "image {k}");
            }
        }
    }

    /// The video comes back a segment per second, every segment starting with a keyframe: all of
    /// them with no interval, an initialization segment followed by all of them being the whole
    /// video; those of an interval of seconds with one.
    /// The catalog queries find an image sequence turned into a video by the values of its
    /// columns, as before: they read its frame index, its only chunk.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn the_catalog_queries_read_the_frame_index(pool: sqlx::Pool<db::DatabaseType>) {
        let (_server, mut client, _) = optimized_server(pool).await;
        let cases = [
            ("image_sequence.frame_id", filter::eq("front_optical"), true),
            ("image_sequence.frame_id", filter::eq("rear_optical"), false),
            ("image_sequence.sample_counter", filter::eq(4), true),
            ("image_sequence.sample_counter", filter::eq(3), false),
            ("image_sequence.format", filter::eq("png"), true),
        ];
        for (field, condition, found) in cases {
            let items = actions::query(
                &mut client,
                proto_query::Filter {
                    ontology: Some(filter::ontology_filter([(field, condition)])),
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{field}: {e}"));
            let locators: Vec<&str> = items
                .iter()
                .flat_map(|item| item.topics.iter().map(|t| t.locator.as_str()))
                .collect();
            assert_eq!(locators.contains(&TOPIC), found, "{field}: {locators:?}");
        }
    }

    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn the_video_is_read_back_in_seconds(pool: sqlx::Pool<db::DatabaseType>) {
        let (_server, mut client, _) = optimized_server(pool).await;

        let batches = get(&mut client, TOPIC, None, video(None)).await.unwrap();
        let metadata = batches[0].schema().metadata().clone();
        assert_eq!(metadata["mosaico:content_type"], "video/mp4");
        assert_eq!(metadata["mosaico:codec"], "ffv1");
        assert!(!metadata.contains_key("mosaico:timescale"));
        assert_eq!(metadata["mosaico:frames"], IMAGES.to_string());
        assert_eq!(
            metadata["mosaico:capture_start_ns"],
            capture_timestamps_ns()[0].to_string()
        );
        // Captured at 0, 400, 900, 1600 and 2100 ms: seconds 0, 1 and 2.
        let files = read_dash(&batches);
        check_dash(
            &files,
            &[(0, &[0, 1, 2]), (1_000_000, &[3]), (2_000_000, &[4])],
        );
        // The initialization segment followed by all of them: every image, at its capture time
        // in microseconds from the first one.
        let whole: Vec<u8> = files[1..].iter().flat_map(|f| f.bytes.clone()).collect();
        let decoded = frames(&whole);
        assert_eq!(decoded.len(), IMAGES);
        for (k, (us, frame)) in decoded.iter().enumerate() {
            assert_eq!(*us, video_us()[k]);
            assert!(*frame == image(k), "image {k}");
        }

        // Intervals of seconds: their segments, each a second of video.
        let cases: [((u32, u32), &[Segment]); 4] = [
            ((0, 1), &[(0, &[0, 1, 2])]),
            ((1, 3), &[(1_000_000, &[3]), (2_000_000, &[4])]),
            ((2, 3), &[(2_000_000, &[4])]),
            (
                (0, 3),
                &[(0, &[0, 1, 2]), (1_000_000, &[3]), (2_000_000, &[4])],
            ),
        ];
        for (interval, expected) in cases {
            let batches = get(&mut client, TOPIC, None, video(Some(interval)))
                .await
                .unwrap();
            check_dash(&read_dash(&batches), expected);
        }

        // Asked of a sequence: the video of the image sequence, the data of the other topic.
        let info =
            actions::get_flight_info_with_image_sequence(&mut client, SEQUENCE, None, video(None))
                .await
                .unwrap();
        assert_eq!(info.endpoint.len(), 2);
        for endpoint in info.endpoint {
            let batches = actions::do_get_with_ticket(&mut client, endpoint.ticket.unwrap())
                .await
                .unwrap();
            let schema = batches[0].schema();
            assert_eq!(
                schema.metadata().contains_key("mosaico:content_type"),
                schema.field_with_name("last").is_ok()
            );
        }
    }

    /// A wrong interval of seconds is an invalid argument, asked with `get_flight_info` or in a
    /// ticket: one that does not start before its end, one that ends after the video (of 3 s), or
    /// one with a time range.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn wrong_intervals_are_rejected(pool: sqlx::Pool<db::DatabaseType>) {
        let (_server, mut client, _) = optimized_server(pool).await;
        let range = types::TimestampRange::between(1_010.into(), 1_030.into());
        let cases = [
            ((0, 0), None),
            ((2, 1), None),
            ((0, 4), None),
            ((3, 4), None),
            ((0, 1), Some(range)),
        ];
        for ((start_s, end_s), range) in cases {
            let case = format!("[{start_s}, {end_s}) s, time range {range:?}");
            let asked = get(&mut client, TOPIC, range, video(Some((start_s, end_s)))).await;
            assert_eq!(
                asked.unwrap_err().code(),
                tonic::Code::InvalidArgument,
                "{case}"
            );

            let ticket = types::flight::TicketTopic {
                locator: TOPIC.parse().unwrap(),
                timestamp_range: range,
                image_sequence_view: types::flight::ImageSequenceView::Video {
                    interval: Some(types::flight::VideoInterval { start_s, end_s }),
                },
            };
            let ticket = arrow_flight::Ticket {
                ticket: marshal::flight::ticket_topic_to_bytes(ticket).into(),
            };
            let got = actions::do_get_with_ticket(&mut client, ticket).await;
            assert_eq!(
                got.unwrap_err().code(),
                tonic::Code::InvalidArgument,
                "{case}"
            );
        }
    }

    /// An image sequence whose third image is not an image, found checking the images: its issue
    /// is a notification, its optimization ends, and it is read back as it was sent.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn an_issue_is_a_notification_and_ends_the_optimization(
        pool: sqlx::Pool<db::DatabaseType>,
    ) {
        let not_an_image = |image: &mut Vec<u8>| *image = b"not an image".to_vec();
        undecodable_image(pool, not_an_image, "declared `png`, but it is not an image").await;
    }

    /// The same, for an image whose structure is right but whose pixel data is corrupted, found
    /// decoding the images.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn a_corrupted_image_is_found_decoding_it(pool: sqlx::Pool<db::DatabaseType>) {
        let corrupted = |image: &mut Vec<u8>| {
            let idat = image.windows(4).position(|w| w == b"IDAT").unwrap();
            image[idat + 8] ^= 0xff;
        };
        undecodable_image(pool, corrupted, "invalid png image").await;
    }

    /// `images` with the image of row 2 changed by `change`.
    fn with_third_image(images: &RecordBatch, change: impl Fn(&mut Vec<u8>)) -> RecordBatch {
        let mut data: Vec<Vec<u8>> = (0..images.num_rows())
            .map(|k| images["data"].as_binary::<i64>().value(k).to_vec())
            .collect();
        change(&mut data[2]);
        let (index, _) = images.schema().column_with_name("data").unwrap();
        let mut columns = images.columns().to_vec();
        columns[index] = Arc::new(LargeBinaryArray::from_iter_values(data.iter()));
        RecordBatch::try_new(images.schema(), columns).unwrap()
    }

    /// Uploads an image sequence whose third image is changed by `corrupt`, optimizes it, and
    /// checks its `undecodable` issue, whose details start with `details`.
    async fn undecodable_image(
        pool: sqlx::Pool<db::DatabaseType>,
        corrupt: impl Fn(&mut Vec<u8>),
        details: &str,
    ) {
        let images = with_third_image(&uploaded_batch(), corrupt);
        let issue = format!("store optimizer/undecodable: image recorded at 1020 ns: {details}");
        the_issue_is_notified(pool, marshal::Format::ImageSequence, images, &issue).await;
    }

    /// Uploads `images` as an image sequence of `format`, optimizes it, and checks its issue: a
    /// notification starting with `issue`, its optimization ended, its data as it was sent.
    async fn the_issue_is_notified(
        pool: sqlx::Pool<db::DatabaseType>,
        format: marshal::Format,
        images: RecordBatch,
        issue: &str,
    ) {
        let (server, mut client) = start(pool.clone()).await;
        upload_images(&mut client, format, images.clone()).await;
        let path_before = topic(&server, TOPIC)
            .await
            .path_in_store()
            .map(|p| p.to_string());

        optimize(pool.clone(), &server).await;

        // The issue, in an `error` notification.
        let notifications = actions::topic_notification_list(&mut client, TOPIC)
            .await
            .unwrap()
            .notifications;
        assert_eq!(notifications.len(), 1, "{notifications:?}");
        assert_eq!(notifications[0].notification_type, "error");
        assert!(
            notifications[0].msg.starts_with(issue),
            "{}",
            notifications[0].msg
        );

        // Its optimization ended, its data as it was sent.
        let record = topic(&server, TOPIC).await;
        assert!(record.optimization_end_timestamp().is_some());
        assert_eq!(record.path_in_store().map(|p| p.to_string()), path_before);
        let (read_back, _) = read_back(&mut client).await;
        assert_eq!(with_types_of(&read_back, images.schema()), images);
        assert_unsupported(get(&mut client, TOPIC, None, video(None)).await);

        // It is not optimized again.
        optimize(pool, &server).await;
        let notifications = actions::topic_notification_list(&mut client, TOPIC)
            .await
            .unwrap()
            .notifications;
        assert_eq!(notifications.len(), 1, "{notifications:?}");
    }

    /// The schema raw images are uploaded with.
    fn raw_schema() -> SchemaRef {
        Arc::new(Schema::new(
            vec![
                Field::new(
                    constants::ARROW_SCHEMA_COLUMN_NAME_INDEX_TIMESTAMP,
                    DataType::Int64,
                    false,
                ),
                Field::new("capture_timestamp_ns", DataType::Int64, false),
                Field::new("width", DataType::UInt32, false),
                Field::new("height", DataType::UInt32, false),
                Field::new("stride", DataType::UInt32, false),
                Field::new("encoding", DataType::Utf8, false),
                Field::new("is_bigendian", DataType::Boolean, true),
                Field::new("data", DataType::LargeBinary, false),
            ]
            .into_iter()
            .chain(ros_fields())
            .collect::<Vec<_>>(),
        ))
    }

    /// The images of [`image`] as raw images of `encodings`: (encoding, padding bytes per row).
    fn raw_batch(encodings: [(&str, u32); IMAGES]) -> RecordBatch {
        let (width, height) = (image(0).info().width, image(0).info().height);
        let strides: Vec<u32> = encodings
            .iter()
            .map(|(_, padding)| 3 * width + padding)
            .collect();
        let data: Vec<Vec<u8>> = (0..IMAGES)
            .map(|k| {
                let layout = mosaicod_image::RawLayout {
                    width,
                    height,
                    stride: strides[k],
                    encoding: encodings[k].0,
                    big_endian: false,
                };
                // Zeros of an encoding it can't be.
                mosaicod_image::encode_raw(&layout, &image(k))
                    .unwrap_or_else(|_| vec![0; strides[k] as usize * height as usize])
            })
            .collect();
        RecordBatch::try_new(
            raw_schema(),
            vec![
                Arc::new(Int64Array::from(timestamps_ns())) as ArrayRef,
                Arc::new(Int64Array::from(capture_timestamps_ns())),
                Arc::new(UInt32Array::from(vec![width; IMAGES])),
                Arc::new(UInt32Array::from(vec![height; IMAGES])),
                Arc::new(UInt32Array::from(strides)),
                Arc::new(StringArray::from_iter_values(
                    encodings.iter().map(|(e, _)| *e),
                )),
                Arc::new(BooleanArray::from(vec![
                    None,
                    Some(false),
                    Some(true),
                    None,
                    None,
                ])),
                Arc::new(LargeBinaryArray::from_iter_values(data.iter())),
            ]
            .into_iter()
            .chain(ros_columns())
            .collect(),
        )
        .unwrap()
    }

    /// Raw images of encodings of the same samples, with and without row padding.
    fn raw_images() -> RecordBatch {
        raw_batch([
            ("bgr8", 4),
            ("rgb8", 0),
            ("8UC3", 2),
            ("bgr8", 0),
            ("rgb8", 4),
        ])
    }

    /// Raw images become a video, and come back byte for byte, layout and ROS fields included.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn raw_images_come_back_byte_for_byte(pool: sqlx::Pool<db::DatabaseType>) {
        let (server, mut client) = start(pool.clone()).await;
        let uploaded = raw_images();
        upload_images(
            &mut client,
            marshal::Format::ImageSequenceRaw,
            uploaded.clone(),
        )
        .await;
        let (before, before_schema) = read_back(&mut client).await;
        assert_eq!(with_types_of(&before, raw_schema()), uploaded);

        optimize(pool, &server).await;
        let notifications = actions::topic_notification_list(&mut client, TOPIC)
            .await
            .unwrap();
        assert!(notifications.notifications.is_empty(), "{notifications:?}");
        assert!(
            topic(&server, TOPIC)
                .await
                .optimization_end_timestamp()
                .is_some()
        );

        // Read back as before the video: the same schema and rows, every byte.
        let (after, schema) = read_back(&mut client).await;
        assert_eq!(schema, before_schema);
        assert_eq!(after, before);
        // A time range, as for any topic: [1010, 1030).
        let range = types::TimestampRange::between(1_010.into(), 1_030.into());
        let some = concat(&get(&mut client, TOPIC, Some(range), None).await.unwrap());
        assert_eq!(some, before.slice(1, 2));

        // Its video: every image at its capture time.
        let files = read_dash(&get(&mut client, TOPIC, None, video(None)).await.unwrap());
        check_dash(
            &files,
            &[(0, &[0, 1, 2]), (1_000_000, &[3]), (2_000_000, &[4])],
        );
    }

    /// A raw image of an encoding a video can't hold yet: `unsupported`, left as it was sent.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn raw_images_of_an_unsupported_encoding_are_left_raw(
        pool: sqlx::Pool<db::DatabaseType>,
    ) {
        let mut encodings = [("rgb8", 0); IMAGES];
        encodings[2] = ("32FC1", 0);
        let images = raw_batch(encodings);
        let issue = "store optimizer/unsupported: image recorded at 1020 ns: unsupported raw \
                     image: encoding `32FC1`";
        the_issue_is_notified(pool, marshal::Format::ImageSequenceRaw, images, issue).await;
    }

    /// A raw image whose row padding is not zero, which a video does not keep: `unsupported`.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn raw_images_padded_with_data_are_left_raw(pool: sqlx::Pool<db::DatabaseType>) {
        let images = with_third_image(&raw_images(), |data| data[49] = 7);
        let issue = "store optimizer/unsupported: image recorded at 1020 ns: unsupported raw \
                     image: row padding";
        the_issue_is_notified(pool, marshal::Format::ImageSequenceRaw, images, issue).await;
    }

    /// A raw image whose bytes don't match its size and stride: `undecodable`.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn raw_images_of_the_wrong_size_are_undecodable(pool: sqlx::Pool<db::DatabaseType>) {
        let images = with_third_image(&raw_images(), |data| data.truncate(100));
        let issue = "store optimizer/undecodable: image recorded at 1020 ns: invalid raw image";
        the_issue_is_notified(pool, marshal::Format::ImageSequenceRaw, images, issue).await;
    }

    /// An image sequence finalized without any image has nothing to optimize: its optimization
    /// ends without a video and without a notification, and it is never optimized again.
    #[sqlx::test(migrator = "mosaicod_db::testing::MIGRATOR")]
    async fn an_empty_image_sequence_has_nothing_to_optimize(pool: sqlx::Pool<db::DatabaseType>) {
        let (server, mut client) = start(pool.clone()).await;
        actions::sequence_create(&mut client, SEQUENCE, "")
            .await
            .unwrap();
        let (_, session) = actions::session_create(&mut client, SEQUENCE)
            .await
            .unwrap();
        let topic_uuid = actions::topic_create_raw(
            &mut client,
            requests::TopicCreate {
                locator: TOPIC.to_owned(),
                session_uuid: session.to_string(),
                serialization_format: marshal::Format::ImageSequenceRaw as i32,
                ontology_tag: "image_sequence_raw".to_owned(),
                user_metadata: b"{}".to_vec(),
            },
        )
        .await
        .unwrap();
        // A topic is finalized only after a `DoPut`: here with no images, as a client that opens
        // the stream and then sends nothing (e.g. every image refused by the ROS bridge).
        let no_images = raw_images().slice(0, 0);
        actions::do_put(&mut client, &topic_uuid, TOPIC, vec![no_images], false)
            .await
            .unwrap();
        actions::session_finalize(&mut client, &session)
            .await
            .unwrap();

        optimize(pool.clone(), &server).await;

        let notifications = actions::topic_notification_list(&mut client, TOPIC)
            .await
            .unwrap()
            .notifications;
        assert!(notifications.is_empty(), "{notifications:?}");
        let record = topic(&server, TOPIC).await;
        assert!(record.optimization_end_timestamp().is_some());
        let data_folder = record.path_in_store().unwrap().data_folder_path();
        assert!(
            facade::topic::image_sequence_video(&server.context(), &data_folder)
                .await
                .unwrap()
                .is_none()
        );

        // Never optimized again.
        optimize(pool, &server).await;
        let notifications = actions::topic_notification_list(&mut client, TOPIC)
            .await
            .unwrap()
            .notifications;
        assert!(notifications.is_empty(), "{notifications:?}");
    }
}

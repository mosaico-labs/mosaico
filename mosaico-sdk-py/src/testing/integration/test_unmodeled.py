import pyarrow as pa

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.enum.session_level_error_policy import SessionLevelErrorPolicy
from mosaicolabs.models.core.helpers import resolve_ontology_class
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import Unmodeled, make_unmodeled_ontology_class
from mosaicolabs.models.data import Time
from mosaicolabs.models.sensors import CameraInfo, CompressedImage, ImageFormat
from mosaicolabs.query.builders import QueryOntologyCatalog
from mosaicolabs.query.queryable_fields import QueryableNumeric

UnmodeledGyro = make_unmodeled_ontology_class(
    "UnmodeledGyro",
    None,
    SerializationFormat.Default,
    pa.struct(
        [
            pa.field(
                "gyro",
                pa.struct(
                    [
                        pa.field("x", pa.float32()),
                        pa.field("y", pa.float32()),
                        pa.field("z", pa.float32()),
                    ]
                ),
                nullable=False,
                metadata={"description": "Test Implementation of Gyroscope"},
            ),
        ]
    ),
)


def test_unmodeled_ingestion_retrieval(mosaico_client: MosaicoClient):
    with mosaico_client:
        # -- Test ingestion --
        with mosaico_client.sequence_create(
            "unmodeled_seq", {}, on_error=SessionLevelErrorPolicy.Delete
        ) as seqw:
            # Create a new topic and attach the unmodeled ontology as it was a default one
            tw = seqw.topic_create("/sensors/gyro/no_schema", {}, UnmodeledGyro)
            assert tw is not None
            # Make an unmodeled ontology instance
            unm_data = UnmodeledGyro(raw_data={"gyro": {"x": 1, "y": 2, "z": 3}})
            # Create and Push a message like a default ontology
            tw.push(Message(timestamp_ns=12345678, data=unm_data))

            # Make an unmodeled ontology instance
            unm_data = UnmodeledGyro(raw_data={"gyro": {"x": 4, "y": 5, "z": 6}})
            # Create and Push a message like a default ontology
            tw.push(Message(timestamp_ns=12345679, data=unm_data))

        # -- Test retrieval --
        th = mosaico_client.topic_handler("unmodeled_seq", "/sensors/gyro/no_schema")
        assert th is not None
        # Info are correctly recognized
        assert th.ontology_tag == UnmodeledGyro.ontology_tag()
        assert th.ontology_schema == UnmodeledGyro.__msco_pyarrow_struct__

        # Test stream retrieval via TopicDataStreamer
        for msg in th.get_data_streamer():
            data = msg.get_data(UnmodeledGyro)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))
            data = msg.get_data(Unmodeled)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))

        # Test stream retrieval via SequenceDataStreamer
        sh = mosaico_client.sequence_handler("unmodeled_seq")
        assert sh is not None
        for topic, msg in sh.get_data_streamer():
            data = msg.get_data(UnmodeledGyro)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))
            data = msg.get_data(Unmodeled)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))

        # Free resources
        mosaico_client.sequence_delete("unmodeled_seq")


# --- Schema-variant scenario ---
# Simulates two rosbags recorded with two different versions of the same ROS
# message type (e.g. a 'temperature' sensor that later gained a 'humidity'
# field), both mapped by an external translation script to the same inferred
# ontology tag. `resolve_ontology_class` is the entry point such a translator
# would use: given a tag and a translated pyarrow schema, it returns a usable
# ontology class, creating a distinct variant when the schema doesn't match
# what's already registered for that tag - instead of silently colliding.
_VARIANT_BASE_TAG = "unmodeled_temperature_variant_test"

_SCHEMA_V1 = pa.struct(
    [
        pa.field(
            "temperature",
            pa.struct([pa.field("celsius", pa.float32())]),
            nullable=False,
        ),
    ]
)

_SCHEMA_V2 = pa.struct(
    [
        pa.field(
            "temperature",
            pa.struct(
                [
                    pa.field("celsius", pa.float32()),
                    pa.field("humidity", pa.float32()),
                ]
            ),
            nullable=False,
        ),
    ]
)


def test_unmodeled_schema_variant_ingestion_and_retrieval(
    mosaico_client: MosaicoClient,
):
    # Resolve both schema variants exactly as an external translator would:
    # same base tag, two different schemas.
    ClsV1 = resolve_ontology_class(ontology_tag=_VARIANT_BASE_TAG, schema=_SCHEMA_V1)
    ClsV2 = resolve_ontology_class(ontology_tag=_VARIANT_BASE_TAG, schema=_SCHEMA_V2)

    assert ClsV1 is not ClsV2
    assert issubclass(ClsV1, Unmodeled) and issubclass(ClsV2, Unmodeled)
    # Both variants report the SAME ontology tag to the platform - only their
    # SDK-local registry key differs. This is what keeps both variants'
    # topics discoverable server-side under one consistent tag, regardless of
    # which schema happened to be seen first by this process.
    assert ClsV1.__ontology_tag__ == _VARIANT_BASE_TAG
    assert ClsV2.__ontology_tag__ == _VARIANT_BASE_TAG
    assert ClsV1.__registry_key__ == _VARIANT_BASE_TAG
    assert (
        ClsV2.__registry_key__ == f"{_VARIANT_BASE_TAG}__{ClsV2.__schema_fingerprint__}"
    )

    seq_v1, seq_v2 = (
        "unmodeled_variant_seq_v1",
        "unmodeled_variant_seq_v2",
    )
    topic_name = "/sensors/temperature"

    with mosaico_client:
        # -- Ingest "bag 1" (schema v1) into its own sequence --
        with mosaico_client.sequence_create(
            seq_v1, {}, on_error=SessionLevelErrorPolicy.Delete
        ) as seqw:
            tw = seqw.topic_create(topic_name, {}, ClsV1)
            assert tw is not None
            tw.push(
                Message(
                    timestamp_ns=1,
                    data=ClsV1(raw_data={"temperature": {"celsius": 21.5}}),
                )
            )
            tw.push(
                Message(
                    timestamp_ns=2,
                    data=ClsV1(raw_data={"temperature": {"celsius": 22.0}}),
                )
            )

        # -- Ingest "bag 2" (schema v2, extra 'humidity' field) into a separate sequence --
        with mosaico_client.sequence_create(
            seq_v2, {}, on_error=SessionLevelErrorPolicy.Delete
        ) as seqw:
            tw = seqw.topic_create(topic_name, {}, ClsV2)
            assert tw is not None
            tw.push(
                Message(
                    timestamp_ns=1,
                    data=ClsV2(
                        raw_data={"temperature": {"celsius": 30.0, "humidity": 55.0}}
                    ),
                )
            )

        # -- Retrieve "bag 1" and confirm it round-trips through the v1 schema --
        th_v1 = mosaico_client.topic_handler(seq_v1, topic_name)
        assert th_v1 is not None
        assert th_v1.ontology_tag == _VARIANT_BASE_TAG
        assert th_v1.ontology_schema == _SCHEMA_V1

        v1_msgs = list(th_v1.get_data_streamer())
        assert len(v1_msgs) == 2
        for msg in v1_msgs:
            data = msg.get_data(Unmodeled)
            assert data is not None
            assert "humidity" not in data.raw_data["temperature"]
            assert data.raw_data["temperature"]["celsius"] > 0

        # -- Retrieve "bag 2" and confirm it round-trips through the v2 schema, independently --
        th_v2 = mosaico_client.topic_handler(seq_v2, topic_name)
        assert th_v2 is not None
        # Both topics report the SAME ontology tag to the
        # server, even though they were written with two different schemas -
        # so a query against `_VARIANT_BASE_TAG` would find both.
        assert th_v2.ontology_tag == th_v1.ontology_tag == _VARIANT_BASE_TAG
        assert th_v2.ontology_schema == _SCHEMA_V2

        v2_msgs = list(th_v2.get_data_streamer())
        assert len(v2_msgs) == 1
        data = v2_msgs[0].get_data(Unmodeled)
        assert data is not None
        assert data.raw_data["temperature"]["celsius"] == 30.0
        assert data.raw_data["temperature"]["humidity"] == 55.0

        # -- Retrieve "bag 2" again through SequenceDataStreamer for the k-way-merge path --
        sh_v2 = mosaico_client.sequence_handler(seq_v2)
        assert sh_v2 is not None
        merged_msgs = list(sh_v2.get_data_streamer())
        assert len(merged_msgs) == 1
        topic, msg = merged_msgs[0]
        assert topic == topic_name
        data = msg.get_data(Unmodeled)
        assert data is not None
        assert data.raw_data["temperature"]["humidity"] == 55.0

        # This would return seq_v1 only
        query_resp = mosaico_client.query(
            QueryOntologyCatalog().with_expression(
                QueryableNumeric(f"{_VARIANT_BASE_TAG}.temperature.celsius").lt(22.0)
            )
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == seq_v1

        # This would return seq_v1 and seq_v2
        query_resp = mosaico_client.query(
            QueryOntologyCatalog().with_expression(
                QueryableNumeric(f"{_VARIANT_BASE_TAG}.temperature.celsius").gt(20.0)
            )
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 2
        assert all(q.sequence.name in (seq_v1, seq_v2) for q in query_resp)

        # Try the same queries using the class wrappers
        # This would return seq_v1 only
        query_resp = mosaico_client.query(
            QueryOntologyCatalog().with_expression(ClsV1.Q.temperature.celsius.lt(22.0))
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == seq_v1

        # This would return seq_v1 and seq_v2
        query_resp = mosaico_client.query(
            QueryOntologyCatalog().with_expression(ClsV1.Q.temperature.celsius.gt(20.0))
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 2
        assert all(q.sequence.name in (seq_v1, seq_v2) for q in query_resp)

        # Free resources
        mosaico_client.sequence_delete(seq_v1)
        mosaico_client.sequence_delete(seq_v2)


def test_bug_pastring_tostringview(mosaico_client: MosaicoClient):
    """
    This test checks that an Ontology containing a pa.string (like CameraInfo) is not interpreted as an Unmodoled ontology
    during reading from the server. This seems to happen because the server changes pa.string into pa.string_view leading
    to a different fingerprint during and therefore creating an Unmodeled ontology (despite CameraInfo being modeled).
    """

    def make_camera_info_msg(meas_time: Time):
        return Message(
            timestamp_ns=meas_time.to_nanoseconds(),
            data=CameraInfo(
                height=1920,
                width=1080,
                distortion_model="distorted",
                distortion_parameters=[1, 2, 3, 4, 5],
                intrinsic_parameters=[1, 2, 3, 4, 5, 6, 7, 8, 9],
                rectification_parameters=[1, 2, 3, 4, 5, 6, 7, 8, 9],
                projection_parameters=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            ),
        )

    camera_info_sequence_name = "camera_info_seq"
    camera_info_topic_name = "/front/camera"
    # -- Test ingestion --
    with mosaico_client.sequence_create(
        camera_info_sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
    ) as seqw:
        # Create a new topic and attach the unmodeled ontology as it was a default one
        tw = seqw.topic_create(camera_info_topic_name, {}, CameraInfo)
        assert tw is not None

        # Create and Push a message like a default ontology
        for time_s in range(5):
            tw.push(make_camera_info_msg(Time(seconds=time_s, nanoseconds=0)))

        # Now read the topic and check that the returned ontology is **not** Unmodeled
    seqhandler = mosaico_client.sequence_handler(camera_info_sequence_name)

    assert seqhandler is not None

    topic_handler = seqhandler.get_topic_handler(camera_info_topic_name)
    streamer = topic_handler.get_data_streamer()

    for camera_info_msg in streamer:
        assert (
            camera_info_msg.data.__schema_fingerprint__
            == CameraInfo.__schema_fingerprint__
        )
        assert issubclass(camera_info_msg.data.__class_type__, CameraInfo)
        assert not issubclass(camera_info_msg.data.__class_type__, Unmodeled)

    # Free resources
    mosaico_client.sequence_delete(camera_info_sequence_name)


def test_bug_pabyte_tobyteview(mosaico_client: MosaicoClient):
    """
    This test checks that an Ontology containing a pa.byte (like CameraInfo) is not interpreted as an Unmodoled ontology
    during reading from the server. This seems to happen because the server changes pa.byte into pa.byte_view leading
    to a different fingerprint during and therefore creating an Unmodeled ontology (despite CameraInfo being modeled).
    """

    def make_compressed_image_msg(meas_time: Time):
        return Message(
            timestamp_ns=meas_time.to_nanoseconds(),
            data=CompressedImage(data=bytes(range(16)), format=ImageFormat.JPEG),
        )

    camera_compressed_sequence_name = "camera_compressed_seq"
    camera_compressed_topic_name = "/front/camera/raw"
    # -- Test ingestion --
    with mosaico_client.sequence_create(
        camera_compressed_sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
    ) as seqw:
        # Create a new topic and attach the unmodeled ontology as it was a default one
        tw = seqw.topic_create(camera_compressed_topic_name, {}, CompressedImage)
        assert tw is not None

        # Create and Push a message like a default ontology
        for time_s in range(5):
            tw.push(make_compressed_image_msg(Time(seconds=time_s, nanoseconds=0)))

        # Now read the topic and check that the returned ontology is **not** Unmodeled
    seqhandler = mosaico_client.sequence_handler(camera_compressed_sequence_name)

    assert seqhandler is not None

    topic_handler = seqhandler.get_topic_handler(camera_compressed_topic_name)
    streamer = topic_handler.get_data_streamer()

    for camera_info_msg in streamer:
        assert (
            camera_info_msg.data.__schema_fingerprint__
            == CompressedImage.__schema_fingerprint__
        )
        assert issubclass(camera_info_msg.data.__class_type__, CompressedImage)
        assert not issubclass(camera_info_msg.data.__class_type__, Unmodeled)

    # Free resources
    mosaico_client.sequence_delete(camera_compressed_sequence_name)

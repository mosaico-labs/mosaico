import pytest
from pyarrow import ArrowInvalid

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.query import QuerySequence, QueryTopic
from testing.integration.config import (
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_MAGNETOMETER_TOPIC,
    UPLOADED_SEQUENCE_NAME,
    UPLOADED_SEQUENCE_W_LIST_NAME,
    UPLOADED_TEMPERATURE_TOPIC,
)


class TestMetadataOperations:
    """
    Tests for metadata supported operations ($eq, $neq, $in_, $lt, $leq,
    $gt, $geq, $between, $ex) except $match.
    """

    # $eq operation
    def test_query_metadata_eq(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", eq=200),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_IMU_FRONT_TOPIC

        mosaico_client.close()

    def test_query_metadata_eq_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", eq=999),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $neq operation
    def test_query_metadata_neq(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("status", neq="inactive"),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_IMU_CAMERA_TOPIC,
            UPLOADED_GPS_TOPIC,
            UPLOADED_MAGNETOMETER_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_metadata_neq_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("status", neq="active"),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $in_ operation
    def test_query_metadata_in(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", in_=[10, 200, 400]),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_IMU_CAMERA_TOPIC,
            UPLOADED_GPS_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_metadata_in_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", in_=[0, 50, 600]),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $lt operation
    def test_query_metadata_lt(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", lt=150),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_GPS_TOPIC,
            UPLOADED_MAGNETOMETER_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_metadata_lt_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", lt=10),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $leq operation
    def test_query_metadata_leq(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", leq=100),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_GPS_TOPIC,
            UPLOADED_MAGNETOMETER_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_metadata_leq_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", leq=9),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $gt operation
    def test_query_metadata_gt(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", gt=150),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_IMU_CAMERA_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_metadata_gt_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", gt=400),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $geq operation
    def test_query_metadata_geq(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", geq=200),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_IMU_CAMERA_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_metadata_geq_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", geq=401),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $between operation
    def test_query_metadata_between(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", between=[50, 300]),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_MAGNETOMETER_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_metadata_between_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("update_rate_hz", between=[500, 600]),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    # $ex operation
    def test_query_metadata_ex(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("accuracy_m", ex=True),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_GPS_TOPIC

        mosaico_client.close()

    def test_query_metadata_ex_no_return(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("nonexistent_key", ex=True),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()


class TestMetadataValueRegEx:
    """Tests for metadata $match operation + RegEx for metadata and sequence + topic"""

    def test_query_topic_name_regex_start_anchor(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        # /front* matches only topics whose name starts with /front
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_name_match("/front*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_IMU_FRONT_TOPIC

        mosaico_client.close()

    def test_query_topic_name_regex_end_anchor(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        # *imu$ matches topics whose name ends with imu (both front and camera imu)
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_name_match("*imu"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        expected = [UPLOADED_IMU_FRONT_TOPIC, UPLOADED_IMU_CAMERA_TOPIC]
        assert len(query_resp[0].topics) == len(expected)
        assert all(t.name in expected for t in query_resp[0].topics)

        mosaico_client.close()

    def test_query_topic_name_regex_catch_all(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        # * matches all topics in the sequence
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_name_match("*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 4

        mosaico_client.close()

    def test_query_sequence_name_regex_start_anchor(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        # *<exact-name> matches exactly one sequence
        query_resp = mosaico_client.query(
            QuerySequence().with_name_match(f"*{UPLOADED_SEQUENCE_NAME}"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        mosaico_client.close()

    def test_query_topic_name_match_unsupported_pattern_rejected(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        with pytest.raises(ArrowInvalid):
            mosaico_client.query(QueryTopic().with_name_match(""))

        mosaico_client.close()

    def test_query_topic_metadata_match(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("sensor_id", match="imu_*_01"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        # One (1) sequence corresponds to this query
        assert len(query_resp) == 1
        # Two (2) topics correspond to this query
        assert len(query_resp[0].topics) == 2
        # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
        expected_topic_name = [UPLOADED_IMU_FRONT_TOPIC, UPLOADED_IMU_CAMERA_TOPIC]

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_name

        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic()
            .with_user_metadata("sensor_id", match="*_##")
            .with_user_metadata("interface.type", match="UART"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        # One (1) sequence corresponds to this query
        assert len(query_resp) == 1
        # Two (1) topics correspond to this query
        assert len(query_resp[0].topics) == 1
        # The target topics are 'UPLOADED_GPS_TOPIC'
        expected_topic_name = [UPLOADED_GPS_TOPIC]

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_name

        # ? matches a single character: 'firmware_version' is '1.2.0' for the front imu
        # and '3.2.0' for the gps, both matching '?.2.0', while the camera imu's '2.1.0' does not
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("firmware_version", match="?.2.0"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        # One (1) sequence corresponds to this query
        assert len(query_resp) == 1
        # Two (2) topics correspond to this query
        assert len(query_resp[0].topics) == 2
        # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_GPS_TOPIC'
        expected_topic_name = [UPLOADED_IMU_FRONT_TOPIC, UPLOADED_GPS_TOPIC]

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_name

        # [] matches a character set: 'vendor' starts with 'g' for the camera imu (gyrolytics)
        # and with 's' for the gps (satnavics), both matching '[gs]*', while the front imu's
        # 'inertix-dynamics' does not
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("vendor", match="[gs]*"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        # One (1) sequence corresponds to this query
        assert len(query_resp) == 1
        # Two (2) topics correspond to this query
        assert len(query_resp[0].topics) == 2
        # The target topics are 'UPLOADED_IMU_CAMERA_TOPIC' and 'UPLOADED_GPS_TOPIC'
        expected_topic_name = [UPLOADED_IMU_CAMERA_TOPIC, UPLOADED_GPS_TOPIC]

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_name

        mosaico_client.close()

    def test_query_sequence_metadata_match(
        self,
        mosaico_client: MosaicoClient,
        inject_mockup_sequences,
    ):
        # * matches multiple characters: 'status' ends with 'processed' for both
        # 'test-query-sequence-1' ('processed') and 'test-query-sequence-4'
        # ('post-processed'), matching '*processed'
        query_resp = mosaico_client.query(
            QuerySequence()
            .with_name_match("test-query-sequence-#")
            .with_user_metadata("status", match="*processed"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        expected_sequence_names = ["test-query-sequence-1", "test-query-sequence-4"]
        # Two (2) sequences correspond to this query
        assert len(query_resp) == len(expected_sequence_names)

        for item in query_resp:
            assert item.sequence.name in expected_sequence_names

        # ? matches a single character: 'visibility' is 'public' only for
        # 'test-query-sequence-3', matching '?ublic' ('private' and 'none' don't fit
        # the pattern's length)
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("visibility", match="?ublic"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        expected_sequence_name = "test-query-sequence-3"
        # One (1) sequence corresponds to this query
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == expected_sequence_name

        # [] matches a character set: 'status' starts with 'r' for 'test-query-sequence-2'
        # ('raw') or 'l' for 'test-query-sequence-3' ('labeled'), matching '[rl]*'
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("status", match="[rl]*"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        expected_sequence_names = ["test-query-sequence-2", "test-query-sequence-3"]
        # Two (2) sequences correspond to this query
        assert len(query_resp) == len(expected_sequence_names)

        for item in query_resp:
            assert item.sequence.name in expected_sequence_names

        # # matches a single digit: every mockup sequence 'name' ends with exactly one
        # digit, matching 'test-query-sequence-#'; combined with a 'visibility' match on
        # 'non*' this narrows down to 'test-query-sequence-4' only
        query_resp = mosaico_client.query(
            QuerySequence()
            .with_name_match("test-query-sequence-#")
            .with_user_metadata("visibility", match="non*"),
        )

        # We do expect a successful query
        assert query_resp is not None and not query_resp.is_empty()
        expected_sequence_name = "test-query-sequence-4"
        # One (1) sequence corresponds to this query
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == expected_sequence_name

        # free resources
        mosaico_client.close()


class TestMetadataKeyGlobPattern:
    def test_query_topic_metadata_single_glob_pattern(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("*.type", match="UART*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_GPS_TOPIC

        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("interface.*", match="UART*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_GPS_TOPIC

        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("interface.*.ip", match="*.10.##"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_IMU_CAMERA_TOPIC

        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("*.*.ip", match="*.10.*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_IMU_CAMERA_TOPIC

        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("*.type", match="*"),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_IMU_CAMERA_TOPIC,
            UPLOADED_GPS_TOPIC,
            UPLOADED_MAGNETOMETER_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        mosaico_client.close()

    def test_query_topic_metadata_double_glob_pattern(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
        inject_synthetic_sequence_w_lists,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("**.ip", match="192.*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_IMU_CAMERA_TOPIC

        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("interface.**.baudrate", geq=1000),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1

        expected_topic_names = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_GPS_TOPIC,
        ]
        assert len(query_resp[0].topics) == len(expected_topic_names)

        for topic in query_resp[0].topics:
            assert topic.name in expected_topic_names

        # # This does not work for now
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_W_LIST_NAME),
            QueryTopic().with_user_metadata("temperature_range_kelvin.**", geq=0),
        )

        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert len(query_resp[0].topics) == 1
        assert query_resp[0].topics[0].name == UPLOADED_TEMPERATURE_TOPIC

        mosaico_client.close()

    def test_query_topic_metadata_no_match_value(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("*.type", match="invented_type"),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    def test_query_topic_metadata_no_match_key(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_user_metadata("*.invented_key", eq="fake_key"),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    def test_query_sequence_metadata_single_glob_pattern(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("*.country", match="IT"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("location.*", match="Milan"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata(
                "vehicle.*.perception", match="perception-*"
            ),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("*.*.perception", match="perception-*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("driver.*", match="*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        mosaico_client.close()

    def test_query_sequence_metadata_double_glob_pattern(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("**.perception", match="perception-*"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata(
                "quality_metrics.**.overall_quality_score", geq=0.9
            ),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

        mosaico_client.close()

    def test_query_sequence_metadata_no_match_value(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("*.country", match="invented_country"),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

    def test_query_sequence_metadata_no_match_key(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("*.invented_key", eq="fake_key"),
        )

        assert query_resp is not None and query_resp.is_empty()

        mosaico_client.close()

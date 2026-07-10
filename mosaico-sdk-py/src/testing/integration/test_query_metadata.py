import pytest
from pyarrow import ArrowInvalid

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.query import QuerySequence, QueryTopic
from testing.integration.config import (
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_MAGNETOMETER_TOPIC,
    UPLOADED_SEQUENCE_NAME,
    # UPLOADED_TEMPERATURE_TOPIC,
)


class TestMetadataValueRegEx:
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

    # FIXME: Impossible to make a regex that using OR
    def _test_query_topic_name_regex_or(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        # (imu|gps) matches topics containing either imu or gps
        query_resp = mosaico_client.query(
            QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
            QueryTopic().with_name_match("(imu|gps)"),
        )
        assert query_resp is not None and not query_resp.is_empty()
        assert len(query_resp) == 1
        expected = [
            UPLOADED_IMU_FRONT_TOPIC,
            UPLOADED_IMU_CAMERA_TOPIC,
            UPLOADED_GPS_TOPIC,
        ]
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
        inject_mockup_sequences,
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

    def test_query_topic_metadata_double_glob_pattern(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
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
        # query_resp = mosaico_client.query(
        #     QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
        #     QueryTopic().with_user_metadata("temperature_range_kelvin.**", geq=0),
        # )

        # QueryTopic().with_user_metadata("temperature_range_kelvin.**", geq=0).to_dict()

        # assert query_resp is not None and not query_resp.is_empty()
        # assert len(query_resp) == 1
        # assert len(query_resp[0].topics) == 1
        # assert query_resp[0].topics[0].name == UPLOADED_TEMPERATURE_TOPIC

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

    def test_query_sequence_metadata_no_match_value(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("*.country", match="invented_country"),
        )

        assert query_resp is not None and query_resp.is_empty()

    def test_query_sequence_metadata_no_match_key(
        self,
        mosaico_client: MosaicoClient,
        inject_synthetic_sequence,
    ):
        query_resp = mosaico_client.query(
            QuerySequence().with_user_metadata("*.invented_key", eq="fake_key"),
        )

        assert query_resp is not None and query_resp.is_empty()

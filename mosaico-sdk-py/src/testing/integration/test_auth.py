import pytest
from pyarrow.flight import FlightUnauthorizedError

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum import SessionLevelErrorPolicy
from mosaicolabs.models.sensors.imu import IMU
from mosaicolabs.query.builders import QuerySequence
from testing.integration.config import UPLOADED_GPS_TOPIC, UPLOADED_SEQUENCE_NAME


# --- Helpers ---
def _test_read_pass(client: MosaicoClient):
    # Must pass
    client.list_sequences()

    # Must pass
    client.list_sequence_notifications(UPLOADED_SEQUENCE_NAME)

    # Must pass
    client.list_topic_notifications(UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC)

    # Must pass
    qresp = client.query(QuerySequence().with_name(UPLOADED_SEQUENCE_NAME))
    assert qresp is not None
    assert len(qresp.items) == 1
    assert qresp.items[0].sequence.name == UPLOADED_SEQUENCE_NAME

    # Read Sequence: must pass
    sh = client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    assert sh is not None
    # Get Data Streamer: must pass
    sds = sh.get_data_streamer()
    # Must pass
    sds.next_timestamp()

    th = client.topic_handler(UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC)
    assert th is not None
    # Get Data Streamer: must pass
    tds = th.get_data_streamer()
    # Must pass
    tds.next_timestamp()

    # Read a Topic: must pass
    th = sh.get_topic_handler(UPLOADED_GPS_TOPIC)
    assert th is not None
    # Get Data Streamer: must pass
    tds = th.get_data_streamer()
    # Must pass
    tds.next_timestamp()


# --- Helpers ---
def _test_read_fail(client: MosaicoClient):
    # Must fail
    with pytest.raises(FlightUnauthorizedError):
        client.list_sequences()

    # Must fail
    with pytest.raises(FlightUnauthorizedError):
        client.list_sequence_notifications(UPLOADED_SEQUENCE_NAME)

    # Must fail
    with pytest.raises(FlightUnauthorizedError):
        client.list_topic_notifications(UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC)

    # Must fail
    with pytest.raises(FlightUnauthorizedError):
        client.query(QuerySequence().with_name(UPLOADED_SEQUENCE_NAME))

    # Read Sequence: must fail
    with pytest.raises(FlightUnauthorizedError):
        client.sequence_handler(UPLOADED_SEQUENCE_NAME)

    # Read Topic: must fail
    with pytest.raises(FlightUnauthorizedError):
        client.topic_handler(UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC)


def _test_write_fail(client: MosaicoClient):
    # Create a new Sequence: must fail
    with pytest.raises(FlightUnauthorizedError):
        with client.sequence_create("unauthorized_sequence_create", {}) as _:
            pass

    # Update a Sequence: must fail
    with pytest.raises(FlightUnauthorizedError):
        with client.sequence_update(
            UPLOADED_SEQUENCE_NAME, SessionLevelErrorPolicy.Delete
        ) as _:
            pass


def _test_write_pass(
    write_enabled_client: MosaicoClient,
    full_fledged_client: MosaicoClient,
):
    # Create a new Sequence: must pass
    with write_enabled_client.sequence_create("authorized_sequence_create", {}) as sw:
        sw.topic_create("test_topic", {}, IMU)
        pass

    full_fledged_client.sequence_delete("authorized_sequence_create")

    # Update a Sequence: must pass
    slocator = ""
    with write_enabled_client.sequence_update(
        UPLOADED_SEQUENCE_NAME, SessionLevelErrorPolicy.Delete
    ) as su:
        slocator = su.session_locator
        su.topic_create("test_topic", {}, IMU)
        pass

    full_fledged_client.session_delete(slocator)


def _test_delete_fail(del_disabled_client: MosaicoClient):
    # Delete a Sequence: must fail
    with pytest.raises(FlightUnauthorizedError):
        del_disabled_client.sequence_delete(UPLOADED_SEQUENCE_NAME)
    with pytest.raises(FlightUnauthorizedError):
        del_disabled_client.clear_sequence_notifications(UPLOADED_SEQUENCE_NAME)
    with pytest.raises(FlightUnauthorizedError):
        del_disabled_client.clear_topic_notifications(
            UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC
        )


def _test_delete_pass(
    del_enabled_client: MosaicoClient,
    full_fledged_client: MosaicoClient,
):
    with pytest.raises(RuntimeError, match="__aborted_sequence_creation__"):
        with full_fledged_client.sequence_create(
            "tmp_sequence_create",
            {},
            SessionLevelErrorPolicy.Delete,  # This can be done with Delete permissions
        ) as sw:
            sw.topic_create("test_topic", {}, IMU)
            raise RuntimeError("__aborted_sequence_creation__")

    del_enabled_client.clear_sequence_notifications(UPLOADED_SEQUENCE_NAME)

    del_enabled_client.clear_topic_notifications(
        UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC
    )


# --- Tests ---


def test_no_auth_failure(
    host,
    port,
    with_auth,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    with pytest.raises(ConnectionError):
        MosaicoClient.connect(host=host, port=port, timeout=1)


def test_wrong_auth(
    with_auth,
    host,
    port,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    with pytest.raises(ConnectionError):
        MosaicoClient.connect(
            host=host, port=port, timeout=1, api_key="msco_wrongauthapikey123_abc12345"
        )


def test_read_only_api_key(
    with_auth,
    host,
    port,
    api_key_read,
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # Test Read-Only API Key
    with MosaicoClient.connect(host=host, port=port, api_key=api_key_read) as client:
        # --- Try reading ---
        _test_read_pass(client)

        # --- Try writing ---
        _test_write_fail(client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_fail(client)


def test_write_only_api_key(
    with_auth,
    host,
    port,
    api_key_write,
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # Test Write API Key
    with MosaicoClient.connect(host=host, port=port, api_key=api_key_write) as client:
        # --- Try reading ---
        _test_read_fail(client)

        # --- Try writing ---
        _test_write_pass(client, mosaico_client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_fail(client)

    mosaico_client.close()


def test_delete_api_key(
    with_auth,
    host,
    port,
    api_key_delete,
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # Test Delete API Key
    with MosaicoClient.connect(host=host, port=port, api_key=api_key_delete) as client:
        ## --- Try reading ---
        _test_read_fail(client)

        # --- Try writing ---
        _test_write_fail(client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_pass(client, mosaico_client)

    mosaico_client.close()


def test_manage_api_key(
    with_auth,
    host,
    port,
    api_key_manage,
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # Test Manage API Key
    with MosaicoClient.connect(host=host, port=port, api_key=api_key_manage) as client:
        ## --- Try reading ---
        _test_read_pass(client)

        # --- Try writing ---
        _test_write_pass(client, client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_pass(client, client)


def test_delete_policy(
    with_auth,
    host,
    port,
    api_key_write,
    api_key_manage,
    mosaico_client: MosaicoClient,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    with MosaicoClient.connect(host=host, port=port, api_key=api_key_write) as client:
        session_locator = ""
        with pytest.raises(Exception, match="unauthorized"):
            with client.sequence_create(
                "unauthorized_sequence_abort",
                {},
                SessionLevelErrorPolicy.Delete,
            ) as sw:
                session_locator = sw._locator
                sw.topic_create("test_topic", {}, IMU)
                raise RuntimeError("__aborted_sequence_creation__")

    # Need manage permissions for doing ALL the following
    with MosaicoClient.connect(host=host, port=port, api_key=api_key_manage) as client:
        # Check that the sequence and related session is still present
        sh = client.sequence_handler("unauthorized_sequence_abort")
        assert sh is not None
        # Just one session
        assert len(sh.sessions) == 1
        session = sh.sessions[0]
        assert session.locator == session_locator
        # The session is unlocked
        assert session.locked is False
        # The session is not finalized!
        assert session.completed_timestamp is None
        assert session.topics == ["/test_topic"]

        # free resources
        mosaico_client.sequence_delete("unauthorized_sequence_abort")

    mosaico_client.close()

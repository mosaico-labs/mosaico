from typing import List, Tuple

import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum import APIKeyPermissionEnum, SessionLevelErrorPolicy
from mosaicolabs.models.query.builders import QuerySequence
from mosaicolabs.models.sensors.imu import IMU
from mosaicolabs.platform.api_key import _get_fingerprint
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


def _test_write_fail(client: MosaicoClient):
    # Create a new Sequence: must fail
    with pytest.raises(Exception, match="unauthorized"):
        with client.sequence_create("unauthorized_sequence_create", {}) as _:
            pass

    sh = client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    assert sh is not None
    # Update a Sequence: must fail
    with pytest.raises(Exception, match="unauthorized"):
        with sh.update(SessionLevelErrorPolicy.Delete) as _:
            pass

    # Delete a Sequence: must fail
    with pytest.raises(Exception, match="unauthorized"):
        client.sequence_delete(UPLOADED_SEQUENCE_NAME)


def _test_write_pass(
    write_enabled_client: MosaicoClient,
    full_fledged_client: MosaicoClient,
):
    # Create a new Sequence: must pass
    with write_enabled_client.sequence_create("authorized_sequence_create", {}) as sw:
        sw.topic_create("test_topic", {}, IMU)
        pass

    full_fledged_client.sequence_delete("authorized_sequence_create")

    sh = write_enabled_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    assert sh is not None
    # Update a Sequence: must pass
    suuid = ""
    with sh.update(SessionLevelErrorPolicy.Delete) as su:
        suuid = su.session_uuid
        su.topic_create("test_topic", {}, IMU)
        pass

    full_fledged_client.session_delete(suuid)


def _test_delete_fail(del_disabled_client: MosaicoClient):
    # Delete a Sequence: must fail
    with pytest.raises(Exception, match="unauthorized"):
        del_disabled_client.sequence_delete(UPLOADED_SEQUENCE_NAME)
    with pytest.raises(Exception, match="unauthorized"):
        del_disabled_client.clear_sequence_notifications(UPLOADED_SEQUENCE_NAME)
    with pytest.raises(Exception, match="unauthorized"):
        del_disabled_client.clear_topic_notifications(
            UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC
        )


def _test_delete_pass(del_enabled_client: MosaicoClient):
    with pytest.raises(RuntimeError, match="__aborted_sequence_creation__"):
        with del_enabled_client.sequence_create(
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


def _test_manage_fail(manage_disabled_client: MosaicoClient):
    # Create a new API Key
    with pytest.raises(Exception, match="unauthorized"):
        manage_disabled_client.api_key_create(
            permission=APIKeyPermissionEnum.Read,
            description="unauthorized api creation",
        )

    # Read the status
    with pytest.raises(Exception, match="unauthorized"):
        manage_disabled_client.api_key_status("abcd1234")

    # Revoke
    with pytest.raises(Exception, match="unauthorized"):
        manage_disabled_client.api_key_revoke("abcd1234")


def _test_manage_pass(manage_enabled_client: MosaicoClient):
    # Create a new API Key
    new_api_key = manage_enabled_client.api_key_create(
        permission=APIKeyPermissionEnum.Read,
        description="authorized api creation",
    )
    assert new_api_key is not None

    ak_fprint = _get_fingerprint(new_api_key)

    # Read the status
    ak_status = manage_enabled_client.api_key_status(ak_fprint)
    assert ak_status is not None
    assert ak_status.is_expired is False
    assert ak_status.description == "authorized api creation"

    # Revoke
    manage_enabled_client.api_key_revoke(ak_fprint)


def _get_api_key(api_keys_list: List[Tuple], perm: APIKeyPermissionEnum) -> str:
    return next(item[0] for item in api_keys_list if perm == item[1])


# --- Tests ---


def test_get_fingerprint():
    assert (
        _get_fingerprint("msco_58qb7dssul32r1bewpziy3rfjuewd0a3_c5505db6") == "c5505db6"
    )

    with pytest.raises(ValueError, match="wrong number of parts"):
        _get_fingerprint("msco_58qb7dssul32r1bewpziy3rfjuewd0a3")

    with pytest.raises(ValueError, match="not alnum"):
        # Payload not alpha AND num
        _get_fingerprint("msco_123457678_abcd1234")

    with pytest.raises(ValueError, match="not alnum"):
        # Payload not alpha AND num
        _get_fingerprint("msco_abcdefghi_abcd1234")

    with pytest.raises(ValueError, match="not alnum"):
        # Payload not alpha AND num
        _get_fingerprint("msco_abcde12345_abcd1234!:")

    with pytest.raises(ValueError, match="not alnum"):
        # Payload not alpha AND num
        _get_fingerprint("msco_abcde12345!:_abcd1234")

    with pytest.raises(ValueError, match="fingerprint"):
        # Fingerprint less than 8 char
        _get_fingerprint("msco_58qb7dssul_c5505")


def test_no_auth_failure(
    host,
    port,
    with_auth,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    with pytest.raises(ConnectionError, match="unauthorized error"):
        MosaicoClient.connect(host=host, port=port, timeout=1)


def test_wrong_auth(
    with_auth,
    host,
    port,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    with pytest.raises(ConnectionError, match="unauthorized error"):
        MosaicoClient.connect(
            host=host, port=port, timeout=1, api_key="msco_wrongauthapikey123_abc12345"
        )


def test_read_only_api_key(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # extract a Read-Only API Key among the one created
    read_only_key = _get_api_key(api_keys_list, APIKeyPermissionEnum.Read)
    # Test Read-Only API Key
    with MosaicoClient.connect(host=host, port=port, api_key=read_only_key) as client:
        # --- Try reading ---
        _test_read_pass(client)

        # --- Try writing ---
        _test_write_fail(client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_fail(client)

        # -- Try Managing --
        _test_manage_fail(client)


def test_write_only_api_key(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # extract a Write API Key among the one created
    write_only_key = _get_api_key(api_keys_list, APIKeyPermissionEnum.Write)
    # Test Write API Key
    with MosaicoClient.connect(host=host, port=port, api_key=write_only_key) as client:
        # --- Try reading ---
        _test_read_pass(client)

        # --- Try writing ---
        _test_write_pass(client, mosaico_client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_fail(client)

        # -- Try Managing --
        _test_manage_fail(client)


def test_delete_api_key(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # extract a Delete API Key among the one created
    mgmt_key = _get_api_key(api_keys_list, APIKeyPermissionEnum.Delete)
    # Test Delete API Key
    with MosaicoClient.connect(host=host, port=port, api_key=mgmt_key) as client:
        ## --- Try reading ---
        _test_read_pass(client)

        # --- Try writing ---
        _test_write_pass(client, client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_pass(client)

        # -- Try Managing --
        _test_manage_fail(client)


def test_manage_api_key(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    inject_synthetic_sequence,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # extract a Manage API Key among the one created
    mgmt_key = _get_api_key(api_keys_list, APIKeyPermissionEnum.Manage)
    # Test Manage API Key
    with MosaicoClient.connect(host=host, port=port, api_key=mgmt_key) as client:
        ## --- Try reading ---
        _test_read_pass(client)

        # --- Try writing ---
        _test_write_pass(client, client)

        # --- Try deleting ---
        # Delete a Sequence: must fail
        _test_delete_pass(client)

        # -- Try Managing --
        _test_manage_pass(client)


def test_delete_policy(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    mosaico_client: MosaicoClient,
):
    if not with_auth:
        pytest.skip("Tests run without '--api-key'")

    # extract a Write API Key among the one created
    write_only_key = _get_api_key(api_keys_list, APIKeyPermissionEnum.Write)

    with MosaicoClient.connect(host=host, port=port, api_key=write_only_key) as client:
        session_uuid = ""
        with pytest.raises(Exception, match="unauthorized"):
            with client.sequence_create(
                "unauthorized_sequence_abort",
                {},
                SessionLevelErrorPolicy.Delete,
            ) as sw:
                session_uuid = sw._uuid
                sw.topic_create("test_topic", {}, IMU)
                raise RuntimeError("__aborted_sequence_creation__")

        # Check that the sequence and related session is still present
        sh = client.sequence_handler("unauthorized_sequence_abort")
        assert sh is not None
        # Just one session
        assert len(sh.sessions) == 1
        session = sh.sessions[0]
        assert session.uuid == session_uuid
        # The session is unlocked
        assert session.locked is False
        # The session is not finalized!
        assert session.completed_timestamp is None
        assert session.topics == ["/test_topic"]

        # free resources
        mosaico_client.sequence_delete("unauthorized_sequence_abort")

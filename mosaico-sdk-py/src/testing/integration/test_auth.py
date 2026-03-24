from typing import List, Tuple

import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum import APIKeyPermissionEnum, SessionLevelErrorPolicy
from testing.integration.config import UPLOADED_GPS_TOPIC, UPLOADED_SEQUENCE_NAME


def test_no_auth_failure(
    host,
    port,
    with_auth,
):
    if with_auth:
        with pytest.raises(ConnectionError, match="unauthorized error"):
            MosaicoClient.connect(host=host, port=port, timeout=1)


def test_wrong_auth(
    with_auth,
    host,
    port,
):
    if with_auth:
        with pytest.raises(ConnectionError, match="unauthorized error"):
            MosaicoClient.connect(
                host=host, port=port, timeout=1, api_key="msco_wrongauthapikey_abc12345"
            )


def test_read_only_api_key(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    _inject_sequence_data_stream,
):
    if with_auth:
        # extract a Read-Only API Key among the one created
        read_only_key: str = next(
            item[0]
            for item in api_keys_list
            if len(item[1]) == 1 and APIKeyPermissionEnum.Read == item[1][0]
        )
        # Test Read-Only API Key
        with MosaicoClient.connect(
            host=host, port=port, api_key=read_only_key
        ) as client:
            # Try writing: must raise
            with pytest.raises(Exception, match="unauthorized"):
                with client.sequence_create(
                    "unauthorized_sequence_create", {}, SessionLevelErrorPolicy.Delete
                ) as _:
                    pass

            # Try reading: must pass
            client.list_sequences()

            # Try reading a sequence: must pass
            sh = client.sequence_handler(UPLOADED_SEQUENCE_NAME)
            assert sh is not None
            sds = sh.get_data_streamer()
            sds.next_timestamp()

            # Try reading a topic: must pass
            th = sh.get_topic_handler(UPLOADED_GPS_TOPIC)
            assert th is not None
            tds = th.get_data_streamer()
            tds.next_timestamp()

            # Try Updating the sequence: must fail
            with pytest.raises(Exception, match="unauthorized"):
                with sh.update(SessionLevelErrorPolicy.Delete) as _:
                    pass


def test_write_only_api_key(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    # _inject_sequence_data_stream,
):
    if with_auth:
        # extract a Read-Only API Key among the one created
        write_only_key: str = next(
            item[0]
            for item in api_keys_list
            if len(item[1]) == 1 and APIKeyPermissionEnum.Write == item[1][0]
        )
        # Test Read-Only API Key
        with MosaicoClient.connect(
            host=host, port=port, api_key=write_only_key
        ) as client:
            # Try writing: must pass
            with client.sequence_create(
                "authorized_sequence_create", {}, SessionLevelErrorPolicy.Delete
            ) as _:
                pass
            client.sequence_delete("authorized_sequence_create")

            # Try reading: must fail
            with pytest.raises(Exception, match="unauthorized"):
                client.list_sequences()

            # Try reading a sequence: must fail
            with pytest.raises(Exception, match="unauthorized"):
                client.sequence_handler(UPLOADED_SEQUENCE_NAME)

            # Try reading a topic: must fail
            with pytest.raises(Exception, match="unauthorized"):
                client.topic_handler(UPLOADED_SEQUENCE_NAME, UPLOADED_GPS_TOPIC)


def test_read_write_api_key(
    with_auth,
    host,
    port,
    api_keys_list: List[Tuple],
    _inject_sequence_data_stream,
):
    if with_auth:
        # extract a Read-Only API Key among the one created
        read_write_key: str = next(
            item[0]
            for item in api_keys_list
            if APIKeyPermissionEnum.Read in item[1]
            and APIKeyPermissionEnum.Write in item[1]
        )
        # Test Read-Only API Key
        with MosaicoClient.connect(
            host=host, port=port, api_key=read_write_key
        ) as client:
            # Try writing: must pass
            with client.sequence_create(
                "unauthorized_sequence_create", {}, SessionLevelErrorPolicy.Delete
            ) as _:
                pass

            client.sequence_delete("unauthorized_sequence_create")

            # # Try reading: must pass
            # client.list_sequences()

            # # Try reading a sequence: must pass
            # sh = client.sequence_handler(UPLOADED_SEQUENCE_NAME)
            # assert sh is not None
            # sds = sh.get_data_streamer()
            # sds.next_timestamp()

            # # Try reading a topic: must pass
            # th = sh.get_topic_handler(UPLOADED_GPS_TOPIC)
            # assert th is not None
            # tds = th.get_data_streamer()
            # tds.next_timestamp()

            # # Try Updating the sequence: must pass
            # sess_uuid = ""
            # with sh.update(SessionLevelErrorPolicy.Delete) as su:
            #     sess_uuid = su.session_uuid
            #     pass

            # client.session_delete(sess_uuid)

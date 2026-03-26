import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum.api_key_permission import APIKeyPermissionEnum
from testing.integration.helpers import (
    DataStreamItem,
    SequenceDataStream,
    sequential_time_generator,
    topic_maker_generator,
    topic_to_maker_factory,
    topic_to_metadata_dict,
    topic_to_ontology_class_dict,
)

from .config import (
    QUERY_SEQUENCES_MOCKUP,
    UPLOADED_SEQUENCE_METADATA,
    UPLOADED_SEQUENCE_NAME,
)


@pytest.fixture(scope="session")
def api_keys_list(host, port, with_auth, api_key_mgmt):
    if with_auth:
        api_keys_list = []
        with MosaicoClient.connect(
            host=host, port=port, api_key=api_key_mgmt
        ) as _client:
            r_key = _client.api_key_create(
                APIKeyPermissionEnum.Read, description="read-only-api-key"
            )
            assert r_key is not None
            api_keys_list.append((r_key, APIKeyPermissionEnum.Read))

            w_key = _client.api_key_create(
                APIKeyPermissionEnum.Write, description="write-api-key"
            )
            assert w_key is not None
            api_keys_list.append((w_key, APIKeyPermissionEnum.Write))

            d_key = _client.api_key_create(
                APIKeyPermissionEnum.Delete, description="delete-api-key"
            )
            assert d_key is not None
            api_keys_list.append((d_key, APIKeyPermissionEnum.Delete))

            m_key = _client.api_key_create(
                APIKeyPermissionEnum.Manage, description="manage-api-key"
            )
            assert m_key is not None
            api_keys_list.append((m_key, APIKeyPermissionEnum.Manage))

            return api_keys_list

    return None


@pytest.fixture(scope="function")
def mosaico_client(host, port, tls_cert_path, api_key_mgmt):
    """Open a client connection FOR EACH function using this fixture"""

    return MosaicoClient.connect(
        host=host, port=port, tls_cert_path=tls_cert_path, api_key=api_key_mgmt
    )


@pytest.fixture(
    scope="session"
)  # the first who calls this function, wins and avoid this is called multiple times
def synthetic_sequence_data_stream(host, port, tls_cert_path, api_key_mgmt):
    """Generate synthetic data, create a sequence and pushes messages"""
    _client = MosaicoClient.connect(
        host=host, port=port, tls_cert_path=tls_cert_path, api_key=api_key_mgmt
    )

    start_time_sec = 1700000000
    start_time_nanosec = 0
    dt_nanosec = 5_000_000  # 5 ms
    steps = 100

    items = []

    time_gen = sequential_time_generator(
        start_sec=start_time_sec,
        start_nanosec=start_time_nanosec,
        step_nanosec=dt_nanosec,
        steps=steps,
    )

    msg_maker_gen = topic_maker_generator(
        topic_to_maker_factory,
    )

    for t in range(steps):
        meas_time = next(time_gen)
        topic, msg_maker = next(msg_maker_gen)
        ontology_type = topic_to_ontology_class_dict[topic]

        msg = msg_maker(meas_time=meas_time)

        items.append(
            DataStreamItem(
                topic=topic,
                msg=msg,
                ontology_class=ontology_type,
            )
        )

    # free resources
    _client.close()
    return SequenceDataStream(
        items=items,
        dt_nanosec=dt_nanosec,
        tstamp_ns_start=items[0].msg.timestamp_ns,
        tstamp_ns_end=items[-1].msg.timestamp_ns,
    )


@pytest.fixture(scope="session")
def inject_synthetic_sequence(
    synthetic_sequence_data_stream, host, port, tls_cert_path, api_key_mgmt
):
    """Generate synthetic data, create a sequence and pushes messages"""
    _client = MosaicoClient.connect(
        host=host, port=port, tls_cert_path=tls_cert_path, api_key=api_key_mgmt
    )

    with _client.sequence_create(
        sequence_name=UPLOADED_SEQUENCE_NAME,
        metadata=UPLOADED_SEQUENCE_METADATA,
    ) as swriter:
        for ds_item in synthetic_sequence_data_stream.items:
            twriter = swriter.get_topic_writer(topic_name=ds_item.topic)
            if twriter is None:
                twriter = swriter.topic_create(
                    topic_name=ds_item.topic,
                    metadata=topic_to_metadata_dict[ds_item.topic],
                    ontology_type=ds_item.ontology_class,
                )
                if twriter is None:
                    raise Exception(
                        f"Unable to create topic '{ds_item.topic}' in sequence '{UPLOADED_SEQUENCE_NAME}'"
                    )

            twriter.push(ds_item.msg)

    # free resources
    _client.close()


@pytest.fixture(scope="session")
def inject_mockup_sequences(host, port, tls_cert_path, api_key_mgmt):
    """Generate synthetic data, create a sequence and pushes messages"""
    _client = MosaicoClient.connect(
        host=host, port=port, tls_cert_path=tls_cert_path, api_key=api_key_mgmt
    )
    for sname, sdata in QUERY_SEQUENCES_MOCKUP.items():
        with _client.sequence_create(
            sequence_name=sname,
            metadata=sdata["metadata"],
        ) as swriter:
            for tdata in sdata["topics"]:
                tname = tdata["name"]
                twriter = swriter.get_topic_writer(topic_name=tname)
                if twriter is None:
                    twriter = swriter.topic_create(
                        topic_name=tname,
                        metadata=tdata["metadata"],
                        ontology_type=tdata["ontology_type"],
                    )
                    if twriter is None:
                        raise Exception(
                            f"Unable to create topic '{tname}' in sequence '{sname}'"
                        )

    # free resources
    _client.close()

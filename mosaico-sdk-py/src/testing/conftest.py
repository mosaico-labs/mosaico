from mosaicolabs.logging import setup_sdk_logging
import pytest
from mosaicolabs.comm import MosaicoClient
from testing.integration.helpers import (
    DataStreamItem,
    SequenceDataStream,
    topic_to_maker_factory,
    topic_to_metadata_dict,
    topic_to_ontology_class_dict,
    sequential_time_generator,
    topic_maker_generator,
)
from .integration.config import (
    UPLOADED_SEQUENCE_METADATA,
    UPLOADED_SEQUENCE_NAME,
    QUERY_SEQUENCES_MOCKUP,
)


def pytest_configure(config):
    """
    Hook called by pytest before any tests are run.
    We use it to sync the SDK's internal logging with pytest's CLI options.
    """
    level_str = config.getoption("--log-cli-level")
    if level_str:
        # Initialize the SDK logger
        setup_sdk_logging(level=level_str.upper(), pretty=True)


def pytest_addoption(parser):
    parser.addoption(
        "--host", action="store", default="localhost", type=str, help="Set client host."
    )
    parser.addoption(
        "--port", action="store", default="6276", type=int, help="Set client port."
    )


@pytest.fixture(scope="session")
def host(request):
    return request.config.getoption("--host")


@pytest.fixture(scope="session")
def port(request):
    return request.config.getoption("--port")


@pytest.fixture(scope="function")
def _client(host, port):
    """Open a client connection FOR EACH function using this fixture"""
    return MosaicoClient.connect(host=host, port=port)


@pytest.fixture(
    scope="session"
)  # the first who calls this function, wins and avoid this is called multiple times
def _make_sequence_data_stream(host, port):
    """Generate synthetic data, create a sequence and pushes messages"""
    _client = MosaicoClient.connect(host=host, port=port)

    start_time_sec = 1700000000
    start_time_nanosec = 0
    dt_nanosec = 5_000_000  # 5 ms
    steps = 100

    out_stream: SequenceDataStream = SequenceDataStream(
        items=[],
        tstamp_ns_start=0,
        tstamp_ns_end=(steps-1)*dt_nanosec,
    )

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

        msg = msg_maker(
            msg_time=t * (dt_nanosec),  # simulation time
            meas_time=meas_time,
        )

        out_stream.items.append(
            DataStreamItem(
                topic=topic,
                msg=msg,
                ontology_class=ontology_type,
            )
        )

    # free resources
    _client.close()
    return out_stream


@pytest.fixture(scope="session")
def _inject_sequence_data_stream(_make_sequence_data_stream, host, port):
    """Generate synthetic data, create a sequence and pushes messages"""
    _client = MosaicoClient.connect(host=host, port=port)

    with _client.sequence_create(
        sequence_name=UPLOADED_SEQUENCE_NAME,
        metadata=UPLOADED_SEQUENCE_METADATA,
    ) as swriter:
        for ds_item in _make_sequence_data_stream.items:
            twriter = swriter.get_topic(topic_name=ds_item.topic)
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
def _inject_sequences_mockup(host, port):
    """Generate synthetic data, create a sequence and pushes messages"""
    _client = MosaicoClient.connect(host=host, port=port)
    for sname, sdata in QUERY_SEQUENCES_MOCKUP.items():
        with _client.sequence_create(
            sequence_name=sname,
            metadata=sdata["metadata"],
        ) as swriter:
            for tdata in sdata["topics"]:
                tname = tdata["name"]
                twriter = swriter.get_topic(topic_name=tname)
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

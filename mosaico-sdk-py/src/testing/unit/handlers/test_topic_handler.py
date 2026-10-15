import json
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from mosaicolabs.handlers import topic_handler as th_module
from mosaicolabs.handlers.topic_handler import TopicHandler
from mosaicolabs.helpers import pack_topic_resource_name
from mosaicolabs.models.core.message import Message

_DATA_SCHEMA = pa.struct([pa.field("x", pa.float64())])


@pytest.fixture
def connection():
    flight_client = MagicMock()
    flight_client.get_flight_info.return_value = MagicMock(endpoints=[MagicMock()])
    flight_client.get_schema.return_value = MagicMock(
        schema=Message._get_schema(_DATA_SCHEMA)
    )
    return MagicMock(flight_client=flight_client)


@pytest.fixture
def handler(connection, monkeypatch) -> TopicHandler:
    app_metadata = MagicMock(timestamp_ns_min=0, timestamp_ns_max=10)
    app_metadata.name = "/imu"
    monkeypatch.setattr(
        th_module.TopicAppMetadata,
        "_from_flight_endpoint",
        classmethod(lambda cls, ep: app_metadata),
    )
    topic_model = MagicMock(sequence_name="seq")
    topic_model.name = "/imu"
    monkeypatch.setattr(
        th_module.Topic,
        "_from_app_metadata",
        classmethod(lambda cls, **kwargs: topic_model),
    )
    handler = TopicHandler._connect(
        sequence_name="seq", topic_name="/imu", connection=connection
    )
    assert handler is not None
    return handler


def test_connect_does_not_fetch_schema(handler, connection):
    connection.flight_client.get_flight_info.assert_called_once()
    connection.flight_client.get_schema.assert_not_called()


def test_metadata_access_does_not_fetch_schema(handler, connection):
    handler.timestamp_ns_min
    handler.timestamp_ns_max
    handler.name
    handler.sequence_name
    connection.flight_client.get_schema.assert_not_called()


def test_ontology_schema_is_fetched_lazily_and_cached(handler, connection):
    assert handler.ontology_schema == _DATA_SCHEMA
    assert handler.ontology_schema == _DATA_SCHEMA
    connection.flight_client.get_schema.assert_called_once()
    descriptor = connection.flight_client.get_schema.call_args.args[0]
    assert json.loads(descriptor.command) == {
        "resource_locator": pack_topic_resource_name("seq", "/imu")
    }

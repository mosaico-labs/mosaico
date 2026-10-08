from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcap.well_known import SchemaEncoding

from mosaicolabs.bridges.mcap import MCAPInjector
from mosaicolabs.bridges.ros import RosbagInjector
from mosaicolabs.watchdog import dispatcher as dispatcher_mod
from mosaicolabs.watchdog.dispatcher import InjectorDispatcher


def make_summary(*channel_encodings):
    """Builds a fake MCAP summary. Each item is the schema encoding of one channel, or
    `None` for a schemaless channel (schema_id 0, not in `schemas`)."""
    schemas, channels = {}, {}
    for i, enc in enumerate(channel_encodings, start=1):
        if enc is None:
            channels[i] = SimpleNamespace(schema_id=0)
        else:
            schemas[i] = SimpleNamespace(encoding=enc)
            channels[i] = SimpleNamespace(schema_id=i)
    return SimpleNamespace(channels=channels, schemas=schemas)


@pytest.fixture
def fake_reader(monkeypatch):
    """Patches `MCAPFileReader` so that `.mcap` files are never opened. Set
    `state["summary"]` to choose what `get_summary()` returns."""
    state = {"summary": None, "opened": []}

    @contextmanager
    def fake(path, *_args, **_kwargs):
        state["opened"].append(path)
        yield SimpleNamespace(
            reader=SimpleNamespace(get_summary=lambda: state["summary"])
        )

    monkeypatch.setattr(dispatcher_mod, "MCAPFileReader", fake)
    return state


def test_list_supported_ext():
    exts = InjectorDispatcher.list_supported_ext()
    assert set(exts) == {".mcap", ".db3", ".bag"}
    assert all(e.startswith(".") for e in exts)


@pytest.mark.parametrize("name", ["a.bag", "a.db3"])
def test_rosbag_extensions_do_not_open_file(fake_reader, name):
    # The file does not even exist: only the extension matters
    assert InjectorDispatcher.get_injector(Path("/nope") / name) is RosbagInjector
    assert fake_reader["opened"] == []


@pytest.mark.parametrize("name", ["a.txt", "a.MCAP", "a.BAG", "a", "a.mcap.tmp"])
def test_unsupported_extension_raises(fake_reader, name):
    with pytest.raises(RuntimeError):
        InjectorDispatcher.get_injector(Path("/nope") / name)
    assert fake_reader["opened"] == []


@pytest.mark.parametrize(
    "encodings, expected",
    [
        ((SchemaEncoding.ROS2,), RosbagInjector),
        ((SchemaEncoding.ROS2IDL,), RosbagInjector),
        ((SchemaEncoding.ROS2, SchemaEncoding.ROS2IDL), RosbagInjector),
        ((), MCAPInjector),  # no channels
        ((SchemaEncoding.ROS1,), MCAPInjector),  # ROS 1 .mcap
        ((SchemaEncoding.JSONSchema,), MCAPInjector),
        ((SchemaEncoding.Protobuf,), MCAPInjector),
        ((SchemaEncoding.ROS2, SchemaEncoding.JSONSchema), MCAPInjector),  # mixed
        ((SchemaEncoding.ROS2, None), MCAPInjector),  # one schemaless channel
        ((None,), MCAPInjector),
    ],
    ids=[
        "ros2msg",
        "ros2idl",
        "ros2-both",
        "empty",
        "ros1",
        "jsonschema",
        "protobuf",
        "mixed",
        "ros2+schemaless",
        "schemaless",
    ],
)
def test_mcap_by_channel_encoding(fake_reader, encodings, expected):
    fake_reader["summary"] = make_summary(*encodings)
    path = Path("/nope/a.mcap")

    assert InjectorDispatcher.get_injector(path) is expected
    assert fake_reader["opened"] == [path]


def test_mcap_without_summary_raises(fake_reader):
    fake_reader["summary"] = None
    with pytest.raises(RuntimeError):
        InjectorDispatcher.get_injector(Path("/nope/a.mcap"))


def test_missing_mcap_raises_file_not_found(tmp_path):
    # Real reader: a file deleted after the scan
    with pytest.raises(FileNotFoundError):
        InjectorDispatcher.get_injector(tmp_path / "gone.mcap")

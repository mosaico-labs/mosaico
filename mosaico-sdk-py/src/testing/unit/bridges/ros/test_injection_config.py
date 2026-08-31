from pathlib import Path

import pytest

from mosaicolabs.bridges.ros.injector import (
    RosbagInjector,
    ROSInjectionConfig,
    _parse_json_arg,
)
from mosaicolabs.enum import TopicLevelErrorPolicy

# --- _parse_json_arg ---


def test_parse_json_arg_empty_input_returns_empty_dict():
    assert _parse_json_arg(None) == {}
    assert _parse_json_arg("") == {}


def test_parse_json_arg_parses_raw_json_string():
    assert _parse_json_arg('{"driver": "John"}') == {"driver": "John"}


def test_parse_json_arg_parses_json_file(tmp_path):
    json_file = tmp_path / "metadata.json"
    json_file.write_text('{"driver": "John"}')

    assert _parse_json_arg(str(json_file)) == {"driver": "John"}


def test_parse_json_arg_invalid_json_and_not_a_file_exits(tmp_path):
    not_a_path = str(tmp_path / "does_not_exist.json")

    with pytest.raises(SystemExit) as exc_info:
        _parse_json_arg(not_a_path)

    assert exc_info.value.code == 1


def test_parse_json_arg_file_with_malformed_json_exits(tmp_path):
    json_file = tmp_path / "malformed.json"
    json_file.write_text("{not valid json")

    with pytest.raises(SystemExit) as exc_info:
        _parse_json_arg(str(json_file))

    assert exc_info.value.code == 1


# --- ROSInjectionConfig defaults ---


def _make_config(**overrides) -> ROSInjectionConfig:
    defaults = dict(file_path=Path("data.mcap"), sequence_name="seq")
    defaults.update(overrides)
    return ROSInjectionConfig(**defaults)


def test_metadata_default_is_not_shared_between_instances():
    first = _make_config()
    second = _make_config()

    assert first.metadata == {}
    assert first.metadata is not second.metadata

    first.metadata["mutated"] = True

    assert "mutated" not in second.metadata


def test_topic_metadata_defaults_to_none():
    assert _make_config().topic_metadata is None


def test_dry_run_defaults_to_false():
    assert _make_config().dry_run is False


def test_update_if_exists_defaults_to_false():
    assert _make_config().update_if_exists is False


# --- RosbagInjector._get_topic_on_error ---


def _make_injector(**overrides) -> RosbagInjector:
    return RosbagInjector(_make_config(**overrides))


def test_get_topic_on_error_default_when_unset():
    injector = _make_injector()

    assert injector._get_topic_on_error("/imu") == TopicLevelErrorPolicy.Raise


def test_get_topic_on_error_uniform_policy_applies_to_all_topics():
    injector = _make_injector(topics_on_error=TopicLevelErrorPolicy.Ignore)

    assert injector._get_topic_on_error("/imu") == TopicLevelErrorPolicy.Ignore
    assert injector._get_topic_on_error("/gps") == TopicLevelErrorPolicy.Ignore


def test_get_topic_on_error_per_topic_dict_overrides_selected_topics():
    injector = _make_injector(topics_on_error={"/imu": TopicLevelErrorPolicy.Finalize})

    assert injector._get_topic_on_error("/imu") == TopicLevelErrorPolicy.Finalize


def test_get_topic_on_error_per_topic_dict_falls_back_to_default_for_others():
    injector = _make_injector(topics_on_error={"/imu": TopicLevelErrorPolicy.Finalize})

    assert injector._get_topic_on_error("/gps") == TopicLevelErrorPolicy.Raise

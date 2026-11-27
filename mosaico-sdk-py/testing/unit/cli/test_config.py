import pytest

from mosaicolabs_cli.utils.config import (
    _flatten_metadata,
    get_config_path,
    load_config,
)


class TestLoadConfig:
    def test_load_missing_file(self, tmp_path):
        path = tmp_path / "nonexistent.toml"
        result = load_config(path)
        assert result == {}

    def test_load_valid_toml(self, tmp_path):
        path = tmp_path / "config.toml"
        path.write_text('[dev]\nhost = "localhost"\nport = "6276"\n')
        result = load_config(path)
        assert result["dev"]["host"] == "localhost"
        assert result["dev"]["port"] == "6276"

    def test_load_invalid_toml_raises(self, tmp_path):
        import typer

        path = tmp_path / "bad.toml"
        path.write_text("this is [[[not valid toml")
        with pytest.raises(typer.Exit):
            load_config(path)


class TestGetConfigPath:
    def test_default_path(self, monkeypatch):
        monkeypatch.delenv("MOSAICO_CONFIG_PATH", raising=False)
        path = get_config_path()
        assert path.name == "config.toml"
        assert ".mosaico" in str(path)

    def test_env_override(self, monkeypatch, tmp_path):
        custom = tmp_path / "custom.toml"
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(custom))
        path = get_config_path()
        assert path == custom


class TestFlattenMetadata:
    def test_flat_dict(self):
        data = {"key1": "val1", "key2": "val2"}
        result = _flatten_metadata(data)
        assert "key1=val1" in result
        assert "key2=val2" in result

    def test_nested_dict(self):
        data = {"outer": {"inner": "deep"}}
        result = _flatten_metadata(data)
        assert "outer.inner=deep" in result

    def test_deeply_nested(self):
        data = {"a": {"b": {"c": "x"}}}
        result = _flatten_metadata(data)
        assert "a.b.c=x" in result

    def test_empty_dict(self):
        assert _flatten_metadata({}) == []

    def test_mixed_types(self):
        data = {"count": 42, "flag": True, "name": "test"}
        result = _flatten_metadata(data)
        assert "count=42" in result
        assert "flag=True" in result
        assert "name=test" in result

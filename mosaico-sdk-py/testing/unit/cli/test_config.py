from mosaicolabs_cli.utils.config import _flatten_metadata


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

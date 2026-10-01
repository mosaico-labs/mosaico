import csv
import io
import json
from functools import partial

import pytest

from mosaicolabs_cli.output import (
    OutputRenderer,
    render_csv,
    render_json_collection,
    render_jsonl,
    resolve_output,
)
from mosaicolabs_cli.output_format import OutputFormat


def test_command_local_handlers_for_the_same_format():
    first = OutputRenderer(
        {OutputFormat.JSON: partial(render_json_collection, collection="profiles")}
    )
    second = OutputRenderer(
        {OutputFormat.JSON: partial(render_json_collection, collection="topics")}
    )
    for renderer, name in [
        (first, "profiles"),
        (second, "topics"),
        (first, "profiles"),
    ]:
        stream = io.StringIO()
        renderer.render(iter([{"name": "example"}]), OutputFormat.JSON, stream=stream)
        assert json.loads(stream.getvalue()) == {
            "schema_version": 1,
            name: [{"name": "example"}],
        }
        assert not stream.closed


def test_jsonl_flushes_before_requesting_next_record_and_accepts_batches():
    class ObservedStream(io.StringIO):
        def __init__(self):
            super().__init__()
            self.flushed = []

        def flush(self):
            self.flushed.append(self.getvalue())

    stream = ObservedStream()
    renderer = OutputRenderer({OutputFormat.JSONL: render_jsonl})

    def records():
        yield {"n": 1}
        assert stream.flushed == ['{"n": 1}\n']
        yield {"n": 2}
        assert stream.flushed[-1] == '{"n": 1}\n{"n": 2}\n'

    renderer.render(records(), OutputFormat.JSONL, stream=stream)
    renderer.render(iter([{"n": 3}]), OutputFormat.JSONL, stream=stream)
    assert [json.loads(line) for line in stream.getvalue().splitlines()] == [
        {"n": 1},
        {"n": 2},
        {"n": 3},
    ]
    assert len(stream.flushed) == 3
    assert not stream.closed


def test_jsonl_preserves_complete_records_when_producer_fails():
    def records():
        yield {"n": 1}
        raise RuntimeError("source interrupted")

    stream = io.StringIO()
    with pytest.raises(RuntimeError, match="source interrupted"):
        render_jsonl(records(), stream)
    assert stream.getvalue() == '{"n": 1}\n'


def test_unsupported_format_does_not_consume_data():
    def records():
        pytest.fail("unsupported format consumed input")
        yield {}

    renderer = OutputRenderer({OutputFormat.JSONL: render_jsonl})
    with pytest.raises(ValueError, match="Supported formats: jsonl"):
        renderer.render(records(), OutputFormat.JSON, stream=io.StringIO())


def test_handlers_are_owned_by_each_renderer():
    handlers = {OutputFormat.JSONL: render_jsonl}
    renderer = OutputRenderer(handlers)
    handlers.clear()
    stream = io.StringIO()
    renderer.render([{"n": 1}], OutputFormat.JSONL, stream=stream)
    assert json.loads(stream.getvalue()) == {"n": 1}


def test_stdout_is_resolved_at_render_time(capsys):
    renderer = OutputRenderer({OutputFormat.JSONL: render_jsonl})
    renderer.render([{"n": 1}], OutputFormat.JSONL)
    assert capsys.readouterr().out == '{"n": 1}\n'


def test_csv_uses_explicit_fields_and_round_trips_quotes_and_newlines():
    stream = io.StringIO(newline="")
    record = {"name": 'a,"b"\nc', "active": True, "omitted": "internal"}
    render_csv([record], stream, fields=("name", "active"))
    assert list(csv.reader(io.StringIO(stream.getvalue()))) == [
        [record["name"], "true"]
    ]
    assert "internal" not in stream.getvalue()


@pytest.mark.parametrize(
    "writer", [render_jsonl, partial(render_csv, fields=("name",))]
)
def test_empty_stream_formats_emit_nothing(writer):
    stream = io.StringIO()
    writer(iter([]), stream)
    assert stream.getvalue() == ""


def test_schema_version_does_not_depend_on_data():
    for records in [[], [{"name": "first"}], [{"name": "second"}]]:
        stream = io.StringIO()
        render_json_collection(iter(records), stream, collection="profiles")
        assert json.loads(stream.getvalue()) == {
            "schema_version": 1,
            "profiles": records,
        }


@pytest.mark.parametrize("tty", [True, False])
def test_format_defaults_and_explicit_override(monkeypatch, tty):
    monkeypatch.setattr("sys.stdout.isatty", lambda: tty)
    assert resolve_output(None) == (OutputFormat.TABLE if tty else OutputFormat.CSV)
    assert resolve_output(None, piped=OutputFormat.JSON) == (
        OutputFormat.TABLE if tty else OutputFormat.JSON
    )
    for output in OutputFormat:
        assert resolve_output(output) == output

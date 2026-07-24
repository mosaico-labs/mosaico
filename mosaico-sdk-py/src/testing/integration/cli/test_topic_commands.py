import json

from typer.testing import CliRunner

from mosaicolabs_cli.main import app
from testing.integration.config import UPLOADED_SEQUENCE_NAME
from testing.integration.helpers import topic_list


class TestTopicLs:
    def test_ls_returns_topics(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(app, ["topic", "ls"], env=cli_env)
        assert result.exit_code == 0
        for topic in topic_list:
            assert topic in result.output

    def test_ls_with_locator_filter(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        target_topic = topic_list[0]
        result = cli_runner.invoke(
            app,
            ["topic", "ls", "--locator", f"*{target_topic}*"],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert target_topic in result.output

    def test_ls_with_limit(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "ls", "--limit", "1"],
            env=cli_env,
        )
        assert result.exit_code == 0

    def test_ls_csv_output(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "ls", "--output", "csv"],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "," in result.output

    def test_ls_no_results(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "ls", "--locator", "nonexistent-topic-xyz-999"],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "No topics found" in result.output

    def test_ls_with_metadata_filter(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "ls", "--metadata", "role=front"],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "/front/imu" in result.output

    def test_ls_multiple_metadata_filters(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            [
                "topic",
                "ls",
                "--metadata",
                "role=front",
                "--metadata",
                "status=active",
            ],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "/front/imu" in result.output

    def test_ls_invalid_metadata_format(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "ls", "--metadata", "no-equals-sign"],
            env=cli_env,
        )
        assert result.exit_code == 1

    def test_ls_table_output_explicit(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "ls", "--output", "table"],
            env=cli_env,
        )
        assert result.exit_code == 0


class TestTopicStat:
    def test_stat_existing_topic(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        target_topic = topic_list[0]
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handler_str = f"{UPLOADED_SEQUENCE_NAME}{target_topic},{ts_min},{ts_max}"

        result = cli_runner.invoke(
            app,
            ["topic", "stat", handler_str],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert target_topic in result.output

    def test_stat_nonexistent_topic(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
    ):
        handler_str = "ghost-seq/ghost-topic,0,999999999"
        result = cli_runner.invoke(
            app,
            ["topic", "stat", handler_str],
            env=cli_env,
        )
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    def test_stat_invalid_format(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "stat", "bad-format-no-commas"],
            env=cli_env,
        )
        assert "Invalid" in result.output or "Error" in result.output

    def test_stat_multiple_topics(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handlers = [
            f"{UPLOADED_SEQUENCE_NAME}{topic},{ts_min},{ts_max}"
            for topic in topic_list[:2]
        ]

        result = cli_runner.invoke(
            app,
            ["topic", "stat"] + handlers,
            env=cli_env,
        )
        assert result.exit_code == 0
        for topic in topic_list[:2]:
            assert topic in result.output


class TestTopicMcat:
    def test_mcat_outputs_json_lines(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        target_topic = topic_list[0]
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handler_str = f"{UPLOADED_SEQUENCE_NAME}{target_topic},{ts_min},{ts_max}"

        result = cli_runner.invoke(
            app,
            ["topic", "mcat", handler_str, "--count", "5"],
            env=cli_env,
        )
        assert result.exit_code == 0
        lines = [line for line in result.output.strip().splitlines() if line.strip()]
        assert len(lines) <= 5
        for line in lines:
            payload = json.loads(line)
            assert "_timestamp" in payload
            assert "_topic" in payload
            assert "_ontology" in payload

    def test_mcat_from_index(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        target_topic = topic_list[0]
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handler_str = f"{UPLOADED_SEQUENCE_NAME}{target_topic},{ts_min},{ts_max}"

        result_all = cli_runner.invoke(
            app,
            ["topic", "mcat", handler_str, "--count", "5"],
            env=cli_env,
        )
        result_offset = cli_runner.invoke(
            app,
            ["topic", "mcat", handler_str, "--from-index", "2", "--count", "3"],
            env=cli_env,
        )
        assert result_all.exit_code == 0
        assert result_offset.exit_code == 0

        all_lines = [
            line for line in result_all.output.strip().splitlines() if line.strip()
        ]
        offset_lines = [
            line for line in result_offset.output.strip().splitlines() if line.strip()
        ]

        if len(all_lines) >= 5:
            first_offset_msg = json.loads(offset_lines[0])
            third_all_msg = json.loads(all_lines[2])
            assert first_offset_msg["_timestamp"] == third_all_msg["_timestamp"]

    def test_mcat_nonexistent_topic(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
    ):
        handler_str = "ghost-seq/ghost-topic,0,999999999"
        result = cli_runner.invoke(
            app,
            ["topic", "mcat", handler_str],
            env=cli_env,
        )
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    def test_mcat_invalid_handler_format(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
    ):
        result = cli_runner.invoke(
            app,
            ["topic", "mcat", "invalid-no-commas"],
            env=cli_env,
        )
        assert "Invalid" in result.output or "Error" in result.output

    def test_mcat_merge_overlapping_handlers(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        target_topic = topic_list[0]
        ts_start = synthetic_sequence_data_stream.tstamp_ns_start
        ts_end = synthetic_sequence_data_stream.tstamp_ns_end
        ts_mid = (ts_start + ts_end) // 2

        handler_1 = f"{UPLOADED_SEQUENCE_NAME}{target_topic},{ts_start},{ts_mid}"
        handler_2 = f"{UPLOADED_SEQUENCE_NAME}{target_topic},{ts_mid},{ts_end}"

        result = cli_runner.invoke(
            app,
            ["topic", "mcat", handler_1, handler_2, "--count", "10"],
            env=cli_env,
        )
        assert result.exit_code == 0
        lines = [line for line in result.output.strip().splitlines() if line.strip()]
        assert len(lines) > 0
        for line in lines:
            payload = json.loads(line)
            assert "_timestamp" in payload

    def test_mcat_all_messages_without_count(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        target_topic = topic_list[0]
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handler_str = f"{UPLOADED_SEQUENCE_NAME}{target_topic},{ts_min},{ts_max}"

        result = cli_runner.invoke(
            app,
            ["topic", "mcat", handler_str],
            env=cli_env,
        )
        assert result.exit_code == 0
        lines = [line for line in result.output.strip().splitlines() if line.strip()]
        assert len(lines) > 5

    def test_mcat_multiple_different_topics(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handler_1 = f"{UPLOADED_SEQUENCE_NAME}{topic_list[0]},{ts_min},{ts_max}"
        handler_2 = f"{UPLOADED_SEQUENCE_NAME}{topic_list[1]},{ts_min},{ts_max}"

        result = cli_runner.invoke(
            app,
            ["topic", "mcat", handler_1, handler_2, "--count", "10"],
            env=cli_env,
        )
        assert result.exit_code == 0
        lines = [line for line in result.output.strip().splitlines() if line.strip()]
        assert len(lines) > 0
        topics_seen = set()
        for line in lines:
            payload = json.loads(line)
            topics_seen.add(payload["_topic"])
        assert len(topics_seen) >= 1

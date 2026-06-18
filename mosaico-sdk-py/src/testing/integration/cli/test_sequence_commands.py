from typer.testing import CliRunner

from mosaicolabs_cli.main import app
from testing.integration.config import (
    QUERY_SEQUENCES_MOCKUP,
    UPLOADED_SEQUENCE_NAME,
)


class TestSequenceLs:
    def test_ls_returns_all_sequences(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        inject_mockup_sequences,
    ):
        result = cli_runner.invoke(app, ["sequence", "ls"], env=cli_env)
        assert result.exit_code == 0
        assert UPLOADED_SEQUENCE_NAME in result.output
        for seq_name in QUERY_SEQUENCES_MOCKUP.keys():
            assert seq_name in result.output

    def test_ls_with_locator_filter(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["sequence", "ls", "--locator", UPLOADED_SEQUENCE_NAME],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert UPLOADED_SEQUENCE_NAME in result.output

    def test_ls_with_locator_wildcard(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_mockup_sequences,
    ):
        result = cli_runner.invoke(
            app,
            ["sequence", "ls", "--locator", "test-query-*"],
            env=cli_env,
        )
        assert result.exit_code == 0
        for seq_name in QUERY_SEQUENCES_MOCKUP.keys():
            assert seq_name in result.output

    def test_ls_with_metadata_filter(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_mockup_sequences,
    ):
        result = cli_runner.invoke(
            app,
            ["sequence", "ls", "--metadata", "status=raw"],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "test-query-sequence-2" in result.output
        assert "test-query-sequence-1" not in result.output

    def test_ls_with_limit(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_mockup_sequences,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["sequence", "ls", "--limit", "2"],
            env=cli_env,
        )
        assert result.exit_code == 0
        lines_with_content = [
            line for line in result.output.strip().splitlines() if line.strip()
        ]
        seq_count = sum(
            1
            for line in lines_with_content
            if "test-query-sequence" in line or UPLOADED_SEQUENCE_NAME in line
        )
        assert seq_count <= 2

    def test_ls_csv_output(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["sequence", "ls", "--locator", UPLOADED_SEQUENCE_NAME, "--output", "csv"],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "," in result.output
        assert UPLOADED_SEQUENCE_NAME in result.output

    def test_ls_no_results(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
    ):
        result = cli_runner.invoke(
            app,
            ["sequence", "ls", "--locator", "nonexistent-sequence-xyz-999"],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "No sequences found" in result.output

    def test_ls_multiple_metadata_filters(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            [
                "sequence",
                "ls",
                "--metadata",
                "status=processed",
                "--metadata",
                "visibility=team-01",
            ],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert UPLOADED_SEQUENCE_NAME in result.output

    def test_ls_invalid_metadata_format(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            ["sequence", "ls", "--metadata", "no-equals-sign"],
            env=cli_env,
        )
        assert result.exit_code == 1

    def test_ls_created_after_filters(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            [
                "sequence",
                "ls",
                "--locator",
                UPLOADED_SEQUENCE_NAME,
                "--created-after",
                "0",
            ],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert UPLOADED_SEQUENCE_NAME in result.output

    def test_ls_created_after_far_future_excludes(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        year_2100_ns = str(4_102_444_800_000_000_000)
        result = cli_runner.invoke(
            app,
            [
                "sequence",
                "ls",
                "--locator",
                UPLOADED_SEQUENCE_NAME,
                "--created-after",
                year_2100_ns,
            ],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "No sequences found" in result.output

    def test_ls_created_before_epoch_excludes(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        result = cli_runner.invoke(
            app,
            [
                "sequence",
                "ls",
                "--locator",
                UPLOADED_SEQUENCE_NAME,
                "--created-before",
                "1",
            ],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert "No sequences found" in result.output

    def test_ls_created_before_far_future_includes(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
    ):
        far_future_ns = str(int(9e18))
        result = cli_runner.invoke(
            app,
            [
                "sequence",
                "ls",
                "--locator",
                UPLOADED_SEQUENCE_NAME,
                "--created-before",
                far_future_ns,
            ],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert UPLOADED_SEQUENCE_NAME in result.output


class TestSequenceStat:
    def test_stat_existing_sequence(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        synthetic_sequence_data_stream,
    ):
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handler_str = f"{UPLOADED_SEQUENCE_NAME},{ts_min},{ts_max}"

        result = cli_runner.invoke(
            app,
            ["sequence", "stat", handler_str],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert UPLOADED_SEQUENCE_NAME in result.output

    def test_stat_nonexistent_sequence(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
    ):
        handler_str = "ghost-sequence-xyz,0,999999999"
        result = cli_runner.invoke(
            app,
            ["sequence", "stat", handler_str],
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
            ["sequence", "stat", "bad-format-no-commas"],
            env=cli_env,
        )
        assert "Invalid" in result.output or "Error" in result.output

    def test_stat_multiple_sequences(
        self,
        cli_runner: CliRunner,
        cli_env: dict,
        inject_synthetic_sequence,
        inject_mockup_sequences,
        synthetic_sequence_data_stream,
    ):
        ts_min = str(synthetic_sequence_data_stream.tstamp_ns_start)
        ts_max = str(synthetic_sequence_data_stream.tstamp_ns_end)
        handler_1 = f"{UPLOADED_SEQUENCE_NAME},{ts_min},{ts_max}"
        mockup_name = list(QUERY_SEQUENCES_MOCKUP.keys())[0]
        handler_2 = f"{mockup_name},0,0"

        result = cli_runner.invoke(
            app,
            ["sequence", "stat", handler_1, handler_2],
            env=cli_env,
        )
        assert result.exit_code == 0
        assert UPLOADED_SEQUENCE_NAME in result.output

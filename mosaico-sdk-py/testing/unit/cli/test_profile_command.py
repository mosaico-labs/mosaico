import pytest
from typer.testing import CliRunner

from mosaicolabs_cli.main import app

runner = CliRunner()


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    config_path = tmp_path / "config.toml"
    monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
    return config_path


class TestProfileAdd:
    def test_add_profile_non_interactive(self, config_file):
        result = runner.invoke(
            app,
            [
                "profile",
                "add",
                "dev",
                "--no-interactive",
                "--host",
                "localhost",
                "--port",
                "6276",
                "--api-key",
                "test-key",
            ],
        )
        assert result.exit_code == 0
        assert "Success" in result.output
        assert config_file.exists()

        content = config_file.read_text()
        assert "localhost" in content
        assert "6276" in content

    def test_add_profile_sets_first_as_default(self, config_file):
        runner.invoke(
            app,
            [
                "profile",
                "add",
                "first",
                "--no-interactive",
                "--host",
                "host1.example.com",
            ],
        )
        content = config_file.read_text()
        assert "default" in content
        assert "true" in content

    def test_add_profile_explicit_default(self, config_file):
        runner.invoke(
            app,
            [
                "profile",
                "add",
                "first",
                "--no-interactive",
                "--host",
                "host1.example.com",
            ],
        )
        runner.invoke(
            app,
            [
                "profile",
                "add",
                "second",
                "--no-interactive",
                "--host",
                "host2.example.com",
                "--default",
            ],
        )
        content = config_file.read_text()
        lines = content.splitlines()

        first_section_idx = lines.index("[first]")
        second_section_idx = lines.index("[second]")

        first_default_line = next(
            line
            for line in lines[first_section_idx:second_section_idx]
            if "default" in line
        )
        second_default_line = next(
            line for line in lines[second_section_idx:] if "default" in line
        )

        assert "false" in first_default_line
        assert "true" in second_default_line

    def test_add_profile_fails_without_host_non_interactive(self, config_file):
        result = runner.invoke(
            app,
            ["profile", "add", "broken", "--no-interactive"],
        )
        assert result.exit_code == 1

    def test_add_profile_with_tls(self, config_file):
        result = runner.invoke(
            app,
            [
                "profile",
                "add",
                "secure",
                "--no-interactive",
                "--host",
                "secure.example.com",
                "--tls",
                "--cert-path",
                "/path/to/cert.pem",
            ],
        )
        assert result.exit_code == 0
        content = config_file.read_text()
        assert "tls = true" in content
        assert "/path/to/cert.pem" in content

    def test_add_profile_host_port_extraction(self, config_file):
        result = runner.invoke(
            app,
            [
                "profile",
                "add",
                "embedded",
                "--no-interactive",
                "--host",
                "myhost.example.com:9999",
            ],
        )
        assert result.exit_code == 0
        content = config_file.read_text()
        assert "myhost.example.com" in content
        assert "9999" in content


class TestProfileRemove:
    def _add_profile(self, name, host="localhost"):
        runner.invoke(
            app,
            [
                "profile",
                "add",
                name,
                "--no-interactive",
                "--host",
                host,
            ],
        )

    def test_remove_existing_profile(self, config_file):
        self._add_profile("to-delete")
        result = runner.invoke(
            app,
            ["profile", "remove", "to-delete", "--force"],
        )
        assert result.exit_code == 0
        assert "removed" in result.output.lower() or "Success" in result.output

    def test_remove_nonexistent_profile(self, config_file):
        result = runner.invoke(
            app,
            ["profile", "remove", "ghost", "--force"],
        )
        assert result.exit_code == 1

    def test_remove_default_promotes_next(self, config_file):
        self._add_profile("primary", "host1")
        self._add_profile("secondary", "host2")
        runner.invoke(
            app,
            ["profile", "remove", "primary", "--force"],
        )
        content = config_file.read_text()
        assert "primary" not in content.split("\n")[0] or "[primary]" not in content
        assert "true" in content


class TestProfileList:
    def test_list_empty(self, config_file):
        result = runner.invoke(app, ["profile", "ls"])
        assert result.exit_code == 0
        assert "No profiles" in result.output

    def test_list_with_profiles(self, config_file):
        runner.invoke(
            app,
            [
                "profile",
                "add",
                "myprofile",
                "--no-interactive",
                "--host",
                "example.com",
            ],
        )
        result = runner.invoke(app, ["profile", "ls"])
        assert result.exit_code == 0
        assert "myprofile" in result.output


class TestProfileDefault:
    def _add_profile(self, name, host="localhost"):
        runner.invoke(
            app,
            [
                "profile",
                "add",
                name,
                "--no-interactive",
                "--host",
                host,
            ],
        )

    def test_set_default(self, config_file):
        self._add_profile("alpha", "host1")
        self._add_profile("beta", "host2")
        result = runner.invoke(app, ["profile", "default", "beta"])
        assert result.exit_code == 0
        assert "beta" in result.output

    def test_set_default_nonexistent(self, config_file):
        result = runner.invoke(app, ["profile", "default", "nope"])
        assert result.exit_code == 1

    def test_set_default_already_default(self, config_file):
        self._add_profile("only")
        result = runner.invoke(app, ["profile", "default", "only"])
        assert result.exit_code == 0
        assert "already" in result.output.lower()

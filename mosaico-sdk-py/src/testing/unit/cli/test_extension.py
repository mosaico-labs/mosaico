import stat
from pathlib import Path

import pytest
from typer.testing import CliRunner

from mosaicolabs_cli.commands.extension import discover_extensions
from mosaicolabs_cli.main import app

runner = CliRunner()


@pytest.fixture
def fake_path(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    monkeypatch.setenv("PATH", str(bin_dir))
    return bin_dir


def _create_executable(directory: Path, name: str):
    exe = directory / name
    exe.write_text("#!/bin/sh\nexit 0\n")
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return exe


class TestDiscoverExtensions:
    def test_no_extensions(self, fake_path):
        result = discover_extensions()
        assert result == {}

    def test_finds_mosaico_prefixed(self, fake_path):
        _create_executable(fake_path, "mosaico-foo")
        _create_executable(fake_path, "mosaico-bar")
        result = discover_extensions()
        assert "foo" in result
        assert "bar" in result

    def test_ignores_non_prefixed(self, fake_path):
        _create_executable(fake_path, "other-tool")
        _create_executable(fake_path, "mosaico-valid")
        result = discover_extensions()
        assert "valid" in result
        assert "tool" not in result
        assert "other-tool" not in result

    def test_ignores_non_executable(self, fake_path):
        non_exec = fake_path / "mosaico-noexec"
        non_exec.write_text("#!/bin/sh\n")
        non_exec.chmod(0o644)
        result = discover_extensions()
        assert "noexec" not in result

    def test_multiple_path_dirs(self, tmp_path, monkeypatch):
        dir1 = tmp_path / "d1"
        dir2 = tmp_path / "d2"
        dir1.mkdir()
        dir2.mkdir()
        _create_executable(dir1, "mosaico-alpha")
        _create_executable(dir2, "mosaico-beta")
        monkeypatch.setenv("PATH", f"{dir1}:{dir2}")
        result = discover_extensions()
        assert "alpha" in result
        assert "beta" in result

    def test_first_in_path_wins(self, tmp_path, monkeypatch):
        dir1 = tmp_path / "d1"
        dir2 = tmp_path / "d2"
        dir1.mkdir()
        dir2.mkdir()
        exe1 = _create_executable(dir1, "mosaico-dup")
        _create_executable(dir2, "mosaico-dup")
        monkeypatch.setenv("PATH", f"{dir1}:{dir2}")
        result = discover_extensions()
        assert result["dup"] == str(exe1.absolute())

    def test_sorted_output(self, fake_path):
        _create_executable(fake_path, "mosaico-zebra")
        _create_executable(fake_path, "mosaico-alpha")
        _create_executable(fake_path, "mosaico-middle")
        result = discover_extensions()
        keys = list(result.keys())
        assert keys == sorted(keys)


class TestExtensionLsCommand:
    def test_ls_no_extensions(self, fake_path, monkeypatch, tmp_path):
        config = tmp_path / "cfg.toml"
        config.write_text('[dev]\nhost = "localhost"\ndefault = true\n')
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config))
        result = runner.invoke(app, ["extension", "ls"])
        assert result.exit_code == 0
        assert "No external extensions" in result.output

    def test_ls_with_extensions(self, fake_path, monkeypatch, tmp_path):
        config = tmp_path / "cfg.toml"
        config.write_text('[dev]\nhost = "localhost"\ndefault = true\n')
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config))
        _create_executable(fake_path, "mosaico-myext")
        result = runner.invoke(app, ["extension", "ls"])
        assert result.exit_code == 0
        assert "myext" in result.output

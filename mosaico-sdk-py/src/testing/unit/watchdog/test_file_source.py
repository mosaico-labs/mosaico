import os
import time
from pathlib import Path

import pytest

from mosaicolabs.watchdog.file_source import FileRef, LocalFileSource

EXTS = (".mcap", ".db3", ".bag")


def touch(root: Path, *rel_paths: str):
    for rel in rel_paths:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()


def listed(source: LocalFileSource):
    return sorted(f.relative_path for f in source.list_files())


class TestFileRef:
    def test_equality_ignores_uri_size_mtime(self):
        a = FileRef("run/a.mcap", "file:///x/run/a.mcap", 1, 1)
        b = FileRef("run/a.mcap", "file:///y/run/a.mcap", 2, 2)
        assert a == b
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_different_relative_path_differs(self):
        assert FileRef("a.mcap", "u", 0, 0) != FileRef("b.mcap", "u", 0, 0)

    @pytest.mark.parametrize(
        "rel, name",
        [("a.mcap", "a.mcap"), ("run_01/drive.mcap", "drive.mcap"), ("", "")],
    )
    def test_name(self, rel, name):
        assert FileRef(rel, "u", 0, 0).name == name


class TestLocalFileSource:
    def test_missing_folder_raises(self, tmp_path):
        with pytest.raises(ValueError):
            LocalFileSource(tmp_path / "missing", EXTS)

    def test_file_instead_of_folder_raises(self, tmp_path):
        touch(tmp_path, "a.mcap")
        with pytest.raises(ValueError):
            LocalFileSource(tmp_path / "a.mcap", EXTS)

    def test_lists_supported_files_recursively(self, tmp_path):
        touch(
            tmp_path,
            "a.mcap",
            "b.db3",
            "c.bag",
            "sub/d.mcap",
            "sub/deep/e.bag",
            "notes.txt",
            ".already_loaded.txt",
            "f.mcap.tmp",
        )
        (tmp_path / "dir.mcap").mkdir()  # a folder with a supported name is skipped

        assert listed(LocalFileSource(tmp_path, EXTS)) == [
            "a.mcap",
            "b.db3",
            "c.bag",
            "sub/d.mcap",
            "sub/deep/e.bag",
        ]

    def test_extension_match_is_case_sensitive(self, tmp_path):
        touch(tmp_path, "upper.MCAP", "lower.mcap")
        assert listed(LocalFileSource(tmp_path, EXTS)) == ["lower.mcap"]

    @pytest.mark.parametrize("ext", ["mcap", ".mcap", "MCAP", ".MCAP"])
    def test_accepted_extensions_are_normalized(self, tmp_path, ext):
        touch(tmp_path, "a.mcap", "b.bag")
        assert listed(LocalFileSource(tmp_path, (ext,))) == ["a.mcap"]

    def test_glob_pattern_matches_at_any_depth(self, tmp_path):
        touch(
            tmp_path,
            "run_01/x.mcap",
            "a/b/run_02/y.mcap",
            "run_01/z.bag",
            "other/w.mcap",
            "top.mcap",
        )
        source = LocalFileSource(tmp_path, EXTS, glob_pattern="run_*/*.mcap")
        assert listed(source) == ["a/b/run_02/y.mcap", "run_01/x.mcap"]

    def test_min_file_age_skips_recent_files(self, tmp_path):
        touch(tmp_path, "fresh.mcap", "old.mcap")
        past = time.time() - 120
        os.utime(tmp_path / "old.mcap", (past, past))

        source = LocalFileSource(tmp_path, EXTS, min_file_age_s=60)
        assert listed(source) == ["old.mcap"]

    @pytest.mark.parametrize("age", [None, 0])
    def test_min_file_age_disabled(self, tmp_path, age):
        touch(tmp_path, "fresh.mcap")
        assert listed(LocalFileSource(tmp_path, EXTS, min_file_age_s=age)) == [
            "fresh.mcap"
        ]

    def test_fileref_fields(self, tmp_path):
        (tmp_path / "sub").mkdir()
        f = tmp_path / "sub" / "a.mcap"
        f.write_bytes(b"12345")

        (ref,) = LocalFileSource(tmp_path, EXTS).list_files()
        assert ref.relative_path == "sub/a.mcap"
        assert ref.uri == f.resolve().as_uri()
        assert ref.size == 5
        assert ref.mtime_ns == f.stat().st_mtime_ns

    def test_list_files_is_a_fresh_scan_each_call(self, tmp_path):
        source = LocalFileSource(tmp_path, EXTS)
        assert listed(source) == []
        touch(tmp_path, "a.mcap")
        assert listed(source) == ["a.mcap"]

    def test_get_local_path(self, tmp_path):
        touch(tmp_path, "sub/a.mcap")
        source = LocalFileSource(tmp_path, EXTS)
        (ref,) = source.list_files()
        assert source.get_local_path(ref) == (tmp_path / "sub" / "a.mcap").resolve()

    def test_path_is_resolved_once(self, tmp_path, monkeypatch):
        touch(tmp_path, "watched/a.mcap")
        monkeypatch.chdir(tmp_path)
        source = LocalFileSource(Path("watched"), EXTS)

        monkeypatch.chdir(tmp_path.parent)  # the working directory changes later
        (ref,) = source.list_files()
        assert source.get_local_path(ref) == (tmp_path / "watched" / "a.mcap").resolve()

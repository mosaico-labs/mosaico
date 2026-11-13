import threading

import pytest

from mosaicolabs.watchdog.file_source import FileRef
from mosaicolabs.watchdog.file_state_store import (
    ALREADY_LOADED_FILE_NAME,
    QUARANTINED_FILE_NAME,
    FileState,
    FileStateStore,
)


def ref(rel_path: str) -> FileRef:
    return FileRef(rel_path, f"file:///x/{rel_path}", 0, 0)


@pytest.fixture
def loaded_file(tmp_path):
    return tmp_path / ALREADY_LOADED_FILE_NAME


@pytest.fixture
def quarantine_file(tmp_path):
    return tmp_path / QUARANTINED_FILE_NAME


class TestInit:
    def test_missing_folder_raises(self, tmp_path):
        with pytest.raises(ValueError):
            FileStateStore(tmp_path / "missing")

    def test_file_instead_of_folder_raises(self, tmp_path):
        (tmp_path / "f").touch()
        with pytest.raises(ValueError):
            FileStateStore(tmp_path / "f")

    def test_empty_folder(self, tmp_path, loaded_file, quarantine_file):
        store = FileStateStore(tmp_path)
        assert store.get_state(ref("a.mcap")) is None
        # Nothing is written until a file is marked
        assert not loaded_file.exists()
        assert not quarantine_file.exists()

    def test_reads_existing_state_files(self, tmp_path, loaded_file, quarantine_file):
        loaded_file.write_text("a.mcap\nsub/b.bag\n", encoding="utf-8")
        quarantine_file.write_text("c.db3\n", encoding="utf-8")

        store = FileStateStore(tmp_path)
        assert store.get_state(ref("a.mcap")) is FileState.LOADED
        assert store.get_state(ref("sub/b.bag")) is FileState.LOADED
        assert store.get_state(ref("c.db3")) is FileState.QUARANTINED
        assert store.get_state(ref("d.mcap")) is None

    def test_line_parsing(self, tmp_path, loaded_file):
        # CRLF endings, blank lines, missing final newline, spaces kept, unicode
        loaded_file.write_bytes(
            "a.mcap\r\n\n   \nmy file .mcap\nélan.bag".encode("utf-8")
        )
        store = FileStateStore(tmp_path)
        assert store.get_state(ref("a.mcap")) is FileState.LOADED
        assert store.get_state(ref("my file .mcap")) is FileState.LOADED
        assert store.get_state(ref("élan.bag")) is FileState.LOADED
        assert store.get_state(ref("   ")) is None
        assert store.get_state(ref("")) is None

    def test_quarantined_wins_over_loaded(self, tmp_path, loaded_file, quarantine_file):
        loaded_file.write_text("a.mcap\n", encoding="utf-8")
        quarantine_file.write_text("a.mcap\n", encoding="utf-8")
        assert (
            FileStateStore(tmp_path).get_state(ref("a.mcap")) is FileState.QUARANTINED
        )

    def test_invalid_utf8_raises(self, tmp_path, loaded_file):
        loaded_file.write_bytes(b"\xff\xfe\xfa")
        with pytest.raises(RuntimeError):
            FileStateStore(tmp_path)

    def test_unreadable_state_file_raises(self, tmp_path, quarantine_file):
        quarantine_file.mkdir()  # exists but cannot be opened as a file
        with pytest.raises(RuntimeError):
            FileStateStore(tmp_path)


class TestMark:
    def test_mark_loaded(self, tmp_path, loaded_file):
        store = FileStateStore(tmp_path)
        store.mark_loaded(ref("a.mcap"))
        store.mark_loaded(ref("sub/b.bag"))

        assert store.get_state(ref("a.mcap")) is FileState.LOADED
        assert loaded_file.read_text(encoding="utf-8") == "a.mcap\nsub/b.bag\n"

    def test_mark_quarantined(self, tmp_path, quarantine_file):
        store = FileStateStore(tmp_path)
        store.mark_quarantined(ref("a.mcap"))

        assert store.get_state(ref("a.mcap")) is FileState.QUARANTINED
        assert quarantine_file.read_text(encoding="utf-8") == "a.mcap\n"

    def test_mark_appends_to_existing_file(self, tmp_path, loaded_file):
        loaded_file.write_text("old.mcap\n", encoding="utf-8")
        FileStateStore(tmp_path).mark_loaded(ref("new.mcap"))
        assert loaded_file.read_text(encoding="utf-8") == "old.mcap\nnew.mcap\n"

    def test_mark_in_queue_is_memory_only(self, tmp_path, loaded_file, quarantine_file):
        store = FileStateStore(tmp_path)
        store.mark_in_queue(ref("a.mcap"))

        assert store.get_state(ref("a.mcap")) is FileState.IN_QUEUE
        assert not loaded_file.exists()
        assert not quarantine_file.exists()
        # A new store (i.e. a Watchdog restart) forgets it
        assert FileStateStore(tmp_path).get_state(ref("a.mcap")) is None

    @pytest.mark.parametrize(
        "mark", ["mark_loaded", "mark_quarantined"], ids=["loaded", "quarantined"]
    )
    def test_mark_replaces_in_queue(self, tmp_path, mark):
        store = FileStateStore(tmp_path)
        store.mark_in_queue(ref("a.mcap"))
        getattr(store, mark)(ref("a.mcap"))
        assert store.get_state(ref("a.mcap")) in (
            FileState.LOADED,
            FileState.QUARANTINED,
        )

    @pytest.mark.parametrize("bad", ["", "   ", "a\nb.mcap", "a.mcap\n", "a\r.mcap"])
    @pytest.mark.parametrize(
        "mark", ["mark_loaded", "mark_quarantined", "mark_in_queue"]
    )
    def test_invalid_path_rejected(self, tmp_path, bad, mark):
        store = FileStateStore(tmp_path)
        with pytest.raises(RuntimeError):
            getattr(store, mark)(ref(bad))

        assert store.get_state(ref(bad)) is None
        for name in (ALREADY_LOADED_FILE_NAME, QUARANTINED_FILE_NAME):
            p = tmp_path / name
            assert not p.exists() or p.read_text(encoding="utf-8") == ""

    def test_concurrent_marks(self, tmp_path, loaded_file):
        store = FileStateStore(tmp_path)
        n_threads, per_thread = 8, 50

        def worker(t):
            for i in range(per_thread):
                r = ref(f"t{t}/f{i}.mcap")
                store.mark_in_queue(r)
                store.mark_loaded(r)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        lines = loaded_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == n_threads * per_thread
        assert len(set(lines)) == n_threads * per_thread


class TestRefresh:
    def test_picks_up_manual_edits(self, tmp_path, loaded_file, quarantine_file):
        store = FileStateStore(tmp_path)
        loaded_file.write_text("a.mcap\n", encoding="utf-8")
        quarantine_file.write_text("b.mcap\n", encoding="utf-8")

        store.refresh()
        assert store.get_state(ref("a.mcap")) is FileState.LOADED
        assert store.get_state(ref("b.mcap")) is FileState.QUARANTINED

    def test_removed_quarantine_line_makes_file_new(self, tmp_path, quarantine_file):
        store = FileStateStore(tmp_path)
        store.mark_quarantined(ref("a.mcap"))

        quarantine_file.write_text("", encoding="utf-8")
        store.refresh()
        assert store.get_state(ref("a.mcap")) is None

    def test_deleted_state_file_counts_as_empty(self, tmp_path, loaded_file):
        store = FileStateStore(tmp_path)
        store.mark_loaded(ref("a.mcap"))

        loaded_file.unlink()
        store.refresh()
        assert store.get_state(ref("a.mcap")) is None

    def test_keeps_in_queue(self, tmp_path):
        store = FileStateStore(tmp_path)
        store.mark_in_queue(ref("a.mcap"))
        store.refresh()
        assert store.get_state(ref("a.mcap")) is FileState.IN_QUEUE

    def test_state_files_win_over_in_queue(
        self, tmp_path, loaded_file, quarantine_file
    ):
        store = FileStateStore(tmp_path)
        store.mark_in_queue(ref("a.mcap"))
        store.mark_in_queue(ref("b.mcap"))
        loaded_file.write_text("a.mcap\n", encoding="utf-8")
        quarantine_file.write_text("b.mcap\n", encoding="utf-8")

        store.refresh()
        assert store.get_state(ref("a.mcap")) is FileState.LOADED
        assert store.get_state(ref("b.mcap")) is FileState.QUARANTINED

    def test_unreadable_file_keeps_previous_state(
        self, tmp_path, loaded_file, quarantine_file
    ):
        store = FileStateStore(tmp_path)
        store.mark_loaded(ref("a.mcap"))
        store.mark_in_queue(ref("q.mcap"))

        # loaded is readable and changed, quarantine is broken: nothing is applied
        loaded_file.write_text("a.mcap\nb.mcap\n", encoding="utf-8")
        quarantine_file.write_bytes(b"\xff\xfe")

        store.refresh()  # does not raise
        assert store.get_state(ref("a.mcap")) is FileState.LOADED
        assert store.get_state(ref("b.mcap")) is None
        assert store.get_state(ref("q.mcap")) is FileState.IN_QUEUE

"""
Unit tests of the Watchdog. No server and no real bag: the injector is replaced by a fake
(`instantiate_injector` and `InjectorDispatcher.get_injector` are patched), and the watched
folder is a `tmp_path` holding empty files, since `LocalFileSource` only looks at names
and modification times.
"""

import sys
import threading
from pathlib import Path
from typing import List

import pytest

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.watchdog import watchdog as wd_mod
from mosaicolabs.watchdog.dispatcher import InjectorDispatcher
from mosaicolabs.watchdog.file_state_store import (
    ALREADY_LOADED_FILE_NAME,
    QUARANTINED_FILE_NAME,
    FileState,
)
from mosaicolabs.watchdog.watchdog import (
    Watchdog,
    WatchdogConfig,
    WatchdogError,
    WatchdogMode,
)

RUN_TIMEOUT_S = 10.0
"""Upper bound for a daemon run in a test: a hang fails the test instead of blocking."""


class FakeInjectorClass:
    """Stands for the injector class returned by the dispatcher."""


class FakeInjector:
    """
    Records each injection. `outcomes` is consumed one item per `run()` call: an
    `InjectionStatus` is returned, an exception is raised. When exhausted, `run()` returns
    `COMPLETED`. `on_run` is called with the injector kwargs before each run.
    """

    def __init__(self):
        self.outcomes: List = []
        self.calls: List[dict] = []
        self.on_run = None

    def factory(self, **kwargs):
        injector = self

        class _Instance:
            def run(self_inner):
                injector.calls.append(kwargs)
                if injector.on_run:
                    injector.on_run(kwargs)
                if injector.outcomes:
                    outcome = injector.outcomes.pop(0)
                    if isinstance(outcome, BaseException):
                        raise outcome
                    return outcome
                return InjectionStatus.COMPLETED

        return _Instance()

    @property
    def call_count(self) -> int:
        return len(self.calls)

    @property
    def injected(self) -> List[str]:
        """Sequence names, in injection order."""
        return [c["sequence_name"] for c in self.calls]


@pytest.fixture
def injector(monkeypatch):
    fake = FakeInjector()
    monkeypatch.setattr(
        InjectorDispatcher,
        "get_injector",
        classmethod(lambda cls, path: FakeInjectorClass),
    )
    monkeypatch.setattr(wd_mod, "instantiate_injector", fake.factory)
    return fake


def touch(root: Path, *rel_paths: str):
    for rel in rel_paths:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()


def make_watchdog(root: Path, files=("a.mcap",), **cfg) -> Watchdog:
    touch(root, *files)
    return Watchdog(WatchdogConfig(path_to_monitor=root, **cfg))


def state_of(wd: Watchdog, rel_path: str):
    return wd.file_store_state._file_cache.get(rel_path)


def lines(path: Path) -> List[str]:
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def queued(wd: Watchdog) -> List[str]:
    return sorted(f.relative_path for f in list(wd._files_queue.queue))


def run_in_thread(wd: Watchdog, timeout: float = RUN_TIMEOUT_S):
    """Runs `wd.run()` in a thread and returns the exception it raised, if any. Fails the
    test if `run()` does not return within `timeout`."""
    result = {}

    def target():
        try:
            wd.run()
        except BaseException as e:  # KeyboardInterrupt included
            result["exc"] = e

    th = threading.Thread(target=target, daemon=True)
    th.start()
    th.join(timeout)
    if th.is_alive():
        wd._stop_event.set()
        th.join(2)
        pytest.fail(f"Watchdog.run() did not return within {timeout}s")
    return result.get("exc")


class FakeClock:
    """Replaces the `time` module in `watchdog.py`: `monotonic()` only moves when the test
    calls `advance()`."""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


class FakeStopEvent:
    """Replaces the Watchdog's stop event: `wait()` returns at once, recording its timeout,
    and the event is set after `stop_after_waits` waits."""

    def __init__(self, stop_after_waits: int):
        self._set = False
        self._stop_after_waits = stop_after_waits
        self.waits: List[float] = []

    def is_set(self) -> bool:
        return self._set

    def set(self):
        self._set = True

    def clear(self):
        self._set = False

    def wait(self, timeout=None) -> bool:
        self.waits.append(timeout)
        if len(self.waits) >= self._stop_after_waits:
            self._set = True
        return self._set


def stop_after(wd: Watchdog, fake: FakeInjector, n: int):
    """Makes the fake injector stop the Watchdog after `n` injections."""

    def on_run(_kwargs):
        if fake.call_count >= n:
            wd._stop_event.set()

    fake.on_run = on_run


# --- Config and constructor


class TestConfigAndInit:
    def test_defaults(self, tmp_path):
        cfg = WatchdogConfig(path_to_monitor=tmp_path)
        assert cfg.mode is WatchdogMode.SINGLEPASS
        assert cfg.on_error is WatchdogError.QUARANTINE
        assert cfg.min_file_age_s is None
        assert not cfg.is_deamon_mode()

    def test_is_daemon_mode(self, tmp_path):
        cfg = WatchdogConfig(path_to_monitor=tmp_path, mode=WatchdogMode.DAEMON)
        assert cfg.is_deamon_mode()

    def test_missing_folder_raises(self, tmp_path):
        with pytest.raises(ValueError):
            Watchdog(WatchdogConfig(path_to_monitor=tmp_path / "missing"))

    def test_file_instead_of_folder_raises(self, tmp_path):
        touch(tmp_path, "a.mcap")
        with pytest.raises(ValueError):
            Watchdog(WatchdogConfig(path_to_monitor=tmp_path / "a.mcap"))

    def test_unreadable_state_file_raises(self, tmp_path):
        (tmp_path / ALREADY_LOADED_FILE_NAME).write_bytes(b"\xff\xfe")
        with pytest.raises(RuntimeError):
            Watchdog(WatchdogConfig(path_to_monitor=tmp_path))

    def test_queue_unbounded_in_single_pass(self, tmp_path):
        wd = Watchdog(WatchdogConfig(path_to_monitor=tmp_path, max_queue_size=3))
        assert wd._files_queue.maxsize == 0

    def test_queue_bounded_in_daemon(self, tmp_path):
        wd = Watchdog(
            WatchdogConfig(
                path_to_monitor=tmp_path, mode=WatchdogMode.DAEMON, max_queue_size=3
            )
        )
        assert wd._files_queue.maxsize == 3


# --- Producer: _scan_folder


class TestScanFolder:
    def test_queues_new_files_and_marks_them(self, tmp_path):
        wd = make_watchdog(tmp_path, ["a.mcap", "sub/b.bag", "c.db3", "notes.txt"])
        wd._scan_folder()

        assert queued(wd) == ["a.mcap", "c.db3", "sub/b.bag"]
        for rel in ("a.mcap", "sub/b.bag", "c.db3"):
            assert state_of(wd, rel) is FileState.IN_QUEUE

    def test_state_files_are_never_queued(self, tmp_path):
        wd = make_watchdog(tmp_path, ["a.mcap"])
        (tmp_path / ALREADY_LOADED_FILE_NAME).write_text("", encoding="utf-8")
        (tmp_path / QUARANTINED_FILE_NAME).write_text("", encoding="utf-8")
        wd._scan_folder()
        assert queued(wd) == ["a.mcap"]

    def test_skips_loaded_and_quarantined(self, tmp_path):
        (tmp_path / ALREADY_LOADED_FILE_NAME).write_text("a.mcap\n", encoding="utf-8")
        (tmp_path / QUARANTINED_FILE_NAME).write_text("b.mcap\n", encoding="utf-8")
        wd = make_watchdog(tmp_path, ["a.mcap", "b.mcap", "c.mcap"])

        wd._scan_folder()
        assert queued(wd) == ["c.mcap"]

    def test_picks_up_state_files_edited_after_start(self, tmp_path):
        wd = make_watchdog(tmp_path, ["a.mcap", "b.mcap"])
        (tmp_path / ALREADY_LOADED_FILE_NAME).write_text("a.mcap\n", encoding="utf-8")

        wd._scan_folder()  # refresh() runs first
        assert queued(wd) == ["b.mcap"]

    def test_second_scan_does_not_requeue(self, tmp_path):
        wd = make_watchdog(tmp_path, ["a.mcap", "b.mcap"])
        wd._scan_folder()
        wd._scan_folder()
        assert queued(wd) == ["a.mcap", "b.mcap"]

    def test_new_file_between_scans(self, tmp_path):
        wd = make_watchdog(tmp_path, ["a.mcap"])
        wd._scan_folder()
        touch(tmp_path, "b.mcap")
        wd._scan_folder()
        assert queued(wd) == ["a.mcap", "b.mcap"]

    def test_unquarantined_file_is_queued_again(self, tmp_path):
        quarantine = tmp_path / QUARANTINED_FILE_NAME
        quarantine.write_text("a.mcap\n", encoding="utf-8")
        wd = make_watchdog(tmp_path, ["a.mcap"])

        wd._scan_folder()
        assert queued(wd) == []

        quarantine.write_text("", encoding="utf-8")  # the user removes the line
        wd._scan_folder()
        assert queued(wd) == ["a.mcap"]

    def test_full_queue_leaves_the_rest_for_next_scan(self, tmp_path):
        files = [f"f{i}.mcap" for i in range(5)]
        wd = make_watchdog(tmp_path, files, mode=WatchdogMode.DAEMON, max_queue_size=2)

        wd._scan_folder()
        first = queued(wd)
        assert len(first) == 2
        # The others are not marked, so they are not lost
        assert [state_of(wd, f) for f in files].count(None) == 3

        # Once the consumer drained the queue, the next scan queues the next ones
        while not wd._files_queue.empty():
            wd._files_queue.get_nowait()
        wd._scan_folder()
        second = queued(wd)
        assert len(second) == 2
        assert not set(first) & set(second)

    def test_stop_event_stops_the_scan(self, tmp_path):
        wd = make_watchdog(tmp_path, ["a.mcap", "b.mcap"])
        wd._stop_event.set()
        wd._scan_folder()
        assert queued(wd) == []

    def test_file_that_cannot_be_marked_is_skipped(self, tmp_path, monkeypatch):
        wd = make_watchdog(tmp_path, ["a.mcap", "bad.mcap", "c.mcap"])
        original = wd.file_store_state.mark_in_queue

        def mark_in_queue(ref):
            if ref.relative_path == "bad.mcap":
                raise RuntimeError("invalid path")
            original(ref)

        monkeypatch.setattr(wd.file_store_state, "mark_in_queue", mark_in_queue)
        wd._scan_folder()
        assert queued(wd) == ["a.mcap", "c.mcap"]

    def test_glob_and_min_age_from_config(self, tmp_path):
        wd = make_watchdog(
            tmp_path,
            ["run_1/a.mcap", "other/b.mcap"],
            glob_pattern="run_*/*.mcap",
            min_file_age_s=3600,  # every file was just created
        )
        wd._scan_folder()
        assert queued(wd) == []

        wd = make_watchdog(tmp_path, [], glob_pattern="run_*/*.mcap")
        wd._scan_folder()
        assert queued(wd) == ["run_1/a.mcap"]


# --- Consumer: _inject_file and the error policies


def scan_and_pop(wd: Watchdog):
    wd._scan_folder()
    return wd._files_queue.get_nowait()


class TestInjectFile:
    def test_success_marks_loaded(self, tmp_path, injector):
        wd = make_watchdog(tmp_path, ["sub/a.mcap"])
        wd._inject_file(scan_and_pop(wd))

        assert injector.call_count == 1
        assert state_of(wd, "sub/a.mcap") is FileState.LOADED
        assert lines(tmp_path / ALREADY_LOADED_FILE_NAME) == ["sub/a.mcap"]
        assert lines(tmp_path / QUARANTINED_FILE_NAME) == []

    def test_injector_receives_config(self, tmp_path, injector):
        wd = make_watchdog(
            tmp_path,
            ["run_01/drive.mcap"],
            host="mosaico.local",
            port=1234,
            mosaico_api_key="key",
            tls_cert_path="/certs/ca.pem",
            enable_tls=True,
            log_level="DEBUG",
        )
        wd._inject_file(scan_and_pop(wd))

        (kwargs,) = injector.calls
        assert kwargs == dict(
            path=(tmp_path / "run_01" / "drive.mcap").resolve(),
            sequence_name="run_01-drive__mcap",
            host="mosaico.local",
            port=1234,
            mosaico_api_key="key",
            tls_cert_path="/certs/ca.pem",
            enable_tls=True,
            log_level="DEBUG",
            injector_cls=FakeInjectorClass,
        )

    @pytest.mark.parametrize(
        "status", [InjectionStatus.COMPLETED, InjectionStatus.DRY_RUN]
    )
    def test_non_cancelled_status_is_success(self, tmp_path, injector, status):
        injector.outcomes = [status]
        wd = make_watchdog(tmp_path)
        wd._inject_file(scan_and_pop(wd))
        assert state_of(wd, "a.mcap") is FileState.LOADED

    def test_quarantine_policy(self, tmp_path, injector):
        injector.outcomes = [RuntimeError("boom")]
        wd = make_watchdog(tmp_path, on_error=WatchdogError.QUARANTINE)
        wd._inject_file(scan_and_pop(wd))

        assert injector.call_count == 1
        assert state_of(wd, "a.mcap") is FileState.QUARANTINED
        assert lines(tmp_path / QUARANTINED_FILE_NAME) == ["a.mcap"]
        assert lines(tmp_path / ALREADY_LOADED_FILE_NAME) == []

    @pytest.mark.parametrize("retry_number", [0, 1, 3])
    def test_retry_policy_exhausted(self, tmp_path, injector, retry_number):
        injector.outcomes = [RuntimeError("boom")] * (retry_number + 1)
        wd = make_watchdog(
            tmp_path, on_error=WatchdogError.RETRY, retry_number=retry_number
        )
        wd._inject_file(scan_and_pop(wd))

        assert injector.call_count == retry_number + 1
        assert state_of(wd, "a.mcap") is FileState.QUARANTINED
        assert lines(tmp_path / QUARANTINED_FILE_NAME) == ["a.mcap"]

    def test_retry_policy_recovers(self, tmp_path, injector):
        injector.outcomes = [RuntimeError("1"), RuntimeError("2")]
        wd = make_watchdog(tmp_path, on_error=WatchdogError.RETRY, retry_number=2)
        wd._inject_file(scan_and_pop(wd))

        assert injector.call_count == 3
        assert state_of(wd, "a.mcap") is FileState.LOADED
        assert lines(tmp_path / QUARANTINED_FILE_NAME) == []

    def test_retry_number_ignored_without_retry_policy(self, tmp_path, injector):
        injector.outcomes = [RuntimeError("boom")] * 5
        wd = make_watchdog(tmp_path, on_error=WatchdogError.QUARANTINE, retry_number=4)
        wd._inject_file(scan_and_pop(wd))
        assert injector.call_count == 1

    def test_raise_policy(self, tmp_path, injector):
        injector.outcomes = [ValueError("boom")]
        wd = make_watchdog(tmp_path, on_error=WatchdogError.RAISE)
        ref = scan_and_pop(wd)

        with pytest.raises(ValueError, match="boom"):
            wd._inject_file(ref)

        assert injector.call_count == 1
        assert state_of(wd, "a.mcap") is FileState.IN_QUEUE
        assert lines(tmp_path / ALREADY_LOADED_FILE_NAME) == []
        assert lines(tmp_path / QUARANTINED_FILE_NAME) == []

    @pytest.mark.parametrize("policy", list(WatchdogError))
    def test_cancelled_raises_keyboard_interrupt(self, tmp_path, injector, policy):
        injector.outcomes = [InjectionStatus.CANCELLED]
        wd = make_watchdog(tmp_path, on_error=policy, retry_number=3)
        ref = scan_and_pop(wd)

        with pytest.raises(KeyboardInterrupt):
            wd._inject_file(ref)

        assert injector.call_count == 1  # never retried
        assert state_of(wd, "a.mcap") is FileState.IN_QUEUE
        assert lines(tmp_path / ALREADY_LOADED_FILE_NAME) == []
        assert lines(tmp_path / QUARANTINED_FILE_NAME) == []

    def test_keyboard_interrupt_from_injector_is_not_retried(self, tmp_path, injector):
        injector.outcomes = [KeyboardInterrupt()]
        wd = make_watchdog(tmp_path, on_error=WatchdogError.RETRY, retry_number=3)
        with pytest.raises(KeyboardInterrupt):
            wd._inject_file(scan_and_pop(wd))
        assert injector.call_count == 1

    def test_dispatcher_error_follows_policy(self, tmp_path, injector, monkeypatch):
        calls = []

        def get_injector(cls, path):
            calls.append(path)
            raise RuntimeError("no summary")

        monkeypatch.setattr(
            InjectorDispatcher, "get_injector", classmethod(get_injector)
        )
        wd = make_watchdog(tmp_path, on_error=WatchdogError.RETRY, retry_number=1)
        wd._inject_file(scan_and_pop(wd))

        assert len(calls) == 2
        assert injector.call_count == 0
        assert state_of(wd, "a.mcap") is FileState.QUARANTINED

    @pytest.mark.parametrize("policy", list(WatchdogError))
    def test_failed_mark_loaded_does_not_reinject(
        self, tmp_path, injector, monkeypatch, policy
    ):
        """An error recording the success is not an injection failure: the file is on the
        server, so it must be neither injected again nor quarantined."""
        wd = make_watchdog(tmp_path, on_error=policy, retry_number=2)
        ref = scan_and_pop(wd)

        def mark_loaded(_ref):
            raise OSError("disk full")

        monkeypatch.setattr(wd.file_store_state, "mark_loaded", mark_loaded)
        with pytest.raises(OSError, match="disk full"):
            wd._inject_file(ref)

        assert injector.call_count == 1
        assert state_of(wd, "a.mcap") is FileState.IN_QUEUE
        assert lines(tmp_path / QUARANTINED_FILE_NAME) == []

    def test_failed_mark_loaded_stops_the_run(self, tmp_path, injector, monkeypatch):
        wd = make_watchdog(tmp_path, ["a.mcap", "b.mcap"])

        def mark_loaded(_ref):
            raise OSError("disk full")

        monkeypatch.setattr(wd.file_store_state, "mark_loaded", mark_loaded)
        exc = run_in_thread(wd)

        assert isinstance(exc, OSError)
        assert injector.call_count == 1


# --- run(): single-pass


class TestRunSinglePass:
    def test_injects_every_new_file_once(self, tmp_path, injector):
        files = ["a.mcap", "sub/b.bag", "c.db3"]
        wd = make_watchdog(tmp_path, files)
        assert run_in_thread(wd) is None

        assert sorted(injector.injected) == ["a__mcap", "c__db3", "sub-b__bag"]
        assert sorted(lines(tmp_path / ALREADY_LOADED_FILE_NAME)) == sorted(files)
        assert wd._files_queue.empty()
        assert wd._producer_th is None

    def test_empty_folder_returns(self, tmp_path, injector):
        wd = make_watchdog(tmp_path, [])
        assert run_in_thread(wd) is None
        assert injector.call_count == 0

    def test_second_run_skips_loaded_files(self, tmp_path, injector):
        make_watchdog(tmp_path, ["a.mcap", "b.mcap"]).run()
        assert injector.call_count == 2

        # A new Watchdog (i.e. a restart) reads the state files
        wd = make_watchdog(tmp_path, ["c.mcap"])
        wd.run()
        assert injector.injected[2:] == ["c__mcap"]

    def test_quarantined_files_are_not_retried_on_next_run(self, tmp_path, injector):
        injector.outcomes = [RuntimeError("boom")]
        make_watchdog(tmp_path, ["a.mcap"]).run()
        make_watchdog(tmp_path, []).run()
        assert injector.call_count == 1

    def test_one_failure_does_not_stop_the_others(self, tmp_path, injector):
        injector.outcomes = [RuntimeError("boom")]
        wd = make_watchdog(tmp_path, ["a.mcap", "b.mcap", "c.mcap"])
        wd.run()

        assert injector.call_count == 3
        assert len(lines(tmp_path / QUARANTINED_FILE_NAME)) == 1
        assert len(lines(tmp_path / ALREADY_LOADED_FILE_NAME)) == 2

    def test_raise_policy_stops_the_run(self, tmp_path, injector):
        injector.outcomes = [ValueError("boom")]
        wd = make_watchdog(
            tmp_path, ["a.mcap", "b.mcap", "c.mcap"], on_error=WatchdogError.RAISE
        )
        exc = run_in_thread(wd)

        assert isinstance(exc, ValueError)
        assert injector.call_count == 1
        assert lines(tmp_path / ALREADY_LOADED_FILE_NAME) == []

    def test_cancel_stops_the_run(self, tmp_path, injector):
        injector.outcomes = [InjectionStatus.CANCELLED]
        wd = make_watchdog(tmp_path, ["a.mcap", "b.mcap"])
        exc = run_in_thread(wd)

        assert isinstance(exc, KeyboardInterrupt)
        assert injector.call_count == 1
        # Not recorded: the next run tries both again
        make_watchdog(tmp_path, []).run()
        assert injector.call_count == 3

    def test_run_clears_a_previous_stop(self, tmp_path, injector):
        wd = make_watchdog(tmp_path)
        wd._stop_event.set()
        wd.run()
        assert injector.call_count == 1


# --- run(): daemon


class TestRunDaemon:
    def daemon(self, root, files=("a.mcap",), **cfg):
        cfg.setdefault("polling_interval_s", 0.01)
        return make_watchdog(root, files, mode=WatchdogMode.DAEMON, **cfg)

    def test_injects_and_stops(self, tmp_path, injector):
        wd = self.daemon(tmp_path, ["a.mcap", "b.mcap"])
        stop_after(wd, injector, 2)

        assert run_in_thread(wd) is None
        assert sorted(injector.injected) == ["a__mcap", "b__mcap"]
        assert wd._producer_th is None

    def test_picks_up_files_added_while_running(self, tmp_path, injector):
        wd = self.daemon(tmp_path, ["a.mcap"])

        def on_run(kwargs):
            if injector.call_count == 1:
                touch(tmp_path, "late.mcap")
            else:
                wd._stop_event.set()

        injector.on_run = on_run
        assert run_in_thread(wd) is None
        assert injector.injected == ["a__mcap", "late__mcap"]

    def test_each_file_injected_once_across_many_scans(self, tmp_path, injector):
        files = [f"f{i}.mcap" for i in range(6)]
        wd = self.daemon(tmp_path, files, max_queue_size=2, polling_interval_s=0.001)
        stop_after(wd, injector, len(files))

        assert run_in_thread(wd) is None
        assert sorted(injector.injected) == sorted(f"f{i}__mcap" for i in range(6))

    def test_raise_policy_joins_producer(self, tmp_path, injector):
        injector.outcomes = [ValueError("boom")]
        wd = self.daemon(tmp_path, on_error=WatchdogError.RAISE)
        producer = {}
        injector.on_run = lambda _kw: producer.setdefault("th", wd._producer_th)

        exc = run_in_thread(wd)
        assert isinstance(exc, ValueError)
        assert producer["th"] is not None and not producer["th"].is_alive()
        assert wd._producer_th is None

    def test_cancel_joins_producer(self, tmp_path, injector):
        injector.outcomes = [InjectionStatus.CANCELLED]
        wd = self.daemon(tmp_path)
        producer = {}
        injector.on_run = lambda _kw: producer.setdefault("th", wd._producer_th)

        exc = run_in_thread(wd)
        assert isinstance(exc, KeyboardInterrupt)
        assert not producer["th"].is_alive()
        assert wd._producer_th is None

    @pytest.mark.parametrize(
        "scan_durations, expected_waits",
        [
            ([0.0, 0.0, 0.0], [10.0, 10.0, 10.0]),
            ([3.0, 9.5, 1.0], [7.0, 0.5, 9.0]),  # counted from the start of the scan
            ([10.0, 25.0], [0.0, 0.0]),  # a scan longer than the interval: no wait
        ],
    )
    def test_polling_interval_is_respected(
        self, tmp_path, monkeypatch, scan_durations, expected_waits
    ):
        """Runs `_producer_loop()` in the test thread, with a fake clock and a fake stop
        event: each scan advances the clock by its duration, each wait is recorded, and
        the event is set after the last expected wait."""
        wd = self.daemon(tmp_path, [], polling_interval_s=10.0)
        clock = FakeClock()
        monkeypatch.setattr(wd_mod, "time", clock)
        stop = FakeStopEvent(stop_after_waits=len(expected_waits))
        monkeypatch.setattr(wd, "_stop_event", stop)

        durations = iter(scan_durations)
        monkeypatch.setattr(wd, "_scan_folder", lambda: clock.advance(next(durations)))

        wd._producer_loop()

        assert stop.waits == pytest.approx(expected_waits)

    def test_producer_loop_survives_failed_scan(self, tmp_path, monkeypatch):
        wd = self.daemon(tmp_path, [], polling_interval_s=10.0)
        monkeypatch.setattr(wd_mod, "time", FakeClock())
        stop = FakeStopEvent(stop_after_waits=3)
        monkeypatch.setattr(wd, "_stop_event", stop)
        outcomes = iter([OSError("unavailable"), None, RuntimeError("boom")])

        def scan():
            exc = next(outcomes)
            if exc:
                raise exc

        monkeypatch.setattr(wd, "_scan_folder", scan)

        wd._producer_loop()  # does not raise

        assert stop.waits == [10.0, 10.0, 10.0]

    def test_producer_loop_does_not_scan_once_stopped(self, tmp_path, monkeypatch):
        wd = self.daemon(tmp_path, [])
        stop = FakeStopEvent(stop_after_waits=0)
        stop.set()
        monkeypatch.setattr(wd, "_stop_event", stop)
        monkeypatch.setattr(wd, "_scan_folder", lambda: pytest.fail("scanned"))

        wd._producer_loop()
        assert stop.waits == []

    def test_stop_producer_wakes_up_from_wait(self, tmp_path):
        wd = self.daemon(tmp_path, [], polling_interval_s=3600)
        wd._start_producer()
        th = wd._producer_th

        wd._stop_producer()  # must not wait for the polling interval
        assert not th.is_alive()
        assert wd._producer_th is None

    def test_start_producer_is_idempotent(self, tmp_path):
        wd = self.daemon(tmp_path, [], polling_interval_s=3600)
        wd._start_producer()
        th = wd._producer_th
        wd._start_producer()
        assert wd._producer_th is th
        wd._stop_producer()

    def test_stop_producer_without_start_is_noop(self, tmp_path):
        wd = self.daemon(tmp_path, [])
        wd._stop_producer()
        assert not wd._stop_event.is_set()


# --- CLI


class TestMain:
    @pytest.fixture
    def captured(self, monkeypatch):
        """Replaces the Watchdog with a fake that records its config; `behavior` can make
        `run()` raise."""
        seen = {"behavior": None}

        class FakeWatchdog:
            def __init__(self, config):
                seen["config"] = config

            def run(self):
                if seen["behavior"]:
                    raise seen["behavior"]

        monkeypatch.setattr(wd_mod, "Watchdog", FakeWatchdog)
        monkeypatch.setattr(wd_mod, "setup_sdk_logging", lambda **kw: None)
        monkeypatch.delenv("MOSAICO_API_KEY", raising=False)
        return seen

    def main(self, monkeypatch, *args):
        monkeypatch.setattr(sys, "argv", ["mosaicolabs.watchdog", *args])
        wd_mod.main()

    def test_defaults(self, monkeypatch, captured, tmp_path):
        self.main(monkeypatch, str(tmp_path))
        cfg = captured["config"]

        assert cfg == WatchdogConfig(
            path_to_monitor=tmp_path,
            mode=WatchdogMode.SINGLEPASS,
            polling_interval_s=5.0,
            glob_pattern=None,
            min_file_age_s=30.0,  # the CLI default differs from WatchdogConfig's None
            on_error=WatchdogError.QUARANTINE,
            retry_number=2,
            max_queue_size=100,
            host="localhost",
            port=6726,
            mosaico_api_key=None,
            tls_cert_path=None,
            enable_tls=False,
            log_level="INFO",
        )

    def test_all_arguments(self, monkeypatch, captured, tmp_path):
        self.main(
            monkeypatch,
            str(tmp_path),
            "--mode", "daemon",
            "--polling-interval", "1.5",
            "--glob-pattern", "run_*/*.mcap",
            "--min-file-age", "0",
            "--on-error", "retry",
            "--retry-number", "5",
            "--max-queue-size", "7",
            "--host", "mosaico.local",
            "--port", "1234",
            "--api-key", "key",
            "--tls-cert", "/certs/ca.pem",
            "--enable-tls",
            "--log", "debug",
        )  # fmt: skip

        assert captured["config"] == WatchdogConfig(
            path_to_monitor=tmp_path,
            mode=WatchdogMode.DAEMON,
            polling_interval_s=1.5,
            glob_pattern="run_*/*.mcap",
            min_file_age_s=0.0,
            on_error=WatchdogError.RETRY,
            retry_number=5,
            max_queue_size=7,
            host="mosaico.local",
            port=1234,
            mosaico_api_key="key",
            tls_cert_path="/certs/ca.pem",
            enable_tls=True,
            log_level="DEBUG",
        )

    def test_api_key_from_env(self, monkeypatch, captured, tmp_path):
        monkeypatch.setenv("MOSAICO_API_KEY", "env-key")
        self.main(monkeypatch, str(tmp_path))
        assert captured["config"].mosaico_api_key == "env-key"

    def test_api_key_argument_wins_over_env(self, monkeypatch, captured, tmp_path):
        monkeypatch.setenv("MOSAICO_API_KEY", "env-key")
        self.main(monkeypatch, str(tmp_path), "--api-key", "arg-key")
        assert captured["config"].mosaico_api_key == "arg-key"

    @pytest.mark.parametrize(
        "args",
        [
            ["--mode", "forever"],
            ["--on-error", "ignore"],
            ["--log", "verbose"],
            ["--port", "abc"],
        ],
    )
    def test_invalid_arguments(self, monkeypatch, captured, tmp_path, args):
        with pytest.raises(SystemExit) as exc:
            self.main(monkeypatch, str(tmp_path), *args)
        assert exc.value.code == 2
        assert "config" not in captured

    def test_missing_path_argument(self, monkeypatch, captured):
        with pytest.raises(SystemExit) as exc:
            self.main(monkeypatch)
        assert exc.value.code == 2

    def test_keyboard_interrupt_exits_130(self, monkeypatch, captured, tmp_path):
        captured["behavior"] = KeyboardInterrupt()
        with pytest.raises(SystemExit) as exc:
            self.main(monkeypatch, str(tmp_path))
        assert exc.value.code == 130

    def test_error_exits_1(self, monkeypatch, captured, tmp_path):
        captured["behavior"] = RuntimeError("boom")
        with pytest.raises(SystemExit) as exc:
            self.main(monkeypatch, str(tmp_path))
        assert exc.value.code == 1

    def test_missing_folder_exits_1(self, monkeypatch, tmp_path):
        # Real Watchdog: the constructor fails
        monkeypatch.setattr(wd_mod, "setup_sdk_logging", lambda **kw: None)
        monkeypatch.setattr(
            sys, "argv", ["mosaicolabs.watchdog", str(tmp_path / "missing")]
        )
        with pytest.raises(SystemExit) as exc:
            wd_mod.main()
        assert exc.value.code == 1

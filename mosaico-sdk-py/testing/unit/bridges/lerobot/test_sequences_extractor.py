import pytest

from mosaicolabs.bridges.lerobot.sequences_extractor import (
    ExtractionStatus,
    LeRobotSequencesExtractor,
    lerobot_sequences_extractor,
)


def _run_script(monkeypatch, tmp_path, status: ExtractionStatus) -> None:
    """Runs the script with `LeRobotSequencesExtractor.run` stubbed to return `status`."""
    monkeypatch.setattr(LeRobotSequencesExtractor, "run", lambda self: status)
    lerobot_sequences_extractor(["seq_a", "--lerobot-path", str(tmp_path / "out")])


def test_partial_extraction_exits_non_zero(monkeypatch, tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        _run_script(monkeypatch, tmp_path, ExtractionStatus.PARTIAL)

    assert exc_info.value.code == 1


def test_completed_extraction_does_not_exit(monkeypatch, tmp_path):
    _run_script(monkeypatch, tmp_path, ExtractionStatus.COMPLETED)

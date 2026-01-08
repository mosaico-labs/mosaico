import io
import logging
from rich.console import Console
from mosaicolabs.logging import get_logger, setup_sdk_logging


def test_null_handler_silence(capsys):
    """Verifies that the logger is silent before setup_sdk_logging is called."""
    # Get a logger for a dummy module
    test_logger = get_logger("mosaicolabs.test_silence")

    # Emit a log message
    test_logger.warning("This should go into the void")

    # Check capsys (captures stdout/stderr)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_mosaico_has_null_handler():
    """Checks that the root mosaicolabs logger defaults to a NullHandler."""
    mosaico_logger = get_logger()

    # Verify at least one handler is a NullHandler
    handlers = mosaico_logger.handlers
    assert any(isinstance(h, logging.NullHandler) for h in handlers), (
        "Mosaico root logger should have a NullHandler by default to prevent CLI warnings."
    )


def test_logs_are_generated_but_swallowed(caplog):
    test_logger = get_logger("mosaicolabs.internal")

    with caplog.at_level(logging.DEBUG):
        test_logger.debug("Internal diagnostic message")

    assert "Internal diagnostic message" in caplog.text


# --- These override the NullHandler: must be called after the null_handler tests


def test_setup_clears_existing_handlers():
    """Verify that multiple calls do not duplicate handlers."""
    # Setup twice
    setup_sdk_logging(level="INFO", pretty=False)
    setup_sdk_logging(level="DEBUG", pretty=False)

    logger = get_logger()
    # Should only have 1 handler despite two setup calls
    assert len(logger.handlers) == 1
    assert logger.level == logging.DEBUG


def test_setup_uses_provided_console():
    """Verify the logger outputs to the specific console provided."""
    custom_output = io.StringIO()
    test_console = Console(file=custom_output, force_terminal=True)

    setup_sdk_logging(level="INFO", pretty=True, console=test_console)
    logger = get_logger()
    logger.info("Test Console Sync")

    output = custom_output.getvalue()
    # Verify 'Rich' formatting and our custom 'name' formatting exists
    assert "mosaicolabs" in output
    assert "Test Console Sync" in output


def test_logger_isolation():
    """Ensure SDK logs do not propagate to the root logger."""
    setup_sdk_logging()
    logger = get_logger()

    # Propagate must be False to avoid duplicate logs in environments like pytest
    assert logger.propagate is False

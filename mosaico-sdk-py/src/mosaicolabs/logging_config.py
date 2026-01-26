import logging as root_logging
import sys
from typing import Optional
from rich.logging import RichHandler


# We import these inside the function or use a try-except
# to ensure 'rich' isn't a hard requirement for the whole SDK
try:
    from rich.logging import RichHandler

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


def setup_sdk_logging(
    level="INFO",
    pretty: bool = False,
    console=None,
    propagate: bool = False,
):
    """
    Configures the global logging strategy for the Mosaico SDK.

    This function initializes the 'mosaicolabs' logger namespace and provides two
    distinct output modes: a high-fidelity 'pretty' mode using the Rich library,
    and a standard stream mode for basic environments.
    It ensures that existing handlers are cleared to prevent duplicate log entries
    during re-initialization.

    Args:
        level (str): The logging threshold (e.g., "DEBUG", "INFO", "WARNING").
            Defaults to "INFO".
        pretty (bool): If True and the 'rich' package is installed, enables
            enhanced terminal output with colors, timestamps, and formatted
            tracebacks.
        console (Optional[rich.console.Console]): An optional Rich Console
            instance. If provided, the logger and any active UI (like progress
            bars) will synchronize to prevent screen flickering. Defaults
            to a new Console(stderr=True).

    Notes:
        - When 'pretty' is enabled, the logger name is styled in 'dim white'
          to keep focus on the message content.
        - Propagation is disabled (propagate=False) to prevent logs from
          bubbling up to the root logger and causing duplicate output in
          test runners like pytest.
    """
    logger = root_logging.getLogger("mosaicolabs")

    # Clear existing handlers to prevent duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    if pretty and RICH_AVAILABLE:
        # --- RICH PATH ---
        # If no console is provided, create a default one
        from rich.console import Console

        console = console or Console(stderr=True)

        handler = RichHandler(
            level=level,
            console=console,
            show_time=True,
            show_path=True,
            markup=True,
            rich_tracebacks=True,
            log_time_format="[%X]",
        )
        formatter = root_logging.Formatter(
            fmt="[dim white]%(name)s[/dim white]: %(message)s", datefmt="[%X]"
        )
        handler.setFormatter(formatter)
        init_message = f"SDK Logging initialized at level: [bold]{level}[/bold]"
        extra = {"markup": True}
    else:
        # --- STANDARD PATH ---
        handler = root_logging.StreamHandler(sys.stderr)
        # Standard format: Time [Level] Name: Message
        formatter = root_logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        init_message = f"SDK Logging initialized at level: {level}"
        extra = {}

        if pretty and not RICH_AVAILABLE:
            print(
                "Warning: 'pretty=True' requested but 'rich' package is not installed."
            )

    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = propagate

    logger.info(init_message, extra=extra)


def get_logger(name: Optional[str] = None):
    """
    Retrieves a logger instance within the Mosaico SDK namespace.

    This function acts as a wrapper for the standard logging.getLogger call.
    If a name is provided, it returns a logger for that specific module
    hierarchy. If no name is provided, it returns the
    base 'mosaicolabs' logger.

    Args:
        name (Optional[str]): The name of the logger.
            Typically passed as __name__ to reflect the module's path
            (e.g., 'mosaicolabs.comm.client').
            If None, the top-level SDK logger is returned.

    Returns:
        logging.Logger: A logger instance configured for the Mosaico SDK
            subsystem.
    """
    if name is not None:
        return root_logging.getLogger(name=name)
    else:
        return root_logging.getLogger("mosaicolabs")

import argparse

from pathlib import Path
import sys

import pytest


def mosaico_testing():
    """
    Console script entry point.
    Parses arguments, sets up configuration, and initiates the injector.
    """
    parser = argparse.ArgumentParser(description="Run pytest on mosaico SDK.")

    # Required Arguments
    parser.add_argument(
        "--port",
        default="6276",
        help="Set client port.",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Set client host.",
    )
    parser.add_argument(
        "-k",
        "--keyword",
        help="Only run tests matching keyword expression (same as pytest -k).",
    )
    parser.add_argument(
        "-s",
        action="store_true",
        help="Disable output capturing (same as pytest -s).",
    )
    parser.add_argument(
        "-x",
        "--exitfirst",
        action="store_true",
        help="Exit pytest after first failure (same as pytest -x, --exitfirst).",
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="Quiet mode.")

    # Connection Arguments
    parser.add_argument("-vv", action="store_true", help="Very-verbose")
    parser.add_argument(
        "-l",
        "--log",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Enable logging with level.",
    )

    args = parser.parse_args()

    src_dir = Path(__file__).resolve().parent
    pytest_args = [str(src_dir)]

    if args.keyword:
        pytest_args += ["-k", args.keyword]

    if args.s:
        pytest_args.append("-s")

    if args.exitfirst:
        pytest_args.append("-x")

    if not args.quiet:
        pytest_args.append("-v")

    if args.vv:
        pytest_args.append("-vv")
    if args.log:
        pytest_args += ["--log-cli-level", args.log.upper()]

    # pass to the conftest.py to create fixtures
    pytest_args += ["--host", args.host]
    pytest_args += ["--port", args.port]

    # run pytest; sys.exit ensures exit code is propagated
    sys.exit(pytest.main(pytest_args))


if __name__ == "__main__":
    mosaico_testing()

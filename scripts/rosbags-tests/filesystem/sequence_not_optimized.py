#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["mosaicolabs"]
#
# [tool.uv.sources]
# mosaicolabs = { path = "../../../mosaico-sdk-py", editable = true }
# ///
"""Ingests the rosbags with raw images without optimizing them, then retrieves their image sequences and checks that their videos are refused, in the local filesystem store (/tmp/mosaico-rosbags-tests)."""

import sys
from pathlib import Path

# The shared modules live in the parent folder.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import Store
from retrieval import main_not_optimized

if __name__ == "__main__":
    sys.exit(main_not_optimized(Store.FS))

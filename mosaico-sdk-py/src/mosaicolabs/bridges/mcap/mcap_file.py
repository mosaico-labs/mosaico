from pathlib import Path
from typing import List, Optional

from mcap.decoder import DecoderFactory
from mcap.reader import McapReader, make_reader


class MCAPFile:
    """
    TODO: write docstring

    Attributes:
        ACCEPTED_EXTENSIONS: Set of supported file extensions {'.mcap'}.
    """

    ACCEPTED_EXTENSIONS = {".mcap"}

    def __init__(self, file_path, decoder_factory: Optional[List[DecoderFactory]]):
        self._file_path: Path = file_path
        self._decoder_factory: List[DecoderFactory] = decoder_factory or []
        self._file = None
        self._reader = None

    def __del__(self):
        self.close()

    def _validate_file(self):
        if not self._file_path.exists():
            raise FileNotFoundError(f"MCAP file not found: {self._file_path}")
        if self._file_path.suffix not in self.ACCEPTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported format '{self._file_path.suffix}'. Supported: {self.ACCEPTED_EXTENSIONS}"
            )

    def _open(self) -> McapReader:

        if self._reader is not None:
            return self._reader

        self._validate_file()

        try:
            self._file = open(self._file_path, "rb")
        except Exception as e:
            raise IOError(f"Could not open mcap file: '{e}'") from e

        self._reader = make_reader(self._file, decoder_factories=self._decoder_factory)

        return self.reader

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None
            self._reader = None

    @property
    def reader(self) -> McapReader:
        if self._reader is None:
            return self._open()

        return self._reader

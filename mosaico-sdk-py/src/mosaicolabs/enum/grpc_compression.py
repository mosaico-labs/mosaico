"""
GRPC Compression Algorithm and Level Enumerations.

This module provides the `GRPCCompressionAlgorithm` and `GRPCCompressionLevel`
enumerations for specifying the compression algorithm and level to be used for gRPC
communication.
"""

from enum import IntEnum


class GRPCCompressionAlgorithm(IntEnum):
    """
    Defines the compression algorithm to be used for gRPC communication.

    It is used to configure the compression algorithm for the connection when
    using the [`GRPCCompression`][mosaicolabs.comm.GRPCCompression] dataclass,
    or as a shorthand when using the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect]
    method.
    """

    Null = 0
    """No compression"""
    Gzip = 2
    """Uses GZIP compression"""
    StreamGzip = 3
    """Experimental stream-level GZIP"""

    # --- Yet Unsupported by backend ---
    _Deflate = 1
    """(Unupported) Uses the zlib DEFLATE algorithm"""


class GRPCCompressionLevel(IntEnum):
    """
    Enumeration representing the gRPC compression level.
    """

    Low = 1
    """Low compression level"""
    Medium = 2
    """Medium compression level"""
    High = 3
    """High compression level"""

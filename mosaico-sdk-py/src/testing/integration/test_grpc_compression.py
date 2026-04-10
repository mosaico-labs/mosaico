from unittest.mock import patch

import pytest

from mosaicolabs import MosaicoClient
from mosaicolabs.comm.connection import GRPCCompression
from mosaicolabs.enum.grpc_compression import (
    GRPCCompressionAlgorithm,
    GRPCCompressionLevel,
)


def test_connect_enables_gzip_options(host, port, with_gzip):
    """
    Verifies that passing enable_gzip=True correctly sets the
    gRPC compression constants in the FlightClient options.
    """
    if not with_gzip:
        pytest.skip("Tests run without '--gzip'")
    # We patch the FlightClient class where it is imported/used in your client module
    with patch("pyarrow.flight.FlightClient") as mock_flight_class:
        # Act
        MosaicoClient.connect(host, port, compression=GRPCCompressionAlgorithm.Gzip)

        # Assert: Check the call arguments to the constructor
        _, kwargs = mock_flight_class.call_args
        generic_options = kwargs.get("generic_options", [])

        # Convert list of tuples to a dict for easier assertion
        options_dict = dict(generic_options)

        # 2 is the enum for GZIP, 1 is the boolean for Enabled
        assert options_dict.get("grpc.compression_enabled") == 1
        assert options_dict.get("grpc.default_compression_level") is None
        assert options_dict.get("grpc.default_compression_algorithm") == 2


def test_connect_enables_gzip_with_level_options(host, port, with_gzip):
    """
    Verifies that passing enable_gzip=True correctly sets the
    gRPC compression constants in the FlightClient options.
    """
    if not with_gzip:
        pytest.skip("Tests run without '--gzip'")
    # We patch the FlightClient class where it is imported/used in your client module
    with patch("pyarrow.flight.FlightClient") as mock_flight_class:
        # Act
        MosaicoClient.connect(
            host,
            port,
            compression=GRPCCompression(
                GRPCCompressionAlgorithm.Gzip, GRPCCompressionLevel.High
            ),
        )

        # Assert: Check the call arguments to the constructor
        _, kwargs = mock_flight_class.call_args
        generic_options = kwargs.get("generic_options", [])

        # Convert list of tuples to a dict for easier assertion
        options_dict = dict(generic_options)

        # 2 is the enum for GZIP, 1 is the boolean for Enabled
        assert options_dict.get("grpc.compression_enabled") == 1
        assert options_dict.get("grpc.default_compression_level") == 3
        assert options_dict.get("grpc.default_compression_algorithm") == 2


def test_connect_disables_gzip_by_default(host, port, with_gzip):
    """Ensures that by default (or False), compression options are not forced."""

    if not with_gzip:
        pytest.skip("Tests run without '--gzip'")

    with patch("pyarrow.flight.FlightClient") as mock_flight_class:
        MosaicoClient.connect(host, port)

        args, kwargs = mock_flight_class.call_args
        generic_options = kwargs.get("generic_options", [])
        options_dict = dict(generic_options)

        # Verify compression is not in the options or is disabled
        assert options_dict.get("grpc.compression_enabled", 0) == 0

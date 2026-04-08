from mosaicolabs import MosaicoField, MosaicoType, Serializable


class EncoderTicks(Serializable):
    """
    Custom Mosaico model for NVIDIA Isaac Nova Encoder Ticks.

    This model represents raw wheel encoder counts and their hardware-specific
    timestamps, providing the base data for dead-reckoning and odometry calculations.

    Attributes:
        left_ticks: Cumulative tick count for the left wheel.
        right_ticks: Cumulative tick count for the right wheel.
        encoder_timestamp: Timestamp of the encoder ticks.
    """

    # --- Pydantic Fields ---
    left_ticks: MosaicoType.uint32 = MosaicoField(
        description="Cumulative counts from the left wheel encoder."
    )
    """Cumulative tick count for the left wheel."""

    right_ticks: MosaicoType.uint32 = MosaicoField(
        description="Cumulative counts from the right wheel encoder."
    )
    """Cumulative tick count for the right wheel."""

    encoder_timestamp: MosaicoType.uint64 = MosaicoField(
        description="Timestamp of the encoder ticks."
    )
    """Timestamp of the encoder ticks."""

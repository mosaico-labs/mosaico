from mosaicolabs import Serializable
from mosaicolabs.models.types import MosaicoField, MosaicoType


class EncoderTicks(Serializable):
    """
    Custom Mosaico model for NVIDIA Isaac Nova Encoder Ticks.

    This model represents raw wheel encoder counts and their hardware-specific
    timestamps, providing the base data for dead-reckoning and odometry calculations.

    ### Structural Integrity
    To pass Mosaico's strict schema alignment check, the names defined in the
    `__msco_pyarrow_struct__` must match the Pydantic field names one-to-one.

    Attributes:
        left_ticks: Cumulative tick count for the left wheel.
        right_ticks: Cumulative tick count for the right wheel.
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
